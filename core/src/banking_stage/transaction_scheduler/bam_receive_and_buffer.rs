//! An implementation of the `ReceiveAndBuffer` trait that receives messages from BAM
//! and buffers from into the the `TransactionStateContainer`. Key thing to note:
//! this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
//! to BAM with a `Retryable` result.
use {
    super::receive_and_buffer::ReceiveAndBuffer,
    crate::{
        bam_response_handle::BamResponseHandle,
        banking_stage::{
            decision_maker::BufferedPacketsDecision,
            transaction_scheduler::{
                receive_and_buffer::{
                    calculate_max_age, calculate_priority_and_cost, DisconnectedError,
                    ReceivingStats,
                },
                transaction_priority_id::TransactionPriorityId,
                transaction_state::TransactionState,
                transaction_state_container::{
                    SharedBytes, StateContainer, TransactionViewState,
                    TransactionViewStateContainer, EXTRA_CAPACITY,
                },
            },
        },
        verified_bam_packet_batch::VerifiedBamPacketBatch,
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    arrayvec::ArrayVec,
    crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError},
    solana_accounts_db::account_locks::validate_account_locks,
    solana_fee_structure::FeeBudgetLimits,
    solana_measure::measure_us,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction::sanitized::MessageHash,
    std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::{Duration, Instant},
    },
};

#[derive(Debug)]
enum PacketHandlingError {
    Sanitization,
    LockValidation,
    ComputeBudget,
    BlacklistedAccount,
    VoteTransaction,
}

pub struct BamReceiveAndBuffer {
    bam_enabled: Arc<AtomicBool>,
    bam_response_handle: BamResponseHandle,
    bam_packet_batch_receiver: Receiver<VerifiedBamPacketBatch>,
    bank_forks: Arc<RwLock<BankForks>>,
    #[allow(unused)]
    blacklisted_accounts: HashSet<Pubkey>,
}

impl BamReceiveAndBuffer {
    pub fn new(
        bam_enabled: Arc<AtomicBool>,
        bam_packet_batch_receiver: Receiver<VerifiedBamPacketBatch>,
        bam_response_handle: BamResponseHandle,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> Self {
        Self {
            bam_enabled,
            bam_response_handle,
            bam_packet_batch_receiver,
            bank_forks,
            blacklisted_accounts,
        }
    }

    fn handle_packet_batch_message(
        &mut self,
        container: &mut TransactionViewStateContainer,
        root_bank: &Bank,
        working_bank: &Bank,
        bam_packet_batch: VerifiedBamPacketBatch,
    ) -> ReceivingStats {
        let mut stats = ReceivingStats::default();

        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();

        // This check exists to ensure that we don't accidentally overflow the transactions ArrayVec below, even though
        // the code should have already checked the max packet batch length
        if bam_packet_batch.packet_batch().len() > EXTRA_CAPACITY {
            self.bam_response_handle
                .send_sanitization_error(bam_packet_batch.meta().seq_id, 0);
            stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
            return stats;
        }

        let mut packet_data = ArrayVec::<_, EXTRA_CAPACITY>::new();
        packet_data.extend(
            bam_packet_batch
                .packet_batch()
                .iter()
                .map(|p| p.data(..).unwrap()),
        );

        let mut packet_index = 0;
        let mut insert_map_error = None;
        match container.try_insert_map_only_with_batch(
            packet_data.as_slice(),
            bam_packet_batch.meta().revert_on_error,
            bam_packet_batch.meta().max_schedule_slot,
            |bytes| match Self::try_handle_packet(
                bytes,
                root_bank,
                working_bank,
                transaction_account_lock_limit,
                &self.blacklisted_accounts,
            ) {
                Ok(state) => {
                    packet_index += 1;
                    Ok(state)
                }
                Err(PacketHandlingError::Sanitization) => {
                    insert_map_error = Some((packet_index, PacketHandlingError::Sanitization));
                    stats.num_dropped_on_parsing_and_sanitization += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(PacketHandlingError::LockValidation) => {
                    insert_map_error = Some((packet_index, PacketHandlingError::LockValidation));
                    stats.num_dropped_on_lock_validation += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(PacketHandlingError::ComputeBudget) => {
                    insert_map_error = Some((packet_index, PacketHandlingError::ComputeBudget));
                    stats.num_dropped_on_compute_budget += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(PacketHandlingError::BlacklistedAccount) => {
                    insert_map_error =
                        Some((packet_index, PacketHandlingError::BlacklistedAccount));
                    stats.num_dropped_on_blacklisted_account += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(PacketHandlingError::VoteTransaction) => {
                    insert_map_error = Some((packet_index, PacketHandlingError::VoteTransaction));
                    stats.num_dropped_on_parsing_and_sanitization += 1;
                    packet_index += 1;
                    Err(())
                }
            },
        ) {
            Ok(Some(batch_id)) => {
                // It's expected BAM does some transaction filtering and streams transactions to the validator only during the slot
                // they're valid, so this buffer should never grow large enough to cause a performance issue.
                // To save on performance, buffer the packet and check the transaction/fee payer when popping off the priority queue.
                container.push_batch_id_into_queue(TransactionPriorityId::new(
                    seq_id_to_priority(bam_packet_batch.meta().seq_id),
                    batch_id,
                ));
            }
            // Ok(None) means an error occurred during insertion, all of the transactions were removed from the container and insert_map_error is set
            Ok(None) => {}
            // container is full, nothing was added
            Err(()) => {
                stats.num_dropped_on_capacity += bam_packet_batch.packet_batch().len();
                self.bam_response_handle
                    .send_container_full_txn_batch_result(bam_packet_batch.meta().seq_id);
            }
        }

        // TODO (LB): send back specific error to BAM
        if let Some((index, error)) = insert_map_error {
            error!("Sanitization error: {:?}", error);
            self.bam_response_handle
                .send_sanitization_error(bam_packet_batch.meta().seq_id, index);
        }

        stats.num_buffered += bam_packet_batch.packet_batch().len();

        stats
    }

    // See TransactionViewReceiveAndBuffer::try_handle_packet
    fn try_handle_packet(
        bytes: SharedBytes,
        root_bank: &Bank,
        working_bank: &Bank,
        transaction_account_lock_limit: usize,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Result<TransactionViewState, PacketHandlingError> {
        let alt_bank = root_bank;
        let sanitized_epoch = root_bank.epoch();

        // Parsing and basic sanitization checks
        let Ok(view) = SanitizedTransactionView::try_new_sanitized(bytes) else {
            return Err(PacketHandlingError::Sanitization);
        };

        let Ok(view) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
            view,
            MessageHash::Compute,
            None,
        ) else {
            return Err(PacketHandlingError::Sanitization);
        };

        // Discard non-vote packets if in vote-only mode.
        if root_bank.vote_only_bank() && !view.is_simple_vote_transaction() {
            return Err(PacketHandlingError::Sanitization);
        }

        if view.is_simple_vote_transaction() {
            return Err(PacketHandlingError::VoteTransaction);
        }

        // Check if the transaction has too many account locks before loading ALTs
        if view.total_num_accounts() as usize > transaction_account_lock_limit {
            return Err(PacketHandlingError::LockValidation);
        }

        // Load addresses for transaction.
        let load_addresses_result = match view.version() {
            TransactionVersion::Legacy => Ok((None, u64::MAX)),
            TransactionVersion::V0 => alt_bank
                .load_addresses_from_ref(view.address_table_lookup_iter())
                .map(|(loaded_addresses, deactivation_slot)| {
                    (Some(loaded_addresses), deactivation_slot)
                }),
        };
        let Ok((loaded_addresses, deactivation_slot)) = load_addresses_result else {
            return Err(PacketHandlingError::Sanitization);
        };

        let Ok(view) = RuntimeTransaction::<ResolvedTransactionView<_>>::try_from(
            view,
            loaded_addresses,
            root_bank.get_reserved_account_keys(),
        ) else {
            return Err(PacketHandlingError::Sanitization);
        };

        if validate_account_locks(view.account_keys(), transaction_account_lock_limit).is_err() {
            return Err(PacketHandlingError::LockValidation);
        }

        if view
            .account_keys()
            .iter()
            .any(|account| blacklisted_accounts.contains(account))
        {
            return Err(PacketHandlingError::BlacklistedAccount);
        }

        let Ok(compute_budget_limits) = view
            .compute_budget_instruction_details()
            .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
        else {
            return Err(PacketHandlingError::ComputeBudget);
        };

        let max_age = calculate_max_age(sanitized_epoch, deactivation_slot, alt_bank.slot());
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let (priority, cost) = calculate_priority_and_cost(&view, &fee_budget_limits, working_bank);

        Ok(TransactionState::new(view, max_age, priority, cost))
    }
}

impl ReceiveAndBuffer for BamReceiveAndBuffer {
    type Transaction = RuntimeTransaction<ResolvedTransactionView<SharedBytes>>;
    type Container = TransactionViewStateContainer;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError> {
        // for very high throughputs, a smaller number is better since we need to return to the scheduler quickly
        const MAX_HANDLE_PACKET_BATCH_COUNT: usize = 256;

        let mut stats = ReceivingStats::default();

        let is_bam_enabled = self.bam_enabled.load(Ordering::Relaxed);

        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };

        let mut count = 0;

        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => {
                // the receive, buffer, schedule loop needs to run hot, so limit the number of batches handled to ensure we can
                // keep feeding the scheduler
                while count < MAX_HANDLE_PACKET_BATCH_COUNT {
                    let bam_packet_batch = match self.bam_packet_batch_receiver.try_recv() {
                        Ok(bam_packet_batch) => bam_packet_batch,
                        Err(TryRecvError::Disconnected) => return Err(DisconnectedError),
                        Err(TryRecvError::Empty) => {
                            // If the channel is empty, work here is done.
                            break;
                        }
                    };

                    stats.num_received += bam_packet_batch.packet_batch().len();

                    // If BAM is not enabled, drain the channel
                    if !is_bam_enabled {
                        stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
                        continue;
                    }

                    // Throw away batches that are marked as discard from bad signature
                    if bam_packet_batch.meta().discard {
                        self.bam_response_handle
                            .send_bad_signature(bam_packet_batch.meta().seq_id);
                        stats.num_dropped_on_parsing_and_sanitization +=
                            bam_packet_batch.packet_batch().len();
                        continue;
                    }

                    // Throw away batches that are outside the maximum schedulable slot
                    if working_bank.slot() > bam_packet_batch.meta().max_schedule_slot {
                        self.bam_response_handle
                            .send_outside_leader_slot_bundle_result(bam_packet_batch.meta().seq_id);
                        stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
                        continue;
                    }

                    stats.accumulate(self.handle_packet_batch_message(
                        container,
                        &root_bank,
                        &working_bank,
                        bam_packet_batch,
                    ));

                    count += 1;
                }
            }
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Forward => {
                // Send back any batches that were received while in Forward/Hold state
                // Don't sleep too long here so one can pick up new bank fast
                let deadline = Instant::now() + Duration::from_millis(10);
                loop {
                    let (batch, receive_time_us) =
                        measure_us!(self.bam_packet_batch_receiver.recv_deadline(deadline));
                    stats.receive_time_us += receive_time_us;

                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(RecvTimeoutError::Disconnected) => return Err(DisconnectedError),
                        Err(RecvTimeoutError::Timeout) => {
                            break;
                        }
                    };
                    stats.num_received += batch.packet_batch().len();
                    stats.num_dropped_without_parsing += batch.packet_batch().len();
                    self.bam_response_handle
                        .send_outside_leader_slot_bundle_result(batch.meta().seq_id);
                }
            }
        }

        Ok(stats)
    }
}

#[allow(unused)]
pub fn seq_id_to_priority(seq_id: u32) -> u64 {
    u64::MAX.saturating_sub(seq_id as u64)
}

#[allow(unused)]
pub fn priority_to_seq_id(priority: u64) -> u32 {
    u32::try_from(u64::MAX.saturating_sub(priority)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{atomic::AtomicBool, Arc, RwLock},
    };

    use solana_keypair::Keypair;
    use solana_ledger::genesis_utils::GenesisConfigInfo;
    use solana_pubkey::Pubkey;
    use solana_runtime::{bank::Bank, bank_forks::BankForks};

    use crate::{
        bam_dependencies::BamOutboundMessage,
        bam_response_handle::BamResponseHandle,
        banking_stage::{
            tests::create_slow_genesis_config,
            transaction_scheduler::{
                bam_receive_and_buffer::{
                    priority_to_seq_id, seq_id_to_priority, BamReceiveAndBuffer,
                },
                transaction_state_container::{StateContainer, TransactionViewStateContainer},
            },
        },
        verified_bam_packet_batch::VerifiedBamPacketBatch,
    };

    #[test]
    fn test_seq_id_to_priority() {
        assert_eq!(seq_id_to_priority(0), u64::MAX);
        assert_eq!(seq_id_to_priority(1), u64::MAX - 1);
    }

    #[test]
    fn test_priority_to_seq_id() {
        assert_eq!(priority_to_seq_id(u64::MAX), 0);
        assert_eq!(priority_to_seq_id(u64::MAX - 1), 1);
    }

    fn test_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);

        let (_bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        (bank_forks, mint_keypair)
    }

    fn setup_bam_receive_and_buffer(
        receiver: crossbeam_channel::Receiver<VerifiedBamPacketBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        Arc<AtomicBool>,
        BamReceiveAndBuffer,
        TransactionViewStateContainer,
        crossbeam_channel::Receiver<BamOutboundMessage>,
    ) {
        let exit = Arc::new(AtomicBool::new(false));
        let (response_sender, response_receiver) =
            crossbeam_channel::unbounded::<BamOutboundMessage>();
        let receive_and_buffer = BamReceiveAndBuffer::new(
            Arc::new(AtomicBool::new(true)),
            receiver,
            BamResponseHandle::new(response_sender),
            bank_forks,
            blacklisted_accounts,
        );
        let container = TransactionViewStateContainer::with_capacity(100, true);
        (exit, receive_and_buffer, container, response_receiver)
    }

    // tests:
    // handle_packet_batch_message:
    // - larger than EXTRA_CAPACITY
    // - test rollback
    // - test capacity exceeded
    // - test vote transaction
    // - handle_packet_batch_message
    // - test capacity consistency
    // - test bad packet
    // - test happy path
    // - test blacklisted account
    //
    // receive_and_buffer_packets:
    // - test forward and hold
    // - test forward
    // - test discard
    // - test not bam enabled
    // - test bank slot greater than work slot
    //
    // todos:
    // - make sure to verify container
    // - test bad

    // fn verify_container<Tx: TransactionWithMeta>(
    //     container: &mut impl StateContainer<Tx>,
    //     expected_length: usize,
    // ) {
    //     let mut actual_length: usize = 0;
    //     while let Some(id) = container.pop() {
    //         let Some((ids, _, _)) = container.get_batch(id.id) else {
    //             panic!(
    //                 "transaction in queue position {} with id {} must exist.",
    //                 actual_length, id.id
    //             );
    //         };
    //         for id in ids {
    //             assert!(
    //                 container.get_transaction(*id).is_some(),
    //                 "Transaction ID {} not found in container",
    //                 id
    //             );
    //         }
    //         actual_length += 1;
    //     }

    //     assert_eq!(actual_length, expected_length);
    // }

    // #[test_case(setup_bam_receive_and_buffer; "testcase-bam")]
    // fn test_receive_and_buffer_simple_transfer<R: ReceiveAndBuffer>(
    //     setup_receive_and_buffer: impl FnOnce(
    //         Receiver<AtomicTxnBatch>,
    //         Arc<RwLock<BankForks>>,
    //         HashSet<Pubkey>,
    //     ) -> (
    //         Arc<AtomicBool>,
    //         R,
    //         R::Container,
    //         Receiver<BamOutboundMessage>,
    //     ),
    // ) {
    //     let (sender, receiver) = unbounded();
    //     let (bank_forks, mint_keypair) = test_bank_forks();
    //     let (exit, mut receive_and_buffer, mut container, _response_sender) =
    //         setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());
    //     let transaction = transfer(
    //         &mint_keypair,
    //         &Pubkey::new_unique(),
    //         1,
    //         bank_forks.read().unwrap().root_bank().last_blockhash(),
    //     );
    //     let data = bincode::serialize(&transaction).expect("serializes");
    //     let bundle = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet { data, meta: None }],
    //         max_schedule_slot: Slot::MAX,
    //     };
    //     sender.send(bundle).unwrap();

    //     let start = Instant::now();
    //     while start.elapsed() < Duration::from_secs(2) {
    //         let ReceivingStats { num_received, .. } = receive_and_buffer
    //             .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
    //             .unwrap();
    //         if num_received > 0 {
    //             break;
    //         }
    //     }

    //     verify_container(&mut container, 1);
    //     exit.store(true, Ordering::Relaxed);
    // }

    // #[test]
    // fn test_receive_and_buffer_invalid_packet() {
    //     let (bank_forks, _mint_keypair) = test_bank_forks();
    //     let (sender, receiver) = unbounded();
    //     let (exit, mut receive_and_buffer, mut container, response_receiver) =
    //         setup_bam_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());

    //     let bundle = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet {
    //             data: vec![],
    //             meta: None,
    //         }],
    //         max_schedule_slot: Slot::MAX,
    //     };
    //     sender.send(bundle).unwrap();

    //     let ReceivingStats { num_received, .. } = receive_and_buffer
    //         .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
    //         .unwrap();

    //     assert_eq!(num_received, 0);
    //     verify_container(&mut container, 0);
    //     let response = response_receiver.recv().unwrap();
    //     assert!(matches!(
    //         response,
    //         BamOutboundMessage::AtomicTxnBatchResult(txn_batch_result) if txn_batch_result.seq_id == 1 &&
    //         matches!(&txn_batch_result.result, Some(atomic_txn_batch_result::Result::NotCommitted(not_committed)) if
    //             matches!(not_committed.reason, Some(Reason::DeserializationError(_))))
    //     ));
    //     exit.store(true, Ordering::Relaxed);
    // }

    // #[test]
    // fn test_batch_deserialize_success() {
    //     let (bank_forks, mint_keypair) = test_bank_forks();
    //     let bundle = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet {
    //             data: bincode::serialize(&transfer(
    //                 &mint_keypair,
    //                 &Pubkey::new_unique(),
    //                 1,
    //                 bank_forks.read().unwrap().root_bank().last_blockhash(),
    //             ))
    //             .unwrap(),
    //             meta: None,
    //         }],
    //         max_schedule_slot: Slot::MAX,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, _batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[bundle], Slot::MAX, &mut stats);

    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_ok());
    //     if let Ok((deserialized_packets, _, seq_id, _max_schedule_slot)) = &results[0] {
    //         assert_eq!(deserialized_packets.len(), 1);
    //         assert_eq!(*seq_id, 1);
    //     }
    // }

    // #[test]
    // fn test_batch_deserialize_empty() {
    //     let (_bank_forks, _mint_keypair) = test_bank_forks();
    //     let batch = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![],
    //         max_schedule_slot: Slot::MAX,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_err());
    //     assert_eq!(batch_stats.num_dropped_without_parsing, 1);
    //     if let Err((reason, seq_id)) = &results[0] {
    //         assert_eq!(*seq_id, 1);
    //         assert!(matches!(reason, Reason::DeserializationError(_)));
    //     }
    // }

    // #[test]
    // fn test_batch_deserialize_invalid_packet() {
    //     let (_bank_forks, _mint_keypair) = test_bank_forks();
    //     let batch = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet {
    //             data: vec![0; PACKET_DATA_SIZE + 1],
    //             meta: None,
    //         }],
    //         max_schedule_slot: Slot::MAX,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, _batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_err());
    //     if let Err((reason, seq_id)) = &results[0] {
    //         assert_eq!(*seq_id, 1);
    //         assert!(matches!(reason, Reason::DeserializationError(_)));
    //     }
    // }

    // #[test]
    // fn test_batch_deserialize_fee_payer_doesnt_exist() {
    //     let (bank_forks, _) = test_bank_forks();
    //     let fee_payer = Keypair::new();
    //     let batch = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet {
    //             data: bincode::serialize(&transfer(
    //                 &fee_payer,
    //                 &Pubkey::new_unique(),
    //                 1,
    //                 bank_forks.read().unwrap().root_bank().last_blockhash(),
    //             ))
    //             .unwrap(),
    //             meta: None,
    //         }],
    //         max_schedule_slot: Slot::MAX,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, _batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_ok());

    //     if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
    //     {
    //         let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
    //             deserialized_packets.clone(),
    //             *seq_id,
    //             *revert_on_error,
    //             *max_schedule_slot,
    //             &bank_forks,
    //             &HashSet::new(),
    //             &mut stats,
    //         );

    //         assert!(result.is_err());
    //         assert_eq!(stats.num_dropped_on_fee_payer, 1);
    //         assert!(matches!(result.err().unwrap(), Reason::TransactionError(_)));
    //     }
    // }

    // #[test]
    // fn test_batch_deserialize_inconsistent() {
    //     let (bank_forks, mint_keypair) = test_bank_forks();
    //     let bundle = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![
    //             Packet {
    //                 data: bincode::serialize(&transfer(
    //                     &mint_keypair,
    //                     &Pubkey::new_unique(),
    //                     1,
    //                     bank_forks.read().unwrap().root_bank().last_blockhash(),
    //                 ))
    //                 .unwrap(),
    //                 meta: None,
    //             },
    //             Packet {
    //                 data: bincode::serialize(&transfer(
    //                     &mint_keypair,
    //                     &Pubkey::new_unique(),
    //                     1,
    //                     bank_forks.read().unwrap().root_bank().last_blockhash(),
    //                 ))
    //                 .unwrap(),
    //                 meta: Some(jito_protos::proto::bam_types::Meta {
    //                     flags: Some(jito_protos::proto::bam_types::PacketFlags {
    //                         revert_on_error: true,
    //                         ..Default::default()
    //                     }),
    //                     ..Default::default()
    //                 }),
    //             },
    //         ],
    //         max_schedule_slot: Slot::MAX,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[bundle], Slot::MAX, &mut stats);
    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_err());
    //     assert_eq!(batch_stats.num_dropped_without_parsing, 1);
    //     if let Err((reason, seq_id)) = &results[0] {
    //         assert_eq!(*seq_id, 1);
    //         assert!(matches!(reason, Reason::DeserializationError(_)));
    //     }
    // }

    // #[test]
    // fn test_batch_deserialize_blacklisted_account() {
    //     let keypair = Keypair::new();
    //     let blacklisted_accounts = HashSet::from([keypair.pubkey()]);

    //     let (bank_forks, mint_keypair) = test_bank_forks();
    //     let batch = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet {
    //             data: bincode::serialize(&transfer(
    //                 &mint_keypair,
    //                 &keypair.pubkey(),
    //                 100,
    //                 bank_forks.read().unwrap().root_bank().last_blockhash(),
    //             ))
    //             .unwrap(),
    //             meta: None,
    //         }],
    //         max_schedule_slot: Slot::MAX,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, _batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_ok());

    //     if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
    //     {
    //         let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
    //             deserialized_packets.clone(),
    //             *seq_id,
    //             *revert_on_error,
    //             *max_schedule_slot,
    //             &bank_forks,
    //             &blacklisted_accounts,
    //             &mut stats,
    //         );

    //         assert!(result.is_err());
    //         assert_eq!(stats.num_dropped_on_blacklisted_account, 1);
    //         assert!(matches!(result.err().unwrap(), Reason::TransactionError(_)));
    //     }
    // }

    // #[test]
    // fn test_batch_deserialize_rejects_vote_transactions() {
    //     let (bank_forks, _mint_keypair) = test_bank_forks();

    //     let vote_keypair = Keypair::new();
    //     let node_keypair = Keypair::new();
    //     let authorized_voter = Keypair::new();
    //     let recent_blockhash = bank_forks.read().unwrap().root_bank().last_blockhash();

    //     let vote_tx = Transaction::new(
    //         &[&node_keypair, &authorized_voter],
    //         Message::new(
    //             &[solana_vote_program::vote_instruction::vote(
    //                 &vote_keypair.pubkey(),
    //                 &authorized_voter.pubkey(),
    //                 solana_vote_program::vote_state::Vote::new(vec![1], recent_blockhash),
    //             )],
    //             Some(&node_keypair.pubkey()),
    //         ),
    //         recent_blockhash,
    //     );

    //     let vote_data = bincode::serialize(&VersionedTransaction::from(vote_tx)).unwrap();

    //     let meta = jito_protos::proto::bam_types::Meta {
    //         flags: Some(jito_protos::proto::bam_types::PacketFlags {
    //             simple_vote_tx: true,
    //             ..Default::default()
    //         }),
    //         size: vote_data.len() as u64,
    //     };

    //     let batch = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet {
    //             data: vote_data,
    //             meta: Some(meta),
    //         }],
    //         max_schedule_slot: Slot::MAX,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, _batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_ok());

    //     if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
    //     {
    //         let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
    //             deserialized_packets.clone(),
    //             *seq_id,
    //             *revert_on_error,
    //             *max_schedule_slot,
    //             &bank_forks,
    //             &HashSet::new(),
    //             &mut stats,
    //         );

    //         assert!(result.is_err());
    //         assert_eq!(stats.num_dropped_on_parsing_and_sanitization, 1);
    //         assert!(matches!(
    //             result.err().unwrap(),
    //             Reason::DeserializationError(_)
    //         ));
    //     }
    // }

    // #[test]
    // fn test_batch_deserialize_reject_wrong_slot() {
    //     let (bank_forks, mint_keypair) = test_bank_forks();
    //     let batch = AtomicTxnBatch {
    //         seq_id: 1,
    //         packets: vec![Packet {
    //             data: bincode::serialize(&transfer(
    //                 &mint_keypair,
    //                 &Pubkey::new_unique(),
    //                 1,
    //                 bank_forks.read().unwrap().root_bank().last_blockhash(),
    //             ))
    //             .unwrap(),
    //             meta: None,
    //         }],
    //         max_schedule_slot: 0,
    //     };

    //     let mut stats = BamReceiveAndBufferMetrics::default();
    //     let (results, _batch_stats) =
    //         BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

    //     assert_eq!(results.len(), 1);
    //     assert!(results[0].is_err());
    // }
}
