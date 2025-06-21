/// An implementation of the `ReceiveAndBuffer` trait that receives messages from JSS
/// and buffers from into the the `TransactionStateContainer`. Key thing to note:
/// this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
/// to JSS with a `Retryable` result.
use std::{
    cmp::min,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use {
    super::{
        receive_and_buffer::ReceiveAndBuffer,
        transaction_state_container::TransactionStateContainer,
    },
    crate::banking_stage::{
        consumer::Consumer,
        decision_maker::BufferedPacketsDecision,
        immutable_deserialized_packet::{DeserializedPacketError, ImmutableDeserializedPacket},
        transaction_scheduler::{
            jss_utils::{convert_deserialize_error_to_proto, convert_txn_error_to_proto},
            receive_and_buffer::{calculate_max_age, calculate_priority_and_cost},
            transaction_state::SanitizedTransactionTTL,
        },
    },
    crossbeam_channel::Sender,
    itertools::Itertools,
    jito_protos::proto::{
        jss_api::{start_scheduler_message::Msg, StartSchedulerMessage},
        jss_types::{
            bundle_result, not_committed::Reason, Bundle, DeserializationErrorReason, Packet,
            SchedulingError,
        },
    },
    solana_accounts_db::account_locks::validate_account_locks,
    solana_runtime::bank_forks::BankForks,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_sdk::{
        clock::MAX_PROCESSING_AGE,
        packet::{PacketFlags, PACKET_DATA_SIZE},
        transaction::SanitizedTransaction,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
};

pub struct JssReceiveAndBuffer {
    jss_enabled: Arc<AtomicBool>,
    bundle_receiver: crossbeam_channel::Receiver<Bundle>,
    response_sender: Sender<StartSchedulerMessage>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl JssReceiveAndBuffer {
    pub fn new(
        jss_enabled: Arc<AtomicBool>,
        bundle_receiver: crossbeam_channel::Receiver<Bundle>,
        response_sender: Sender<StartSchedulerMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        Self {
            jss_enabled,
            bundle_receiver,
            response_sender,
            bank_forks,
        }
    }

    fn deserialize_jss_packets<'a>(
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Result<Vec<ImmutableDeserializedPacket>, (usize, DeserializedPacketError)> {
        let mut result = Vec::with_capacity(packets.size_hint().0);
        for (index, packet) in packets.enumerate() {
            let mut solana_packet = solana_sdk::packet::Packet::default();
            solana_packet.meta_mut().size = packet.data.len();
            solana_packet.meta_mut().set_discard(false);
            let len_to_copy = min(packet.data.len(), PACKET_DATA_SIZE);
            solana_packet.buffer_mut()[0..len_to_copy]
                .copy_from_slice(&packet.data[0..len_to_copy]);
            if let Some(meta) = &packet.meta {
                if let Some(addr) = &meta.addr.parse().ok() {
                    solana_packet.meta_mut().addr = *addr;
                }
                solana_packet.meta_mut().port = meta.port as u16;
                if let Some(flags) = &meta.flags {
                    if flags.simple_vote_tx {
                        solana_packet
                            .meta_mut()
                            .flags
                            .insert(PacketFlags::SIMPLE_VOTE_TX);
                    }
                    if flags.forwarded {
                        solana_packet
                            .meta_mut()
                            .flags
                            .insert(PacketFlags::FORWARDED);
                    }
                    if flags.repair {
                        solana_packet.meta_mut().flags.insert(PacketFlags::REPAIR);
                    }
                }
            }
            result
                .push(ImmutableDeserializedPacket::new(solana_packet).map_err(|err| (index, err))?);
        }

        Ok(result)
    }

    fn send_bundle_not_committed_result(&self, seq_id: u32, reason: Reason) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(reason),
                        },
                    )),
                },
            )),
        });
    }

    fn send_no_leader_slot_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::OutsideLeaderSlot as i32,
                            )),
                        },
                    )),
                },
            )),
        });
    }

    fn send_container_full_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::ContainerFull as i32,
                            )),
                        },
                    )),
                },
            )),
        });
    }

    fn parse_bundle(
        bundle: &Bundle,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<ParsedBundle, Reason> {
        if bundle.packets.is_empty() {
            return Err(Reason::DeserializationError(
                jito_protos::proto::jss_types::DeserializationError {
                    index: 0,
                    reason: DeserializationErrorReason::Empty as i32,
                },
            ));
        }

        if bundle.packets.len() > 5 {
            return Err(Reason::DeserializationError(
                jito_protos::proto::jss_types::DeserializationError {
                    index: 0,
                    reason: DeserializationErrorReason::SanitizeError as i32,
                },
            ));
        }

        let Ok(revert_on_error) = bundle
            .packets
            .iter()
            .map(|p| {
                p.meta
                    .as_ref()
                    .and_then(|meta| meta.flags.as_ref())
                    .is_some_and(|flags| flags.revert_on_error)
            })
            .all_equal_value()
        else {
            return Err(Reason::DeserializationError(
                jito_protos::proto::jss_types::DeserializationError {
                    index: 0,
                    reason: DeserializationErrorReason::InconsistentBundle as i32,
                },
            ));
        };

        let mut parsed_packets =
            Self::deserialize_jss_packets(bundle.packets.iter()).map_err(|(index, err)| {
                let reason = convert_deserialize_error_to_proto(&err);
                Reason::DeserializationError(jito_protos::proto::jss_types::DeserializationError {
                    index: index as u32,
                    reason: reason as i32,
                })
            })?;

        let (root_bank, working_bank) = {
            let bank_forks = bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };
        let alt_resolved_slot = root_bank.slot();
        let sanitized_epoch = root_bank.epoch();
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();
        let vote_only = working_bank.vote_only_bank();

        let mut packets = vec![];
        let mut cost: u64 = 0;
        let mut transaction_ttls = vec![];

        // Checks are taken from receive_and_buffer.rs:
        // SanitizedTransactionReceiveAndBuffer::buffer_packets
        for (index, parsed_packet) in parsed_packets.drain(..).enumerate() {
            // Check 1
            let Some((tx, deactivation_slot)) = parsed_packet.build_sanitized_transaction(
                vote_only,
                root_bank.as_ref(),
                root_bank.get_reserved_account_keys(),
            ) else {
                return Err(Reason::DeserializationError(
                    jito_protos::proto::jss_types::DeserializationError {
                        index: 0,
                        reason: DeserializationErrorReason::SanitizeError as i32,
                    },
                ));
            };

            // Check 2
            if let Err(err) =
                validate_account_locks(tx.message().account_keys(), transaction_account_lock_limit)
            {
                let reason = convert_txn_error_to_proto(err);
                return Err(Reason::TransactionError(
                    jito_protos::proto::jss_types::TransactionError {
                        index: index as u32,
                        reason: reason as i32,
                    },
                ));
            }

            // Check 3
            let fee_budget_limits = match tx
                .compute_budget_instruction_details()
                .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
            {
                Ok(fee_budget_limits) => fee_budget_limits,
                Err(err) => {
                    let reason = convert_txn_error_to_proto(err);
                    return Err(Reason::TransactionError(
                        jito_protos::proto::jss_types::TransactionError {
                            index: index as u32,
                            reason: reason as i32,
                        },
                    ));
                }
            };

            // Check 4
            let lock_results: [_; 1] = core::array::from_fn(|_| Ok(()));
            let check_results = working_bank.check_transactions(
                std::slice::from_ref(&tx),
                &lock_results,
                MAX_PROCESSING_AGE,
                &mut TransactionErrorMetrics::default(),
            );
            if let Some(Err(err)) = check_results.first() {
                let reason = convert_txn_error_to_proto(err.clone());
                return Err(Reason::TransactionError(
                    jito_protos::proto::jss_types::TransactionError {
                        index: index as u32,
                        reason: reason as i32,
                    },
                ));
            }

            // Check 5
            if let Err(err) = Consumer::check_fee_payer_unlocked(
                &working_bank,
                &tx,
                &mut TransactionErrorMetrics::default(),
            ) {
                let reason = convert_txn_error_to_proto(err);
                return Err(Reason::TransactionError(
                    jito_protos::proto::jss_types::TransactionError {
                        index: index as u32,
                        reason: reason as i32,
                    },
                ));
            }

            let max_age = calculate_max_age(sanitized_epoch, deactivation_slot, alt_resolved_slot);

            let (_, txn_cost) =
                calculate_priority_and_cost(&tx, &fee_budget_limits.into(), &working_bank);
            cost = cost.saturating_add(txn_cost);
            transaction_ttls.push(SanitizedTransactionTTL {
                transaction: tx,
                max_age,
            });
            packets.push(Arc::new(parsed_packet));
        }

        let priority = seq_id_to_priority(bundle.seq_id);

        Ok(ParsedBundle {
            transaction_ttls,
            packets,
            cost,
            priority,
            revert_on_error,
        })
    }
}

struct ParsedBundle {
    pub transaction_ttls: Vec<SanitizedTransactionTTL<RuntimeTransaction<SanitizedTransaction>>>,
    pub packets: Vec<Arc<ImmutableDeserializedPacket>>,
    pub cost: u64,
    priority: u64,
    pub revert_on_error: bool,
}

impl ReceiveAndBuffer for JssReceiveAndBuffer {
    type Transaction = RuntimeTransaction<SanitizedTransaction>;
    type Container = TransactionStateContainer<Self::Transaction>;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        _: &mut super::scheduler_metrics::SchedulerTimingMetrics,
        _: &mut super::scheduler_metrics::SchedulerCountMetrics,
        decision: &crate::banking_stage::decision_maker::BufferedPacketsDecision,
    ) -> Result<usize, ()> {
        if !self.jss_enabled.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(5));
            return Ok(0);
        }

        let mut result = 0;
        const MAX_BUNDLES_PER_RECV: usize = 24;
        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => {
                while result < MAX_BUNDLES_PER_RECV {
                    let Ok(bundle) = self.bundle_receiver.try_recv() else {
                        break;
                    };
                    let ParsedBundle {
                        transaction_ttls,
                        packets,
                        cost,
                        priority,
                        revert_on_error,
                    } = match Self::parse_bundle(&bundle, &self.bank_forks) {
                        Ok(parsed) => parsed,
                        Err(reason) => {
                            self.send_bundle_not_committed_result(bundle.seq_id, reason);
                            continue;
                        }
                    };
                    if container
                        .insert_new_batch(
                            transaction_ttls,
                            packets,
                            priority,
                            cost,
                            revert_on_error,
                            bundle.max_schedule_slot,
                        )
                        .is_none()
                    {
                        self.send_container_full_bundle_result(bundle.seq_id);
                        continue;
                    };

                    result += 1;
                }
            }
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Forward => {
                // Send back any bundles that were received while in Forward/Hold state
                let deadline = Instant::now() + Duration::from_millis(100);
                while let Ok(bundle) = self.bundle_receiver.recv_deadline(deadline) {
                    self.send_no_leader_slot_bundle_result(bundle.seq_id);
                }
            }
        }

        Ok(result)
    }
}

pub fn seq_id_to_priority(seq_id: u32) -> u64 {
    u64::MAX.saturating_sub(seq_id as u64)
}

pub fn priority_to_seq_id(priority: u64) -> u32 {
    u32::try_from(u64::MAX.saturating_sub(priority)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::{
            tests::create_slow_genesis_config,
            transaction_scheduler::{
                scheduler_metrics::{SchedulerCountMetrics, SchedulerTimingMetrics},
                transaction_state_container::StateContainer,
            },
        },
        crossbeam_channel::{unbounded, Receiver},
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_pubkey::Pubkey,
        solana_runtime::bank::Bank,
        solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
        solana_sdk::{signature::Keypair, system_transaction::transfer},
        test_case::test_case,
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

    fn setup_jss_receive_and_buffer(
        receiver: crossbeam_channel::Receiver<Bundle>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> (
        JssReceiveAndBuffer,
        TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>>,
        crossbeam_channel::Receiver<StartSchedulerMessage>,
    ) {
        let (response_sender, response_receiver) =
            crossbeam_channel::unbounded::<StartSchedulerMessage>();
        let receive_and_buffer = JssReceiveAndBuffer::new(
            Arc::new(AtomicBool::new(true)),
            receiver,
            response_sender,
            bank_forks,
        );
        let container = TransactionStateContainer::with_capacity(100);
        (receive_and_buffer, container, response_receiver)
    }

    // verify container state makes sense:
    // 1. Number of transactions matches expectation
    // 2. All transactions IDs in priority queue exist in the map
    fn verify_container<Tx: TransactionWithMeta>(
        container: &mut impl StateContainer<Tx>,
        expected_length: usize,
    ) {
        let mut actual_length: usize = 0;
        while let Some(id) = container.pop() {
            let Some((ids, _, _)) = container.get_batch(id.id) else {
                panic!(
                    "transaction in queue position {} with id {} must exist.",
                    actual_length, id.id
                );
            };
            for id in ids {
                assert!(
                    container.get_transaction_ttl(*id).is_some(),
                    "Transaction ID {} not found in container",
                    id
                );
            }
            actual_length += 1;
        }

        assert_eq!(actual_length, expected_length);
    }

    #[test_case(setup_jss_receive_and_buffer; "testcase-jss")]
    fn test_receive_and_buffer_simple_transfer<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<Bundle>,
            Arc<RwLock<BankForks>>,
        )
            -> (R, R::Container, Receiver<StartSchedulerMessage>),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container, _response_sender) =
            setup_receive_and_buffer(receiver, bank_forks.clone());
        let mut timing_metrics = SchedulerTimingMetrics::default();
        let mut count_metrics = SchedulerCountMetrics::default();

        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let data = bincode::serialize(&transaction).expect("serializes");
        let bundle = Bundle {
            seq_id: 1,
            packets: vec![Packet { data, meta: None }],
            max_schedule_slot: 0,
        };
        sender.send(bundle).unwrap();

        let num_received = receive_and_buffer
            .receive_and_buffer_packets(
                &mut container,
                &mut timing_metrics,
                &mut count_metrics,
                &BufferedPacketsDecision::Hold,
            )
            .unwrap();

        assert_eq!(num_received, 1);
        verify_container(&mut container, 1);
    }

    #[test]
    fn test_receive_and_buffer_invalid_packet() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (sender, receiver) = unbounded();
        let (mut receive_and_buffer, mut container, response_receiver) =
            setup_jss_receive_and_buffer(receiver, bank_forks.clone());

        // Create an invalid packet with no data
        let bundle = Bundle {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![],
                meta: None,
            }],
            max_schedule_slot: 0,
        };
        sender.send(bundle).unwrap();

        let result = receive_and_buffer
            .receive_and_buffer_packets(
                &mut container,
                &mut SchedulerTimingMetrics::default(),
                &mut SchedulerCountMetrics::default(),
                &BufferedPacketsDecision::Hold,
            )
            .unwrap();

        assert_eq!(result, 0);
        verify_container(&mut container, 0);
        let response = response_receiver.recv().unwrap();
        assert!(matches!(
            response.msg,
            Some(Msg::BundleResult(bundle_result)) if bundle_result.seq_id == 1 &&
            matches!(&bundle_result.result, Some(bundle_result::Result::NotCommitted(not_committed)) if
                matches!(not_committed.reason, Some(Reason::DeserializationError(_))))
        ));
    }

    #[test]
    fn test_parse_bundle_success() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bundle = Bundle {
            seq_id: 1,
            packets: vec![Packet {
                data: bincode::serialize(&transfer(
                    &mint_keypair,
                    &Pubkey::new_unique(),
                    1,
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                ))
                .unwrap(),
                meta: None,
            }],
            max_schedule_slot: 0,
        };
        let result = JssReceiveAndBuffer::parse_bundle(&bundle, &bank_forks);
        assert!(result.is_ok());
        let parsed_bundle = result.unwrap();
        assert_eq!(parsed_bundle.packets.len(), 1);
        assert_eq!(parsed_bundle.transaction_ttls.len(), 1);
    }

    #[test]
    fn test_parse_bundle_empty() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let bundle = Bundle {
            seq_id: 1,
            packets: vec![],
            max_schedule_slot: 0,
        };
        let result = JssReceiveAndBuffer::parse_bundle(&bundle, &bank_forks);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::DeserializationError(jito_protos::proto::jss_types::DeserializationError {
                index: 0,
                reason: DeserializationErrorReason::Empty as i32,
            })
        );
    }

    #[test]
    fn test_parse_bundle_invalid_packet() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let bundle = Bundle {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![0; PACKET_DATA_SIZE + 1], // Invalid size
                meta: None,
            }],
            max_schedule_slot: 0,
        };
        let result = JssReceiveAndBuffer::parse_bundle(&bundle, &bank_forks);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::DeserializationError(jito_protos::proto::jss_types::DeserializationError {
                index: 0,
                reason: DeserializationErrorReason::BincodeError as i32,
            })
        );
    }

    #[test]
    fn test_parse_bundle_fee_payer_doesnt_exist() {
        let (bank_forks, _) = test_bank_forks();
        let fee_payer = Keypair::new();
        let bundle = Bundle {
            seq_id: 1,
            packets: vec![Packet {
                data: bincode::serialize(&transfer(
                    &fee_payer,
                    &Pubkey::new_unique(),
                    1,
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                ))
                .unwrap(),
                meta: None,
            }],
            max_schedule_slot: 0,
        };
        let result = JssReceiveAndBuffer::parse_bundle(&bundle, &bank_forks);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::TransactionError(jito_protos::proto::jss_types::TransactionError {
                index: 0,
                reason: jito_protos::proto::jss_types::TransactionErrorReason::AccountNotFound
                    as i32,
            })
        );
    }

    #[test]
    fn test_parse_bundle_inconsistent() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bundle = Bundle {
            seq_id: 1,
            packets: vec![
                Packet {
                    data: bincode::serialize(&transfer(
                        &mint_keypair,
                        &Pubkey::new_unique(),
                        1,
                        bank_forks.read().unwrap().root_bank().last_blockhash(),
                    ))
                    .unwrap(),
                    meta: None,
                },
                Packet {
                    data: bincode::serialize(&transfer(
                        &mint_keypair,
                        &Pubkey::new_unique(),
                        1,
                        bank_forks.read().unwrap().root_bank().last_blockhash(),
                    ))
                    .unwrap(),
                    meta: Some(jito_protos::proto::jss_types::Meta {
                        flags: Some(jito_protos::proto::jss_types::PacketFlags {
                            revert_on_error: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                },
            ],
            max_schedule_slot: 0,
        };
        let result = JssReceiveAndBuffer::parse_bundle(&bundle, &bank_forks);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::DeserializationError(jito_protos::proto::jss_types::DeserializationError {
                index: 0,
                reason: DeserializationErrorReason::InconsistentBundle as i32,
            })
        );
    }
}
