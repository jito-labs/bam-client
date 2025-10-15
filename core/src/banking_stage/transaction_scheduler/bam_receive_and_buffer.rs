//! An implementation of the `ReceiveAndBuffer` trait that receives messages from BAM
//! and buffers from into the the `TransactionStateContainer`. Key thing to note:
//! this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
//! to BAM with a `Retryable` result.
use crate::bam_response_handle::BamResponseHandle;
use crate::banking_stage::consumer::Consumer;
use crate::banking_stage::transaction_scheduler::receive_and_buffer::calculate_max_age;
use crate::banking_stage::transaction_scheduler::receive_and_buffer::calculate_priority_and_cost;
use crate::banking_stage::transaction_scheduler::receive_and_buffer::DisconnectedError;
use crate::banking_stage::transaction_scheduler::receive_and_buffer::ReceivingStats;
use crate::banking_stage::transaction_scheduler::transaction_priority_id::TransactionPriorityId;
use crate::banking_stage::transaction_scheduler::transaction_state::TransactionState;
use crate::banking_stage::transaction_scheduler::transaction_state_container::SharedBytes;

use crate::banking_stage::transaction_scheduler::transaction_state_container::StateContainer;
use crate::banking_stage::transaction_scheduler::transaction_state_container::TransactionViewState;
use crate::banking_stage::transaction_scheduler::transaction_state_container::TransactionViewStateContainer;
use crate::banking_stage::transaction_scheduler::transaction_state_container::EXTRA_CAPACITY;
use crate::verified_bam_packet_batch::VerifiedBamPacketBatch;
use agave_transaction_view::resolved_transaction_view::ResolvedTransactionView;
use agave_transaction_view::transaction_version::TransactionVersion;
use agave_transaction_view::transaction_view::SanitizedTransactionView;

use arrayvec::ArrayVec;
use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError};
use solana_clock::MAX_PROCESSING_AGE;
use solana_svm_transaction::svm_message::SVMMessage;

use solana_accounts_db::account_locks::validate_account_locks;

use solana_fee_structure::FeeBudgetLimits;
use solana_measure::measure_us;

// use solana_perf::sigverify::ed25519_verify_cpu;
use solana_pubkey::Pubkey;
use solana_runtime::bank::Bank;
use solana_runtime_transaction::transaction_meta::StaticMeta;
use solana_svm::transaction_error_metrics::TransactionErrorMetrics;
use solana_transaction::sanitized::MessageHash;

use std::time::Instant;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use {
    super::receive_and_buffer::ReceiveAndBuffer,
    crate::banking_stage::decision_maker::BufferedPacketsDecision,
    solana_runtime::bank_forks::BankForks,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
};

#[derive(Debug)]
enum PacketHandlingError {
    Sanitization,
    LockValidation,
    ComputeBudget,
    BlacklistedAccount,
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
        stats.num_received += bam_packet_batch.packet_batch().len();

        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();
        let mut error_counters = TransactionErrorMetrics::default();

        // Throw away batches that are marked as discard from bad signature
        if bam_packet_batch.meta().discard {
            self.bam_response_handle
                .send_bad_signature(bam_packet_batch.meta().seq_id);
            stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
            return stats;
        }

        // Throw away batches that are outside the maximum schedulable slot
        if bam_packet_batch.meta().max_schedule_slot > working_bank.slot() {
            self.bam_response_handle
                .send_outside_leader_slot_bundle_result(bam_packet_batch.meta().seq_id);
            stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
            return stats;
        }

        // The 5 packet check exists when creating the VerifiedBamPacketBatch, but might get removed in the future.
        // This check exists to ensure that we don't accidentally overflow the transactions ArrayVec below.
        if bam_packet_batch.packet_batch().len() > EXTRA_CAPACITY {
            self.bam_response_handle
                .send_sanitization_error(bam_packet_batch.meta().seq_id, 0);
            stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
            return stats;
        }

        let packet_data = bam_packet_batch
            .packet_batch()
            .iter()
            .filter_map(|p| p.data(..))
            .collect::<Vec<_>>();

        let lock_results: [_; EXTRA_CAPACITY] = core::array::from_fn(|_| Ok(()));

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
            },
        ) {
            Ok(Some(batch_id)) => {
                let transaction_ids = {
                    let batch_info = container.get_batch(batch_id).expect("batch must exist");
                    let mut transaction_ids = ArrayVec::<_, EXTRA_CAPACITY>::new();
                    transaction_ids.extend(batch_info.transaction_ids.iter().cloned());
                    transaction_ids
                };

                // Note: mega-batching these transaction checks would probably speed things up
                let mut transactions = ArrayVec::<_, EXTRA_CAPACITY>::new();
                transactions.extend(transaction_ids.iter().map(|id| {
                    container
                        .get_transaction(*id)
                        .expect("transaction must exist")
                }));
                let check_results = working_bank.check_transactions::<RuntimeTransaction<_>>(
                    &transactions,
                    &lock_results[..transactions.len()],
                    MAX_PROCESSING_AGE,
                    &mut error_counters,
                );

                let mut error = None;
                for (index, (result, _transaction_id)) in
                    check_results.iter().zip(transaction_ids.iter()).enumerate()
                {
                    match result {
                        Ok(_) => {
                            if let Err(e) = Consumer::check_fee_payer_unlocked(
                                working_bank,
                                transactions[index],
                                &mut error_counters,
                            ) {
                                stats.num_dropped_on_fee_payer += transaction_ids.len();
                                error = Some((index, e));
                                break;
                            }
                        }
                        Err(e) => {
                            error = Some((index, e.clone()));
                            break;
                        }
                    }
                }
                drop(transactions);

                if let Some((error_index, error)) = error {
                    container.remove_batch_by_id(batch_id);
                    self.bam_response_handle.send_not_committed_result(
                        bam_packet_batch.meta().seq_id,
                        error_index,
                        error,
                    );
                } else {
                    container.push_batch_id_into_queue(TransactionPriorityId::new(
                        seq_id_to_priority(bam_packet_batch.meta().seq_id),
                        batch_id,
                    ));
                }
            }
            // Ok(None) means an error occurred during insertion, all of the transactions were removed from the container and insert_map_error is set
            Ok(None) => {}
            // container is full, nothing was added
            Err(()) => {
                // error!("Container is full, nothing was added");
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
        let is_bam_enabled = self.bam_enabled.load(Ordering::Relaxed);
        let mut stats = ReceivingStats::default();

        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };

        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => loop {
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
                    stats.num_dropped_without_parsing += stats.num_received;
                    continue;
                }

                stats.accumulate(self.handle_packet_batch_message(
                    container,
                    &root_bank,
                    &working_bank,
                    bam_packet_batch,
                ));
            },
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
                    self.bam_response_handle
                        .send_outside_leader_slot_bundle_result(batch.meta().seq_id);
                    stats.num_dropped_without_parsing += 1;
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

// #[derive(Default)]
// struct BamReceiveAndBufferMetrics {
//     total_us: u64,
//     deserialization_us: u64,
//     sanitization_us: u64,
//     lock_validation_us: u64,
//     fee_budget_extraction_us: u64,
//     check_transactions_us: u64,
//     fee_payer_check_us: u64,
//     blacklist_check_us: u64,
//     pub sigverify_metrics: SigverifyMetrics,
// }

// impl BamReceiveAndBufferMetrics {
//     fn has_data(&self) -> bool {
//         self.total_us > 0
//             || self.deserialization_us > 0
//             || self.sanitization_us > 0
//             || self.lock_validation_us > 0
//             || self.fee_budget_extraction_us > 0
//             || self.check_transactions_us > 0
//             || self.fee_payer_check_us > 0
//             || self.blacklist_check_us > 0
//             || self.sigverify_metrics.total_packets_verified > 0
//     }

//     fn report(&mut self) {
//         if !self.has_data() {
//             return;
//         }

//         datapoint_info!(
//             "bam-receive-and-buffer",
//             ("total_us", self.total_us, i64),
//             ("deserialization_us", self.deserialization_us, i64),
//             ("sanitization_us", self.sanitization_us, i64),
//             ("lock_validation_us", self.lock_validation_us, i64),
//             (
//                 "fee_budget_extraction_us",
//                 self.fee_budget_extraction_us,
//                 i64
//             ),
//             ("check_transactions_us", self.check_transactions_us, i64),
//             ("fee_payer_check_us", self.fee_payer_check_us, i64),
//             ("blacklist_check_us", self.blacklist_check_us, i64),
//         );
//         self.sigverify_metrics.report();
//         *self = Self::default();
//     }

//     fn increment_total_us(&mut self, us: u64) {
//         self.total_us = self.total_us.saturating_add(us);
//     }

//     fn increment_deserialization_us(&mut self, us: u64) {
//         self.deserialization_us = self.deserialization_us.saturating_add(us);
//     }

//     fn increment_sanitization_us(&mut self, us: u64) {
//         self.sanitization_us = self.sanitization_us.saturating_add(us);
//     }

//     fn increment_lock_validation_us(&mut self, us: u64) {
//         self.lock_validation_us = self.lock_validation_us.saturating_add(us);
//     }

//     fn increment_fee_budget_extraction_us(&mut self, us: u64) {
//         self.fee_budget_extraction_us = self.fee_budget_extraction_us.saturating_add(us);
//     }

//     fn increment_check_transactions_us(&mut self, us: u64) {
//         self.check_transactions_us = self.check_transactions_us.saturating_add(us);
//     }

//     fn increment_fee_payer_check_us(&mut self, us: u64) {
//         self.fee_payer_check_us = self.fee_payer_check_us.saturating_add(us);
//     }

//     fn increment_blacklist_check_us(&mut self, us: u64) {
//         self.blacklist_check_us = self.blacklist_check_us.saturating_add(us);
//     }
// }

// struct SigverifyMetrics {
//     pub verify_batches_pp_us_hist: Histogram,
//     pub batch_packets_len_hist: Histogram,
//     pub total_verify_time_us: u64,
//     pub total_packets_verified: usize,
//     pub total_batches_verified: usize,
// }

// impl Default for SigverifyMetrics {
//     fn default() -> Self {
//         Self {
//             verify_batches_pp_us_hist: Histogram::new(),
//             batch_packets_len_hist: Histogram::new(),
//             total_verify_time_us: 0,
//             total_packets_verified: 0,
//             total_batches_verified: 0,
//         }
//     }
// }

// impl SigverifyMetrics {
//     pub fn report(&self) {
//         if self.total_packets_verified == 0 {
//             return;
//         }

//         datapoint_info!(
//             "bam-receive-and-buffer_sigverify-stats",
//             ("total_verify_time_us", self.total_verify_time_us, i64),
//             ("total_packets_verified", self.total_packets_verified, i64),
//             ("total_batches_verified", self.total_batches_verified, i64),
//             (
//                 "verify_batches_pp_us_p50",
//                 self.verify_batches_pp_us_hist.percentile(50.0).unwrap_or(0),
//                 i64
//             ),
//             (
//                 "verify_batches_pp_us_p75",
//                 self.verify_batches_pp_us_hist.percentile(75.0).unwrap_or(0),
//                 i64
//             ),
//             (
//                 "verify_batches_pp_us_p90",
//                 self.verify_batches_pp_us_hist.percentile(90.0).unwrap_or(0),
//                 i64
//             ),
//             (
//                 "verify_batches_pp_us_p99",
//                 self.verify_batches_pp_us_hist.percentile(99.0).unwrap_or(0),
//                 i64
//             ),
//             (
//                 "batch_packets_len_p50",
//                 self.batch_packets_len_hist.percentile(50.0).unwrap_or(0),
//                 i64
//             ),
//             (
//                 "batch_packets_len_p75",
//                 self.batch_packets_len_hist.percentile(75.0).unwrap_or(0),
//                 i64
//             ),
//             (
//                 "batch_packets_len_p90",
//                 self.batch_packets_len_hist.percentile(90.0).unwrap_or(0),
//                 i64
//             ),
//             (
//                 "batch_packets_len_p99",
//                 self.batch_packets_len_hist.percentile(99.0).unwrap_or(0),
//                 i64
//             ),
//         );
//     }

//     pub fn increment_verify_batches_pp_us(&mut self, us: u64, packet_count: usize) {
//         if packet_count > 0 {
//             let per_packet_us = (us as f64 / packet_count as f64).round() as u64;
//             self.verify_batches_pp_us_hist
//                 .increment(per_packet_us)
//                 .unwrap();
//         }
//     }

//     pub fn increment_batch_packets_len(&mut self, packet_count: usize) {
//         if packet_count > 0 {
//             self.batch_packets_len_hist
//                 .increment(packet_count as u64)
//                 .unwrap();
//         }
//     }

//     pub fn increment_total_verify_time(&mut self, us: u64) {
//         self.total_verify_time_us += us;
//     }

//     pub fn increment_total_packets_verified(&mut self, count: usize) {
//         self.total_packets_verified += count;
//     }

//     pub fn increment_total_batches_verified(&mut self, count: usize) {
//         self.total_batches_verified += count;
//     }
// }

#[cfg(test)]
mod tests {
    use solana_signer::Signer;
    use solana_system_transaction::transfer;
    use {
        super::*,
        crate::banking_stage::{
            tests::create_slow_genesis_config,
            transaction_scheduler::transaction_state_container::StateContainer,
        },
        crossbeam_channel::{unbounded, Receiver},
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_runtime::bank::Bank,
        solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
        solana_transaction::versioned::VersionedTransaction,
        solana_transaction::Transaction,
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

    fn setup_bam_receive_and_buffer(
        receiver: crossbeam_channel::Receiver<AtomicTxnBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        Arc<AtomicBool>,
        BamReceiveAndBuffer,
        TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>>,
        crossbeam_channel::Receiver<BamOutboundMessage>,
    ) {
        let exit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let (response_sender, response_receiver) =
            crossbeam_channel::unbounded::<BamOutboundMessage>();
        let receive_and_buffer = BamReceiveAndBuffer::new(
            Arc::new(AtomicBool::new(true)),
            receiver,
            response_sender,
            bank_forks,
            blacklisted_accounts,
        );
        let container = TransactionStateContainer::with_capacity(100);
        (exit, receive_and_buffer, container, response_receiver)
    }

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
                    container.get_transaction(*id).is_some(),
                    "Transaction ID {} not found in container",
                    id
                );
            }
            actual_length += 1;
        }

        assert_eq!(actual_length, expected_length);
    }

    #[test_case(setup_bam_receive_and_buffer; "testcase-bam")]
    fn test_receive_and_buffer_simple_transfer<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<AtomicTxnBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (
            Arc<AtomicBool>,
            R,
            R::Container,
            Receiver<BamOutboundMessage>,
        ),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (exit, mut receive_and_buffer, mut container, _response_sender) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());
        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let data = bincode::serialize(&transaction).expect("serializes");
        let bundle = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet { data, meta: None }],
            max_schedule_slot: Slot::MAX,
        };
        sender.send(bundle).unwrap();

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            let ReceivingStats { num_received, .. } = receive_and_buffer
                .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
                .unwrap();
            if num_received > 0 {
                break;
            }
        }

        verify_container(&mut container, 1);
        exit.store(true, Ordering::Relaxed);
    }

    #[test]
    fn test_receive_and_buffer_invalid_packet() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (sender, receiver) = unbounded();
        let (exit, mut receive_and_buffer, mut container, response_receiver) =
            setup_bam_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());

        let bundle = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![],
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };
        sender.send(bundle).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 0);
        verify_container(&mut container, 0);
        let response = response_receiver.recv().unwrap();
        assert!(matches!(
            response,
            BamOutboundMessage::AtomicTxnBatchResult(txn_batch_result) if txn_batch_result.seq_id == 1 &&
            matches!(&txn_batch_result.result, Some(atomic_txn_batch_result::Result::NotCommitted(not_committed)) if
                matches!(not_committed.reason, Some(Reason::DeserializationError(_))))
        ));
        exit.store(true, Ordering::Relaxed);
    }

    #[test]
    fn test_batch_deserialize_success() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bundle = AtomicTxnBatch {
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
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[bundle], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        if let Ok((deserialized_packets, _, seq_id, _max_schedule_slot)) = &results[0] {
            assert_eq!(deserialized_packets.len(), 1);
            assert_eq!(*seq_id, 1);
        }
    }

    #[test]
    fn test_batch_deserialize_empty() {
        let (_bank_forks, _mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
        assert_eq!(batch_stats.num_dropped_without_parsing, 1);
        if let Err((reason, seq_id)) = &results[0] {
            assert_eq!(*seq_id, 1);
            assert!(matches!(reason, Reason::DeserializationError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_invalid_packet() {
        let (_bank_forks, _mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![0; PACKET_DATA_SIZE + 1],
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
        if let Err((reason, seq_id)) = &results[0] {
            assert_eq!(*seq_id, 1);
            assert!(matches!(reason, Reason::DeserializationError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_fee_payer_doesnt_exist() {
        let (bank_forks, _) = test_bank_forks();
        let fee_payer = Keypair::new();
        let batch = AtomicTxnBatch {
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
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
        {
            let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                *max_schedule_slot,
                &bank_forks,
                &HashSet::new(),
                &mut stats,
            );

            assert!(result.is_err());
            assert_eq!(stats.num_dropped_on_fee_payer, 1);
            assert!(matches!(result.err().unwrap(), Reason::TransactionError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_inconsistent() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bundle = AtomicTxnBatch {
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
                    meta: Some(jito_protos::proto::bam_types::Meta {
                        flags: Some(jito_protos::proto::bam_types::PacketFlags {
                            revert_on_error: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                },
            ],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[bundle], Slot::MAX, &mut stats);
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
        assert_eq!(batch_stats.num_dropped_without_parsing, 1);
        if let Err((reason, seq_id)) = &results[0] {
            assert_eq!(*seq_id, 1);
            assert!(matches!(reason, Reason::DeserializationError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_blacklisted_account() {
        let keypair = Keypair::new();
        let blacklisted_accounts = HashSet::from([keypair.pubkey()]);

        let (bank_forks, mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: bincode::serialize(&transfer(
                    &mint_keypair,
                    &keypair.pubkey(),
                    100,
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                ))
                .unwrap(),
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
        {
            let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                *max_schedule_slot,
                &bank_forks,
                &blacklisted_accounts,
                &mut stats,
            );

            assert!(result.is_err());
            assert_eq!(stats.num_dropped_on_blacklisted_account, 1);
            assert!(matches!(result.err().unwrap(), Reason::TransactionError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_rejects_vote_transactions() {
        let (bank_forks, _mint_keypair) = test_bank_forks();

        let vote_keypair = Keypair::new();
        let node_keypair = Keypair::new();
        let authorized_voter = Keypair::new();
        let recent_blockhash = bank_forks.read().unwrap().root_bank().last_blockhash();

        let vote_tx = Transaction::new(
            &[&node_keypair, &authorized_voter],
            Message::new(
                &[solana_vote_program::vote_instruction::vote(
                    &vote_keypair.pubkey(),
                    &authorized_voter.pubkey(),
                    solana_vote_program::vote_state::Vote::new(vec![1], recent_blockhash),
                )],
                Some(&node_keypair.pubkey()),
            ),
            recent_blockhash,
        );

        let vote_data = bincode::serialize(&VersionedTransaction::from(vote_tx)).unwrap();

        let meta = jito_protos::proto::bam_types::Meta {
            flags: Some(jito_protos::proto::bam_types::PacketFlags {
                simple_vote_tx: true,
                ..Default::default()
            }),
            size: vote_data.len() as u64,
        };

        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vote_data,
                meta: Some(meta),
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
        {
            let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                *max_schedule_slot,
                &bank_forks,
                &HashSet::new(),
                &mut stats,
            );

            assert!(result.is_err());
            assert_eq!(stats.num_dropped_on_parsing_and_sanitization, 1);
            assert!(matches!(
                result.err().unwrap(),
                Reason::DeserializationError(_)
            ));
        }
    }

    #[test]
    fn test_batch_deserialize_reject_wrong_slot() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
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

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) =
            BamReceiveAndBuffer::batch_deserialize_and_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }
}
