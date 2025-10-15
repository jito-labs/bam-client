//! An implementation of the `ReceiveAndBuffer` trait that receives messages from BAM
//! and buffers from into the the `TransactionStateContainer`. Key thing to note:
//! this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
//! to BAM with a `Retryable` result.
use crate::bam_response_handle::BamResponseHandle;
use crate::banking_stage::decision_maker::DecisionMaker;
use crate::banking_stage::scheduler_messages::TransactionResult;
use crate::banking_stage::transaction_scheduler::bam_priority_graph::BamPriorityGraphContainer;
use crate::banking_stage::transaction_scheduler::bam_priority_graph::InsertError;
use crate::banking_stage::transaction_scheduler::bam_priority_graph::InsertResult;
use crate::banking_stage::transaction_scheduler::receive_and_buffer::ReceivingStats;
use crate::verified_bam_packet_batch::VerifiedBamPacketBatch;

use crossbeam_channel::{Receiver, RecvTimeoutError};

use jito_protos::proto::bam_types::TransactionCommittedResult;
// use solana_perf::sigverify::ed25519_verify_cpu;
use solana_pubkey::Pubkey;
use solana_runtime::bank::Bank;

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
    crate::banking_stage::decision_maker::BufferedPacketsDecision,
    solana_runtime::bank_forks::BankForks,
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
    bam_priority_graph_container: BamPriorityGraphContainer,
    exit: Arc<AtomicBool>,
    decision_maker: DecisionMaker,
}

impl BamReceiveAndBuffer {
    pub fn new(
        bam_enabled: Arc<AtomicBool>,
        bam_packet_batch_receiver: Receiver<VerifiedBamPacketBatch>,
        bam_response_handle: BamResponseHandle,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
        bam_priority_graph_container: BamPriorityGraphContainer,
        exit: Arc<AtomicBool>,
        decision_maker: DecisionMaker,
    ) -> Self {
        Self {
            bam_enabled,
            bam_response_handle,
            bam_packet_batch_receiver,
            bank_forks,
            blacklisted_accounts,
            bam_priority_graph_container,
            exit,
            decision_maker,
        }
    }

    fn maybe_report_stats(stats: &mut ReceivingStats, last_report: &mut Instant) {
        if stats.num_received > 0 && last_report.elapsed() > Duration::from_secs(1) {
            // TODO (LB): emit metrics
            *stats = ReceivingStats::default();
            *last_report = Instant::now();
        }
    }

    pub(crate) fn run(&self) {
        let mut stats = ReceivingStats::default();
        let mut last_report = Instant::now();
        let mut last_slot = None;

        while !self.exit.load(Ordering::Relaxed) {
            Self::maybe_report_stats(&mut stats, &mut last_report);

            let bam_packet_batch = match self
                .bam_packet_batch_receiver
                .recv_timeout(Duration::from_millis(100))
            {
                Ok(bam_packet_batch) => bam_packet_batch,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    warn!("BAM packet batch receiver disconnected, exiting...");
                    break;
                }
            };

            // info!("Received sequence id: {:?}", bam_packet_batch.meta().seq_id);

            stats.num_received += bam_packet_batch.packet_batch().len();

            let decision = self.decision_maker.make_consume_or_forward_decision();
            let new_leader_slot = decision.bank().map(|b| b.slot());
            if new_leader_slot != last_slot {
                info!(
                    "Detected slot change, draining priority graph: {:?}",
                    new_leader_slot
                );
                let mut num_drained = 0;
                self.bam_priority_graph_container
                    .drain_with_callback(|batch_priority_id| {
                        let sequence_id = priority_to_seq_id(batch_priority_id.priority);
                        self.bam_response_handle.send_result(
                            sequence_id,
                            false,
                            vec![TransactionResult::Committed(
                                TransactionCommittedResult::default(),
                            )],
                        );
                        num_drained += 1;
                    });
                info!("Drained {} batches", num_drained);
                last_slot = new_leader_slot;
            }

            match decision {
                BufferedPacketsDecision::Consume(working_bank) => {
                    let root_bank = self.bank_forks.read().unwrap().root_bank();

                    stats.accumulate(self.handle_packet_batch_message(
                        &root_bank,
                        &working_bank,
                        bam_packet_batch,
                    ));
                    while !working_bank.is_complete() {
                        if let Ok(bam_packet_batch) = self.bam_packet_batch_receiver.try_recv() {
                            stats.accumulate(self.handle_packet_batch_message(
                                &root_bank,
                                &working_bank,
                                bam_packet_batch,
                            ));
                        }
                    }
                }
                BufferedPacketsDecision::Forward => {
                    stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
                    self.bam_response_handle
                        .send_outside_leader_slot_bundle_result(bam_packet_batch.meta().seq_id);
                }
                BufferedPacketsDecision::ForwardAndHold => {
                    // info!("no bank brother");
                    stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
                    self.bam_response_handle
                        .send_outside_leader_slot_bundle_result(bam_packet_batch.meta().seq_id);
                }
                BufferedPacketsDecision::Hold => {
                    // info!("no bank brother");
                    stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
                    self.bam_response_handle
                        .send_outside_leader_slot_bundle_result(bam_packet_batch.meta().seq_id);
                }
            }
        }
    }

    fn handle_packet_batch_message(
        &self,
        root_bank: &Arc<Bank>,
        working_bank: &Arc<Bank>,
        bam_packet_batch: VerifiedBamPacketBatch,
    ) -> ReceivingStats {
        let mut stats = ReceivingStats::default();

        let seq_id = bam_packet_batch.meta().seq_id;
        let revert_on_error = bam_packet_batch.meta().revert_on_error;

        // Inserts into both the container and the priority graph
        let InsertResult {
            result,
            receiving_stats,
        } = self
            .bam_priority_graph_container
            .try_insert_and_notify_workers(
                bam_packet_batch,
                root_bank,
                working_bank,
                &self.blacklisted_accounts,
            );

        match result {
            Ok(()) => {
                // let the slot change drain this guy
            }
            Err((index, InsertError::BadSignature)) => {
                self.bam_response_handle.send_bad_signature(seq_id);
            }
            Err((index, InsertError::OutsideLeaderSlot)) => {
                self.bam_response_handle
                    .send_outside_leader_slot_bundle_result(seq_id);
            }
            Err((index, e)) => {
                self.bam_response_handle
                    .send_sanitization_error(seq_id, index);
            }
        }
        stats.accumulate(receiving_stats);

        stats
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
