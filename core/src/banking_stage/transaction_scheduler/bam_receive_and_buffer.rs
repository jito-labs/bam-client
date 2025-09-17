//! An implementation of the `ReceiveAndBuffer` trait that receives messages from BAM
//! and buffers from into the the `TransactionStateContainer`. Key thing to note:
//! this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
//! to BAM with a `Retryable` result.
use crate::banking_stage::scheduler_messages::MaxAge;
use crate::banking_stage::transaction_scheduler::receive_and_buffer::DisconnectedError;
use crate::{
    bam_dependencies::BamOutboundMessage,
    banking_stage::transaction_scheduler::receive_and_buffer::ReceivingStats,
};
use crossbeam_channel::RecvTimeoutError;
use solana_clock::MAX_PROCESSING_AGE;
use solana_measure::{measure::Measure, measure_us};
use solana_packet::{PacketFlags, PACKET_DATA_SIZE};

use solana_perf::sigverify::ed25519_verify_cpu;
use solana_pubkey::Pubkey;
use solana_sanitize::SanitizeError;
use solana_transaction::sanitized::SanitizedTransaction;
use std::{
    cmp::min,
    collections::HashSet,
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
            bam_utils::{convert_deserialize_error_to_proto, convert_txn_error_to_proto},
            receive_and_buffer::{calculate_max_age, calculate_priority_and_cost},
        },
    },
    crossbeam_channel::Sender,
    itertools::Itertools,
    histogram::Histogram,
    jito_protos::proto::bam_types::{
        atomic_txn_batch_result, not_committed::Reason, AtomicTxnBatch, DeserializationErrorReason,
        Packet, SchedulingError,
    },
    solana_accounts_db::account_locks::validate_account_locks,
    solana_runtime::bank_forks::BankForks,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
};

pub struct BamReceiveAndBuffer {
    bam_enabled: Arc<AtomicBool>,
    bundle_receiver: crossbeam_channel::Receiver<AtomicTxnBatch>,
    response_sender: Sender<BamOutboundMessage>,
    bank_forks: Arc<RwLock<BankForks>>,
    blacklisted_accounts: HashSet<Pubkey>,
    stats: SigverifyStats,
    last_report: Instant,
}

pub struct SigverifyStats {
    pub verify_batches_pp_us_hist: Histogram,
    pub batch_packets_len_hist: Histogram,
    pub total_verify_time_us: u64,
    pub total_packets_verified: usize,
    pub total_batches_verified: usize,
}

impl Default for SigverifyStats {
    fn default() -> Self {
        Self {
            verify_batches_pp_us_hist: Histogram::new(),
            batch_packets_len_hist: Histogram::new(),
            total_verify_time_us: 0,
            total_packets_verified: 0,
            total_batches_verified: 0,
        }
    }
}

impl SigverifyStats {
    pub fn maybe_report(&self) {
        if self.total_packets_verified == 0 {
            return;
        }

        datapoint_info!(
            "jito-bam-receive-and-buffer_sigverify-stats",
            ("total_verify_time_us", self.total_verify_time_us, i64),
            ("total_packets_verified", self.total_packets_verified, i64),
            ("total_batches_verified", self.total_batches_verified, i64),
            ("verify_batches_pp_us_p50", self.verify_batches_pp_us_hist.percentile(50.0).unwrap_or(0), i64),
            ("verify_batches_pp_us_p75", self.verify_batches_pp_us_hist.percentile(75.0).unwrap_or(0), i64),
            ("verify_batches_pp_us_p90", self.verify_batches_pp_us_hist.percentile(90.0).unwrap_or(0), i64),
            ("verify_batches_pp_us_p99", self.verify_batches_pp_us_hist.percentile(99.0).unwrap_or(0), i64),
            ("batch_packets_len_p50", self.batch_packets_len_hist.percentile(50.0).unwrap_or(0), i64),
            ("batch_packets_len_p75", self.batch_packets_len_hist.percentile(75.0).unwrap_or(0), i64),
            ("batch_packets_len_p90", self.batch_packets_len_hist.percentile(90.0).unwrap_or(0), i64),
            ("batch_packets_len_p99", self.batch_packets_len_hist.percentile(99.0).unwrap_or(0), i64),
        );
    }

    pub fn increment_verify_batches_pp_us(&mut self, us: u64, packet_count: usize) {
        if packet_count > 0 {
            let per_packet_us = (us as f64 / packet_count as f64).round() as u64;
            self.verify_batches_pp_us_hist.increment(per_packet_us).unwrap();
        }
    }

    pub fn increment_batch_packets_len(&mut self, packet_count: usize) {
        if packet_count > 0 {
            self.batch_packets_len_hist.increment(packet_count as u64).unwrap();
        }
    }
    
    pub fn increment_total_verify_time(&mut self, us: u64) {
        self.total_verify_time_us += us;
    }

    pub fn increment_total_packets_verified(&mut self, count: usize) {
        self.total_packets_verified += count;
    }

    pub fn increment_total_batches_verified(&mut self, count: usize) {
        self.total_batches_verified += count;
    }
}

impl BamReceiveAndBuffer {
    pub fn new(
        bam_enabled: Arc<AtomicBool>,
        bundle_receiver: crossbeam_channel::Receiver<AtomicTxnBatch>,
        response_sender: Sender<BamOutboundMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> Self {
        Self {
            bam_enabled,
            bundle_receiver,
            response_sender,
            bank_forks,
            blacklisted_accounts,
            stats: SigverifyStats::default(),
            last_report: Instant::now(),
        }
    }

    fn send_bundle_not_committed_result(&self, seq_id: u32, reason: Reason) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(reason),
                        },
                    )),
                },
            ));
    }

    fn send_no_leader_slot_txn_batch_result(&self, seq_id: u32) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::OutsideLeaderSlot as i32,
                            )),
                        },
                    )),
                },
            ));
    }

    fn send_container_full_txn_batch_result(&self, seq_id: u32) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::ContainerFull as i32,
                            )),
                        },
                    )),
                },
            ));
    }

    fn parse_deserialized_batch(
        deserialized_batch: Vec<ImmutableDeserializedPacket>,
        seq_id: u32,
        revert_on_error: bool,
        bank_forks: &Arc<RwLock<BankForks>>,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> (Result<ParsedBatch, Reason>, ReceivingStats) {
        let mut stats = ReceivingStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_parsing_and_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity: 0,
            num_buffered: 0,
            num_dropped_on_blacklisted_account: 0,
            receive_time_us: 0,
            buffer_time_us: 0,
        };

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
        let mut txns_max_age = vec![];

        // Checks are taken from receive_and_buffer.rs:
        // SanitizedTransactionReceiveAndBuffer::buffer_packets
        for (index, parsed_packet) in deserialized_batch.into_iter().enumerate() {
            // Check 0: Reject vote transactions
            if parsed_packet.is_simple_vote() {
                stats.num_dropped_on_parsing_and_sanitization += 1;
                return (
                    Err(Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: index as u32,
                            reason: DeserializationErrorReason::VoteTransactionFailure as i32,
                        },
                    )),
                    stats,
                );
            }

            // Check 1: Ensure the transaction is valid
            let Some((tx, deactivation_slot)) = parsed_packet.build_sanitized_transaction(
                vote_only,
                root_bank.as_ref(),
                root_bank.get_reserved_account_keys(),
            ) else {
                stats.num_dropped_on_parsing_and_sanitization += 1;
                return (
                    Err(Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: index as u32,
                            reason: DeserializationErrorReason::SanitizeError as i32,
                        },
                    )),
                    stats,
                );
            };

            // Check 2: Ensure no duplicates and valid number of account locks
            if let Err(err) =
                validate_account_locks(tx.message().account_keys(), transaction_account_lock_limit)
            {
                let reason = convert_txn_error_to_proto(err);
                stats.num_dropped_on_lock_validation += 1;
                return (
                    Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: reason as i32,
                        },
                    )),
                    stats,
                );
            }

            // Check 3: Ensure the compute budget limits are valid
            let fee_budget_limits = match tx
                .compute_budget_instruction_details()
                .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
            {
                Ok(fee_budget_limits) => fee_budget_limits,
                Err(err) => {
                    let reason = convert_txn_error_to_proto(err);
                    stats.num_dropped_on_compute_budget += 1;
                    return (
                        Err(Reason::TransactionError(
                            jito_protos::proto::bam_types::TransactionError {
                                index: index as u32,
                                reason: reason as i32,
                            },
                        )),
                        stats,
                    );
                }
            };

            // Check 4: Ensure valid blockhash and blockhash is not too old
            let lock_results: [_; 1] = core::array::from_fn(|_| Ok(()));
            let check_results = working_bank.check_transactions(
                std::slice::from_ref(&tx),
                &lock_results,
                MAX_PROCESSING_AGE,
                &mut TransactionErrorMetrics::default(),
            );
            if let Some(Err(err)) = check_results.first() {
                let reason = convert_txn_error_to_proto(err.clone());
                stats.num_dropped_on_age += 1;
                return (
                    Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: reason as i32,
                        },
                    )),
                    stats,
                );
            }

            // Check 5: Ensure the fee payer has enough to pay for the transaction fee
            if let Err(err) = Consumer::check_fee_payer_unlocked(
                &working_bank,
                &tx,
                &mut TransactionErrorMetrics::default(),
            ) {
                let reason = convert_txn_error_to_proto(err);
                stats.num_dropped_on_fee_payer += 1;
                return (
                    Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: reason as i32,
                        },
                    )),
                    stats,
                );
            }

            // Check 6: Ensure none of the accounts touch blacklisted accounts
            if tx
                .message()
                .account_keys()
                .iter()
                .any(|key| blacklisted_accounts.contains(key))
            {
                stats.num_dropped_on_blacklisted_account += 1;
                return (
                    Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: DeserializationErrorReason::SanitizeError as i32,
                        },
                    )),
                    stats,
                );
            }

            let max_age = calculate_max_age(sanitized_epoch, deactivation_slot, alt_resolved_slot);

            let (_, txn_cost) =
                calculate_priority_and_cost(&tx, &fee_budget_limits.into(), &working_bank);
            cost = cost.saturating_add(txn_cost);
            txns_max_age.push((tx, max_age));
            packets.push(Arc::new(parsed_packet));
        }

        let priority = seq_id_to_priority(seq_id);

        (
            Ok(ParsedBatch {
                txns_max_age,
                cost,
                priority,
                revert_on_error,
            }),
            stats,
        )
    }

    fn batch_receive_until(&self, recv_timeout: Duration, packet_count_upperbound: usize) -> Result<(usize, Vec<AtomicTxnBatch>), RecvTimeoutError> {
        let start = Instant::now();

        let batch = self.bundle_receiver.recv_timeout(recv_timeout)?;
        let mut num_packets_received = batch.packets.len();
        let mut atomic_txn_batches = vec![batch];

        while let Ok(batch) = self.bundle_receiver.try_recv() {
            trace!("got more packet batches in bam receive and buffer");
            num_packets_received += batch.packets.len();
            atomic_txn_batches.push(batch);

            // todo: might want to switch the upperbound to be on number of batches instead of packets
            if start.elapsed() >= recv_timeout || num_packets_received >= packet_count_upperbound {
                break;
            }
        }

        Ok((num_packets_received, atomic_txn_batches)) 
    }

    /// Check basic constraints and extract revert_on_error flags
    fn prevalidate_batches<'a>(
        atomic_txn_batches: &'a [AtomicTxnBatch]
    ) -> (Vec<Result<(&'a AtomicTxnBatch, bool, u32), (Reason, u32)>>, ReceivingStats) {
        let mut stats = ReceivingStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_parsing_and_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity: 0,
            num_buffered: 0,
            num_dropped_on_blacklisted_account: 0,
            receive_time_us: 0,
            buffer_time_us: 0,
        };

        let prevalidated = atomic_txn_batches
            .iter()
            .map(|atomic_txn_batch| {
                if atomic_txn_batch.packets.is_empty() {
                    stats.num_dropped_without_parsing += 1;
                    return Err((Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: 0,
                            reason: DeserializationErrorReason::Empty as i32,
                        },
                    ), atomic_txn_batch.seq_id));
                }

                if atomic_txn_batch.packets.len() > 5 {
                    stats.num_dropped_without_parsing += 1;
                    return Err((Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: 0,
                            reason: DeserializationErrorReason::SanitizeError as i32,
                        },
                    ), atomic_txn_batch.seq_id));
                }

                let Ok(revert_on_error) = atomic_txn_batch
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
                    stats.num_dropped_without_parsing += 1;
                    return Err((Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: 0,
                            reason: DeserializationErrorReason::InconsistentBundle as i32,
                        },
                    ), atomic_txn_batch.seq_id));
                };

                Ok((atomic_txn_batch, revert_on_error, atomic_txn_batch.seq_id))
            })
            .collect();

        (prevalidated, stats)
    }

    fn batch_deserialize_and_verify(atomic_txn_batches: Vec<AtomicTxnBatch>, sigverify_stats: &mut SigverifyStats) -> (Vec<Result<(Vec<ImmutableDeserializedPacket>, bool, u32), (Reason, u32)>>, ReceivingStats) {
        fn proto_packet_to_packet(from_packet: &Packet) -> solana_packet::Packet {
            let mut to_packet = solana_packet::Packet::default();
            to_packet.meta_mut().size = from_packet.data.len();
            to_packet.meta_mut().set_discard(false);

            let copy_len = min(PACKET_DATA_SIZE, from_packet.data.len());
            to_packet.buffer_mut()[0..copy_len].copy_from_slice(&from_packet.data[0..copy_len]);

            if let Some(meta) = &from_packet.meta {
                to_packet.meta_mut().size = meta.size as usize;
                if let Some(flags) = &meta.flags {
                    if flags.simple_vote_tx {
                        to_packet
                            .meta_mut()
                            .flags
                            .insert(PacketFlags::SIMPLE_VOTE_TX);
                    }
                }
            }
            to_packet
        }

        let mut stats = ReceivingStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_parsing_and_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity: 0,
            num_buffered: 0,
            num_dropped_on_blacklisted_account: 0,
            receive_time_us: 0,
            buffer_time_us: 0,
        };

        let (pre_validated, preverify_stats) = Self::prevalidate_batches(&atomic_txn_batches);
        stats.accumulate(preverify_stats);

        let mut packet_batches: Vec<solana_perf::packet::PacketBatch> = Vec::new();
        let mut packet_count = 0;
        pre_validated.iter().flatten().for_each(|result| {
            let solana_packet_batch: Vec<solana_packet::Packet> = result.0
                .packets
                .iter()
                .map(proto_packet_to_packet)
                .collect();
            packet_count += solana_packet_batch.len();
            packet_batches.push(solana_perf::packet::PinnedPacketBatch::new(solana_packet_batch).into());
        });

        let mut verify_packet_batch_time_us = Measure::start("verify_packet_batch_time_us");
        ed25519_verify_cpu(&mut packet_batches, false, packet_count);
        verify_packet_batch_time_us.stop();

        sigverify_stats.increment_verify_batches_pp_us(verify_packet_batch_time_us.as_us(), packet_count);
        sigverify_stats.increment_batch_packets_len(packet_count);
        sigverify_stats.increment_total_verify_time(verify_packet_batch_time_us.as_us());
        sigverify_stats.increment_total_packets_verified(packet_count);
        sigverify_stats.increment_total_batches_verified(packet_batches.len());

        let mut packet_batch_iter = packet_batches.iter();
        let results = pre_validated
            .into_iter()
            .map(|pre_result| {
                pre_result.and_then(|(_, revert_on_error, seq_id)| {
                    let batch = packet_batch_iter.next().unwrap();
                    
                    let mut pkt_to_idp = |packet: &solana_perf::packet::PacketRef, i: usize, seq_id: u32| -> Result<ImmutableDeserializedPacket, (Reason, u32)> {
                        if packet.meta().discard() {
                            let reason = convert_deserialize_error_to_proto(&DeserializedPacketError::SanitizeError(SanitizeError::InvalidValue));
                            stats.num_dropped_on_parsing_and_sanitization += 1;
                            return Err((Reason::DeserializationError(
                                jito_protos::proto::bam_types::DeserializationError {
                                    index: i as u32,
                                    reason: reason as i32,
                                },
                            ), seq_id));
                        }
                        
                        match ImmutableDeserializedPacket::new((&packet.to_bytes_packet()).into()) {
                            Ok(deserialized) => Ok(deserialized),
                            Err(_) => {
                                stats.num_dropped_on_parsing_and_sanitization += 1;
                                Err((Reason::DeserializationError(
                                    jito_protos::proto::bam_types::DeserializationError {
                                        index: i as u32,
                                        reason: DeserializationErrorReason::SanitizeError as i32,
                                    },
                                ), seq_id))
                            }
                        }
                    };

                    let deserialized = batch
                        .iter()
                        .enumerate()
                        .map(|(i, pkt)| pkt_to_idp(&pkt, i, seq_id))
                        .collect::<Result<Vec<_>, _>>()?;
                    
                    Ok((deserialized, revert_on_error, seq_id))
                })
            })
            .collect();

        (results, stats)
    }
}

struct ParsedBatch {
    pub txns_max_age: Vec<(RuntimeTransaction<SanitizedTransaction>, MaxAge)>,
    pub cost: u64,
    priority: u64,
    pub revert_on_error: bool,
}

impl ReceiveAndBuffer for BamReceiveAndBuffer {
    type Transaction = RuntimeTransaction<SanitizedTransaction>;
    type Container = TransactionStateContainer<Self::Transaction>;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError> {
        let is_bam_enabled = self.bam_enabled.load(Ordering::Relaxed);

        let mut stats = ReceivingStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_parsing_and_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity: 0,
            num_buffered: 0,
            num_dropped_on_blacklisted_account: 0,
            receive_time_us: 0,
            buffer_time_us: 0,
        };

        const PACKET_BURST_LIMIT: usize = 1_000;
        const TIMEOUT: Duration = Duration::from_millis(10);
        const REPORT_INTERVAL: u128 = 2_000; // milliseconds, report every 2 seconds

        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => loop {
                let (batches, receive_time_us) = measure_us!(self.batch_receive_until(
                    TIMEOUT,
                    PACKET_BURST_LIMIT
                ));
                stats.receive_time_us += receive_time_us;

                let batches = match batches {
                    Ok((_, batches)) => {
                        stats.num_received += batches.len();
                        batches
                    },
                    Err(RecvTimeoutError::Disconnected) => return Err(DisconnectedError),
                    Err(RecvTimeoutError::Timeout) => {
                        // No more work to do
                        break;
                    }
                };

                // If BAM is not enabled, drain the channel
                if !is_bam_enabled {
                    stats.num_dropped_without_parsing += stats.num_received;
                    continue;
                }

                let (deserialized_batches_results, deserialize_stats) =
                    Self::batch_deserialize_and_verify(batches, &mut self.stats);
                stats.accumulate(deserialize_stats);

                for result in deserialized_batches_results {
                    match result {
                        Ok((deserialized_batch, revert_on_error, seq_id)) => {
                            let (parse_result, parse_stats) = Self::parse_deserialized_batch(
                                deserialized_batch,
                                seq_id,
                                revert_on_error,
                                &self.bank_forks,
                                &self.blacklisted_accounts,
                            );
                            stats.accumulate(parse_stats);

                            let ParsedBatch {
                                txns_max_age,
                                cost,
                                priority,
                                revert_on_error,
                            } = match parse_result {
                                Ok(parsed) => parsed,
                                Err(reason) => {
                                    self.send_bundle_not_committed_result(seq_id, reason);
                                    continue;
                                }
                            };

                            if container
                                .insert_new_batch(
                                    txns_max_age,
                                    priority,
                                    cost,
                                    revert_on_error,
                                    u64::MAX, // max_schedule_slot is not used in BAM mode
                                )
                                .is_none()
                            {
                                stats.num_dropped_on_capacity += 1;
                                self.send_container_full_txn_batch_result(seq_id);
                                continue;
                            };
                        }
                        Err((reason, seq_id)) => {
                            self.send_bundle_not_committed_result(seq_id, reason);
                            continue;
                    }
                }
            }
            },
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Forward => {
                // Send back any batches that were received while in Forward/Hold state
                let deadline = Instant::now() + Duration::from_millis(100);
                loop {
                    let (batch, receive_time_us) =
                        measure_us!(self.bundle_receiver.recv_deadline(deadline));
                    stats.receive_time_us += receive_time_us;

                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(RecvTimeoutError::Disconnected) => return Err(DisconnectedError),
                        Err(RecvTimeoutError::Timeout) => {
                            break;
                        }
                    };
                    self.send_no_leader_slot_txn_batch_result(batch.seq_id);
                    stats.num_dropped_without_parsing += 1;
                }
            }
        }

        if self.last_report.elapsed().as_millis() > REPORT_INTERVAL {
            self.stats.maybe_report();
            self.last_report = Instant::now();
            // reset stats after reporting
            self.stats = SigverifyStats::default();
        }

        Ok(stats)
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
        BamReceiveAndBuffer,
        TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>>,
        crossbeam_channel::Receiver<BamOutboundMessage>,
    ) {
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
        (receive_and_buffer, container, response_receiver)
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
        )
            -> (R, R::Container, Receiver<BamOutboundMessage>),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container, _response_sender) =
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
            max_schedule_slot: 0,
        };
        sender.send(bundle).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        verify_container(&mut container, 1);
    }

    #[test]
    fn test_receive_and_buffer_invalid_packet() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (sender, receiver) = unbounded();
        let (mut receive_and_buffer, mut container, response_receiver) =
            setup_bam_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());

        let bundle = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![],
                meta: None,
            }],
            max_schedule_slot: 0,
        };
        sender.send(bundle).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        verify_container(&mut container, 0);
        let response = response_receiver.recv().unwrap();
        assert!(matches!(
            response,
            BamOutboundMessage::AtomicTxnBatchResult(txn_batch_result) if txn_batch_result.seq_id == 1 &&
            matches!(&txn_batch_result.result, Some(atomic_txn_batch_result::Result::NotCommitted(not_committed)) if
                matches!(not_committed.reason, Some(Reason::DeserializationError(_))))
        ));
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
            max_schedule_slot: 0,
        };
        
        let mut stats = SigverifyStats::default();
        let (results, _batch_stats) = BamReceiveAndBuffer::batch_deserialize_and_verify(vec![bundle], &mut stats);
        
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        if let Ok((deserialized_packets, _, seq_id)) = &results[0] {
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
            max_schedule_slot: 0,
        };
        
        let mut stats = SigverifyStats::default();
        let (results, batch_stats) = BamReceiveAndBuffer::batch_deserialize_and_verify(vec![batch], &mut stats);
        
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
            max_schedule_slot: 0,
        };
        
        let mut stats = SigverifyStats::default();
        let (results, _batch_stats) = BamReceiveAndBuffer::batch_deserialize_and_verify(vec![batch], &mut stats);
        
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
            max_schedule_slot: 0,
        };
        
        let mut stats = SigverifyStats::default();
        let (results, _batch_stats) = BamReceiveAndBuffer::batch_deserialize_and_verify(vec![batch], &mut stats);
        
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        
        if let Ok((deserialized_packets, revert_on_error, seq_id)) = &results[0] {
            let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                &bank_forks,
                &HashSet::new(),
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
            max_schedule_slot: 0,
        };
        
        let mut stats = SigverifyStats::default();
        let (results, batch_stats) = BamReceiveAndBuffer::batch_deserialize_and_verify(vec![bundle], &mut stats);
        
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
            max_schedule_slot: 0,
        };
        
        let mut stats = SigverifyStats::default();
        let (results, _batch_stats) = BamReceiveAndBuffer::batch_deserialize_and_verify(vec![batch], &mut stats);
        
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        
        if let Ok((deserialized_packets, revert_on_error, seq_id)) = &results[0] {
            let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                &bank_forks,
                &blacklisted_accounts,
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
            max_schedule_slot: 0,
        };
        
        let mut stats = SigverifyStats::default();
        let (results, _batch_stats) = BamReceiveAndBuffer::batch_deserialize_and_verify(vec![batch], &mut stats);
        
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        
        if let Ok((deserialized_packets, revert_on_error, seq_id)) = &results[0] {
            let (result, stats) = BamReceiveAndBuffer::parse_deserialized_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                &bank_forks,
                &HashSet::new(),
            );
            
            assert!(result.is_err());
            assert_eq!(stats.num_dropped_on_parsing_and_sanitization, 1);
            assert!(matches!(result.err().unwrap(), Reason::DeserializationError(_)));
        }
    }
}