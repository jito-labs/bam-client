//! An implementation of the `ReceiveAndBuffer` trait that receives messages from BAM
//! and buffers from into the the `TransactionStateContainer`. Key thing to note:
//! this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
//! to BAM with a `Retryable` result.
use crate::banking_stage::scheduler_messages::MaxAge;
use crate::banking_stage::transaction_scheduler::receive_and_buffer::DisconnectedError;
use crossbeam_channel::{RecvTimeoutError, TryRecvError};
use solana_clock::MAX_PROCESSING_AGE;
use solana_measure::measure_us;
use solana_packet::{PacketFlags, PACKET_DATA_SIZE};
use solana_pubkey::Pubkey;
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
use crate::bam_dependencies::BamOutboundMessage;

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
    jito_protos::proto::{
        bam_types::{
            atomic_txn_batch_result, not_committed::Reason, AtomicTxnBatch,
            DeserializationErrorReason, Packet, SchedulingError,
        },
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

    last_metrics_report: Instant,
    metrics: BamReceiveAndBufferMetrics,
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
            last_metrics_report: Instant::now(),
            metrics: BamReceiveAndBufferMetrics::default(),
        }
    }

    fn deserialize_packets<'a>(
        in_packets: impl Iterator<Item = &'a Packet>,
        metrics: &mut BamReceiveAndBufferMetrics,
    ) -> Result<Vec<ImmutableDeserializedPacket>, (usize, DeserializedPacketError)> {
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

        let mut result = Vec::with_capacity(in_packets.size_hint().0);
        for (i, p) in in_packets.enumerate() {
            let solana_packet = proto_packet_to_packet(p);
            // sigverify packet
            // we don't use solana_packet here, so we don't need to call set_discard()
            // if !verify_packet(&mut (&mut solana_packet).into(), false) {
            //     return Err((
            //         i,
            //         DeserializedPacketError::SanitizeError(SanitizeError::InvalidValue),
            //     ));
            // }
            
            let (packet_result, duration_us) = measure_us!(
                ImmutableDeserializedPacket::new((&solana_packet).into()).map_err(|e| (i, e)));
            metrics.increment_deserialization_us(duration_us);
            result.push(packet_result?);
        }

        Ok(result)
    }

    fn send_bundle_not_committed_result(&self, seq_id: u32, reason: Reason) {
        let _ = self.response_sender.try_send(BamOutboundMessage::AtomicTxnBatchResult(
            jito_protos::proto::bam_types::AtomicTxnBatchResult {
                seq_id,
                result: Some(atomic_txn_batch_result::Result::NotCommitted(
                    jito_protos::proto::bam_types::NotCommitted {
                        reason: Some(reason),
                    },
                )),
            }));
    }

    fn send_no_leader_slot_txn_batch_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(BamOutboundMessage::AtomicTxnBatchResult(
            jito_protos::proto::bam_types::AtomicTxnBatchResult {
                seq_id,
                result: Some(atomic_txn_batch_result::Result::NotCommitted(
                    jito_protos::proto::bam_types::NotCommitted {
                        reason: Some(Reason::SchedulingError(
                            SchedulingError::OutsideLeaderSlot as i32,
                        )),
                    },
                )),
            }));
    }

    fn send_container_full_txn_batch_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(BamOutboundMessage::AtomicTxnBatchResult(
            jito_protos::proto::bam_types::AtomicTxnBatchResult {
                seq_id,
                result: Some(atomic_txn_batch_result::Result::NotCommitted(
                    jito_protos::proto::bam_types::NotCommitted {
                        reason: Some(Reason::SchedulingError(
                            SchedulingError::ContainerFull as i32,
                        )),
                    },
                )),
            }));
    }

    fn parse_batch(
        batch: &AtomicTxnBatch,
        bank_forks: &Arc<RwLock<BankForks>>,
        blacklisted_accounts: &HashSet<Pubkey>,
        metrics: &mut BamReceiveAndBufferMetrics,
    ) -> Result<ParsedBatch, Reason> {
        if batch.packets.is_empty() {
            return Err(Reason::DeserializationError(
                jito_protos::proto::bam_types::DeserializationError {
                    index: 0,
                    reason: DeserializationErrorReason::Empty as i32,
                },
            ));
        }

        if batch.packets.len() > 5 {
            return Err(Reason::DeserializationError(
                jito_protos::proto::bam_types::DeserializationError {
                    index: 0,
                    reason: DeserializationErrorReason::SanitizeError as i32,
                },
            ));
        }

        let Ok(revert_on_error) = batch
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
                jito_protos::proto::bam_types::DeserializationError {
                    index: 0,
                    reason: DeserializationErrorReason::InconsistentBundle as i32,
                },
            ));
        };

        let mut parsed_packets =
            Self::deserialize_packets(batch.packets.iter(), metrics).map_err(|(index, err)| {
                let reason = convert_deserialize_error_to_proto(&err);
                Reason::DeserializationError(jito_protos::proto::bam_types::DeserializationError {
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
        let mut txns_max_age = vec![];

        // Checks are taken from receive_and_buffer.rs:
        // SanitizedTransactionReceiveAndBuffer::buffer_packets
        for (index, parsed_packet) in parsed_packets.drain(..).enumerate() {
            // Check 0: Reject vote transactions
            if parsed_packet.is_simple_vote() {
                return Err(Reason::DeserializationError(
                    jito_protos::proto::bam_types::DeserializationError {
                        index: index as u32,
                        reason: DeserializationErrorReason::VoteTransactionFailure as i32,
                    },
                ));
            }

            // Check 1: Ensure the transaction is valid
            let (Some((tx, deactivation_slot)), duration_us) = measure_us!(parsed_packet.build_sanitized_transaction(
                vote_only,
                root_bank.as_ref(),
                root_bank.get_reserved_account_keys(),
            )) else {
                return Err(Reason::DeserializationError(
                    jito_protos::proto::bam_types::DeserializationError {
                        index: 0,
                        reason: DeserializationErrorReason::SanitizeError as i32,
                    },
                ));
            };
            metrics.increment_sanitization_us(duration_us);

            // Check 2: Ensure no duplicates and valid number of account locks
            if let Err(err) =
                validate_account_locks(tx.message().account_keys(), transaction_account_lock_limit)
            {
                let reason = convert_txn_error_to_proto(err);
                return Err(Reason::TransactionError(
                    jito_protos::proto::bam_types::TransactionError {
                        index: index as u32,
                        reason: reason as i32,
                    },
                ));
            }
            metrics.increment_lock_validation_us(duration_us);

            // Check 3: Ensure the compute budget limits are valid
            let (result, duration_us) = measure_us!(tx
                .compute_budget_instruction_details()
                .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set));
            metrics.increment_fee_budget_extraction_us(duration_us);
            let fee_budget_limits = match result
            {
                Ok(fee_budget_limits) => fee_budget_limits,
                Err(err) => {
                    let reason = convert_txn_error_to_proto(err);
                    return Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: reason as i32,
                        },
                    ));
                }
            };

            // Check 4: Ensure valid blockhash and blockhash is not too old
            let lock_results: [_; 1] = core::array::from_fn(|_| Ok(()));
            let (check_results, duration_us) = measure_us!(working_bank.check_transactions(
                std::slice::from_ref(&tx),
                &lock_results,
                MAX_PROCESSING_AGE,
                &mut TransactionErrorMetrics::default(),
            ));
            metrics.increment_check_transactions_us(duration_us);
            if let Some(Err(err)) = check_results.first() {
                let reason = convert_txn_error_to_proto(err.clone());
                return Err(Reason::TransactionError(
                    jito_protos::proto::bam_types::TransactionError {
                        index: index as u32,
                        reason: reason as i32,
                    },
                ));
            }

            // Check 5: Ensure the fee payer has enough to pay for the transaction fee
            let (result, duration_us) = measure_us!(Consumer::check_fee_payer_unlocked(
                &working_bank,
                &tx,
                &mut TransactionErrorMetrics::default(),
            ));
            metrics.increment_fee_payer_check_us(duration_us);
            if let Err(err) = result {
                let reason = convert_txn_error_to_proto(err);
                return Err(Reason::TransactionError(
                    jito_protos::proto::bam_types::TransactionError {
                        index: index as u32,
                        reason: reason as i32,
                    },
                ));
            }

            // Check 6: Ensure none of the accounts touch blacklisted accounts
            let (contains_blacklisted_account, duration_us) = measure_us!(tx
                .message()
                .account_keys()
                .iter()
                .any(|key| blacklisted_accounts.contains(key)));
            metrics.increment_blacklist_check_us(duration_us);
            if contains_blacklisted_account {
                return Err(Reason::TransactionError(
                    jito_protos::proto::bam_types::TransactionError {
                        index: index as u32,
                        reason: DeserializationErrorReason::SanitizeError as i32,
                    },
                ));
            }

            let max_age = calculate_max_age(sanitized_epoch, deactivation_slot, alt_resolved_slot);

            let (_, txn_cost) =
                calculate_priority_and_cost(&tx, &fee_budget_limits.into(), &working_bank);
            cost = cost.saturating_add(txn_cost);
            txns_max_age.push((tx, max_age));
            packets.push(Arc::new(parsed_packet));
        }

        let priority = seq_id_to_priority(batch.seq_id);

        Ok(ParsedBatch {
            txns_max_age,
            cost,
            priority,
            revert_on_error,
        })
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
        _: &mut super::scheduler_metrics::SchedulerTimingMetrics,
        _: &mut super::scheduler_metrics::SchedulerCountMetrics,
        decision: &BufferedPacketsDecision,
    ) -> Result<usize, DisconnectedError> {
        let mut result = 0;
        let is_bam_enabled = self.bam_enabled.load(Ordering::Relaxed);
        let start = Instant::now();
        const MAX_RECV_TIME: Duration = Duration::from_millis(5);

        const METRICS_REPORT_INTERVAL: Duration = Duration::from_millis(20);
        if self.last_metrics_report.elapsed() > METRICS_REPORT_INTERVAL {
            self.metrics.report();
            self.last_metrics_report = Instant::now();
        }

        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => loop {
                if start.elapsed() > MAX_RECV_TIME {
                    return Ok(result);
                }

                let batch = match self.bundle_receiver.try_recv() {
                    Ok(batch) => batch,
                    Err(TryRecvError::Disconnected) => return Err(DisconnectedError),
                    Err(TryRecvError::Empty) => {
                        // If the channel is empty, work here is done.
                        return Ok(result);
                    }
                };

                // If BAM is not enabled, drain the channel
                if !is_bam_enabled {
                    continue;
                }

                let (parse_result, duration_us) = measure_us!(Self::parse_batch(&batch, &self.bank_forks, &self.blacklisted_accounts, &mut self.metrics));
                self.metrics.increment_total_us(duration_us);

                let ParsedBatch {
                    txns_max_age,
                    cost,
                    priority,
                    revert_on_error,
                } = match parse_result {
                    Ok(parsed) => parsed,
                    Err(reason) => {
                        self.send_bundle_not_committed_result(batch.seq_id, reason);
                        continue;
                    }
                };
                if container
                    .insert_new_batch(
                        txns_max_age,
                        priority,
                        cost,
                        revert_on_error,
                        batch.max_schedule_slot,
                    )
                    .is_none()
                {
                    self.send_container_full_txn_batch_result(batch.seq_id);
                    continue;
                };

                result = result.saturating_add(1);
                return Ok(result);
            },
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Forward => {
                // Send back any batches that were received while in Forward/Hold state
                let deadline = Instant::now() + Duration::from_millis(100);
                loop {
                    let batch = match self.bundle_receiver.recv_deadline(deadline) {
                        Ok(batch) => batch,
                        Err(RecvTimeoutError::Disconnected) => return Err(DisconnectedError),
                        Err(RecvTimeoutError::Timeout) => {
                            return Ok(0);
                        }
                    };
                    self.send_no_leader_slot_txn_batch_result(batch.seq_id);
                }
            }
        }
    }
}

pub fn seq_id_to_priority(seq_id: u32) -> u64 {
    u64::MAX.saturating_sub(seq_id as u64)
}

pub fn priority_to_seq_id(priority: u64) -> u32 {
    u32::try_from(u64::MAX.saturating_sub(priority)).unwrap_or(u32::MAX)
}

#[derive(Default)]
struct BamReceiveAndBufferMetrics {
    total_us: u64,
    deserialization_us: u64,
    sanitization_us: u64,
    lock_validation_us: u64,
    fee_budget_extraction_us: u64,
    check_transactions_us: u64,
    fee_payer_check_us: u64,
    blacklist_check_us: u64,
}

impl BamReceiveAndBufferMetrics {
    fn report(&mut self) {
        datapoint_info!(
            "bam-receive-and-buffer",
            ("total_us", self.total_us, i64),
            ("deserialization_us", self.deserialization_us, i64),
            ("sanitization_us", self.sanitization_us, i64),
            ("lock_validation_us", self.lock_validation_us, i64),
            ("fee_budget_extraction_us", self.fee_budget_extraction_us, i64),
            ("check_transactions_us", self.check_transactions_us, i64),
            ("fee_payer_check_us", self.fee_payer_check_us, i64),
            ("blacklist_check_us", self.blacklist_check_us, i64),
        );
        *self = Self::default();
    }

    fn increment_total_us(&mut self, us: u64) {
        self.total_us = self.total_us.saturating_add(us);
    }

    fn increment_deserialization_us(&mut self, us: u64) {
        self.deserialization_us = self.deserialization_us.saturating_add(us);
    }

    fn increment_sanitization_us(&mut self, us: u64) {
        self.sanitization_us = self.sanitization_us.saturating_add(us);
    }

    fn increment_lock_validation_us(&mut self, us: u64) {
        self.lock_validation_us = self.lock_validation_us.saturating_add(us);
    }

    fn increment_fee_budget_extraction_us(&mut self, us: u64) {
        self.fee_budget_extraction_us = self.fee_budget_extraction_us.saturating_add(us);
    }

    fn increment_check_transactions_us(&mut self, us: u64) {
        self.check_transactions_us = self.check_transactions_us.saturating_add(us);
    }

    fn increment_fee_payer_check_us(&mut self, us: u64) {
        self.fee_payer_check_us = self.fee_payer_check_us.saturating_add(us);
    }

    fn increment_blacklist_check_us(&mut self, us: u64) {
        self.blacklist_check_us = self.blacklist_check_us.saturating_add(us);
    }
}

#[cfg(test)]
mod tests {
    use solana_signer::Signer;
    use solana_system_transaction::transfer;
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
        let mut timing_metrics = SchedulerTimingMetrics::default();
        let mut count_metrics = SchedulerCountMetrics::default();

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
            setup_bam_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());

        // Create an invalid packet with no data
        let bundle = AtomicTxnBatch {
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
            response,
            BamOutboundMessage::AtomicTxnBatchResult(txn_batch_result) if txn_batch_result.seq_id == 1 &&
            matches!(&txn_batch_result.result, Some(atomic_txn_batch_result::Result::NotCommitted(not_committed)) if
                matches!(not_committed.reason, Some(Reason::DeserializationError(_))))
        ));
    }

    #[test]
    fn test_parse_bundle_success() {
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
        let result = BamReceiveAndBuffer::parse_batch(&bundle, &bank_forks, &HashSet::new());
        assert!(result.is_ok());
        let parsed_bundle = result.unwrap();
        assert_eq!(parsed_bundle.txns_max_age.len(), 1);
    }

    #[test]
    fn test_parse_bundle_empty() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![],
            max_schedule_slot: 0,
        };
        let result = BamReceiveAndBuffer::parse_batch(&batch, &bank_forks, &HashSet::new());
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::DeserializationError(jito_protos::proto::bam_types::DeserializationError {
                index: 0,
                reason: DeserializationErrorReason::Empty as i32,
            })
        );
    }

    #[test]
    fn test_parse_bundle_invalid_packet() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![0; PACKET_DATA_SIZE + 1], // Invalid size
                meta: None,
            }],
            max_schedule_slot: 0,
        };
        let result = BamReceiveAndBuffer::parse_batch(&batch, &bank_forks, &HashSet::new());
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::DeserializationError(jito_protos::proto::bam_types::DeserializationError {
                index: 0,
                reason: DeserializationErrorReason::SanitizeError as i32,
            })
        );
    }

    #[test]
    fn test_parse_bundle_fee_payer_doesnt_exist() {
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
        let result = BamReceiveAndBuffer::parse_batch(&batch, &bank_forks, &HashSet::new());
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::TransactionError(jito_protos::proto::bam_types::TransactionError {
                index: 0,
                reason: jito_protos::proto::bam_types::TransactionErrorReason::AccountNotFound
                    as i32,
            })
        );
    }

    #[test]
    fn test_parse_bundle_inconsistent() {
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
        let result = BamReceiveAndBuffer::parse_batch(&bundle, &bank_forks, &HashSet::new());
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::DeserializationError(jito_protos::proto::bam_types::DeserializationError {
                index: 0,
                reason: DeserializationErrorReason::InconsistentBundle as i32,
            })
        );
    }

    #[test]
    fn test_parse_bundle_blacklisted_account() {
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
        let result = BamReceiveAndBuffer::parse_batch(&batch, &bank_forks, &blacklisted_accounts);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::TransactionError(jito_protos::proto::bam_types::TransactionError {
                index: 0,
                reason: DeserializationErrorReason::SanitizeError as i32,
            })
        );
    }

    #[test]
    fn test_deserialize_packets_invalid_signature() {
        // Create a transaction with invalid signature
        let (_bank_forks, mint_keypair) = test_bank_forks();
        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            solana_hash::Hash::default(),
        );

        // Serialize the transaction
        let mut data = bincode::serialize(&transaction).unwrap();
        // Corrupt the signature by modifying some bytes
        if data.len() > 10 {
            data[5] ^= 0xFF; // Flip bits to corrupt signature
            data[10] ^= 0xFF;
        }

        let packets = [Packet { data, meta: None }];

        // This should fail due to invalid signature
        let result = BamReceiveAndBuffer::deserialize_packets(packets.iter());
        assert!(result.is_err());

        if let Err((index, error)) = result {
            assert_eq!(index, 0);
            assert!(matches!(error, DeserializedPacketError::SanitizeError(_)));
        }
    }

    #[test]
    fn test_deserialize_packets_valid_non_vote_transaction() {
        // Create a regular non-vote transaction
        let (_bank_forks, mint_keypair) = test_bank_forks();
        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            solana_hash::Hash::default(),
        );

        // Sign the transaction properly
        let mut tx = transaction;
        tx.sign(&[&mint_keypair], solana_hash::Hash::default());

        let data = bincode::serialize(&tx).unwrap();

        // Create packet without vote transaction flag (non-vote)
        let packet = Packet {
            data: data.clone(),
            meta: Some(jito_protos::proto::bam_types::Meta {
                flags: Some(jito_protos::proto::bam_types::PacketFlags {
                    simple_vote_tx: false, // This is a non-vote transaction
                    ..Default::default()
                }),
                size: data.len() as u64,
            }),
        };

        // Valid transactions should succeed with valid signature
        let result = BamReceiveAndBuffer::deserialize_packets([packet].iter());
        assert!(result.is_ok());

        if let Ok(packets) = result {
            assert_eq!(packets.len(), 1);
        }
    }

    #[test]
    fn test_parse_bundle_rejects_vote_transactions() {
        let (bank_forks, _mint_keypair) = test_bank_forks();

        // Create a proper vote transaction
        let vote_keypair = Keypair::new();
        let node_keypair = Keypair::new();
        let authorized_voter = Keypair::new();
        let recent_blockhash = bank_forks.read().unwrap().root_bank().last_blockhash();

        // Create a vote transaction
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

        // Serialize the transaction
        let vote_data = bincode::serialize(&VersionedTransaction::from(vote_tx)).unwrap();

        // Create a packet with the vote transaction
        let meta = jito_protos::proto::bam_types::Meta {
            flags: Some(jito_protos::proto::bam_types::PacketFlags {
                simple_vote_tx: true, // this triggers parsed_packet.is_simple_vote()
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

        // Test that parse_batch rejects vote transactions
        let result = BamReceiveAndBuffer::parse_batch(&batch, &bank_forks, &HashSet::default());
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            Reason::DeserializationError(jito_protos::proto::bam_types::DeserializationError {
                index: 0,
                reason: DeserializationErrorReason::VoteTransactionFailure as i32,
            })
        );
    }
}
