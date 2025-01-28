use std::{collections::{HashSet, VecDeque}, sync::{atomic::AtomicBool, Arc, RwLock}};

use ahash::HashMap;
use chrono::Duration;
use itertools::Itertools;
use jito_protos::proto::jss_types::{MicroBlock, Packet};
use prio_graph::{AccessKind, TopLevelId};
use rand::seq::IteratorRandom;
use solana_bundle::bundle_execution::load_and_execute_bundle;
use solana_entry::poh;
use solana_ledger::{blockstore_processor::TransactionStatusSender, token_balances::collect_token_balances};
use solana_measure::measure_us;
use solana_poh::poh_recorder::{PohRecorder, RecordTransactionsSummary, TransactionRecorder};
use solana_runtime::{
    bank::{Bank, ExecutedTransactionCounts}, prioritization_fee_cache::PrioritizationFeeCache,
    vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{
    bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle},
    clock::MAX_PROCESSING_AGE,
    packet::PacketFlags,
    pubkey::Pubkey,
    transaction::SanitizedTransaction,
};
use solana_svm::transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig};
use solana_transaction_status::PreBalanceInfo;

use crate::{
    banking_stage::{
        committer::Committer, immutable_deserialized_packet::ImmutableDeserializedPacket, leader_slot_timing_metrics::LeaderExecuteAndCommitTimings
    },
    bundle_stage::{self, MAX_BUNDLE_RETRY_DURATION},
};

pub struct JssExecutor2 {
    microblock_sender: crossbeam_channel::Sender<MicroBlock>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

const WORKER_THREAD_COUNT: usize = 1;

impl JssExecutor2 {
    pub fn new(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let bundle_committer = bundle_stage::committer::Committer::new(
            transaction_status_sender.clone(),
            replay_vote_sender.clone(),
            prioritization_fee_cache.clone(),
        );
        let transaction_commiter = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
        );

        let (microblock_sender, microblock_receiver) = crossbeam_channel::bounded(50);

        let mut threads = Vec::new();

        let mut workers = Vec::new();
        for _ in 0..WORKER_THREAD_COUNT {
            let (sender, receiver) = crossbeam_channel::bounded(1);
            workers.push(Worker::new(sender));
            let poh_recorder = poh_recorder.clone();
            let bundle_committer = bundle_committer.clone();
            let transaction_commiter = transaction_commiter.clone();
            let exit = exit.clone();
            threads.push(std::thread::spawn(move || {
                Self::spawn_worker(poh_recorder, bundle_committer.clone(), transaction_commiter.clone(), receiver, exit);
            }));
        }

        threads.push(std::thread::spawn(|| {
            Self::spawn_management_thread(microblock_receiver, poh_recorder, workers, exit);
        }));

        Self {
            microblock_sender,
            threads,
        }
    }

    fn parse_transactions<'a>(
        bank: &Bank,
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Vec<SanitizedTransaction> {
        let txns = packets
            .map(|packet| {
                let mut solana_packet = solana_sdk::packet::Packet::default();
                solana_packet.meta_mut().size = packet.data.len() as usize;
                solana_packet.meta_mut().set_discard(false);
                solana_packet.buffer_mut()[0..packet.data.len()].copy_from_slice(&packet.data);
                if let Some(meta) = &packet.meta {
                    solana_packet.meta_mut().size = meta.size as usize;
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
                        if flags.tracer_packet {
                            solana_packet
                                .meta_mut()
                                .flags
                                .insert(PacketFlags::TRACER_PACKET);
                        }
                        if flags.repair {
                            solana_packet.meta_mut().flags.insert(PacketFlags::REPAIR);
                        }
                    }
                }
                let packet = ImmutableDeserializedPacket::new(solana_packet).ok()?;
                let sanitized_transaction = packet.build_sanitized_transaction(
                    false,
                    bank,
                    bank.get_reserved_account_keys(),
                )?;
                Some(sanitized_transaction)
            })
            .collect_vec();
        if txns.iter().any(Option::is_none) {
            return vec![];
        }
        txns.into_iter().map(|x| x.unwrap()).collect_vec()
    }

    pub fn execute_and_commit_and_record_micro_block(&mut self, micro_block: MicroBlock) -> bool {
        self.microblock_sender.try_send(micro_block).is_ok()
    }

    fn spawn_management_thread(
        microblock_receiver: crossbeam_channel::Receiver<MicroBlock>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut workers: Vec<Worker>,
        exit: Arc<AtomicBool>,
    ) {
        let mut bundles_scheduled = 0;
        let mut last_metrics = std::time::Instant::now();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
                continue;
            };
            if !bank_start.should_working_bank_still_be_processing_txs() {
                continue;
            }

            let mut bundles = HashMap::default();
            let mut prio_graph = prio_graph::PrioGraph::new(|id: &BundleId, _graph_node| *id);
            let mut next_bundle_id: u64 = 0;

            let mut schedule_next = |prio_graph: &mut prio_graph::PrioGraph<_, _, _, _>, bundles: &mut HashMap<_, _>, bundles_scheduled: &mut u64| {
                for worker in workers.iter_mut().filter(|w| !w.is_full()) {
                    for bundle_id in worker.get_unblocked() {
                        prio_graph.unblock(&bundle_id);
                    }

                    let (bundle_id, txns) = match prio_graph.pop() {
                        Some(bundle) => (bundle, bundles.remove(&bundle).unwrap()),
                        None => {
                            return;
                        }
                    };

                    if worker.send(bundle_id, txns) {
                        *bundles_scheduled += 1;
                    }
                }
            };

            while bank_start.should_working_bank_still_be_processing_txs() {
                if let Ok(micro_block) = microblock_receiver.try_recv() {
                    let start = std::time::Instant::now();
                    let len = micro_block.bundles.len();
                    for bundle in micro_block.bundles {
                        let transactions = Self::parse_transactions(
                            &bank_start.working_bank,
                            bundle.packets.iter(),
                        );
                        let id = next_bundle_id;
                        let bundle_id = BundleId { id };
                        next_bundle_id += 1;
                        prio_graph.insert_transaction(
                            bundle_id,
                            transactions
                                .iter()
                                .map(|tx| Self::get_transaction_account_access(tx))
                                .flatten(),
                        );
                        bundles.insert(bundle_id, transactions);
                        schedule_next(&mut prio_graph, &mut bundles, &mut bundles_scheduled);
                    }
                    info!(
                        "Received micro block with {} bundles; ingestion_time={}",
                        len,
                        start.elapsed().as_millis()
                    );
                }

                if last_metrics.elapsed().as_secs() > 1 {
                    info!(
                        "mempool_size={} scheduled={}",
                        bundles.len(),
                        bundles_scheduled
                    );
                    bundles_scheduled = 0;
                    last_metrics = std::time::Instant::now();
                }

                schedule_next(&mut prio_graph, &mut bundles, &mut bundles_scheduled);
            }

            for worker in workers.iter_mut() {
                worker.clear();
            }

            info!("unscheduled={}", bundles.len());
        }
    }

    fn spawn_worker(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut bundle_committer: bundle_stage::committer::Committer,
        mut transaction_commiter: Committer,
        receiver: crossbeam_channel::Receiver<(BundleId, Vec<SanitizedTransaction>)>,
        exit: Arc<AtomicBool>,
    ) {
        let recorder = poh_recorder.read().unwrap().new_recorder();
        let mut executing_time_us = 0;
        let mut overall_start = std::time::Instant::now();
        let mut bank_start = None;
        let mut empty_polls = 0;
        let mut got_first_bundle = false;
        let mut good_bank = false;
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if bank_start.is_none() {
                bank_start = poh_recorder.read().unwrap().bank_start();
                if bank_start.is_some() {
                    overall_start = std::time::Instant::now();
                    executing_time_us = 0;
                }
            }
            let Some(current_bank_start) = bank_start.as_ref() else {
                continue;
            };
            if good_bank && !current_bank_start.should_working_bank_still_be_processing_txs() {
                good_bank = false;
                bank_start = None;
                info!(
                    "worker_time_us={} executing_time_us={} executing_percent={} empty_polls={}",
                    overall_start.elapsed().as_micros(),
                    executing_time_us,
                    executing_time_us as f64 / overall_start.elapsed().as_micros() as f64,
                    empty_polls
                );
                empty_polls = 0;
                got_first_bundle = false;
                continue;
            }
            let Ok((_, txns)) = receiver.try_recv() else {
                empty_polls += if bank_start.is_some() { 1 } else { 0 };
                continue;
            };
            good_bank = true;
            if !got_first_bundle {
                got_first_bundle = true;
                empty_polls = 0;
            }
            let start = std::time::Instant::now();
            if txns.len() == 1 {
                Self::execute_commit_record_transaction(
                    &current_bank_start.working_bank,
                    &recorder,
                    &mut transaction_commiter,
                    txns,
                );
            } else {
                Self::execute_commit_record_bundle(
                    &current_bank_start.working_bank,
                    &recorder,
                    &mut bundle_committer,
                    txns,
                );
            }
            executing_time_us += start.elapsed().as_micros();
        }
    }

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_transaction_account_access(
        transaction: &SanitizedTransaction,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + '_ {
        let message = transaction.message();
        message
            .account_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| {
                if message.is_writable(index) {
                    (*key, AccessKind::Write)
                } else {
                    (*key, AccessKind::Read)
                }
            })
    }

    pub fn execute_commit_record_bundle(
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        committer: &mut bundle_stage::committer::Committer,
        txns: Vec<SanitizedTransaction>,
    ) -> bool {
        let len = txns.len();
        let bundle_id = derive_bundle_id_from_sanitized_transactions(&txns);
        let sanitized_bundle = SanitizedBundle {
            transactions: txns,
            bundle_id: bundle_id.clone(),
        };

        let default_accounts = vec![None; len];
        let transaction_status_sender_enabled = committer.transaction_status_sender_enabled();
        let mut bundle_execution_results = load_and_execute_bundle(
            &bank,
            &sanitized_bundle,
            MAX_PROCESSING_AGE,
            &MAX_BUNDLE_RETRY_DURATION,
            transaction_status_sender_enabled,
            &None,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        if let Err(err) = bundle_execution_results.result() {
            error!("Error executing bundle {}: {:?}", bundle_id, err);
            return false;
        }

        let (executed_batches, _execution_results_to_transactions_us) =
            measure_us!(bundle_execution_results.executed_transaction_batches());

        let _freeze_lock = bank.freeze_lock();
        let (last_blockhash, lamports_per_signature) =
            bank.last_blockhash_and_lamports_per_signature();
        let RecordTransactionsSummary {
            result: record_transactions_result,
            record_transactions_timings: _,
            starting_transaction_index,
        } = recorder.record_transactions(bank.slot(), executed_batches);
        if record_transactions_result.is_err() {
            return false;
        }

        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        committer.commit_bundle(
            &mut bundle_execution_results,
            last_blockhash,
            lamports_per_signature,
            starting_transaction_index,
            &bank,
            &mut execute_and_commit_timings,
        );

        true
    }

    pub fn execute_commit_record_transaction(
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        committer: &mut Committer,
        transactions: Vec<SanitizedTransaction>,
    ) -> bool {
        assert_eq!(transactions.len(), 1);
        let batch = bank.prepare_sanitized_batch_with_results(
            &transactions,
            std::iter::once(None).map(|_: Option<()>| Ok(())),
            None,
            None,
        );

        let mut pre_balance_info = PreBalanceInfo::default();
        let transaction_status_sender_enabled = committer.transaction_status_sender_enabled();
        let (_, collect_balances_us) = measure_us!({
            // If the extra meta-data services are enabled for RPC, collect the
            // pre-balances for native and token programs.
            if transaction_status_sender_enabled {
                pre_balance_info.native = bank.collect_balances(&batch);
                pre_balance_info.token =
                    collect_token_balances(bank, &batch, &mut pre_balance_info.mint_decimals, None)
            }
        });

        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let mut results = bank.load_and_execute_transactions(
            &batch,
            MAX_PROCESSING_AGE,
            &mut execute_and_commit_timings.execute_timings,
            TransactionProcessingConfig {
                account_overrides: None,
                check_program_modification_slot: bank.check_program_modification_slot(),
                compute_budget: bank.compute_budget(),
                log_messages_bytes_limit: None,
                limit_to_load_programs: true,
                recording_config: ExecutionRecordingConfig::new_single_setting(
                    false
                ),
                transaction_account_lock_limit: Some(bank.get_transaction_account_lock_limit()),
            },
        );
        if results.executed_transactions_count == 0 {
            return false;
        }

        let _freeze_lock = bank.freeze_lock();
        let (last_blockhash, lamports_per_signature) = bank.last_blockhash_and_lamports_per_signature();


        let (executed_transactions, execution_results_to_transactions_us) =
            measure_us!(results.execution_results
                .iter()
                .zip(batch.sanitized_transactions())
                .filter_map(|(execution_result, tx)| {
                    if execution_result.was_executed() {
                        Some(tx.to_versioned_transaction())
                    } else {
                        None
                    }
                })
                .collect_vec());
        let RecordTransactionsSummary {
            result: record_transactions_result,
            record_transactions_timings: _,
            starting_transaction_index,
        } = recorder.record_transactions(bank.slot(), vec![executed_transactions]);
        if record_transactions_result.is_err() {
            return false;
        }

        committer.commit_transactions(
            &batch,
            &mut results.loaded_transactions,
            results.execution_results,
            last_blockhash,
            lamports_per_signature,
            starting_transaction_index,
            bank,
            &mut pre_balance_info,
            &mut execute_and_commit_timings,
            results.signature_count,
            results.executed_transactions_count,
            results.executed_non_vote_transactions_count,
            results.executed_with_successful_result_count);

        true
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
struct BundleId {
    id: u64,
}

impl TopLevelId<Self> for BundleId {
    fn id(&self) -> Self {
        *self
    }
}

struct Worker {
    sender: crossbeam_channel::Sender<(BundleId, Vec<SanitizedTransaction>)>,
    queue: VecDeque<BundleId>,
}

impl Worker {
    fn new(sender: crossbeam_channel::Sender<(BundleId, Vec<SanitizedTransaction>)>) -> Self {
        Self {
            sender,
            queue: VecDeque::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    fn send(&mut self, bundle_id: BundleId, txns: Vec<SanitizedTransaction>) -> bool {
        if self.sender.try_send((bundle_id, txns)).is_ok() {
            self.queue.push_back(bundle_id);
            true
        } else {
            false
        }
    }

    fn get_unblocked(&mut self) -> Vec<BundleId> {
        let mut unblocked = Vec::new();
        let diff = self.queue.len().saturating_sub(self.sender.len());
        for _ in 0..diff {
            let Some(blocking) = self.queue.pop_front() else {
                break;
            };
            unblocked.push(blocking);
        }

        unblocked
    }

    fn clear(&mut self) {
        self.queue.clear();
    }
}