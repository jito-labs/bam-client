// Executor design:
// - Micro-blocks are turned into serialized transactions and inserted into Prio-Graph by scheduling thread.
// - Each worker thread has a channel of size 1 for new bundles to execute.
// - The scheduling thread continually scans the worker channels to see if one is empty; as soon as it is;
//   it unblocks the transactions blocked by what was in the channel before; and pops the next one off the prio graph;
//   sending it to the channel. Transaction execution far outweighs the time for the scheduling thread to re-check the channel.
// - When the working bank is no longer valid; the Prio-graph and mempool are drained. Thinking about a feedback loop for JSS to know it scheduled too much.

use std::{
    collections::VecDeque,
    sync::{atomic::AtomicBool, Arc, RwLock},
    thread,
    time::Duration,
};

use ahash::HashMap;
use itertools::Itertools;
use jito_protos::proto::jss_types::{MicroBlock, Packet};
use prio_graph::{AccessKind, GraphNode, TopLevelId};
use solana_bundle::bundle_execution::load_and_execute_bundle;
use solana_ledger::{
    blockstore_processor::TransactionStatusSender, token_balances::collect_token_balances,
};
use solana_measure::measure_us;
use solana_poh::poh_recorder::{PohRecorder, RecordTransactionsSummary, TransactionRecorder};
use solana_runtime::{
    bank::Bank, prioritization_fee_cache::PrioritizationFeeCache,
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
        self, immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
    },
    bundle_stage::{self, MAX_BUNDLE_RETRY_DURATION},
};

pub struct JssExecutor2 {
    microblock_sender: crossbeam_channel::Sender<Vec<Vec<SanitizedTransaction>>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl JssExecutor2 {
    pub fn new(
        worker_thread_count: usize,
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
        let transaction_commiter = banking_stage::committer::Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
        );

        let (microblock_sender, microblock_receiver) = crossbeam_channel::bounded(50);

        let (mut threads, workers) = Self::spawn_workers(
            worker_thread_count,
            poh_recorder.clone(),
            bundle_committer.clone(),
            transaction_commiter.clone(),
            exit.clone(),
        );

        threads.push(
            std::thread::Builder::new()
                .name("jss_executor_manager".to_string())
                .spawn(|| {
                    Self::spawn_management_thread(microblock_receiver, poh_recorder, workers, exit);
                })
                .unwrap(),
        );

        Self {
            microblock_sender,
            threads,
        }
    }

    pub fn join(self) {
        for thread in self.threads.into_iter() {
            thread.join().unwrap();
        }
    }

    // Serialize transactions from micro-block and send them to the scheduling thread
    pub fn schedule_microblock(&mut self, bank: &Bank, micro_block: MicroBlock) -> bool {
        let bundles = micro_block
            .bundles
            .iter()
            .map(|bundle| Self::parse_transactions(bank, bundle.packets.iter()))
            .filter(|txns| !txns.is_empty())
            .collect_vec();
        self.microblock_sender.try_send(bundles).is_ok()
    }

    /// Loop responsible for scheduling transactions to workers
    fn spawn_management_thread(
        microblock_receiver: crossbeam_channel::Receiver<Vec<Vec<SanitizedTransaction>>>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut workers: Vec<Worker>,
        exit: Arc<AtomicBool>,
    ) {
        let mut bundles_scheduled = 0;
        let mut bundles = Vec::new();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
                continue;
            };
            if !bank_start.should_working_bank_still_be_processing_txs() {
                continue;
            }

            let mut prio_graph =
                prio_graph::PrioGraph::new(|id: &BundleExecutionId, _graph_node| *id);
            let mut microblock_count = 0;

            while bank_start.should_working_bank_still_be_processing_txs() {
                Self::maybe_ingest_new_microblock(
                    &microblock_receiver,
                    &mut prio_graph,
                    &mut microblock_count,
                    &mut bundles,
                    &mut bundles_scheduled,
                    &mut workers,
                );
                Self::schedule_next_batch(
                    &mut prio_graph,
                    &mut bundles,
                    &mut bundles_scheduled,
                    &mut workers,
                );
            }

            for worker in workers.iter_mut() {
                worker.clear();
            }

            info!(
                "slot={} microblock_count={} scheduled={} unscheduled={}",
                bank_start.working_bank.slot(),
                microblock_count,
                bundles_scheduled,
                bundles.len()
            );
            bundles_scheduled = 0;
            bundles.clear();
        }
    }

    /// Ingests new micro-blocks and inserts them into the prio-graph.
    /// If a worker is available between incoming transactions, it schedules the next batch
    /// so that no workers has to wait.
    fn maybe_ingest_new_microblock<
        C: std::hash::Hash + Eq + Clone + TopLevelId<BundleExecutionId> + Copy,
        D: Fn(&BundleExecutionId, &GraphNode<BundleExecutionId>) -> C,
    >(
        microblock_receiver: &crossbeam_channel::Receiver<Vec<Vec<SanitizedTransaction>>>,
        prio_graph: &mut prio_graph::PrioGraph<BundleExecutionId, Pubkey, C, D>,
        microblock_count: &mut u64,
        bundles: &mut Vec<Option<Vec<SanitizedTransaction>>>,
        bundles_scheduled: &mut u64,
        workers: &mut Vec<Worker>,
    ) {
        if let Ok(micro_block) = microblock_receiver.try_recv() {
            *microblock_count += 1;
            for transactions in micro_block {
                let id = bundles.len();
                let bundle_id = BundleExecutionId { id };
                prio_graph.insert_transaction(
                    bundle_id,
                    Self::get_bundle_account_access(transactions.as_slice()),
                );
                bundles.push(Some(transactions));
                Self::schedule_next_batch(prio_graph, bundles, bundles_scheduled, workers);
            }
        }
    }

    /// Schedules the next batch of transactions to workers, by checking if any worker is available.
    /// If a worker is available, it pops the next bundle from the prio-graph and sends it to the worker.
    fn schedule_next_batch<
        C: std::hash::Hash + Eq + Clone + TopLevelId<BundleExecutionId> + Copy,
        D: Fn(&BundleExecutionId, &GraphNode<BundleExecutionId>) -> C,
    >(
        prio_graph: &mut prio_graph::PrioGraph<BundleExecutionId, Pubkey, C, D>,
        bundles: &mut Vec<Option<Vec<SanitizedTransaction>>>,
        bundles_scheduled: &mut u64,
        workers: &mut Vec<Worker>,
    ) {
        for worker in workers.iter_mut().filter(|w| !w.is_full()) {
            for bundle_id in worker.get_unblocked() {
                prio_graph.unblock(&bundle_id);
            }

            let (bundle_id, txns) = match prio_graph.pop() {
                Some(bundle) => (bundle, bundles[bundle.id].take().unwrap()),
                None => {
                    return;
                }
            };

            if worker.send(bundle_id, txns) {
                *bundles_scheduled += 1;
            }
        }
    }

    fn spawn_workers(
        worker_thread_count: usize,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bundle_committer: bundle_stage::committer::Committer,
        transaction_commiter: banking_stage::committer::Committer,
        exit: Arc<AtomicBool>,
    ) -> (Vec<std::thread::JoinHandle<()>>, Vec<Worker>) {
        let mut threads = Vec::new();
        let mut workers = Vec::new();
        for id in 0..worker_thread_count {
            let (sender, receiver) = crossbeam_channel::bounded(1);
            workers.push(Worker::new(sender));
            let poh_recorder = poh_recorder.clone();
            let bundle_committer = bundle_committer.clone();
            let transaction_commiter = transaction_commiter.clone();
            let exit = exit.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("jss_executor_worker_{}", id))
                    .spawn(move || {
                        Self::spawn_worker(
                            poh_recorder,
                            bundle_committer.clone(),
                            transaction_commiter.clone(),
                            receiver,
                            exit,
                        );
                    })
                    .unwrap(),
            );
        }
        (threads, workers)
    }

    /// Loop responsible for executing transactions and bundles
    fn spawn_worker(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut bundle_committer: bundle_stage::committer::Committer,
        mut transaction_commiter: banking_stage::committer::Committer,
        receiver: crossbeam_channel::Receiver<(BundleExecutionId, Vec<SanitizedTransaction>)>,
        exit: Arc<AtomicBool>,
    ) {
        let recorder = poh_recorder.read().unwrap().new_recorder();
        let mut bank_start = None;
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if bank_start.is_none() {
                bank_start = poh_recorder.read().unwrap().bank_start();
            }
            let Some(current_bank_start) = bank_start.as_ref() else {
                continue;
            };
            if !current_bank_start.should_working_bank_still_be_processing_txs() {
                bank_start = None;
                continue;
            }
            let Ok((_, txns)) = receiver.recv_timeout(Duration::from_millis(1)) else {
                continue;
            };
            Self::execute_record_commit(
                &current_bank_start.working_bank,
                &recorder,
                &mut bundle_committer,
                &mut transaction_commiter,
                txns,
            );
        }
    }

    /// Executes and records transactions or bundles; using the
    /// length of the transactions to determine which branch to take
    pub fn execute_record_commit(
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        bundle_committer: &mut bundle_stage::committer::Committer,
        transaction_commiter: &mut banking_stage::committer::Committer,
        transactions: Vec<SanitizedTransaction>,
    ) -> bool {
        if transactions.len() == 1 {
            Self::execute_commit_record_transactions(
                bank,
                recorder,
                transaction_commiter,
                transactions,
            )
        } else {
            // TODO: properly handle jito tips
            Self::execute_commit_record_bundle(bank, recorder, bundle_committer, transactions)
        }
    }

    fn execute_commit_record_bundle(
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        committer: &mut bundle_stage::committer::Committer,
        transactions: Vec<SanitizedTransaction>,
    ) -> bool {
        let len = transactions.len();
        let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);
        let sanitized_bundle = SanitizedBundle {
            transactions,
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

    fn execute_commit_record_transactions(
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        committer: &mut banking_stage::committer::Committer,
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
        if transaction_status_sender_enabled {
            pre_balance_info.native = bank.collect_balances(&batch);
            pre_balance_info.token =
                collect_token_balances(bank, &batch, &mut pre_balance_info.mint_decimals, None)
        }

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
                    transaction_status_sender_enabled,
                ),
                transaction_account_lock_limit: Some(bank.get_transaction_account_lock_limit()),
            },
        );
        if results.executed_transactions_count == 0 {
            return false;
        }

        let _freeze_lock = bank.freeze_lock();
        let (last_blockhash, lamports_per_signature) =
            bank.last_blockhash_and_lamports_per_signature();

        let executed_transactions = results
            .execution_results
            .iter()
            .zip(batch.sanitized_transactions())
            .filter_map(|(execution_result, tx)| {
                if execution_result.was_executed() {
                    Some(tx.to_versioned_transaction())
                } else {
                    None
                }
            })
            .collect_vec();
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
            results.executed_with_successful_result_count,
        );

        true
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

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_bundle_account_access(
        transactions: &[SanitizedTransaction],
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + '_ {
        transactions
            .iter()
            .flat_map(|tx| Self::get_transaction_account_access(tx))
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
}

/// Used to determine the priority of the bundle for execution.
/// Since microblocks are already sorted, FIFO assignment of ids
/// can be used to determine priority.
#[derive(Hash, Eq, PartialEq, Clone, Copy)]
struct BundleExecutionId {
    id: usize,
}

impl TopLevelId<Self> for BundleExecutionId {
    fn id(&self) -> Self {
        *self
    }
}

impl Ord for BundleExecutionId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id).reverse()
    }
}

impl PartialOrd for BundleExecutionId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Worker management struct for scheduling thread
struct Worker {
    sender: crossbeam_channel::Sender<(BundleExecutionId, Vec<SanitizedTransaction>)>,
    queue: VecDeque<BundleExecutionId>,
}

impl Worker {
    fn new(
        sender: crossbeam_channel::Sender<(BundleExecutionId, Vec<SanitizedTransaction>)>,
    ) -> Self {
        Self {
            sender,
            queue: VecDeque::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    fn send(&mut self, bundle_id: BundleExecutionId, txns: Vec<SanitizedTransaction>) -> bool {
        if self.sender.try_send((bundle_id, txns)).is_ok() {
            self.queue.push_back(bundle_id);
            true
        } else {
            false
        }
    }

    fn get_unblocked(&mut self) -> Vec<BundleExecutionId> {
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
