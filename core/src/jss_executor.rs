// Executor design:
// - Micro-blocks are turned into serialized transactions and inserted into Prio-Graph by scheduling thread.
// - Each worker thread has a channel of size 1 for new bundles to execute.
// - The scheduling thread continually scans the worker channels to see if one is empty; as soon as it is;
//   it unblocks the transactions blocked by what was in the channel before; and pops the next one off the prio graph;
//   sending it to the channel. Transaction execution far outweighs the time for the scheduling thread to re-check the channel.
// - When the working bank is no longer valid; the Prio-graph and mempool are drained. Thinking about a feedback loop for JSS to know it scheduled too much.

use std::{
    collections::VecDeque,
    sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, RwLock},
    time::Duration,
};

use itertools::Itertools;
use jito_protos::proto::jss_types::{MicroBlock, Packet};
use prio_graph::{AccessKind, GraphNode, TopLevelId};
use solana_bundle::{bundle_execution::load_and_execute_bundle, derive_bundle_id_from_sanitized_transactions, SanitizedBundle};
use solana_ledger::{
    blockstore_processor::TransactionStatusSender, token_balances::collect_token_balances,
};
use solana_measure::measure_us;
use solana_poh::poh_recorder::{
    BankStart, PohRecorder, RecordTransactionsSummary, TransactionRecorder,
};
use solana_runtime::{
    bank::Bank, prioritization_fee_cache::PrioritizationFeeCache,
    vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{
    clock::MAX_PROCESSING_AGE,
    packet::PacketFlags,
    pubkey::Pubkey,
    transaction::SanitizedTransaction,
};
use solana_svm::{transaction_error_metrics::TransactionErrorMetrics, transaction_processing_result::TransactionProcessingResultExtensions, transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig}};
use solana_transaction_status::PreBalanceInfo;

use crate::{
    banking_stage::{
        self, immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
    },
    bundle_stage::{self, MAX_BUNDLE_RETRY_DURATION},
};

pub struct JssExecutor {
    microblock_sender: crossbeam_channel::Sender<Vec<Vec<SanitizedTransaction>>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl JssExecutor {
    pub fn new(
        worker_thread_count: usize,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let (bundle_committer, transaction_commiter) = Self::build_committers(
            replay_vote_sender.clone(),
            transaction_status_sender.clone(),
            prioritization_fee_cache.clone(),
        );

        let successful_count = Arc::new(AtomicUsize::new(0));

        let (worker_threads, worker_handles) = Self::spawn_workers(
            worker_thread_count,
            poh_recorder.clone(),
            bundle_committer.clone(),
            transaction_commiter.clone(),
            exit.clone(),
            successful_count.clone(),
        );

        const MICROBLOCK_CHANNEL_SIZE: usize = 50;
        let (microblock_sender, microblock_receiver) =
            crossbeam_channel::bounded(MICROBLOCK_CHANNEL_SIZE);
        let manager_thread = std::thread::Builder::new()
            .name("jss_executor_manager".to_string())
            .spawn(|| {
                Self::spawn_management_thread(
                    microblock_receiver,
                    poh_recorder,
                    worker_handles,
                    exit,
                    successful_count,
                );
            })
            .unwrap();

        Self {
            microblock_sender,
            threads: std::iter::once(manager_thread)
                .chain(worker_threads)
                .collect(),
        }
    }

    fn build_committers(
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) -> (
        bundle_stage::committer::Committer,
        banking_stage::committer::Committer,
    ) {
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
        (bundle_committer, transaction_commiter)
    }

    fn get_working_bank_start(poh_recorder: &Arc<RwLock<PohRecorder>>) -> Option<BankStart> {
        let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
            return None;
        };
        if bank_start.should_working_bank_still_be_processing_txs() {
            Some(bank_start)
        } else {
            None
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
        successful_count: Arc<AtomicUsize>,
    ) {
        let mut bundles_scheduled = 0;
        let mut bundles = Vec::new();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let Some(bank_start) = Self::get_working_bank_start(&poh_recorder) else {
                continue;
            };

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
                worker.wait_til_finish();
            }

            info!(
                "slot={} microblock_count={} scheduled={} unscheduled={} successful={}",
                bank_start.working_bank.slot(),
                microblock_count,
                bundles_scheduled,
                bundles.len() - bundles_scheduled as usize,
                successful_count.load(Ordering::Relaxed),
            );
            bundles_scheduled = 0;
            bundles.clear();
            successful_count.store(0, Ordering::Relaxed);
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
                //Self::schedule_next_batch(prio_graph, bundles, bundles_scheduled, workers);
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
        for (i, worker) in workers.iter_mut().enumerate().filter(|(_,w)| !w.is_full()) {
            for bundle_id in worker.get_unblocking_bundles() {
                prio_graph.unblock(&bundle_id);
            }

            let mut batch_for_execution = BatchForExecution::default();
            while !batch_for_execution.is_full() {
                let Some(bundle_id) = prio_graph.pop() else {
                    break;
                };
                let Some(transactions) = bundles[bundle_id.id].take() else {
                    warn!("Bundle {} already scheduled", bundle_id.id);
                    continue;
                };
                batch_for_execution.add(bundle_id, transactions);
            }
            if batch_for_execution.is_empty() {
                continue;
            }

            let batch_size = batch_for_execution.len();
            if worker.send(batch_for_execution) {
                *bundles_scheduled += batch_size as u64;
            }
        }
    }

    fn spawn_workers(
        worker_thread_count: usize,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bundle_committer: bundle_stage::committer::Committer,
        transaction_commiter: banking_stage::committer::Committer,
        exit: Arc<AtomicBool>,
        successful_count: Arc<AtomicUsize>,
    ) -> (Vec<std::thread::JoinHandle<()>>, Vec<Worker>) {
        let mut threads = Vec::new();
        let mut workers = Vec::new();
        for id in 0..worker_thread_count {
            let (sender, receiver) = crossbeam_channel::bounded(1);
            let (result_sender, result_receiver) = crossbeam_channel::bounded(1);
            workers.push(Worker::new(sender, result_receiver));
            let poh_recorder = poh_recorder.clone();
            let bundle_committer = bundle_committer.clone();
            let transaction_commiter = transaction_commiter.clone();
            let exit = exit.clone();
            let successful_count = successful_count.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("jss_executor_worker_{}", id))
                    .spawn(move || {
                        Self::spawn_worker(
                            poh_recorder,
                            bundle_committer.clone(),
                            transaction_commiter.clone(),
                            receiver,
                            result_sender,
                            exit,
                            successful_count,
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
        receiver: crossbeam_channel::Receiver<BatchForExecution>,
        sender: crossbeam_channel::Sender<Vec<BundleExecutionId>>,
        exit: Arc<AtomicBool>,
        successful_count: Arc<AtomicUsize>,
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
            let Ok(BatchForExecution{ ids, txns }) = receiver.recv_timeout(Duration::from_millis(1)) else {
                continue;
            };
            if Self::execute_record_commit(
                &current_bank_start.working_bank,
                &recorder,
                &mut bundle_committer,
                &mut transaction_commiter,
                txns,
            ){
                successful_count.fetch_add(1, Ordering::Relaxed);
            }
            sender.send(ids).unwrap();
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
        Self::execute_commit_record_transactions(
            bank,
            recorder,
            transaction_commiter,
            transactions,
        )
        

        // TODO: handle bundles
        //Self::execute_commit_record_bundle(bank, recorder, bundle_committer, transactions)
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
        let batch = bank.prepare_sanitized_batch_with_results(
            &transactions,
            transactions.iter().map(|_| Ok(())),
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
        let results = bank.load_and_execute_transactions(
            &batch,
            MAX_PROCESSING_AGE,
            &mut execute_and_commit_timings.execute_timings,
            &mut TransactionErrorMetrics::default(),
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

        results.processing_results.iter().for_each(|result| {
            if let Err(err) = result {
                error!("    Error executing transaction: {:?}", err);
            }
        });

        if results.processed_counts.processed_transactions_count == 0 {
            return false;
        }

        let _freeze_lock = bank.freeze_lock();
        let (last_blockhash, lamports_per_signature) =
            bank.last_blockhash_and_lamports_per_signature();

        let processed_transactions = results
            .processing_results
            .iter()
            .zip(batch.sanitized_transactions())
            .filter_map(|(execution_result, tx)| {
                if execution_result.was_processed() {
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
        } = recorder.record_transactions(bank.slot(), vec![processed_transactions]);
        if record_transactions_result.is_err() {
            return false;
        }

        committer.commit_transactions(
            &batch,
            results.processing_results,
            starting_transaction_index,
            &bank,
            &mut pre_balance_info,
            &mut execute_and_commit_timings,
            &results.processed_counts);
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
        txns.into_iter().map(|x| x.unwrap().0).collect_vec()
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

struct BatchForExecution {
    ids: Vec<BundleExecutionId>,
    txns: Vec<SanitizedTransaction>,
}

impl Default for BatchForExecution {
    fn default() -> Self {
        Self {
            ids: Vec::new(),
            txns: Vec::new(),
        }
    }
}

impl BatchForExecution {
    fn is_full(&self) -> bool {
        self.txns.len() == 1
    }

    fn is_empty(&self) -> bool {
        self.txns.is_empty()
    }

    fn add(&mut self, id: BundleExecutionId, txns: Vec<SanitizedTransaction>) {
        self.ids.push(id);
        self.txns.extend(txns);
    }

    fn len(&self) -> usize {
        self.txns.len()
    }
}

// Worker management struct for scheduling thread
struct Worker {
    sender: crossbeam_channel::Sender<BatchForExecution>,
    in_flight: usize,
    receiver: crossbeam_channel::Receiver<Vec<BundleExecutionId>>,
}

impl Worker {
    /// Creates a new worker with the given sender.
    fn new(
        sender: crossbeam_channel::Sender<BatchForExecution>,
        receiver: crossbeam_channel::Receiver<Vec<BundleExecutionId>>,
    ) -> Self {
        Self {
            sender,
            in_flight: 0,
            receiver,
        }
    }

    /// Returns true if the worker's channel is currently full.
    fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    /// Sends a bundle to the worker, while saving it in a local queue;
    /// used later to unblock transactions when the worker has picked up the bundle.
    fn send(&mut self, batch: BatchForExecution) -> bool {
        if let Ok(_) = self.sender.send(batch) {
            self.in_flight += 1;
            true
        } else {
            false
        }
    }

    /// Gets the ids of bundles that have been picked up by the worker;
    /// therefore opening the door for new transactions to be scheduled.
    fn get_unblocking_bundles(&mut self) -> Vec<BundleExecutionId> {
        let mut result = Vec::new();
        while let Ok(ids) = self.receiver.try_recv() {
            self.in_flight -= 1;
            result.extend(ids);
        }
        result
    }

    fn wait_til_finish(&mut self) {
        while self.in_flight > 0 {
            self.get_unblocking_bundles();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{Builder, JoinHandle},
        time::Duration,
    };

    use crossbeam_channel::Receiver;
    use jito_protos::proto::jss_types::{self, Bundle, MicroBlock};
    use solana_ledger::{
        blockstore::Blockstore, genesis_utils::GenesisConfigInfo, get_tmp_ledger_path_auto_delete,
        leader_schedule_cache::LeaderScheduleCache,
    };
    use solana_poh::{
        poh_recorder::{PohRecorder, Record, WorkingBankEntry},
        poh_service::PohService,
    };
    use solana_program_test::programs::spl_programs;
    use solana_runtime::{
        bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config_with_leader_ex,
        installed_scheduler_pool::BankWithScheduler,
        prioritization_fee_cache::PrioritizationFeeCache,
    };
    use solana_sdk::{
        fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE},
        genesis_config::ClusterType,
        native_token::sol_to_lamports,
        poh_config::PohConfig,
        pubkey::Pubkey,
        rent::Rent,
        signature::Keypair,
        signer::Signer,
        system_transaction::transfer,
        transaction::VersionedTransaction,
    };
    use solana_vote_program::vote_state::VoteState;

    pub(crate) fn simulate_poh(
        record_receiver: Receiver<Record>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) -> JoinHandle<()> {
        let poh_recorder = poh_recorder.clone();
        let is_exited = poh_recorder.read().unwrap().is_exited.clone();
        let tick_producer = Builder::new()
            .name("solana-simulate_poh".to_string())
            .spawn(move || loop {
                PohService::read_record_receiver_and_process(
                    &poh_recorder,
                    &record_receiver,
                    Duration::from_millis(10),
                );
                if is_exited.load(Ordering::Relaxed) {
                    break;
                }
            });
        tick_producer.unwrap()
    }

    pub fn create_test_recorder(
        bank: &Arc<Bank>,
        blockstore: Arc<Blockstore>,
        poh_config: Option<PohConfig>,
        leader_schedule_cache: Option<Arc<LeaderScheduleCache>>,
    ) -> (
        Arc<AtomicBool>,
        Arc<RwLock<PohRecorder>>,
        JoinHandle<()>,
        Receiver<WorkingBankEntry>,
    ) {
        let leader_schedule_cache = match leader_schedule_cache {
            Some(provided_cache) => provided_cache,
            None => Arc::new(LeaderScheduleCache::new_from_bank(bank)),
        };
        let exit = Arc::new(AtomicBool::new(false));
        let poh_config = poh_config.unwrap_or_default();

        let (mut poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            blockstore,
            &leader_schedule_cache,
            &poh_config,
            exit.clone(),
        );
        poh_recorder.set_bank(
            BankWithScheduler::new_without_scheduler(bank.clone()),
            false,
        );

        let poh_recorder = Arc::new(RwLock::new(poh_recorder));
        let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

        (exit, poh_recorder, poh_simulator, entry_receiver)
    }

    struct TestFixture {
        genesis_config_info: GenesisConfigInfo,
        leader_keypair: Keypair,
        bank: Arc<Bank>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        poh_simulator: JoinHandle<()>,
        entry_receiver: Receiver<WorkingBankEntry>,
        bank_forks: Arc<RwLock<BankForks>>,
    }

    fn create_test_fixture(mint_sol: u64) -> TestFixture {
        let mint_keypair = Keypair::new();
        let leader_keypair = Keypair::new();
        let voting_keypair = Keypair::new();

        let rent = Rent::default();

        let mut genesis_config = create_genesis_config_with_leader_ex(
            sol_to_lamports(mint_sol as f64),
            &mint_keypair.pubkey(),
            &leader_keypair.pubkey(),
            &voting_keypair.pubkey(),
            &solana_sdk::pubkey::new_rand(),
            rent.minimum_balance(VoteState::size_of()) + sol_to_lamports(1_000_000.0),
            sol_to_lamports(1_000_000.0),
            FeeRateGovernor {
                // Initialize with a non-zero fee
                lamports_per_signature: DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE / 2,
                ..FeeRateGovernor::default()
            },
            rent.clone(), // most tests don't expect rent
            ClusterType::Development,
            spl_programs(&rent),
        );
        genesis_config.ticks_per_slot *= 8;

        // workaround for https://github.com/solana-labs/solana/issues/30085
        // the test can deploy and use spl_programs in the genensis slot without waiting for the next one
        let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), 1));

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (exit, poh_recorder, poh_simulator, entry_receiver) =
            create_test_recorder(&bank, blockstore, Some(PohConfig::default()), None);

        let validator_pubkey = voting_keypair.pubkey();
        TestFixture {
            genesis_config_info: GenesisConfigInfo {
                genesis_config,
                mint_keypair,
                voting_keypair,
                validator_pubkey,
            },
            leader_keypair,
            bank,
            bank_forks,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
        }
    }

    // Converts a versioned transaction to a jds packet
    pub fn jds_packet_from_versioned_tx(tx: &VersionedTransaction) -> jss_types::Packet {
        let tx_data = bincode::serialize(tx).expect("serializes");
        let size = tx_data.len() as u64;
        jss_types::Packet {
            data: tx_data,
            meta: Some(jss_types::Meta {
                size,
                ..Default::default()
            }),
        }
    }

    pub fn get_executed_txns(
        entry_receiver: &Receiver<WorkingBankEntry>,
        wait: Duration,
    ) -> Vec<VersionedTransaction> {
        let mut transactions = Vec::new();
        let start = std::time::Instant::now();
        while start.elapsed() < wait {
            let Ok(WorkingBankEntry {
                bank: _wbe_bank,
                entries_ticks,
            }) = entry_receiver.try_recv()
            else {
                continue;
            };
            for (entry, _) in entries_ticks {
                if !entry.transactions.is_empty() {
                    transactions.extend(entry.transactions);
                }
            }
        }
        transactions
    }

    #[test]
    fn test_execution_simple() {
        let TestFixture {
            genesis_config_info,
            leader_keypair: _leader_keypair,
            bank,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
            bank_forks: _bank_forks,
        } = create_test_fixture(1);

        let (replay_vote_sender, _) = crossbeam_channel::unbounded();

        let mut executor = super::JssExecutor::new(
            1,
            poh_recorder.clone(),
            replay_vote_sender,
            None,
            Arc::new(PrioritizationFeeCache::default()),
            exit.clone(),
        );

        let successful_bundle = Bundle {
            packets: vec![jds_packet_from_versioned_tx(&VersionedTransaction::from(
                transfer(
                    &genesis_config_info.mint_keypair,
                    &genesis_config_info.mint_keypair.pubkey(),
                    100000,
                    genesis_config_info.genesis_config.hash(),
                ),
            ))],
        };
        let failed_bundle = Bundle {
            packets: vec![
                // This one would go through
                jds_packet_from_versioned_tx(&VersionedTransaction::from(transfer(
                    &genesis_config_info.mint_keypair,
                    &genesis_config_info.mint_keypair.pubkey(),
                    1000,
                    genesis_config_info.genesis_config.hash(),
                ))),
                // This one would fail; therefore both should fail
                jds_packet_from_versioned_tx(&VersionedTransaction::from(transfer(
                    &genesis_config_info.mint_keypair,
                    &genesis_config_info.mint_keypair.pubkey(),
                    1000000000,
                    genesis_config_info.genesis_config.hash(),
                ))),
            ],
        };
        let microblock = MicroBlock {
            bundles: vec![successful_bundle, failed_bundle],
        };

        // See if the transaction is executed
        executor.schedule_microblock(&bank, microblock.clone());
        let txns = get_executed_txns(&entry_receiver, Duration::from_secs(3));
        assert_eq!(txns.len(), 2);

        // Make sure if you try the same thing again, it doesn't work
        executor.schedule_microblock(&bank, microblock);
        let txns = get_executed_txns(&entry_receiver, Duration::from_secs(3));
        assert_eq!(txns.len(), 0);

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }
}
