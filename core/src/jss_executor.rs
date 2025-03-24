// Executor design:
// - Micro-blocks are turned into serialized transactions and inserted into Prio-Graph by scheduling thread.
// - Each worker thread has a channel of size 1 for new bundles to execute.
// - The scheduling thread continually scans the worker channels to see if one is empty; as soon as it is;
//   it unblocks the transactions blocked by what was in the channel before; and pops the next one off the prio graph;
//   sending it to the channel. Transaction execution far outweighs the time for the scheduling thread to re-check the channel.
// - When the working bank is no longer valid; the Prio-graph and mempool are drained. Thinking about a feedback loop for JSS to know it scheduled too much.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use itertools::Itertools;
use jito_protos::proto::jss_types::{MicroBlock, Packet};
use prio_graph::{AccessKind, GraphNode, TopLevelId};
use sha2::Digest;
use solana_bundle::{
    bundle_execution::load_and_execute_bundle, derive_bundle_id_from_sanitized_transactions,
    SanitizedBundle,
};
use solana_ledger::{
    blockstore_processor::TransactionStatusSender, token_balances::collect_token_balances,
};
use solana_poh::poh_recorder::{
    BankStart, PohRecorder, RecordTransactionsSummary, TransactionRecorder,
};
use solana_runtime::{
    bank::Bank, prioritization_fee_cache::PrioritizationFeeCache,
    vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{
    clock::MAX_PROCESSING_AGE, packet::PacketFlags, pubkey::Pubkey, signature::Keypair,
    transaction::SanitizedTransaction,
};
use solana_svm::{
    transaction_error_metrics::TransactionErrorMetrics,
    transaction_processing_result::TransactionProcessingResultExtensions,
    transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig},
};
use solana_svm_transaction::svm_message::SVMMessage;
use solana_transaction_status::PreBalanceInfo;

use crate::{
    banking_stage::{
        self, committer::CommitTransactionDetails,
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_metrics::LeaderSlotMetricsTracker,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings, qos_service::QosService,
    },
    bundle_stage::{self, bundle_account_locker::BundleAccountLocker, MAX_BUNDLE_RETRY_DURATION},
    proxy::block_engine_stage::BlockBuilderFeeInfo,
    tip_manager::TipManager,
};

pub struct JssExecutor {
    microblock_sender: crossbeam_channel::Sender<Vec<ParsedBundle>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl JssExecutor {
    pub fn new(
        worker_thread_count: usize,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
        tip_manager: TipManager,
        exit: Arc<AtomicBool>,
        keypair: Arc<Keypair>,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        bundle_account_locker: BundleAccountLocker,
        retry_bundle_sender: crossbeam_channel::Sender<[u8; 32]>,
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
            tip_manager.clone(),
            successful_count.clone(),
            keypair,
            block_builder_fee_info,
            bundle_account_locker,
            retry_bundle_sender,
        );

        const MICROBLOCK_CHANNEL_SIZE: usize = 100_000;
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
            .map(|bundle| ParsedBundle {
                revert_on_error: bundle.revert_on_error,
                transactions: Self::parse_transactions(bank, bundle.packets.iter()),
            })
            .filter(|b: &ParsedBundle| !b.transactions.is_empty())
            .collect_vec();
        self.microblock_sender.try_send(bundles).is_ok()
    }

    /// Loop responsible for scheduling transactions to workers
    fn spawn_management_thread(
        microblock_receiver: crossbeam_channel::Receiver<Vec<ParsedBundle>>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut workers: Vec<Worker>,
        exit: Arc<AtomicBool>,
        successful_count: Arc<AtomicUsize>,
    ) {
        info!("spawned management thread");

        let mut metrics = JssSchedulerMetrics::default();
        let mut bundles = Vec::new();
        let mut prio_graph = prio_graph::PrioGraph::new(|id: &BundleExecutionId, _graph_node| *id);

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if !poh_recorder.read().unwrap().would_be_leader(0) {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }

            if poh_recorder.read().unwrap().bank_start().is_none() {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }

            bundles.clear();
            prio_graph.clear();
            successful_count.store(0, Ordering::Relaxed);

            while poh_recorder.read().unwrap().would_be_leader(0) {
                Self::maybe_ingest_new_microblock(
                    &microblock_receiver,
                    &mut prio_graph,
                    &mut bundles,
                    &mut workers,
                    &mut metrics,
                );
                Self::schedule_next_batch(
                    &mut prio_graph,
                    &mut bundles,
                    &mut workers,
                    &mut metrics,
                );
            }

            for worker in workers.iter_mut() {
                worker.wait_til_finish();
            }

            metrics.report();
        }
    }

    /// Ingests new micro-blocks and inserts them into the prio-graph.
    /// If a worker is available between incoming transactions, it schedules the next batch
    /// so that no workers has to wait.
    fn maybe_ingest_new_microblock<
        C: std::hash::Hash + Eq + Clone + TopLevelId<BundleExecutionId> + Copy,
        D: Fn(&BundleExecutionId, &GraphNode<BundleExecutionId>) -> C,
    >(
        microblock_receiver: &crossbeam_channel::Receiver<Vec<ParsedBundle>>,
        prio_graph: &mut prio_graph::PrioGraph<BundleExecutionId, Pubkey, C, D>,
        bundles: &mut Vec<Option<ParsedBundleWithId>>,
        workers: &mut Vec<Worker>,
        metrics: &mut JssSchedulerMetrics,
    ) {
        if let Ok(micro_block) = microblock_receiver.try_recv() {
            metrics.bundles_received += micro_block.len();
            for bundle in micro_block {
                let id = bundles.len();
                let bundle_id = BundleExecutionId { id };
                prio_graph.insert_transaction(
                    bundle_id,
                    Self::get_bundle_account_access(&bundle.transactions),
                );
                bundles.push(Some(ParsedBundleWithId {
                    id: bundle_id,
                    bundle,
                }));
                Self::schedule_next_batch(prio_graph, bundles, workers, metrics);
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
        bundles: &mut Vec<Option<ParsedBundleWithId>>,
        workers: &mut Vec<Worker>,
        metrics: &mut JssSchedulerMetrics,
    ) {
        for (_, worker) in workers.iter_mut().enumerate().filter(|(_, w)| !w.is_full()) {
            for bundle_id in worker.get_unblocking_bundles() {
                prio_graph.unblock(&bundle_id);
            }

            let Some(bundle_id) = prio_graph.pop() else {
                continue;
            };
            let Some(bundle) = bundles[bundle_id.id].take() else {
                continue;
            };

            if worker.send(bundle) {
                metrics.bundles_schueduled += 1;
            }
        }
    }

    fn spawn_workers(
        worker_thread_count: usize,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bundle_committer: bundle_stage::committer::Committer,
        transaction_commiter: banking_stage::committer::Committer,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        successful_count: Arc<AtomicUsize>,
        keypair: Arc<Keypair>,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        bundle_account_locker: BundleAccountLocker,
        retry_bundle_sender: crossbeam_channel::Sender<[u8; 32]>,
    ) -> (Vec<std::thread::JoinHandle<()>>, Vec<Worker>) {
        let mut threads = Vec::new();
        let mut workers = Vec::new();
        let last_tip_updated_slot = Arc::new(Mutex::new(0));
        const JSS_ID_OFFSET: usize = 1000;
        for id in 0..worker_thread_count {
            let id = id + JSS_ID_OFFSET;
            let (sender, receiver) = crossbeam_channel::bounded(1);
            let (result_sender, result_receiver) = crossbeam_channel::bounded(1);
            workers.push(Worker::new(sender, result_receiver));
            let poh_recorder = poh_recorder.clone();
            let bundle_committer = bundle_committer.clone();
            let transaction_commiter = transaction_commiter.clone();
            let exit = exit.clone();
            let tip_manager = tip_manager.clone();
            let successful_count = successful_count.clone();
            let keypair = keypair.clone();
            let last_tip_updated_slot = last_tip_updated_slot.clone();
            let block_builder_fee_info = block_builder_fee_info.clone();
            let bundle_account_locker = bundle_account_locker.clone();
            let retry_bundle_sender = retry_bundle_sender.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("jss_executor_worker_{}", id))
                    .spawn(move || {
                        Self::spawn_worker(
                            id,
                            poh_recorder,
                            bundle_committer.clone(),
                            transaction_commiter.clone(),
                            receiver,
                            result_sender,
                            exit,
                            tip_manager,
                            successful_count,
                            keypair,
                            last_tip_updated_slot,
                            block_builder_fee_info,
                            bundle_account_locker,
                            retry_bundle_sender,
                        );
                    })
                    .unwrap(),
            );
        }
        (threads, workers)
    }

    /// Loop responsible for executing transactions and bundles
    fn spawn_worker(
        id: usize,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut bundle_committer: bundle_stage::committer::Committer,
        mut transaction_commiter: banking_stage::committer::Committer,
        receiver: crossbeam_channel::Receiver<ParsedBundleWithId>,
        sender: crossbeam_channel::Sender<BundleExecutionId>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        successful_count: Arc<AtomicUsize>,
        keypair: Arc<Keypair>,
        last_tip_updated_slot: Arc<Mutex<u64>>,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        bundle_account_locker: BundleAccountLocker,
        retry_bundle_sender: crossbeam_channel::Sender<[u8; 32]>,
    ) {
        info!("spawned worker thread {}", id);

        let qos_service = QosService::new(id as u32);
        let mut metrics = JssWorkerMetrics::new(id as u32);
        let recorder = poh_recorder.read().unwrap().new_recorder();
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let Some(current_bank_start) = poh_recorder.read().unwrap().bank_start() else {
                // If we get a bundle when no bank is active; we can just ignore it.
                while let Ok(bundle) = receiver.try_recv() {
                    sender.send(bundle.id).unwrap();
                }
                std::thread::sleep(Duration::from_micros(500));
                continue;
            };
            let current_block_builder_fee_info = block_builder_fee_info.lock().unwrap().clone();
            while current_bank_start.should_working_bank_still_be_processing_txs() {
                let Ok(ParsedBundleWithId {
                    id: ids,
                    bundle:
                        ParsedBundle {
                            revert_on_error,
                            transactions: txns,
                        },
                }) = receiver.recv_timeout(Duration::from_millis(1))
                else {
                    continue;
                };
                let bundle_id = Self::generate_bundle_id(&txns);
                let result = Self::execute_record_commit(
                    &current_bank_start,
                    &qos_service,
                    &recorder,
                    &mut bundle_committer,
                    &mut transaction_commiter,
                    revert_on_error,
                    txns,
                    &tip_manager,
                    &keypair,
                    &last_tip_updated_slot,
                    &current_block_builder_fee_info,
                    &bundle_account_locker,
                );
                sender.send(ids).unwrap();

                if result.is_success() {
                    successful_count.fetch_add(1, Ordering::Relaxed);
                } else if result.is_retryable() {
                    retry_bundle_sender.try_send(bundle_id).unwrap();
                }
            }
        }
    }

    fn generate_bundle_id(transactions: &[SanitizedTransaction]) -> [u8; 32] {
        let mut hasher = sha2::Sha256::new();
        for transaction in transactions {
            let Some(signature) = transaction.signatures().first() else {
                continue;
            };
            hasher.update(signature.as_ref());
        }

        hasher.finalize().into()
    }

    /// Executes and records transactions or bundles
    fn execute_record_commit(
        bank_start: &BankStart,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        bundle_committer: &mut bundle_stage::committer::Committer,
        transaction_commiter: &mut banking_stage::committer::Committer,
        revert_on_error: bool,
        transactions: Vec<SanitizedTransaction>,
        tip_manager: &TipManager,
        keypair: &Keypair,
        last_tip_updated_slot: &Mutex<u64>,
        block_builder_fee_info: &BlockBuilderFeeInfo,
        bundle_account_locker: &BundleAccountLocker,
    ) -> ExecutionResult {
        if revert_on_error {
            let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);
            let sanitized_bundle = SanitizedBundle {
                transactions,
                bundle_id,
            };

            let mut last_tip_updated_slot_guard = last_tip_updated_slot.lock().unwrap();
            if bank_start.working_bank.slot() != *last_tip_updated_slot_guard
                && Self::bundle_touches_tip_pdas(&sanitized_bundle, &tip_manager.get_tip_accounts())
            {
                if !Self::handle_tip_programs(
                    &bank_start,
                    &qos_service,
                    &recorder,
                    bundle_committer,
                    tip_manager,
                    keypair,
                    block_builder_fee_info,
                    bundle_account_locker,
                ) {
                    return ExecutionResult::Failure;
                }
                *last_tip_updated_slot_guard = bank_start.working_bank.slot();
            }
            drop(last_tip_updated_slot_guard);

            Self::execute_commit_record_bundle(
                &bank_start.working_bank,
                qos_service,
                recorder,
                bundle_committer,
                sanitized_bundle,
                bundle_account_locker,
            )
        } else {
            Self::execute_commit_record_transactions(
                &bank_start.working_bank,
                qos_service,
                recorder,
                transaction_commiter,
                transactions,
                bundle_account_locker,
            )
        }
    }

    fn handle_tip_programs(
        bank_start: &BankStart,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        bundle_committer: &mut bundle_stage::committer::Committer,
        tip_manager: &TipManager,
        keypair: &Keypair,
        block_builder_fee_info: &BlockBuilderFeeInfo,
        bundle_account_locker: &BundleAccountLocker,
    ) -> bool {
        let initialize_tip_programs_bundle =
            tip_manager.get_initialize_tip_programs_bundle(&bank_start.working_bank, keypair);
        if let Some(init_bundle) = initialize_tip_programs_bundle {
            if !Self::execute_commit_record_bundle(
                &bank_start.working_bank,
                qos_service,
                recorder,
                bundle_committer,
                init_bundle,
                bundle_account_locker,
            )
            .is_success()
            {
                info!("Failed to initialize tip programs");
                return false;
            }
        }

        let tip_crank_bundle = tip_manager.get_tip_programs_crank_bundle(
            &bank_start.working_bank,
            keypair,
            block_builder_fee_info,
        );
        if let Ok(Some(crank_bundle)) = tip_crank_bundle {
            if !Self::execute_commit_record_bundle(
                &bank_start.working_bank,
                qos_service,
                recorder,
                bundle_committer,
                crank_bundle,
                bundle_account_locker,
            )
            .is_success()
            {
                info!("Failed to crank tip programs");
                return false;
            }
        }

        true
    }

    fn execute_commit_record_bundle(
        bank: &Arc<Bank>,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        committer: &mut bundle_stage::committer::Committer,
        sanitized_bundle: SanitizedBundle,
        bundle_account_locker: &BundleAccountLocker,
    ) -> ExecutionResult {
        let lock = bundle_account_locker.prepare_locked_bundle(&sanitized_bundle, &bank);
        if lock.is_err() {
            return ExecutionResult::Failure;
        }

        // See if we have enough room in the block to execute the bundle
        let (transaction_qos_cost_results, skipped_count) = qos_service
            .select_and_accumulate_transaction_costs(
                bank,
                &sanitized_bundle.transactions,
                std::iter::repeat(Ok(())),
                &|_| 0,
            );
        if skipped_count > 0 {
            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
            info!("Dropped bundle due to QoS constraints");
            return ExecutionResult::Retryable;
        }

        // Execute the bundle
        let default_accounts = vec![None; sanitized_bundle.transactions.len()];
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
            error!("Error executing bundle: {:?}", err);
            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
            return ExecutionResult::Failure;
        }

        // Record the transactions
        let executed_batches = bundle_execution_results.executed_transaction_batches();
        let freeze_lock = bank.freeze_lock();
        let RecordTransactionsSummary {
            result: record_transactions_result,
            record_transactions_timings: _,
            starting_transaction_index,
        } = recorder.record_transactions(bank.slot(), executed_batches);
        if record_transactions_result.is_err() {
            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
            return ExecutionResult::Failure;
        }

        // Commit the transactions
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let (_, commit_bundle_details) = committer.commit_bundle(
            &mut bundle_execution_results,
            starting_transaction_index,
            &bank,
            &mut execute_and_commit_timings,
        );

        // Drop the freeze lock
        drop(freeze_lock);

        let commit_transaction_details = commit_bundle_details
            .commit_transaction_details
            .into_iter()
            .flat_map(|commit_details| {
                commit_details
                    .into_iter()
                    .filter(|d| matches!(d, CommitTransactionDetails::Committed { .. }))
            })
            .collect();

        QosService::remove_or_update_costs(
            transaction_qos_cost_results.iter(),
            Some(&commit_transaction_details),
            bank,
        );

        ExecutionResult::Success
    }

    fn execute_commit_record_transactions(
        bank: &Arc<Bank>,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        committer: &mut banking_stage::committer::Committer,
        transactions: Vec<SanitizedTransaction>,
        bundle_account_locker: &BundleAccountLocker,
    ) -> ExecutionResult {
        let (transaction_qos_cost_results, _) = qos_service
            .select_and_accumulate_transaction_costs(
                bank,
                &transactions,
                std::iter::repeat(Ok(())),
                &|_| 0,
            );
        if transaction_qos_cost_results.iter().any(|r| r.is_err()) {
            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
            return ExecutionResult::Retryable;
        }

        let bundle_account_locks = bundle_account_locker.account_locks();
        let batch = bank.prepare_sanitized_batch_with_results(
            &transactions,
            transaction_qos_cost_results.iter().map(|r| match r {
                Ok(_cost) => Ok(()),
                Err(err) => Err(err.clone()),
            }),
            Some(&bundle_account_locks.read_locks()),
            Some(&bundle_account_locks.write_locks()),
        );
        drop(bundle_account_locks);

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

        results
            .processing_results
            .iter()
            .zip(transactions.iter())
            .for_each(|(result, txn)| {
                if let Err(err) = result {
                    error!(
                        "    Error executing transaction err={:?}, fee_payer={}",
                        err,
                        txn.fee_payer()
                    );
                }
            });

        if results.processed_counts.processed_transactions_count == 0 {
            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
            return ExecutionResult::Failure;
        }

        let freeze_lock = bank.freeze_lock();

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
            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
            return ExecutionResult::Failure;
        }

        let (_, commit_transactions_result) = committer.commit_transactions(
            &batch,
            results.processing_results,
            starting_transaction_index,
            &bank,
            &mut pre_balance_info,
            &mut execute_and_commit_timings,
            &results.processed_counts,
        );

        // Drop the freeze lock
        drop(freeze_lock);

        QosService::remove_or_update_costs(
            transaction_qos_cost_results.iter(),
            Some(&commit_transactions_result),
            bank,
        );

        ExecutionResult::Success
    }

    fn bundle_touches_tip_pdas(bundle: &SanitizedBundle, tip_pdas: &HashSet<Pubkey>) -> bool {
        bundle.transactions.iter().any(|tx| {
            tx.message()
                .account_keys()
                .iter()
                .any(|a| tip_pdas.contains(a))
        })
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

enum ExecutionResult {
    Success,
    Failure,
    Retryable,
}

impl ExecutionResult {
    fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable)
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

struct ParsedBundle {
    revert_on_error: bool,
    transactions: Vec<SanitizedTransaction>,
}

struct ParsedBundleWithId {
    id: BundleExecutionId,
    bundle: ParsedBundle,
}

// Worker management struct for scheduling thread
struct Worker {
    sender: crossbeam_channel::Sender<ParsedBundleWithId>,
    in_flight: usize,
    receiver: crossbeam_channel::Receiver<BundleExecutionId>,
}

impl Worker {
    /// Creates a new worker with the given sender.
    fn new(
        sender: crossbeam_channel::Sender<ParsedBundleWithId>,
        receiver: crossbeam_channel::Receiver<BundleExecutionId>,
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

    /// Sends a bundle to the worker; while incrementing the in-flight count.
    fn send(&mut self, bundle: ParsedBundleWithId) -> bool {
        if let Ok(_) = self.sender.send(bundle) {
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
            result.push(ids);
        }
        result
    }

    fn wait_til_finish(&mut self) {
        while self.in_flight > 0 {
            self.get_unblocking_bundles();
        }
    }
}

#[derive(Default)]
struct JssSchedulerMetrics {
    bundles_received: usize,
    bundles_schueduled: usize,
}

impl JssSchedulerMetrics {
    fn report(&mut self) {
        datapoint_info!(
            "jss_scheduler_metrics",
            ("bundles_received", self.bundles_received, i64),
            ("bundles_scheduled", self.bundles_schueduled, i64),
        );
        *self = Self::default();
    }
}

// Per worker tracking of:
// - LeaderSlotMetrics

struct JssWorkerMetrics {
    leader_slot_metrics_tracker: LeaderSlotMetricsTracker,
}

impl JssWorkerMetrics {
    fn new(id: u32) -> Self {
        Self {
            leader_slot_metrics_tracker: LeaderSlotMetricsTracker::new(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{Builder, JoinHandle},
        time::Duration,
    };

    use crossbeam_channel::Receiver;
    use jito_protos::proto::jss_types::{self, Bundle, MicroBlock};
    use jito_tip_distribution::sdk::derive_tip_distribution_account_address;
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

    use crate::{
        bundle_stage::bundle_account_locker::BundleAccountLocker,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
    };

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
        let (retry_bundle_sender, _) = crossbeam_channel::unbounded();

        let mut executor = super::JssExecutor::new(
            1,
            poh_recorder.clone(),
            replay_vote_sender,
            None,
            Arc::new(PrioritizationFeeCache::default()),
            TipManager::new(TipManagerConfig::default()),
            exit.clone(),
            Arc::new(Keypair::new()),
            Arc::new(Mutex::new(BlockBuilderFeeInfo::default())),
            BundleAccountLocker::default(),
            retry_bundle_sender,
        );

        let successful_bundle = Bundle {
            revert_on_error: false,
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
            revert_on_error: true,
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
        assert_eq!(txns.len(), 1);

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

    fn get_tip_manager(vote_account: &Pubkey) -> TipManager {
        TipManager::new(TipManagerConfig {
            tip_payment_program_id: Pubkey::from_str("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt")
                .unwrap(),
            tip_distribution_program_id: Pubkey::from_str(
                "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
            )
            .unwrap(),
            tip_distribution_account_config: TipDistributionAccountConfig {
                merkle_root_upload_authority: Pubkey::new_unique(),
                vote_account: *vote_account,
                commission_bps: 10,
            },
        })
    }

    #[test]
    fn test_handle_tip_programs() {
        let TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
            bank_forks: _bank_forks,
        } = create_test_fixture(1);

        let (replay_vote_sender, _) = crossbeam_channel::unbounded();
        let (retry_bundle_sender, _) = crossbeam_channel::unbounded();
        let keypair = Arc::new(leader_keypair);
        let block_builder_pubkey = Pubkey::new_unique();

        let tip_manager = get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
        let mut executor = super::JssExecutor::new(
            1,
            poh_recorder.clone(),
            replay_vote_sender,
            None,
            Arc::new(PrioritizationFeeCache::default()),
            tip_manager.clone(),
            exit.clone(),
            keypair.clone(),
            Arc::new(Mutex::new(BlockBuilderFeeInfo {
                block_builder: block_builder_pubkey.clone(),
                block_builder_commission: 5,
            })),
            BundleAccountLocker::default(),
            retry_bundle_sender,
        );

        let tip_accounts = tip_manager.get_tip_accounts();
        let tip_account = tip_accounts.iter().collect::<Vec<_>>()[0];
        let successful_bundle = Bundle {
            revert_on_error: true,
            packets: vec![jds_packet_from_versioned_tx(&VersionedTransaction::from(
                transfer(
                    &genesis_config_info.mint_keypair,
                    &tip_account,
                    100000,
                    genesis_config_info.genesis_config.hash(),
                ),
            ))],
        };

        let microblock = MicroBlock {
            bundles: vec![successful_bundle],
        };
        executor.schedule_microblock(&bank, microblock);
        let transactions = get_executed_txns(&entry_receiver, Duration::from_secs(3));

        // expect to see initialize tip payment program, tip distribution program,
        // initialize tip distribution account, change tip receiver + change block builder
        assert_eq!(
            transactions[0],
            tip_manager
                .initialize_tip_payment_program_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[1],
            tip_manager
                .initialize_tip_distribution_config_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[2],
            tip_manager
                .initialize_tip_distribution_account_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        // the first tip receiver + block builder are the initializer (keypair.pubkey()) as set by the
        // TipPayment program during initialization
        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();
        assert_eq!(
            transactions[3],
            tip_manager
                .build_change_tip_receiver_and_block_builder_tx(
                    &keypair.pubkey(),
                    &derive_tip_distribution_account_address(
                        &tip_manager.tip_distribution_program_id(),
                        &genesis_config_info.validator_pubkey,
                        bank_start.working_bank.epoch()
                    )
                    .0,
                    &bank_start.working_bank,
                    &keypair,
                    &keypair.pubkey(),
                    &block_builder_pubkey,
                    5
                )
                .to_versioned_transaction()
        );

        assert_eq!(transactions.len(), 5);

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }
}
