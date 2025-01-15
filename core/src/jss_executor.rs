use std::sync::{atomic::AtomicBool, mpsc::Sender, Arc, RwLock};

use ahash::{HashMap, HashMapExt, HashSetExt};
use crossbeam_channel::Receiver;
use itertools::Itertools;
use nohash::{IntMap, IntSet};
use solana_bundle::bundle_execution::load_and_execute_bundle;
use solana_measure::measure_us;

use jito_protos::proto::jss_types::{MicroBlock, Packet};
use solana_poh::poh_recorder::{PohRecorder, RecordTransactionsSummary, TransactionRecorder};
use solana_runtime::{
    bank::Bank, prioritization_fee_cache::PrioritizationFeeCache,
    vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{
    bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle}, clock::MAX_PROCESSING_AGE, packet::PacketFlags, pubkey::Pubkey, transaction::SanitizedTransaction
};

use crate::{
    banking_stage::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
    },
    bundle_stage::{self, MAX_BUNDLE_RETRY_DURATION},
};

#[derive(Clone)]
pub struct JssExecutor {
    poh_recorder: Arc<RwLock<PohRecorder>>,
    committer: bundle_stage::committer::Committer,
}

impl JssExecutor {
    pub fn new(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        replay_vote_sender: ReplayVoteSender,
    ) -> Self {
        Self {
            poh_recorder,
            committer: bundle_stage::committer::Committer::new(
                None, // TODO
                replay_vote_sender,
                Arc::new(PrioritizationFeeCache::default()), // TODO
            ),
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

    fn is_lock_blocked(
        locks: &[Lock],
        write_locked: &IntSet<u32>,
        read_locked: &IntMap<u32, usize>,
    ) -> bool {
        for lock in locks {
            if lock.write {
                if write_locked.contains(&lock.account_id) {
                    return true;
                }
            } else {
                if write_locked.contains(&lock.account_id)
                    || read_locked.contains_key(&lock.account_id)
                {
                    return true;
                }
            }
        }

        false
    }

    fn get_locks(
        transactions: &[SanitizedTransaction],
        lock_id_assigner: &mut LockIdAssigner,
    ) -> Vec<Lock> {
        let mut result = vec![];
        for txn in transactions {
            for (index, account) in txn.message().account_keys().iter().enumerate() {
                let account_id = lock_id_assigner.assign_or_get_id(*account);
                result.push(Lock {
                    account_id,
                    write: txn.message().is_writable(index),
                });
            }
        }
        result
    }

    fn lock_accounts(
        locks: &[Lock],
        write_locked: &mut IntSet<u32>,
        read_locked: &mut IntMap<u32, usize>,
    ) {
        for lock in locks {
            if lock.write {
                write_locked.insert(lock.account_id);
            } else {
                read_locked
                    .entry(lock.account_id)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
            }
        }
    }

    fn unlock_accounts(
        locks: &[Lock],
        write_locked: &mut IntSet<u32>,
        read_locked: &mut IntMap<u32, usize>,
    ) {
        for lock in locks {
            if lock.write {
                write_locked.remove(&lock.account_id);
            } else {
                let count = read_locked.get_mut(&lock.account_id).unwrap();
                *count -= 1;
                if *count == 0 {
                    read_locked.remove(&lock.account_id);
                }
            }
        }
    }

    fn spawn_worker_thread(
        exit: Arc<AtomicBool>,
        request_receiver: Receiver<BundleContext>,
        response_sender: crossbeam_channel::Sender<JssExecutorWorkerExecutionResult>,
        bank: Arc<Bank>,
        recorder: TransactionRecorder,
        mut committer: bundle_stage::committer::Committer,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            while !exit.load(std::sync::atomic::Ordering::Relaxed) {
                let Ok(context) = request_receiver.try_recv() else {
                    continue;
                };
                let success = Self::execute_commit_record_bundle(
                    &bank,
                    &recorder,
                    &mut committer,
                    context.transactions.clone(),
                );
                response_sender
                    .send(JssExecutorWorkerExecutionResult { context, success })
                    .unwrap();
            }
        })
    }

    fn prepare_workers(
        num_workers: usize,
        bank: Arc<Bank>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        committer: bundle_stage::committer::Committer,
        exit: Arc<AtomicBool>,
    ) -> ExecutionWorkers {
        let (request_sender, request_receiver) =
            crossbeam_channel::bounded::<BundleContext>(num_workers);
        let (response_sender, response_receiver) =
            crossbeam_channel::bounded::<JssExecutorWorkerExecutionResult>(num_workers);
        let worker_threads = (0..num_workers)
            .into_iter()
            .map(|_| {
                Self::spawn_worker_thread(
                    exit.clone(),
                    request_receiver.clone(),
                    response_sender.clone(),
                    bank.clone(),
                    poh_recorder.read().unwrap().new_recorder(),
                    committer.clone(),
                )
            })
            .collect_vec();
        ExecutionWorkers {
            worker_threads,
            exit,
            request_sender,
            response_receiver,
        }
    }

    // Ideas for optimization:
    // - start iteration from the min(last) completed bundle index
    //   (this allows us to skip bundles that are already scheduled or still blocked)
    // - schedule up to 2 in contention bundles at once (so worker thread is never idle)
    // - Unblock locks with an 'ack' from the worker thread
    // - Use prio-graph instead to determine the order of execution
    fn schedule_next_bundles(
        context: &mut MicroblockExecutionContext,
        request_sender: &crossbeam_channel::Sender<BundleContext>,
        response_receiver: &crossbeam_channel::Receiver<JssExecutorWorkerExecutionResult>,
        worker_thread_count: usize,
    ) {
        // If we have enough inflight bundles, stop scheduling now
        if context.inflight_bundles_count >= worker_thread_count {
            return;
        }

        // Update the first unprocessed bundle index
        let skip_ahead = context
            .bundles
            .iter()
            .skip(context.first_unprocessed_bundle_index)
            .position(|b| !matches!(b, QueuedBundle::Scheduled));

        if let Some(skip_ahead) = skip_ahead {
            context.first_unprocessed_bundle_index += skip_ahead;
        } else {
            // Everything is scheduled
            return;
        }

        for queued_bundle in context
            .bundles
            .iter_mut()
            .skip(context.first_unprocessed_bundle_index)
        {
            // If the transaction has already been scheduled, skip it (obviously)
            if matches!(queued_bundle, QueuedBundle::Scheduled) {
                continue;
            }

            // If we have a completed bundle waiting; terminate the loop
            // because there might be better bundles to schedule
            if !response_receiver.is_empty() {
                break;
            }

            // If these packets haven't been parsed yet, parse them and save them
            // so that we never have to this again for this bundle
            // Also hash all the locks and assign int ids to them
            // This is kinda sad because I love parsing, re-parsing, serializing, and deserializing
            // I think CPU registers should hold JSON strings
            if let QueuedBundle::Unparsed(packets) = queued_bundle {
                let transactions = Self::parse_transactions(&context.bank, packets.iter());
                let locks = Self::get_locks(&transactions, &mut context.lock_assigner);
                *queued_bundle = QueuedBundle::Waiting {
                    transactions,
                    locks,
                };
            }
            let QueuedBundle::Waiting {
                transactions,
                locks,
            } = queued_bundle
            else {
                panic!("QueuedBundle::Waiting expected");
            };

            // Check if this bundle is blocked by any locks
            if Self::is_lock_blocked(&locks, &context.write_locked, &context.read_locked) {
                continue;
            }

            // Finally: lock the accounts, send the bundle, and mark it as scheduled
            Self::lock_accounts(&locks, &mut context.write_locked, &mut context.read_locked);
            request_sender
                .send(BundleContext {
                    transactions: std::mem::take(transactions),
                    locks: std::mem::take(locks),
                })
                .unwrap();
            *queued_bundle = QueuedBundle::Scheduled;
            context.inflight_bundles_count += 1;
            if context.inflight_bundles_count >= worker_thread_count {
                break;
            }
        }
    }

    pub fn receive_finished_bundles(
        context: &mut MicroblockExecutionContext,
        response_receiver: &crossbeam_channel::Receiver<JssExecutorWorkerExecutionResult>,
        executed_sender: &Sender<JssExecutorExecutionResult>,
    ) {
        while let Ok(executed_bundle) = response_receiver.try_recv() {
            Self::unlock_accounts(
                &executed_bundle.context.locks,
                &mut context.write_locked,
                &mut context.read_locked,
            );
            let msg = if executed_bundle.success {
                JssExecutorExecutionResult::Success(derive_bundle_id_from_sanitized_transactions(
                    &executed_bundle.context.transactions,
                ))
            } else {
                JssExecutorExecutionResult::Failure {
                    bundle_id: derive_bundle_id_from_sanitized_transactions(
                        &executed_bundle.context.transactions,
                    ),
                    cus: 0,
                }
            };
            executed_sender.send(msg).unwrap();
            context.inflight_bundles_count -= 1;
            context.completed_bundles_count += 1;
        }
    }

    pub fn schedule_microblock(
        &mut self,
        micro_block: MicroBlock,
        executed_sender: Sender<JssExecutorExecutionResult>,
    ) {
        // Grab bank and create exit signal
        let bank = self.poh_recorder.read().unwrap().bank().unwrap();
        let exit = Arc::new(AtomicBool::new(false));

        // Spawn the worker threads that will be executing the bundles
        const WORKER_THREAD_COUNT: usize = 4;
        let ExecutionWorkers {
            worker_threads,
            exit,
            request_sender,
            response_receiver,
        } = Self::prepare_workers(
            WORKER_THREAD_COUNT,
            bank.clone(),
            self.poh_recorder.clone(),
            self.committer.clone(),
            exit.clone(),
        );

        let mut execution_context = MicroblockExecutionContext::new(bank, micro_block);
        while self.poh_recorder.read().unwrap().has_bank() && execution_context.keep_going() {
            Self::receive_finished_bundles(
                &mut execution_context,
                &response_receiver,
                &executed_sender,
            );
            Self::schedule_next_bundles(
                &mut execution_context,
                &request_sender,
                &response_receiver,
                WORKER_THREAD_COUNT,
            );
        }

        exit.store(true, std::sync::atomic::Ordering::Relaxed);
        worker_threads.into_iter().for_each(|t| t.join().unwrap());
    }

    pub fn execute_and_commit_and_record_micro_block(
        &mut self,
        micro_block: MicroBlock,
        executed_sender: Sender<JssExecutorExecutionResult>,
    ) {
        return self.schedule_microblock(micro_block, executed_sender);
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

        if let Err(_) = bundle_execution_results.result() {
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
}

pub struct JssExecutorWorkerExecutionResult {
    context: BundleContext,
    success: bool,
}

pub enum JssExecutorExecutionResult {
    Success(String /*BundleId*/),
    Failure { bundle_id: String, cus: u32 },
}

struct ExecutionWorkers {
    worker_threads: Vec<std::thread::JoinHandle<()>>,
    exit: Arc<AtomicBool>,
    request_sender: crossbeam_channel::Sender<BundleContext>,
    response_receiver: crossbeam_channel::Receiver<JssExecutorWorkerExecutionResult>,
}

struct Lock {
    account_id: u32,
    write: bool,
}

enum QueuedBundle {
    Unparsed(Vec<Packet>),
    Waiting {
        transactions: Vec<SanitizedTransaction>,
        locks: Vec<Lock>,
    },
    Scheduled,
}

struct BundleContext {
    transactions: Vec<SanitizedTransaction>,
    locks: Vec<Lock>,
}

pub struct MicroblockExecutionContext {
    bank: Arc<Bank>,
    bundles: Vec<QueuedBundle>,
    lock_assigner: LockIdAssigner,
    write_locked: IntSet<u32>,
    read_locked: IntMap<u32, usize>,
    total_bundles_count: usize,
    inflight_bundles_count: usize,
    completed_bundles_count: usize,
    first_unprocessed_bundle_index: usize,
}

impl MicroblockExecutionContext {
    pub fn new(bank: Arc<Bank>, microblock: MicroBlock) -> Self {
        Self {
            bank,
            lock_assigner: LockIdAssigner::new(),
            total_bundles_count: microblock.bundles.len(),
            bundles: microblock
                .bundles
                .into_iter()
                .map(|bundle| QueuedBundle::Unparsed(bundle.packets))
                .collect_vec(),
            write_locked: IntSet::new(),
            read_locked: IntMap::new(),
            inflight_bundles_count: 0,
            completed_bundles_count: 0,
            first_unprocessed_bundle_index: 0,
        }
    }

    pub fn keep_going(&self) -> bool {
        self.completed_bundles_count < self.total_bundles_count
    }
}

struct LockIdAssigner {
    next_id: u32,
    mapping: HashMap<Pubkey, u32>,
}

impl LockIdAssigner {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            mapping: HashMap::new(),
        }
    }

    pub fn assign_or_get_id(&mut self, pubkey: Pubkey) -> u32 {
        if let Some(id) = self.mapping.get(&pubkey) {
            return *id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.mapping.insert(pubkey, id);
        id
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
                bank: wbe_bank,
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
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
            bank_forks: _bank_forks,
        } = create_test_fixture(1);

        let (replay_vote_sender, _) = crossbeam_channel::unbounded();

        let mut executor = super::JssExecutor::new(poh_recorder, replay_vote_sender);

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

        let (executed_sender, _executed_receiver) = std::sync::mpsc::channel();

        // See if the transaction is executed
        executor
            .execute_and_commit_and_record_micro_block(microblock.clone(), executed_sender.clone());
        let txns = get_executed_txns(&entry_receiver, Duration::from_secs(3));
        assert_eq!(txns.len(), 1);

        // Make sure if you try the same thing again, it doesn't work
        executor.execute_and_commit_and_record_micro_block(microblock.clone(), executed_sender);
        let txns = get_executed_txns(&entry_receiver, Duration::from_secs(3));
        assert_eq!(txns.len(), 0);

        executor
            .poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }
}
