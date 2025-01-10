use std::{sync::{atomic::AtomicBool, mpsc::Sender, Arc, RwLock}, time::Duration};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use crossbeam_channel::Receiver;
use itertools::Itertools;
use solana_bundle::bundle_execution::load_and_execute_bundle;
use solana_measure::measure_us;

use jito_protos::proto::jds_types::{MicroBlock, Packet};
use solana_poh::poh_recorder::{self, PohRecorder, RecordTransactionsSummary, TransactionRecorder};
use solana_runtime::{bank::Bank, prioritization_fee_cache::PrioritizationFeeCache, vote_sender_types::ReplayVoteSender};
use solana_sdk::{bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle}, packet::PacketFlags, pubkey::Pubkey, transaction::SanitizedTransaction};

use crate::{banking_stage::{immutable_deserialized_packet::ImmutableDeserializedPacket, leader_slot_timing_metrics::LeaderExecuteAndCommitTimings}, bundle_stage};

pub struct JssActuator {
    poh_recorder: Arc<RwLock<PohRecorder>>,
    committer: bundle_stage::committer::Committer,
}

impl JssActuator {
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
            )
        }
    }

    fn parse_transactions<'a>(
        bank: &Bank,
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Vec<SanitizedTransaction> {
        let txns = packets.map(|packet| {
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
                        solana_packet.meta_mut().flags.insert(PacketFlags::SIMPLE_VOTE_TX);
                    }
                    if flags.forwarded {
                        solana_packet.meta_mut().flags.insert(PacketFlags::FORWARDED);
                    }
                    if flags.tracer_packet {
                        solana_packet.meta_mut().flags.insert(PacketFlags::TRACER_PACKET);
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
                bank.get_reserved_account_keys())?;
            Some(sanitized_transaction)
        }).collect_vec();
        if txns.iter().any(Option::is_none) {
            return vec![];
        }
        txns.into_iter().map(|x| x.unwrap()).collect_vec()
    }

    fn is_lock_blocked(
        transactions: &[SanitizedTransaction],
        write_locked: &HashSet<Pubkey>,
        read_locked: &HashMap<Pubkey, usize>,
    ) -> bool {
        for txn in transactions {
            for (index, account) in txn.message().account_keys().iter().enumerate() {
                if txn.message().is_writable(index) {
                    if write_locked.contains(account) || read_locked.contains_key(account) {
                        return true;
                    }
                } else {
                    if write_locked.contains(account) {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn lock_accounts(
        transactions: &[SanitizedTransaction],
        write_locked: &mut HashSet<Pubkey>,
        read_locked: &mut HashMap<Pubkey, usize>,
    ) {
        for txn in transactions {
            for (index, account) in txn.message().account_keys().iter().enumerate() {
                if txn.message().is_writable(index) {
                    write_locked.insert(*account);
                } else {
                    read_locked.entry(*account).and_modify(|e| *e += 1).or_insert(1);
                }
            }
        }
    }

    fn unlock_accounts(
        transactions: &[SanitizedTransaction],
        write_locked: &mut HashSet<Pubkey>,
        read_locked: &mut HashMap<Pubkey, usize>,
    ) {
        for txn in transactions {
            for (index, account) in txn.message().account_keys().iter().enumerate() {
                if txn.message().is_writable(index) {
                    write_locked.remove(account);
                } else {
                    let count = read_locked.get_mut(account).unwrap();
                    *count -= 1;
                    if *count == 0 {
                        read_locked.remove(account);
                    }
                }
            }
        }
    }

    fn spawn_worker_thread(
        exit: Arc<AtomicBool>,
        request_receiver: Receiver<Vec<SanitizedTransaction>>,
        response_sender: crossbeam_channel::Sender<JssActuatorWorkerExecutionResult>,
        bank: Arc<Bank>,
        recorder: TransactionRecorder,
        mut committer: bundle_stage::committer::Committer,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            while !exit.load(std::sync::atomic::Ordering::Relaxed) {
                let Ok(transactions) = request_receiver.try_recv() else {
                    continue;
                };
                if Self::execute_commit_record_bundle(&bank, &recorder, &mut committer, transactions.clone()) {
                    response_sender.send(JssActuatorWorkerExecutionResult{
                        transactions,
                        success: true,
                    }).unwrap();
                } else {
                    response_sender.send(JssActuatorWorkerExecutionResult{
                        transactions,
                        success: false,
                    }).unwrap();
                }
            }
        })
    }

    fn prepare_workers(
        num_workers: usize,
        bank: Arc<Bank>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        committer: bundle_stage::committer::Committer,
        exit: Arc<AtomicBool>)
        -> ExecutionWorkers
    {
        let (request_sender, request_receiver) =
        crossbeam_channel::bounded::<Vec<SanitizedTransaction>>(num_workers);
        let (response_sender, response_receiver) =
            crossbeam_channel::bounded::<JssActuatorWorkerExecutionResult>(num_workers);
        let worker_threads = (0..num_workers).into_iter().map(|_| {
            Self::spawn_worker_thread(
                exit.clone(),
                request_receiver.clone(),
                response_sender.clone(),
                bank.clone(),
                poh_recorder.read().unwrap().new_recorder(),
                committer.clone())
        }).collect_vec();

        ExecutionWorkers {
            workers: worker_threads,
            exit,
            request_sender,
            response_receiver,
        }
    }



    // TODO: optimize this function:
    // Quick:
    // - start iteration from the first unprocessed bundle
    // - Assign an int id to each bundle for faster check of 'already_scheduled'
    // - Assign Pubkeys -> int for much faster re-hashing
    // - Check if scheduled txns completed during iteration to break out sooner
    //   and unblock better bundles sooner
    //
    // Otherwise:
    // - Switch to priograph
    pub fn schedule_next_bundles(
        &mut self,
        bundles: &mut [Vec<SanitizedTransaction>],
        request_sender: &crossbeam_channel::Sender<Vec<SanitizedTransaction>>,
        write_locked: &mut HashSet<Pubkey>,
        read_locked: &mut HashMap<Pubkey, usize>,
        inflight_bundles_count: &mut usize,
        worker_thread_count: usize,
    ) {
        for transactions in bundles.iter_mut() {
            if *inflight_bundles_count >= worker_thread_count {
                break;
            }
            if transactions.is_empty() || Self::is_lock_blocked(&transactions, write_locked, read_locked) {
                continue;
            }
            Self::lock_accounts(&transactions, write_locked, read_locked);
            request_sender.send(std::mem::take(transactions)).unwrap();
            *inflight_bundles_count += 1;
        }
    }

    pub fn receive_finished_bundles(
        &mut self,
        response_receiver: &crossbeam_channel::Receiver<JssActuatorWorkerExecutionResult>,
        write_locked: &mut HashSet<Pubkey>,
        read_locked: &mut HashMap<Pubkey, usize>,
        executed_sender: &Sender<JssActuatorExecutionResult>,
        inflight_bundles_count: &mut usize,
        completed_bundles_count: &mut usize,
    ) {
        while let Ok(executed_bundle) = response_receiver.try_recv() {
            Self::unlock_accounts(&executed_bundle.transactions, write_locked, read_locked);
            let msg = if executed_bundle.success {
                JssActuatorExecutionResult::Success(
                    derive_bundle_id_from_sanitized_transactions(&executed_bundle.transactions))
            } else {
                JssActuatorExecutionResult::Failure { 
                    bundle_id: derive_bundle_id_from_sanitized_transactions(&executed_bundle.transactions),
                    cus: 0,
                }
            };
            executed_sender.send(msg).unwrap();
            *inflight_bundles_count -= 1;
            *completed_bundles_count += 1;
        }
    }

    pub fn execute_and_commit_and_record_micro_block_multi_threaded(
        &mut self,
        micro_block: MicroBlock,
        executed_sender: Sender<JssActuatorExecutionResult>,
    ) {
        let bank = self.poh_recorder.read().unwrap().bank().unwrap();
        const WORKER_THREAD_COUNT: usize = 4;
        let exit = Arc::new(AtomicBool::new(false));

        let ExecutionWorkers {
            workers: worker_threads,
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

        let mut write_locked = HashSet::with_capacity(0);
        let mut read_locked = HashMap::new();
        let mut inflight_bundles_count = 0;
        let mut completed_bundles_count = 0;

        let mut bundles = micro_block.bundles.iter().map(|bundle| {
            Self::parse_transactions(&bank, bundle.packets.iter())
        }).collect_vec();

        while completed_bundles_count < micro_block.bundles.len() {
            self.receive_finished_bundles(
                &response_receiver,
                &mut write_locked,
                &mut read_locked,
                &executed_sender,
                &mut inflight_bundles_count,
                &mut completed_bundles_count);
            self.schedule_next_bundles(
                &mut bundles,
                &request_sender,
                &mut write_locked,
                &mut read_locked,
                &mut inflight_bundles_count,
                WORKER_THREAD_COUNT);
        }

        exit.store(true, std::sync::atomic::Ordering::Relaxed);
        worker_threads.into_iter().for_each(|t| t.join().unwrap());
    }

    pub fn execute_and_commit_and_record_micro_block(&mut self, micro_block: MicroBlock, executed_sender: Sender<JssActuatorExecutionResult>) {
        return self.execute_and_commit_and_record_micro_block_multi_threaded(micro_block, executed_sender);
    }

    pub fn execute_commit_record_bundle(
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        committer: &mut bundle_stage::committer::Committer,
        txns: Vec<SanitizedTransaction>) -> bool
    {
        let len = txns.len();
        let bundle_id = derive_bundle_id_from_sanitized_transactions(&txns);
        let sanitized_bundle = SanitizedBundle{
            transactions: txns,
            bundle_id: bundle_id.clone(),
        };

        let default_accounts = vec![None; len];
        let mut bundle_execution_results = load_and_execute_bundle(
            &bank,
            &sanitized_bundle,
            20,
            &Duration::from_secs(1),
            false,
            &None,
            false,
            None,
            &default_accounts,
            &default_accounts);

        if let Err(_) = bundle_execution_results.result() {
            return false;
        }

        let (executed_batches, _execution_results_to_transactions_us) =
            measure_us!(bundle_execution_results.executed_transaction_batches());

        let _freeze_lock = bank.freeze_lock();
        let (last_blockhash, lamports_per_signature) = bank.last_blockhash_and_lamports_per_signature();
        let RecordTransactionsSummary {
                result: record_transactions_result,
                record_transactions_timings: _,
                starting_transaction_index,
            } =  recorder.record_transactions(bank.slot(), executed_batches);
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
            &mut execute_and_commit_timings);

        true
    }
}

pub struct JssActuatorWorkerExecutionResult {
    transactions: Vec<SanitizedTransaction>,
    success: bool,
}

pub enum JssActuatorExecutionResult {
    Success(String /*BundleId*/),
    Failure{
        bundle_id: String,
        cus: u64,
    },
}

struct ExecutionWorkers {
    workers: Vec<std::thread::JoinHandle<()>>,
    exit: Arc<AtomicBool>,
    request_sender: crossbeam_channel::Sender<Vec<SanitizedTransaction>>,
    response_receiver: crossbeam_channel::Receiver<JssActuatorWorkerExecutionResult>,
}

#[cfg(test)]
mod tests {
    use std::{sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, thread::{Builder, JoinHandle}, time::Duration};

    use crossbeam_channel::Receiver;
    use jito_protos::proto::jds_types::{self, Bundle, MicroBlock};
    use solana_ledger::{blockstore::Blockstore, genesis_utils::GenesisConfigInfo, get_tmp_ledger_path_auto_delete, leader_schedule_cache::LeaderScheduleCache};
    use solana_poh::{poh_recorder::{PohRecorder, Record, WorkingBankEntry}, poh_service::PohService};
    use solana_program_test::programs::spl_programs;
    use solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config_with_leader_ex, installed_scheduler_pool::BankWithScheduler};
    use solana_sdk::{fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE}, genesis_config::ClusterType, native_token::sol_to_lamports, poh_config::PohConfig, pubkey::Pubkey, rent::Rent, signature::Keypair, signer::Signer, system_transaction::transfer, transaction::VersionedTransaction};
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
    pub fn jds_packet_from_versioned_tx(tx: &VersionedTransaction) -> jds_types::Packet {
        let tx_data = bincode::serialize(tx).expect("serializes");
        let size = tx_data.len() as u64;
        jds_types::Packet {
            data: tx_data,
            meta: Some(jds_types::Meta {
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
            }) = entry_receiver.try_recv() else {
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
    fn test_actuation_simple() {
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

        let mut actuator = super::JssActuator::new(poh_recorder, replay_vote_sender);

        let successful_bundle = Bundle {
            packets: vec![jds_packet_from_versioned_tx(&VersionedTransaction::from(transfer(
                &genesis_config_info.mint_keypair,
                &genesis_config_info.mint_keypair.pubkey(),
                100000,
                genesis_config_info.genesis_config.hash(),
            )))],
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

        let (executed_sender, executed_receiver) = std::sync::mpsc::channel();

        // See if the transaction is executed
        actuator.execute_and_commit_and_record_micro_block(microblock.clone(), executed_sender.clone());
        let txns = get_executed_txns(&entry_receiver, Duration::from_secs(3));
        assert_eq!(txns.len(), 1);

        // Make sure if you try the same thing again, it doesn't work
        actuator.execute_and_commit_and_record_micro_block(microblock.clone(), executed_sender);
        let txns = get_executed_txns(&entry_receiver, Duration::from_secs(3));
        assert_eq!(txns.len(), 0);

        actuator.poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }
}