#![feature(test)]

extern crate test;

use solana_core::jss_actuator::JssActuator;
use test::Bencher;


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

#[bench]
fn bench_jss_actuator(b: &mut Bencher) {
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
    let mut actuator = JssActuator::new(poh_recorder.clone(), replay_vote_sender);

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

    poh_recorder
        .write()
        .unwrap()
        .is_exited
        .store(true, Ordering::Relaxed);
    exit.store(true, Ordering::Relaxed);
    poh_simulator.join().unwrap();

    b.iter(|| {
        // TODO: make this actually useful (LOL)
        let (executed_sender, _executed_receiver) = std::sync::mpsc::channel();
        actuator.execute_and_commit_and_record_micro_block(microblock.clone(), executed_sender);
    });
}
