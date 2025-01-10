use std::{sync::{mpsc::Sender, Arc, RwLock}, time::Duration};

use itertools::Itertools;
use solana_bundle::bundle_execution::load_and_execute_bundle;
use solana_measure::measure_us;
/// Receives pre-scheduled microblocks and attempts to 'actuate' them by applying the transactions to the state.

use jito_protos::proto::jds_types::{MicroBlock, Packet};
use solana_poh::poh_recorder::{PohRecorder, RecordTransactionsSummary};
use solana_runtime::{bank::Bank, prioritization_fee_cache::PrioritizationFeeCache, vote_sender_types::ReplayVoteSender};
use solana_sdk::{bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle}, packet::PacketFlags, transaction::SanitizedTransaction};

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

    pub fn execute_and_commit_and_record_micro_block(&mut self, micro_block: MicroBlock, executed_sender: Sender<JssActuatorExecutionResult>) {
        let bank = self.poh_recorder.read().unwrap().bank().unwrap();
        let transaction_recorder = self.poh_recorder.read().unwrap().new_recorder();

        // Try to execute everything in the block
        for bundle in micro_block.bundles {
            let transactions = Self::parse_transactions(&bank, bundle.packets.iter());
            if transactions.is_empty() {
                continue;
            }

            let len = transactions.len();
            let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);
            let sanitized_bundle = SanitizedBundle{
                transactions,
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
                continue;
            }

            let (executed_batches, _execution_results_to_transactions_us) =
                measure_us!(bundle_execution_results.executed_transaction_batches());

            let _freeze_lock = bank.freeze_lock();
            let (last_blockhash, lamports_per_signature) = bank.last_blockhash_and_lamports_per_signature();
            let RecordTransactionsSummary {
                    result: record_transactions_result,
                    record_transactions_timings: _,
                    starting_transaction_index,
                } =  transaction_recorder.record_transactions(bank.slot(), executed_batches);
            if record_transactions_result.is_err() {
                continue;
            }

            let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
            self.committer.commit_bundle(
                &mut bundle_execution_results,
                last_blockhash,
                lamports_per_signature,
                starting_transaction_index,
                &bank,
                &mut execute_and_commit_timings);

            executed_sender.send(JssActuatorExecutionResult::Success(bundle_id)).unwrap();
        }
    }
}

pub enum JssActuatorExecutionResult {
    Success(String /*BundleId*/),
    Failure{
        bundle_id: String,
        cus: u64,
    },
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