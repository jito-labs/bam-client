use {
    crate::bam_dependencies::BamDependencies,
    ahash::HashSet,
    solana_clock::Slot,
    solana_poh::poh_recorder::{PohRecorder, SharedWorkingBank},
    solana_runtime::bank_forks::BankForks,
    std::{
        collections::BTreeSet,
        sync::{Arc, Mutex, RwLock},
        time::Instant,
    },
};

pub struct BamFallbackManager {
    thread: std::thread::JoinHandle<()>,
    slot_sender: crossbeam_channel::Sender<u64>,
    last_sent_slot: u64,
}

pub struct FallbackEvaluationSlots {
    failing_slots: Vec<Slot>,
    passing_slots: HashSet<Slot>,
    total_slots_evaluated: u32,
    most_recent_slot: Option<Slot>,
}

impl BamFallbackManager {
    pub fn new(
        exit: Arc<std::sync::atomic::AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bam_txns_per_slot_threshold: Arc<RwLock<u64>>,
        bam_url: Arc<Mutex<Option<String>>>,
        dependencies: BamDependencies,
    ) -> Self {
        let (slot_sender, slot_receiver) = crossbeam_channel::bounded(10_000);
        Self {
            thread: std::thread::spawn(move || {
                Self::run(
                    exit,
                    slot_receiver,
                    poh_recorder,
                    bam_txns_per_slot_threshold,
                    bam_url,
                    dependencies,
                )
            }),
            slot_sender,
            last_sent_slot: 0,
        }
    }

    fn run(
        exit: Arc<std::sync::atomic::AtomicBool>,
        slot_receiver: crossbeam_channel::Receiver<Slot>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bam_txns_per_slot_threshold: Arc<RwLock<Slot>>,
        bam_url: Arc<Mutex<Option<String>>>,
        dependencies: BamDependencies,
    ) {
        let mut scheduled_leader_slots: BTreeSet<Slot> = BTreeSet::new();

        const DURATION_BETWEEN_CHECKS: std::time::Duration = std::time::Duration::from_secs(15);
        const DISCONNECTED_DRAIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        let mut last_check_time = Instant::now();
        let mut last_drain_time = Instant::now();

        let shared_working_bank = poh_recorder.read().unwrap().shared_working_bank();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let is_connected = bam_url.lock().unwrap().is_some();

            if is_connected {
                while let Ok(slot) = slot_receiver.try_recv() {
                    scheduled_leader_slots.insert(slot);
                }
            } else {
                let now = Instant::now();
                if now.duration_since(last_drain_time) >= DISCONNECTED_DRAIN_INTERVAL {
                    while slot_receiver.try_recv().is_ok() {}
                    last_drain_time = now;
                    datapoint_error!("bam-fallback-manager-disconnected", ("count", 1, i64));
                    error!("BAM Fallback Manager is disconnected");
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }

            let now = Instant::now();
            if now.duration_since(last_check_time).as_secs() < DURATION_BETWEEN_CHECKS.as_secs() {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            last_check_time = now;

            let current_slot = poh_recorder.read().unwrap().current_poh_slot();
            let evaluation = Self::num_leader_slots_missing_txns(
                &scheduled_leader_slots,
                current_slot,
                &dependencies.bank_forks,
                *bam_txns_per_slot_threshold.read().unwrap(),
            );

            if let Some(most_recent) = evaluation.most_recent_slot {
                if evaluation.failing_slots.contains(&most_recent) {
                    while Self::should_wait_for_disconnect(&poh_recorder, &shared_working_bank) {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }

                    let disconnect_url = bam_url
                        .lock()
                        .unwrap()
                        .as_ref()
                        .map_or("None".to_string(), |u| u.clone());
                    datapoint_error!(
                        "bam_fallback_manager-disconnected",
                        ("count", 1, i64),
                        ("triggered_by_slot", most_recent, i64),
                        ("bam_url", disconnect_url, String)
                    );
                    error!(
                        "BAM Fallback triggered! Triggered by slot {}. Disconnecting from BAM at url {:?}",
                        most_recent,
                        disconnect_url
                    );
                    *bam_url.lock().unwrap() = None;
                    scheduled_leader_slots.clear();
                } else {
                    let min_valid_slot = current_slot.saturating_sub(64);
                    scheduled_leader_slots.retain(|&slot| {
                        slot >= min_valid_slot && !evaluation.passing_slots.contains(&slot)
                    });
                }
            }

            datapoint_info!(
                "bam_fallback_manager-evaluation",
                ("leader_slots_tracked", scheduled_leader_slots.len(), i64),
                ("slots_below_threshold", evaluation.failing_slots.len(), i64),
                (
                    "total_slots_evaluated",
                    evaluation.total_slots_evaluated,
                    i64
                ),
                (
                    "most_recent_slot",
                    evaluation.most_recent_slot.unwrap_or_default(),
                    i64
                )
            );
            info!(
                "leader_slots_tracked={}, slots_below_threshold={}, total_slots_evaluated={}, most_recent_slot={:?}",
                scheduled_leader_slots.len(),
                evaluation.failing_slots.len(),
                evaluation.total_slots_evaluated,
                evaluation.most_recent_slot
            );
        }
        warn!("BAM Fallback Manager thread exiting");
    }

    /// Check the tracked leader slots for non-vote transaction counts below the threshold.
    /// Returns slots organized into passing and failing sets, as well as the number of slots evaluated.
    /// A slot is considered passing if it has a non-vote transaction count >= threshold.
    /// A slot is considered failing if it has a non-vote transaction count < threshold.
    /// A slot is also considered passing if it is not rooted or an ancestor of the root.
    /// The evaluation only considers slots in the range [current_slot - 64, current_slot - 32].
    /// Slots are evaluated from newest to oldest. If the most recent slot fails, we trigger a disconnect.
    fn num_leader_slots_missing_txns(
        scheduled_leader_slots: &BTreeSet<Slot>,
        current_slot: Slot,
        bank_forks: &Arc<RwLock<BankForks>>,
        bam_txns_per_slot_threshold: u64,
    ) -> FallbackEvaluationSlots {
        let mut failing_slots = Vec::new();
        let mut passing_slots = HashSet::default();

        let (root_slot, ancestors, min_valid_slot, max_valid_slot) = {
            let bank_forks = bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let root_slot = root_bank.slot();
            let ancestors = root_bank.proper_ancestors_set();

            let min = current_slot.saturating_sub(64);
            let max = current_slot.saturating_sub(32).max(root_slot);

            (root_slot, ancestors, min, max)
        };

        let mut total_slots_evaluated = 0;
        let mut most_recent_slot = None;

        for &slot in scheduled_leader_slots
            .range(min_valid_slot..=max_valid_slot)
            .rev()
        {
            total_slots_evaluated += 1;

            if !ancestors.contains(&slot) && slot != root_slot {
                passing_slots.insert(slot);
                continue;
            }

            let slot_non_vote_txs = {
                let bank_forks = bank_forks.read().unwrap();
                bank_forks.get(slot).map(|bank| {
                    let non_vote_tx_count = bank.non_vote_transaction_count_since_restart();
                    let parent_count = bank
                        .parent()
                        .map(|p| p.non_vote_transaction_count_since_restart())
                        .unwrap_or(0);

                    non_vote_tx_count.saturating_sub(parent_count)
                })
            };

            if let Some(txs) = slot_non_vote_txs {
                if txs < bam_txns_per_slot_threshold {
                    failing_slots.push(slot);
                    warn!(
                        "BAM Fallback: Slot {} has non-vote tx count {} < threshold {}",
                        slot, txs, bam_txns_per_slot_threshold
                    );
                    if most_recent_slot.is_none() {
                        most_recent_slot = Some(slot);
                        break;
                    }
                } else {
                    passing_slots.insert(slot);
                }
            }
        }

        FallbackEvaluationSlots {
            failing_slots,
            passing_slots,
            total_slots_evaluated,
            most_recent_slot,
        }
    }

    pub fn send_slot(&mut self, slot: Slot) -> Result<(), crossbeam_channel::TrySendError<Slot>> {
        if slot <= self.last_sent_slot {
            return Ok(());
        }
        self.last_sent_slot = slot;
        self.slot_sender.try_send(slot)
    }

    fn should_wait_for_disconnect(
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        shared_working_bank: &SharedWorkingBank,
    ) -> bool {
        if let Some(bank) = shared_working_bank.load() {
            if !bank.is_frozen() {
                return true;
            }
        }

        let poh_recorder = poh_recorder.read().unwrap();
        // Check if we're somwewhere in the middle of a leader rotation
        // The range is [leader_first_tick_height, leader_last_tick_height]
        poh_recorder.would_be_leader(0)
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::{bank::Bank, genesis_utils::create_genesis_config},
        solana_system_transaction as system_transaction,
    };

    #[test]
    fn test_num_leader_slots_missing_txns_below_threshold() {
        let genesis = create_genesis_config(10_000_000_000);
        let parent = Arc::new(Bank::new_for_tests(&genesis.genesis_config));
        let slot = 100u64;
        let bank =
            Bank::new_from_parent(parent.clone(), &solana_pubkey::Pubkey::new_unique(), slot);

        let bank_forks = BankForks::new_rw_arc(bank);
        let bank_ref = bank_forks.read().unwrap().get(slot).unwrap();

        let payer = genesis.mint_keypair;
        let bh = bank_ref.last_blockhash();

        let tx = system_transaction::transfer(&payer, &solana_pubkey::Pubkey::new_unique(), 1, bh);
        let _results = bank_ref.process_transaction(&tx);

        assert!(bank_forks
            .write()
            .unwrap()
            .set_root(slot, None, None)
            .is_ok());

        let mut scheduled_leader_slots = BTreeSet::new();
        scheduled_leader_slots.insert(slot);

        let current_slot = slot + 35;
        let threshold = 10u64;

        let result = BamFallbackManager::num_leader_slots_missing_txns(
            &scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result.failing_slots.len(), 1);
        assert!(result.failing_slots.contains(&slot));
        assert_eq!(result.total_slots_evaluated, 1);
    }

    #[test]
    fn test_num_leader_slots_missing_txns_above_threshold() {
        let genesis = create_genesis_config(10_000_000_000);
        let parent = Arc::new(Bank::new_for_tests(&genesis.genesis_config));
        let slot = 100u64;
        let bank =
            Bank::new_from_parent(parent.clone(), &solana_pubkey::Pubkey::new_unique(), slot);

        let bank_forks = BankForks::new_rw_arc(bank);
        let bank_ref = bank_forks.read().unwrap().get(slot).unwrap();

        let payer = genesis.mint_keypair;
        let bh = bank_ref.last_blockhash();
        for _ in 0..20 {
            let tx =
                system_transaction::transfer(&payer, &solana_pubkey::Pubkey::new_unique(), 1, bh);
            let _results = bank_ref.process_transaction(&tx);
        }

        assert!(bank_forks
            .write()
            .unwrap()
            .set_root(slot, None, None)
            .is_ok());

        let mut scheduled_leader_slots = BTreeSet::new();
        scheduled_leader_slots.insert(slot);

        let current_slot = slot + 35;
        let threshold = 10u64;

        let result = BamFallbackManager::num_leader_slots_missing_txns(
            &scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result.failing_slots.len(), 0);
        assert_eq!(result.passing_slots.len(), 1);
        assert!(result.passing_slots.contains(&slot));
    }

    #[test]
    fn test_num_leader_slots_missing_txns_non_rooted_slot() {
        let genesis = create_genesis_config(10_000_000_000);
        let parent = Arc::new(Bank::new_for_tests(&genesis.genesis_config));
        let rooted_slot = 100u64;
        let non_rooted_slot = 101u64;

        let rooted_bank = Bank::new_from_parent(
            parent.clone(),
            &solana_pubkey::Pubkey::new_unique(),
            rooted_slot,
        );
        let non_rooted_bank = Bank::new_from_parent(
            parent.clone(),
            &solana_pubkey::Pubkey::new_unique(),
            non_rooted_slot,
        );

        let bank_forks = BankForks::new_rw_arc(rooted_bank);
        bank_forks.write().unwrap().insert(non_rooted_bank);
        assert!(bank_forks
            .write()
            .unwrap()
            .set_root(rooted_slot, None, None)
            .is_ok());

        let mut scheduled_leader_slots = BTreeSet::new();
        scheduled_leader_slots.insert(non_rooted_slot);

        let current_slot = non_rooted_slot + 35;
        let threshold = 10u64;

        let result = BamFallbackManager::num_leader_slots_missing_txns(
            &scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result.failing_slots.len(), 0);
        assert_eq!(result.passing_slots.len(), 1);
        assert!(result.passing_slots.contains(&non_rooted_slot));
    }

    #[test]
    fn test_num_leader_slots_missing_txns_stale_slots_cleanup() {
        let genesis = create_genesis_config(10_000_000_000);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank);

        let mut scheduled_leader_slots = BTreeSet::new();
        scheduled_leader_slots.insert(10);
        scheduled_leader_slots.insert(20);
        scheduled_leader_slots.insert(30);

        let current_slot = 100u64;
        let threshold = 10u64;

        let result = BamFallbackManager::num_leader_slots_missing_txns(
            &scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result.failing_slots.len(), 0);
        assert_eq!(result.passing_slots.len(), 0);
        assert_eq!(result.total_slots_evaluated, 0);
    }

    #[test]
    fn test_num_leader_slots_validator_recovered() {
        let genesis = create_genesis_config(10_000_000_000);
        let genesis_bank = Arc::new(Bank::new_for_tests(&genesis.genesis_config));

        let first_slot = 100u64;
        let bank = Bank::new_from_parent(
            genesis_bank.clone(),
            &solana_pubkey::Pubkey::new_unique(),
            first_slot,
        );
        let bank_forks = BankForks::new_rw_arc(bank);

        let mut scheduled_leader_slots = BTreeSet::new();
        let payer = genesis.mint_keypair;

        for slot in first_slot + 1..=first_slot + 3 {
            let parent = bank_forks.read().unwrap().get(slot - 1).unwrap();
            let bank =
                Bank::new_from_parent(parent.clone(), &solana_pubkey::Pubkey::new_unique(), slot);
            let bank_scheduler = bank_forks.write().unwrap().insert(bank);
            let bank = bank_scheduler.clone_without_scheduler();

            if slot != first_slot + 3 {
                let tx = system_transaction::transfer(
                    &payer,
                    &solana_pubkey::Pubkey::new_unique(),
                    1,
                    bank.last_blockhash(),
                );
                assert_eq!(bank.process_transaction(&tx), Ok(()));
            } else {
                for _ in 0..20 {
                    let tx = system_transaction::transfer(
                        &payer,
                        &solana_pubkey::Pubkey::new_unique(),
                        1,
                        bank.last_blockhash(),
                    );
                    assert_eq!(bank.process_transaction(&tx), Ok(()));
                }
            }
            scheduled_leader_slots.insert(slot);
        }

        assert!(bank_forks
            .write()
            .unwrap()
            .set_root(*scheduled_leader_slots.last().unwrap(), None, None)
            .is_ok());

        let current_slot = scheduled_leader_slots.last().unwrap() + 35;
        let threshold = 10u64;

        let result = BamFallbackManager::num_leader_slots_missing_txns(
            &scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result.failing_slots.len(), 0);
        assert_eq!(result.passing_slots.len(), 1);
        assert!(result
            .passing_slots
            .contains(scheduled_leader_slots.last().unwrap()));
        assert_eq!(result.total_slots_evaluated, 3);
    }
}
