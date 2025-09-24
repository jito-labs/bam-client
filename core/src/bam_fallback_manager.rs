use {
    crate::bam_dependencies::BamDependencies,
    solana_clock::Slot,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::bank_forks::BankForks,
    std::{
        collections::BTreeSet,
        sync::{Arc, Mutex, RwLock},
        time::Instant,
    },
};

const MAX_FAIL_SLOTS_PER_CHECK: u32 = 3;

pub struct BamFallbackManager {
    thread: std::thread::JoinHandle<()>,
    slot_sender: crossbeam_channel::Sender<u64>,
    last_sent_slot: u64,
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

        // 400ms * 32 slots = ~12.8s; round to 15s
        const DURATION_BETWEEN_CHECKS: std::time::Duration = std::time::Duration::from_secs(15);
        let mut last_check_time = Instant::now();

        // For checking if we're in a leader slot when we need to fallback
        let shared_working_bank = poh_recorder.read().unwrap().shared_working_bank();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            while let Ok(slot) = slot_receiver.try_recv() {
                scheduled_leader_slots.insert(slot);
            }

            let now = Instant::now();
            if now.duration_since(last_check_time).as_secs() < DURATION_BETWEEN_CHECKS.as_secs() {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            last_check_time = now;

            let slots_below_threshold = {
                let current_slot = poh_recorder.read().unwrap().current_poh_slot();
                Self::num_leader_slots_missing_txns(
                    &mut scheduled_leader_slots,
                    current_slot,
                    &dependencies.bank_forks,
                    *bam_txns_per_slot_threshold.read().unwrap(),
                )
            };

            if slots_below_threshold > MAX_FAIL_SLOTS_PER_CHECK {
                // Wait until not in a leader slot to disconnect from BAM
                while let Some(bank) = shared_working_bank.load() {
                    if !bank.is_frozen() {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    } else {
                        break;
                    }
                }
                error!("BAM Fallback triggered! Disconnecting from BAM");
                // Clear BAM URL to trigger fallback
                *bam_url.lock().unwrap() = None;
                scheduled_leader_slots.clear();
            }

            info!(
                "leader_slots_tracked={:?}, slots_below_threshold={}",
                scheduled_leader_slots.len(),
                slots_below_threshold
            );
        }
        warn!("BAM Fallback Manager thread exiting");
    }

    /// Check the tracked leader slots for non-vote transaction counts below the threshold.
    /// Returns the count of slots that hit the threshold. Early-outs if the count reaches the MAX_FAIL_SLOTS_PER_CHECK.
    /// Also cleans up old slots beyond the valid window (32-64 slots old).
    fn num_leader_slots_missing_txns(
        scheduled_leader_slots: &mut BTreeSet<Slot>,
        current_slot: Slot,
        bank_forks: &Arc<RwLock<BankForks>>,
        bam_txns_per_slot_threshold: u64,
    ) -> u32 {
        let mut threshold_hit_count = 0;
        let mut slots_to_remove = Vec::new();

        let (root_slot, ancestors, min_valid_slot, max_valid_slot) = {
            let bank_forks = bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let root_slot = root_bank.slot();
            let ancestors = root_bank.proper_ancestors_set();

            let min = current_slot.saturating_sub(64);
            let max = current_slot.saturating_sub(32).max(root_slot);

            (root_slot, ancestors, min, max)
        };

        for &slot in scheduled_leader_slots.range(min_valid_slot..=max_valid_slot) {
            if !ancestors.contains(&slot) && slot != root_slot {
                // remove non-rooted slots
                slots_to_remove.push(slot);
                continue;
            }

            let slot_non_vote_txs = {
                let bank_forks = bank_forks.read().unwrap();
                bank_forks.get(slot).and_then(|bank| {
                    let non_vote_tx_count = bank.non_vote_transaction_count_since_restart();
                    let parent_count = bank
                        .parent()
                        .map(|p| p.non_vote_transaction_count_since_restart())
                        .unwrap_or(0);

                    let slot_txs = non_vote_tx_count.saturating_sub(parent_count);

                    if slot_txs < bam_txns_per_slot_threshold {
                        Some(slot_txs)
                    } else {
                        // Remove slots that are above the threshold
                        slots_to_remove.push(slot);
                        None
                    }
                })
            };

            if let Some(txs) = slot_non_vote_txs {
                threshold_hit_count += 1;
                warn!(
                    "BAM Fallback: Slot {} has non-vote tx count {} < threshold {}",
                    slot, txs, bam_txns_per_slot_threshold
                );
            }
        }

        // Clean up stale, passing (above-threshold) and non-rooted slots
        scheduled_leader_slots
            .retain(|&slot| slot >= min_valid_slot && !slots_to_remove.contains(&slot));

        threshold_hit_count
    }

    pub fn send_slot(&mut self, slot: Slot) -> Result<(), crossbeam_channel::TrySendError<Slot>> {
        if slot <= self.last_sent_slot {
            return Ok(());
        }
        self.last_sent_slot = slot;
        self.slot_sender.try_send(slot)
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
            &mut scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result, 1);
        assert_eq!(scheduled_leader_slots.len(), 1);
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
            &mut scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result, 0);
        assert!(scheduled_leader_slots.is_empty());
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
        // Only root the first bank
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
            &mut scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result, 0);
        assert!(scheduled_leader_slots.is_empty());
    }

    #[test]
    fn test_num_leader_slots_missing_txns_stale_slots_cleanup() {
        let genesis = create_genesis_config(10_000_000_000);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank);

        let mut scheduled_leader_slots = BTreeSet::new();
        // Add slots that will be outside the valid window
        scheduled_leader_slots.insert(10);
        scheduled_leader_slots.insert(20);
        scheduled_leader_slots.insert(30);

        let current_slot = 100u64;
        let threshold = 10u64;

        let result = BamFallbackManager::num_leader_slots_missing_txns(
            &mut scheduled_leader_slots,
            current_slot,
            &bank_forks,
            threshold,
        );

        assert_eq!(result, 0);
        assert!(scheduled_leader_slots.is_empty());
    }
}
