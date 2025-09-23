
use {
    crate::bam_dependencies::BamDependencies,
    solana_poh::poh_recorder::PohRecorder,
    std::{
        collections::BTreeSet,
        sync::{Arc, RwLock, Mutex},
        time::Instant,
    },
};

use {
    solana_clock::Slot,
    solana_runtime::bank_forks::BankForks,
};

const CONSECUTIVE_SLOTS_THRESHOLD: u32 = 3;

pub struct BamFallbackManager {
    thread: std::thread::JoinHandle<()>,
    slot_sender: crossbeam_channel::Sender<u64>,
    previous_slot: u64,
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
                Self::run(exit, slot_receiver, poh_recorder, bam_txns_per_slot_threshold, bam_url, dependencies)
            }),
            slot_sender,
            previous_slot: 0,
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
        let mut leader_slots_to_track: BTreeSet<Slot> = BTreeSet::new();
        let mut consecutive_threshold_hits;
        const MAX_TRACKED_SLOTS: usize = 64;

        // 400ms * 32 slots = ~12.8s; round to 15s
        const DURATION_BETWEEN_CHECKS: std::time::Duration = std::time::Duration::from_secs(15);
        let mut last_check_time = Instant::now();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            while let Ok(slot) = slot_receiver.try_recv() {
                leader_slots_to_track.insert(slot);

                // Remove oldest if exceeding max tracked slots
                while leader_slots_to_track.len() > MAX_TRACKED_SLOTS {
                    if let Some(&oldest) = leader_slots_to_track.iter().next() {
                        leader_slots_to_track.remove(&oldest);
                    }
                }
            }

            let now = Instant::now();
            if now.duration_since(last_check_time).as_secs() < DURATION_BETWEEN_CHECKS.as_secs() {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            last_check_time = now;
            
            let current_slot = poh_recorder.read().unwrap().current_poh_slot();
            let slots_below_threshold = Self::check_txns_at_slot(
                &mut leader_slots_to_track,
                current_slot,
                &dependencies.bank_forks,
                *bam_txns_per_slot_threshold.read().unwrap(),
            );
            
            if slots_below_threshold > 0 {
                consecutive_threshold_hits = slots_below_threshold;
                
                if consecutive_threshold_hits >= CONSECUTIVE_SLOTS_THRESHOLD {
                    error!("BAM Fallback triggered! Disconnecting from BAM");
                    // Clear BAM URL to trigger fallback
                    *bam_url.lock().unwrap() = None;
                    leader_slots_to_track.clear();
                    consecutive_threshold_hits = 0;
                }
            } else {
                // Reset on successful check
                consecutive_threshold_hits = 0;
            }
            
            info!("slots_tracked={:?}, below_threshold={}", 
                  leader_slots_to_track.len(), consecutive_threshold_hits);
        }
        warn!("BAM Fallback Manager thread exiting");
    }

    /// Check the tracked leader slots for non-vote transaction counts below the threshold.
    /// Returns the count of slots that hit the threshold. Early-outs if the count reaches the consecutive_slots_threshold.
    /// Also cleans up old slots beyond the valid window (32-64 slots old).
    fn check_txns_at_slot(
        leader_slots_to_track: &mut BTreeSet<Slot>,
        current_slot: Slot,
        bank_forks: &Arc<RwLock<BankForks>>,
        bam_txns_per_slot_threshold: Slot,
    ) -> u32 {
        let mut threshold_hit_count = 0;
        
        let bank_forks = bank_forks.read().unwrap();
        let root = bank_forks.root();
        
        // Define the valid window for checking (32-64 slots old)
        // Only want to check slots that have reached consensus
        let min_valid_slot = current_slot.saturating_sub(64).max(root);
        let max_valid_slot = current_slot.saturating_sub(32);
        
        let checkable_slots: Vec<Slot> = leader_slots_to_track
            .range(min_valid_slot..=max_valid_slot)
            .copied()
            .collect();
        
        for slot in &checkable_slots {
            if let Some(slot_non_vote_txs) = Self::check_bank_at_slot(&bank_forks, *slot, bam_txns_per_slot_threshold) {
                threshold_hit_count += 1;
                warn!("BAM Fallback: Slot {} has non-vote tx count {} < threshold {}", 
                      slot, slot_non_vote_txs, bam_txns_per_slot_threshold);
            }
        }
        
        for slot in checkable_slots {
            leader_slots_to_track.remove(&slot);
        }
        
        let stale_slots: Vec<Slot> = leader_slots_to_track
            .range(..min_valid_slot)
            .copied()
            .collect();
        
        for slot in stale_slots {
            leader_slots_to_track.remove(&slot);
        }
        
        threshold_hit_count
    }

    /// Check if the bank at the given slot has non-vote transaction count below the threshold.
    /// Returns Some if the threshold is hit (i.e., non-vote tx count is below the threshold), with the count.
    /// Returns None otherwise.
    fn check_bank_at_slot(
        bank_forks: &BankForks,
        slot: u64,
        bam_txns_per_slot_threshold: u64,
    ) -> Option<u64> {
        let Some(bank) = bank_forks.get(slot) else {
            error!("Bank not found for slot {} in check_bank_at_slot", slot);
            return None;
        };
        
        let non_vote_tx_count = bank.non_vote_transaction_count_since_restart();
        let parent_non_vote_count = bank.parent()
            .map(|p| p.non_vote_transaction_count_since_restart())
            .unwrap_or(0);

        let slot_non_vote_txs = non_vote_tx_count.saturating_sub(parent_non_vote_count);

        if slot_non_vote_txs < bam_txns_per_slot_threshold {
            return Some(slot_non_vote_txs);
        }
        None
    }

    pub fn send_slot(&mut self, slot: Slot) -> bool {
        if slot <= self.previous_slot {
            return false;
        }
        self.previous_slot = slot;
        self.slot_sender.try_send(slot).is_ok()
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config},
        solana_signer::Signer,
        solana_system_interface::instruction::transfer as system_transfer,
        solana_transaction::Transaction,
    };

    #[test]
    fn check_bank_at_slot_returns_count_when_below_threshold() {
        let mut genesis = create_genesis_config(10_000_000_000);
        genesis
            .genesis_config
            .fee_rate_governor
            .lamports_per_signature = 5_000;
        let parent = std::sync::Arc::new(Bank::new_for_tests(&genesis.genesis_config));
        let slot = 100u64;
        let bank = Bank::new_from_parent(
            parent,
            &solana_pubkey::Pubkey::new_unique(),
            slot,
        );
        let bank_forks = BankForks::new_rw_arc(bank);
        let bank_ref = bank_forks.read().unwrap().get(slot).unwrap().clone();

        let payer = genesis.mint_keypair;
        let bh = bank_ref.last_blockhash();
        let transfer_ix = system_transfer(&payer.pubkey(), &payer.pubkey(), 1);
        let tx = Transaction::new_signed_with_payer(
            &[transfer_ix],
            Some(&payer.pubkey()),
            &[&payer],
            bh,
        );
        let _results = bank_ref.process_transaction(&tx);

        let threshold = 10u64;
        let forks_read = bank_forks.read().unwrap();
        let result = BamFallbackManager::check_bank_at_slot(&forks_read, slot, threshold);

        assert!(result.is_some());
        assert!(result.unwrap() < threshold);
    }

    #[test]
    fn check_bank_at_slot_returns_none_for_missing_bank() {
        let genesis = create_genesis_config(1_000_000).genesis_config;
        let parent = std::sync::Arc::new(Bank::new_for_tests(&genesis));
        let slot = 42u64;
        let bank = Bank::new_from_parent(parent, &solana_pubkey::Pubkey::default(), slot);
        let bank_forks = BankForks::new_rw_arc(bank);
        let forks_read = bank_forks.read().unwrap();

        let result = BamFallbackManager::check_bank_at_slot(&forks_read, slot + 1, 10);

        assert!(result.is_none());
    }
}