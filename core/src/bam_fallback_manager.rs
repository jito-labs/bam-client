
use {
    crate::bam_dependencies::BamDependencies,
    solana_poh::poh_recorder::PohRecorder,
    std::{
        collections::BTreeSet,
        sync::{Arc, RwLock},
        time::Instant,
    },
};

use {
    solana_clock::Slot,
    solana_runtime::bank_forks::BankForks,
};

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
        dependencies: BamDependencies,
    ) -> Self {
        let (slot_sender, slot_receiver) = crossbeam_channel::bounded(10_000);
        Self {
            thread: std::thread::spawn(move || {
                Self::run(exit, slot_receiver, poh_recorder, bam_txns_per_slot_threshold, dependencies)
            }),
            slot_sender,
            previous_slot: 0,
        }
    }

    fn run(
        exit: Arc<std::sync::atomic::AtomicBool>,
        slot_receiver: crossbeam_channel::Receiver<u64>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bam_txns_per_slot_threshold: Arc<RwLock<u64>>,
        dependencies: BamDependencies,
    ) {
        let mut leader_slots_to_track = BTreeSet::new();

        // 400ms * 32 slots = ~12.8s; round to 15s
        const DURATION_BETWEEN_CHECKS: std::time::Duration = std::time::Duration::from_secs(15);
        const CONSECUTIVE_SLOTS_THRESHOLD: u32 = 3;

        let mut last_check_time = Instant::now();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            while let Ok(slot) = slot_receiver.try_recv() {
                leader_slots_to_track.insert(slot);
            }

            let now = Instant::now();
            if now.duration_since(last_check_time).as_secs() < DURATION_BETWEEN_CHECKS.as_secs() {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            last_check_time = now;
            
            let current_slot = poh_recorder.read().unwrap().current_poh_slot();
            let threshold_hit_count = Self::check_txns_at_slot(
                &leader_slots_to_track,
                current_slot,
                &dependencies.bank_forks,
                *bam_txns_per_slot_threshold.read().unwrap(),
            );
            if threshold_hit_count >= CONSECUTIVE_SLOTS_THRESHOLD {
                error!("BAM Fallback triggered! Disconnecting from BAM");
                dependencies.bam_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                leader_slots_to_track.clear();
            }
            info!("slots_tracked={:?}", leader_slots_to_track);
        }

        warn!("BAM Fallback Manager thread exiting");
    }

    fn check_txns_at_slot(
        leader_slots_to_track: &BTreeSet<Slot>,
        current_slot: Slot,
        bank_forks: &Arc<RwLock<BankForks>>,
        bam_txns_per_slot_threshold: Slot,
    ) -> u32 {
        let mut threshold_hit_count = 0;

        for slot in leader_slots_to_track.iter().copied() {
            let bank_forks = bank_forks.read().unwrap();
            let root = bank_forks.root();

            if current_slot.saturating_sub(slot) < 32 || slot < root {
                continue;
            }

            if let Some(slot_non_vote_txs) = Self::check_bank_at_slot(&bank_forks, slot, bam_txns_per_slot_threshold) {
                threshold_hit_count += 1;
                warn!("BAM Fallback: Slot {} has non-vote tx count {} < threshold {}", slot, slot_non_vote_txs, bam_txns_per_slot_threshold);
            }
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