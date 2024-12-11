use std::sync::{atomic::AtomicBool, Arc, RwLock};

use crossbeam_channel::Receiver;
use solana_perf::packet::PacketBatch;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::bank_forks::BankForks;

use crate::sigverify::SigverifyTracerPacketStats;

pub(crate) struct JdsStage {}

impl JdsStage {
    pub fn new(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        tpu_vote_receiver: Receiver<Arc<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        JdsStage {}
    }
}