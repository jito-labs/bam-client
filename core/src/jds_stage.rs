use std::sync::{atomic::AtomicBool, Arc, RwLock};

use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::bank_forks::BankForks;

pub(crate) struct JdsStage {}

impl JdsStage {
    pub fn new(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        JdsStage {}
    }
}