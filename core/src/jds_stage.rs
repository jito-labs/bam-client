use std::sync::{atomic::AtomicBool, Arc};

pub(crate) struct JdsStage {}

impl JdsStage {
    pub fn new(jds_enabled: Arc<AtomicBool>) -> Self {
        JdsStage {}
    }
}