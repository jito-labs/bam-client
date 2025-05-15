use std::sync::{atomic::AtomicBool, Arc, RwLock};

use jito_protos::proto::jss_types::Bundle;
use solana_runtime::bank_forks::BankForks;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;

use super::{receive_and_buffer::ReceiveAndBuffer, transaction_state_container::TransactionStateContainer};


pub struct JssReceiveAndBuffer {
    jss_enabled: Arc<AtomicBool>,
    bundle_receiver: crossbeam_channel::Receiver<Bundle>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl JssReceiveAndBuffer {
    pub fn new(
        jss_enabled: Arc<AtomicBool>,
        bundle_receiver: crossbeam_channel::Receiver<Bundle>,
        bank_forks: Arc<RwLock<BankForks>>
    ) -> Self {
        Self {
            jss_enabled,
            bundle_receiver,
            bank_forks,
        }
    }
}

impl ReceiveAndBuffer for JssReceiveAndBuffer {
    type Transaction = RuntimeTransaction<SanitizedTransaction>;
    type Container = TransactionStateContainer<Self::Transaction>;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        timing_metrics: &mut super::scheduler_metrics::SchedulerTimingMetrics,
        count_metrics: &mut super::scheduler_metrics::SchedulerCountMetrics,
        decision: &crate::banking_stage::decision_maker::BufferedPacketsDecision,
    ) -> Result<usize, ()> {
        todo!()
    }
}