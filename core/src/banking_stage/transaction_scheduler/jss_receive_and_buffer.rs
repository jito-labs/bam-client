use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use crossbeam_channel::Sender;
use jito_protos::proto::{
    jss_api::{start_scheduler_message::Msg, StartSchedulerMessage},
    jss_types::{bundle_result, Bundle},
};
use solana_runtime::bank_forks::BankForks;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;

use crate::banking_stage::decision_maker::BufferedPacketsDecision;

use super::{
    receive_and_buffer::ReceiveAndBuffer, transaction_state_container::TransactionStateContainer,
};

pub struct JssReceiveAndBuffer {
    jss_enabled: Arc<AtomicBool>,
    bundle_receiver: crossbeam_channel::Receiver<Bundle>,
    response_sender: Sender<StartSchedulerMessage>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl JssReceiveAndBuffer {
    pub fn new(
        jss_enabled: Arc<AtomicBool>,
        bundle_receiver: crossbeam_channel::Receiver<Bundle>,
        response_sender: Sender<StartSchedulerMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        Self {
            jss_enabled,
            bundle_receiver,
            response_sender,
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
        if !self.jss_enabled.load(Ordering::Relaxed) {
            return Ok(0);
        }

        let mut result = 0;
        match decision {
            BufferedPacketsDecision::Consume(bank_start) => {
                while let Ok(bundle) = self.bundle_receiver.try_recv() {
                    let mut transactions = Vec::new();
                    if container.insert_new_batch(
                        transaction_ttl,
                        packets,
                        bundle.seq_id as u64,
                        0, /*TODO_DG*/
                    ) {
                        result += bundle.packets.len();
                    }
                }
            }
            BufferedPacketsDecision::ForwardAndHold
            | BufferedPacketsDecision::Forward
            | BufferedPacketsDecision::Hold => {
                while let Ok(bundle) = self.bundle_receiver.try_recv() {
                    let _ = self.response_sender.try_send(StartSchedulerMessage {
                        msg: Some(Msg::BundleResult(
                            jito_protos::proto::jss_types::BundleResult {
                                seq_id: bundle.seq_id as u32,
                                result: Some(bundle_result::Result::Retryable(
                                    (jito_protos::proto::jss_types::Retryable {}),
                                )),
                            },
                        )),
                    });
                }
            }
        }

        Ok(result)
    }
}
