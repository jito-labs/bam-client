use std::time::Instant;

use ahash::HashMap;
use crossbeam_channel::{Receiver, Sender};
use jito_protos::proto::jss_api::{start_scheduler_message::Msg, StartSchedulerMessage};
use prio_graph::{AccessKind, GraphNode, PrioGraph, TopLevelId};
use solana_pubkey::Pubkey;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_svm_transaction::svm_message::SVMMessage;

use crate::banking_stage::scheduler_messages::{ConsumeWork, FinishedConsumeWork, TransactionBatchId};

use super::{
    scheduler::{Scheduler, SchedulingSummary}, scheduler_error::SchedulerError, transaction_priority_id::TransactionPriorityId, transaction_state::SanitizedTransactionTTL, transaction_state_container::StateContainer
};

type SchedulerPrioGraph = PrioGraph<
    TransactionPriorityId,
    Pubkey,
    TransactionPriorityId,
    fn(&TransactionPriorityId, &GraphNode<TransactionPriorityId>) -> TransactionPriorityId,
>;

#[inline(always)]
fn passthrough_priority(
    id: &TransactionPriorityId,
    _graph_node: &GraphNode<TransactionPriorityId>,
) -> TransactionPriorityId {
    *id
}

pub struct FifoBatchScheduler<Tx: TransactionWithMeta> {
    consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    response_sender: Sender<StartSchedulerMessage>,

    next_batch_id: u64,
    priority_ids: HashMap<usize, TransactionPriorityId>,
    prio_graph: SchedulerPrioGraph,
}

impl<Tx: TransactionWithMeta> FifoBatchScheduler<Tx> {
    pub fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        response_sender: Sender<StartSchedulerMessage>,
    ) -> Self {
        Self {
            consume_work_senders,
            finished_consume_work_receiver,
            response_sender,
            next_batch_id: 0,
            priority_ids: HashMap::default(),
            prio_graph: PrioGraph::new(passthrough_priority),
        }
    }

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_transaction_account_access(
        transaction: &SanitizedTransactionTTL<impl SVMMessage>,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + '_ {
        let message = &transaction.transaction;
        message
            .account_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| {
                if message.is_writable(index) {
                    (*key, AccessKind::Write)
                } else {
                    (*key, AccessKind::Read)
                }
            })
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx> for FifoBatchScheduler<Tx> {
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        _pre_graph_filter: impl Fn(&[&Tx], &mut [bool]),
        pre_lock_filter: impl Fn(&Tx) -> bool,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let start_time = Instant::now();
        let mut num_scheduled = 0;

        // Insert all incoming transactions into the prio-graph
        while let Some(next_bundle_id) = container.pop() {
            let Some(txn) = container.get_transaction_ttl(next_bundle_id.id) else {
                continue;
            };
            self.prio_graph.insert_transaction(next_bundle_id, Self::get_transaction_account_access(&txn));
            self.priority_ids.insert(
                next_bundle_id.id,
                next_bundle_id,
            );
            info!("inserted transaction to prio-graph");
        }

        // Schedule any available transactions in prio-graph
        while let Some(next_id) = self.prio_graph.pop() {
            // Get from container
            let Some(txn) = container.get_mut_transaction_state(next_id.id) else {
                continue;
            };
            let sanitized_transaction_ttl = txn.transition_to_pending();
            
            if !pre_lock_filter(&sanitized_transaction_ttl.transaction) {
                continue;
            }

            // Choose a completely random worker index (lol)
            let worker_index = rand::random::<usize>() % self.consume_work_senders.len();
            let consume_work_sender = &self.consume_work_senders[worker_index];

            let work = ConsumeWork {
                batch_id: TransactionBatchId::new(self.next_batch_id),
                ids: vec![next_id.id],
                transactions: vec![sanitized_transaction_ttl.transaction],
                max_ages: vec![sanitized_transaction_ttl.max_age],
                revert_on_error: false,
                respond_with_extra_info: false,
            };
            // Send the work to the worker
            let _ = consume_work_sender
                .send(work);

            self.next_batch_id += 1;
            num_scheduled += 1;
        }

        Ok(SchedulingSummary {
            num_scheduled,
            num_unschedulable: 0,
            num_filtered_out: 0,
            filter_time_us: start_time.elapsed().as_micros() as u64,
        })
    }

    fn receive_completed(
        &mut self,
        container: &mut impl StateContainer<Tx>,
    ) -> Result<(usize, usize), SchedulerError> {
        let mut num_transactions = 0;
        while let Ok(result) = self.finished_consume_work_receiver.try_recv() {
            num_transactions += result.work.ids.len();

            // Send the result back to the scheduler
            if let Some(extra_info) = result.extra_info {
                for (bundle_result, seq_id) in
                    extra_info.results.into_iter().zip(result.work.ids.iter())
                {
                    let _ = self.response_sender.try_send(StartSchedulerMessage {
                        msg: Some(Msg::BundleResult(
                            jito_protos::proto::jss_types::BundleResult {
                                seq_id: *seq_id as u32,
                                result: Some(bundle_result),
                            },
                        )),
                    });
                }
            }

            // Unblock the next blocked bundle in the prio-graph
            for id in result.work.ids {
                let Some(priority_id) = self.priority_ids.remove(&id) else {
                    continue;
                };
                container.remove_by_id(id);
                self.prio_graph.unblock(&priority_id);
            }
        }

        Ok((num_transactions, 0))
    }
}
