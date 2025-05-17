use std::time::Instant;

use ahash::HashMap;
use crossbeam_channel::{Receiver, Sender};
use jito_protos::proto::jss_api::{start_scheduler_message::Msg, StartSchedulerMessage};
use prio_graph::{AccessKind, GraphNode, PrioGraph};
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

    priority_ids: HashMap<TransactionBatchId, TransactionPriorityId>,
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
            priority_ids: HashMap::default(),
            prio_graph: PrioGraph::new(passthrough_priority),
        }
    }

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_transactions_account_access<'a>(
        transactions: impl Iterator<Item = &'a SanitizedTransactionTTL<impl SVMMessage + 'a>> + 'a,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + 'a {
        transactions
            .flat_map(|transaction| {
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
        while let Some(next_batch_id) = container.pop() {
            let Some((batch_ids, _)) = container.get_batch(next_batch_id.id) else {
                continue;
            };

            let txns = batch_ids
                .iter()
                .filter_map(|txn_id| {
                    container.get_transaction_ttl(*txn_id)
                })
                .collect::<Vec<_>>();
            if txns.len() != batch_ids.len() {
                container.push_ids_into_queue(std::iter::once(next_batch_id));
                continue;
            }

            self.prio_graph.insert_transaction(next_batch_id, Self::get_transactions_account_access(txns.into_iter()));
            info!("Pushed bundle into prio-graph");
        }

        // Schedule any available transactions in prio-graph
        while let Some(next_batch_id) = self.prio_graph.pop() {
            let Some((batch_ids, revert_on_error)) = container.get_batch(next_batch_id.id) else {
                info!("Batch {} not found in container", next_batch_id.id);
                continue;
            };

            info!("Scheduling batch {} revert_on_error={}", next_batch_id.id, revert_on_error);

            let transactions = batch_ids
                .iter()
                .filter_map(|txn_id| {
                    let result = container.get_mut_transaction_state(*txn_id)?;
                    let result = result.transition_to_pending();
                    if !pre_lock_filter(&result.transaction) {
                        return None;
                    }
                    Some(result)
                })
                .collect::<Vec<_>>();
    
            let max_ages = transactions.iter().map(|txn| {
                txn.max_age
            }).collect::<Vec<_>>();

            let transactions = transactions
                .into_iter()
                .map(|txn| {
                    txn.transaction
                })
                .collect::<Vec<_>>();

            // Choose a completely random worker index (lol)
            let worker_index = rand::random::<usize>() % self.consume_work_senders.len();
            let consume_work_sender = &self.consume_work_senders[worker_index];

            let work = ConsumeWork {
                batch_id: TransactionBatchId::new(next_batch_id.id as u64),
                ids: batch_ids,
                transactions: transactions,
                max_ages,
                revert_on_error,
                respond_with_extra_info: true,
            };
            // Send the work to the worker
            let _ = consume_work_sender
                .send(work);
            self.priority_ids.insert(
                TransactionBatchId::new(next_batch_id.id as u64),
                next_batch_id,
            );

            info!("Sent batch {} to worker {}", next_batch_id.id, worker_index);

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
            info!("Received result for batch {}", result.work.batch_id);
            num_transactions += result.work.ids.len();

            // Send the result back to the scheduler
            if let Some(extra_info) = result.extra_info {
                info!("Received extra info for batch {}", result.work.batch_id);
                for (bundle_result, seq_id) in
                    extra_info.results.into_iter().zip(result.work.ids.iter())
                {
                    let _ = self.response_sender.try_send(StartSchedulerMessage {
                        msg: Some(Msg::BundleResult(
                            jito_protos::proto::jss_types::BundleResult {
                                seq_id: result.work.batch_id.0 as u32,
                                result: Some(bundle_result),
                            },
                        )),
                    });
                }
            }

            let batch_id = result.work.batch_id;
            let Some(priority_id) = self.priority_ids.remove(&batch_id) else {
                info!("Batch {} not found in priority ids", batch_id);
                continue;
            };
            let Some((batch_ids, _)) = container.get_batch(priority_id.id) else {
                info!("Batch {} not found in container", priority_id.id);
                continue;
            };
            // Remove all the transactions
            for id in batch_ids {
                container.remove_by_id(id);
            }

            // Remove the batch
            self.prio_graph.unblock(&priority_id);
            container.remove_by_id(priority_id.id);
        }

        Ok((num_transactions, 0))
    }
}
