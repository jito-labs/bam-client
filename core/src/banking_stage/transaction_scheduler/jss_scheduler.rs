/// A Scheduled implementation that pulls batches off the container, and then
/// schedules them to workers in a FIFO, account-aware manner. This is facilitated by the
/// `PrioGraph` data structure, which is a directed graph that tracks the dependencies.
/// Currently a very simple implementation that probably under pipelines the workers.
use std::time::Instant;

use ahash::HashMap;
use crossbeam_channel::{Receiver, Sender};
use jito_protos::proto::{
    jss_api::{start_scheduler_message::Msg, StartSchedulerMessage},
    jss_types::bundle_result,
};
use prio_graph::{AccessKind, GraphNode, PrioGraph};
use solana_pubkey::Pubkey;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_sdk::clock::Slot;
use solana_svm_transaction::svm_message::SVMMessage;

use crate::banking_stage::{
    decision_maker::BufferedPacketsDecision,
    scheduler_messages::{ConsumeWork, FinishedConsumeWork, TransactionBatchId},
};

use super::{
    scheduler::{Scheduler, SchedulingSummary},
    scheduler_error::SchedulerError,
    transaction_priority_id::TransactionPriorityId,
    transaction_state::SanitizedTransactionTTL,
    transaction_state_container::StateContainer,
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

const MAX_SCHEDULED_PER_WORKER: usize = 2;

pub struct JssScheduler<Tx: TransactionWithMeta> {
    workers_scheduled_count: Vec<usize>,
    consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    response_sender: Sender<StartSchedulerMessage>,

    inflight_batch_info: HashMap<TransactionBatchId, InflightBatchInfo>,
    prio_graph: SchedulerPrioGraph,
    slot: Slot,
}

struct InflightBatchInfo {
    pub priority_id: TransactionPriorityId,
    pub worker_index: usize,
}

impl<Tx: TransactionWithMeta> JssScheduler<Tx> {
    pub fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        response_sender: Sender<StartSchedulerMessage>,
    ) -> Self {
        Self {
            workers_scheduled_count: vec![0; consume_work_senders.len()],
            consume_work_senders,
            finished_consume_work_receiver,
            response_sender,
            inflight_batch_info: HashMap::default(),
            prio_graph: PrioGraph::new(passthrough_priority),
            slot: 0,
        }
    }

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_transactions_account_access<'a>(
        transactions: impl Iterator<Item = &'a SanitizedTransactionTTL<impl SVMMessage + 'a>> + 'a,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + 'a {
        transactions.flat_map(|transaction| {
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

    fn pull_into_prio_graph<S: StateContainer<Tx>>(&mut self, container: &mut S) {
        // Insert all incoming transactions into the prio-graph
        while let Some(next_batch_id) = container.pop() {
            let Some((batch_ids, _)) = container.get_batch(next_batch_id.id) else {
                panic!("Batch {} not found in container", next_batch_id.id);
            };

            let txns = batch_ids
                .iter()
                .filter_map(|txn_id| container.get_transaction_ttl(*txn_id))
                .collect::<Vec<_>>();
            if txns.len() != batch_ids.len() {
                error!("All batch transactions not found in container");
                continue;
            }

            self.prio_graph.insert_transaction(
                next_batch_id,
                Self::get_transactions_account_access(txns.into_iter()),
            );
        }
    }

    fn get_available_worker(&mut self) -> Option<usize> {
        // Find a worker that has less than the max scheduled transactions
        for (worker_index, count) in self.workers_scheduled_count.iter_mut().enumerate() {
            if *count < MAX_SCHEDULED_PER_WORKER {
                return Some(worker_index);
            }
        }
        None
    }

    fn send_to_workers(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        num_scheduled: &mut usize,
    ) {
        // Schedule any available transactions in prio-graph
        while let Some(worker_index) = self.get_available_worker() {
            let Some(next_batch_id) = self.prio_graph.pop() else {
                break;
            };

            let Some((batch_ids, revert_on_error)) = container.get_batch(next_batch_id.id) else {
                panic!("Batch {} not found in container", next_batch_id.id);
            };

            let transactions = batch_ids
                .iter()
                .filter_map(|txn_id| {
                    let result = container.get_mut_transaction_state(*txn_id)?;
                    let result = result.transition_to_pending();
                    Some(result)
                })
                .collect::<Vec<_>>();

            let max_ages = transactions
                .iter()
                .map(|txn| txn.max_age)
                .collect::<Vec<_>>();

            let transactions = transactions
                .into_iter()
                .map(|txn| txn.transaction)
                .collect::<Vec<_>>();

            let work = ConsumeWork {
                batch_id: TransactionBatchId::new(next_batch_id.id as u64),
                ids: batch_ids,
                transactions: transactions,
                max_ages,
                revert_on_error,
                respond_with_extra_info: true,
            };
            // Send the work to the worker
            let consume_work_sender = &self.consume_work_senders[worker_index];
            let _ = consume_work_sender.send(work);
            self.inflight_batch_info.insert(
                TransactionBatchId::new(next_batch_id.id as u64),
                InflightBatchInfo {
                    priority_id: next_batch_id,
                    worker_index,
                },
            );
            self.workers_scheduled_count[worker_index] += 1;

            *num_scheduled += 1;
        }
    }

    fn send_retryable_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id: seq_id,
                    result: Some(bundle_result::Result::Retryable(
                        jito_protos::proto::jss_types::Retryable {},
                    )),
                },
            )),
        });
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx> for JssScheduler<Tx> {
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        _pre_graph_filter: impl Fn(&[&Tx], &mut [bool]),
        _pre_lock_filter: impl Fn(&Tx) -> bool,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let start_time = Instant::now();
        let mut num_scheduled = 0;

        self.pull_into_prio_graph(container);
        self.send_to_workers(container, &mut num_scheduled);

        Ok(SchedulingSummary {
            num_scheduled,
            num_unschedulable: 0,
            num_filtered_out: 0,
            filter_time_us: start_time.elapsed().as_micros() as u64,
        })
    }

    /// Receive completed batches of transactions without blocking.
    /// This also handles checking if the slot has ended and if so, it will
    /// drain the container and prio-graph, sending back 'retryable' results
    /// back to JSS.
    fn receive_completed(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        decision: &BufferedPacketsDecision,
    ) -> Result<(usize, usize), SchedulerError> {
        let mut num_transactions = 0;
        while let Ok(result) = self.finished_consume_work_receiver.try_recv() {
            num_transactions += result.work.ids.len();

            // Send the result back to the scheduler
            if let Some(extra_info) = result.extra_info {
                for bundle_result in extra_info.results.into_iter() {
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
            let Some(inflight_batch_info) = self.inflight_batch_info.remove(&batch_id) else {
                error!("Batch {} not found in inflight info", batch_id.0);
                continue;
            };
            self.prio_graph.unblock(&inflight_batch_info.priority_id);
            container.remove_by_id(inflight_batch_info.priority_id.id);
            self.workers_scheduled_count[inflight_batch_info.worker_index] -= 1;
        }

        let Some(bank_start) = decision.bank_start() else {
            return Ok((num_transactions, 0));
        };
        let incoming_slot = bank_start.working_bank.slot();
        if self.slot == incoming_slot {
            return Ok((num_transactions, 0));
        }

        self.slot = incoming_slot;

        // Drain container and send back 'retryable'
        while let Some(next_batch_id) = container.pop() {
            self.send_retryable_bundle_result(next_batch_id.id as u32);
            container.remove_by_id(next_batch_id.id);
        }

        // Drain prio-graph and send back 'retryable'
        while let Some((next_batch_id, _)) = self.prio_graph.pop_and_unblock() {
            self.send_retryable_bundle_result(next_batch_id.id as u32);
            self.inflight_batch_info
                .remove(&TransactionBatchId::new(next_batch_id.id as u64));
        }

        Ok((num_transactions, 0))
    }
}
