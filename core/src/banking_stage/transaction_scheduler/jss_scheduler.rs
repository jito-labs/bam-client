/// A Scheduled implementation that pulls batches off the container, and then
/// schedules them to workers in a FIFO, account-aware manner. This is facilitated by the
/// `PrioGraph` data structure, which is a directed graph that tracks the dependencies.
/// Currently a very simple implementation that probably under pipelines the workers.
use std::time::Instant;
use {
    super::{
        jss_receive_and_buffer::priority_to_seq_id,
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_error::SchedulerError,
        transaction_priority_id::TransactionPriorityId,
        transaction_state::SanitizedTransactionTTL,
        transaction_state_container::StateContainer,
    },
    crate::banking_stage::{
        decision_maker::BufferedPacketsDecision,
        scheduler_messages::{
            ConsumeWork, FinishedConsumeWork, NotCommittedReason, TransactionBatchId,
            TransactionResult,
        },
        transaction_scheduler::jss_utils::convert_txn_error_to_proto,
    },
    ahash::HashMap,
    crossbeam_channel::{Receiver, Sender},
    jito_protos::proto::{
        jss_api::{start_scheduler_message::Msg, StartSchedulerMessage},
        jss_types::{bundle_result, not_committed::Reason, SchedulingError},
    },
    prio_graph::{AccessKind, GraphNode, PrioGraph},
    solana_pubkey::Pubkey,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_sdk::clock::Slot,
    solana_svm_transaction::svm_message::SVMMessage,
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

const MAX_SCHEDULED_PER_WORKER: usize = 3;

pub struct JssScheduler<Tx: TransactionWithMeta> {
    workers_scheduled_count: Vec<usize>,
    consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    response_sender: Sender<StartSchedulerMessage>,

    next_batch_id: u64,
    inflight_batch_info: HashMap<TransactionBatchId, InflightBatchInfo>,
    prio_graph: SchedulerPrioGraph,
    slot: Option<Slot>,
}

// A structure to hold information about inflight batches.
// A batch can either be one 'revert_on_error' batch or multiple
// 'non-revert_on_error' batches that are scheduled together.
struct InflightBatchInfo {
    pub priority_ids: Vec<TransactionPriorityId>,
    pub worker_index: usize,
    pub slot: Slot,
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
            next_batch_id: 0,
            inflight_batch_info: HashMap::default(),
            prio_graph: PrioGraph::new(passthrough_priority),
            slot: None,
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

    /// Insert all incoming transactions into the `PrioGraph`.
    fn pull_into_prio_graph<S: StateContainer<Tx>>(&mut self, container: &mut S) {
        while let Some(next_batch_id) = container.pop() {
            let Some((batch_ids, _)) = container.get_batch(next_batch_id.id) else {
                error!("Batch {} not found in container", next_batch_id.id);
                continue;
            };
            let txns = batch_ids
                .iter()
                .filter_map(|txn_id| container.get_transaction_ttl(*txn_id));

            self.prio_graph.insert_transaction(
                next_batch_id,
                Self::get_transactions_account_access(txns.into_iter()),
            );
        }
    }

    fn get_best_available_worker(&mut self) -> Option<usize> {
        let mut best_worker_index = None;
        let mut best_worker_count = MAX_SCHEDULED_PER_WORKER;
        for (worker_index, count) in self.workers_scheduled_count.iter_mut().enumerate() {
            if *count == 0 {
                return Some(worker_index);
            }
            if *count < MAX_SCHEDULED_PER_WORKER &&
                (best_worker_index.is_none() || *count < best_worker_count)
            {
                best_worker_index = Some(worker_index);
                best_worker_count = *count;
            }
        }
        best_worker_index
    }

    fn send_to_workers(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        num_scheduled: &mut usize,
    ) {
        let Some(slot) = self.slot else {
            warn!("Slot is not set, cannot schedule transactions");
            return;
        };

        // Schedule any available transactions in prio-graph
        while let Some(worker_index) = self.get_best_available_worker() {
            let batches_for_scheduling = self.get_batches_for_scheduling(container);
            if batches_for_scheduling.is_empty() {
                break;
            }
            for (priority_ids, revert_on_error) in batches_for_scheduling {
                let batch_id = self.get_next_schedule_id();
                let txn_ids = priority_ids
                    .iter()
                    .filter_map(|priority_id| container.get_batch(priority_id.id))
                    .flat_map(|(batch_ids, _)| batch_ids.into_iter())
                    .collect::<Vec<_>>();
                let work = Self::generate_work(batch_id, txn_ids, revert_on_error, container);
                self.send_to_worker(worker_index, priority_ids, work, slot);
                *num_scheduled += 1;
            }
        }
    }

    /// Get batches of transactions for scheduling.
    /// Build a normal txn batch up to a maximum of `MAX_TXN_PER_BATCH` transactions;
    /// but if a 'revert_on_error' batch is encountered, the WIP batch is sent immediately
    /// and the 'revert_on_error' batch is sent afterwards.
    fn get_batches_for_scheduling(
        &mut self,
        container: &mut impl StateContainer<Tx>,
    ) -> Vec<(Vec<TransactionPriorityId>, bool)> {
        const MAX_TXN_PER_BATCH: usize = 16;
        let mut result = vec![];
        let mut current_batch_ids = vec![];
        while let Some(next_batch_id) = self.prio_graph.pop() {
            let Some((_, revert_on_error)) = container.get_batch(next_batch_id.id) else {
                continue;
            };

            if revert_on_error {
                if !current_batch_ids.is_empty() {
                    result.push((std::mem::take(&mut current_batch_ids), false));
                }
                result.push((vec![next_batch_id], true));
                break;
            } else {
                current_batch_ids.push(next_batch_id);
            }

            if current_batch_ids.len() >= MAX_TXN_PER_BATCH {
                break;
            }
        }
        if !current_batch_ids.is_empty() {
            result.push((current_batch_ids, false));
        }
        result
    }

    fn send_to_worker(
        &mut self,
        worker_index: usize,
        priority_ids: Vec<TransactionPriorityId>,
        work: ConsumeWork<Tx>,
        slot: Slot,
    ) {
        let consume_work_sender = &self.consume_work_senders[worker_index];
        let batch_id = work.batch_id;
        let _ = consume_work_sender.send(work);
        self.inflight_batch_info.insert(
            batch_id,
            InflightBatchInfo {
                priority_ids,
                worker_index,
                slot,
            },
        );
        self.workers_scheduled_count[worker_index] += 1;
    }

    fn get_next_schedule_id(&mut self) -> TransactionBatchId {
        let result = TransactionBatchId::new(self.next_batch_id);
        self.next_batch_id += 1;
        result
    }

    fn generate_work(
        batch_id: TransactionBatchId,
        ids: Vec<usize>,
        revert_on_error: bool,
        container: &mut impl StateContainer<Tx>,
    ) -> ConsumeWork<Tx> {
        let transactions = ids
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

        ConsumeWork {
            batch_id,
            ids,
            transactions,
            max_ages,
            revert_on_error,
            respond_with_extra_info: true,
        }
    }

    fn send_no_leader_slot_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::OutsideLeaderSlot as i32,
                            )),
                        },
                    )),
                },
            )),
        });
    }

    fn send_back_result(&self, seq_id: u32, result: bundle_result::Result) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(result),
                },
            )),
        });
    }

    /// Generates a `bundle_result::Result` based on the processed results for 'revert_on_error' batches.
    fn generate_revert_on_error_bundle_result(
        processed_results: &[TransactionResult],
    ) -> bundle_result::Result {
        if processed_results
            .iter()
            .all(|result| matches!(result, TransactionResult::Committed(_)))
        {
            let transaction_results = processed_results
                .iter()
                .filter_map(|result| {
                    if let TransactionResult::Committed(processed) = result {
                        Some(processed.clone())
                    } else {
                        None
                    }
                })
                .collect();
            bundle_result::Result::Committed(jito_protos::proto::jss_types::Committed {
                transaction_results,
            })
        } else {
            // Get first NotCommit Reason that is not BatchRevert
            let (index, not_commit_reason) = processed_results
                .iter()
                .enumerate()
                .find_map(|(index, result)| {
                    if let TransactionResult::NotCommitted(reason) = result {
                        if matches!(reason, &NotCommittedReason::BatchRevert) {
                            None
                        } else {
                            Some((index, reason.clone()))
                        }
                    } else {
                        None
                    }
                })
                .unwrap_or((0, NotCommittedReason::PohTimeout));

            bundle_result::Result::NotCommitted(jito_protos::proto::jss_types::NotCommitted {
                reason: Some(Self::convert_reason_to_proto(index, not_commit_reason)),
            })
        }
    }

    /// Generates a `bundle_result::Result` based on the processed result of a single transaction.
    fn generate_bundle_result(processed: &TransactionResult) -> bundle_result::Result {
        match processed {
            TransactionResult::Committed(result) => {
                bundle_result::Result::Committed(jito_protos::proto::jss_types::Committed {
                    transaction_results: vec![result.clone()],
                })
            }
            TransactionResult::NotCommitted(reason) => {
                let (index, not_commit_reason) = match reason {
                    NotCommittedReason::PohTimeout => (0, NotCommittedReason::PohTimeout),
                    NotCommittedReason::BatchRevert => (0, NotCommittedReason::BatchRevert),
                    NotCommittedReason::Error(err) => (0, NotCommittedReason::Error(err.clone())),
                };
                bundle_result::Result::NotCommitted(jito_protos::proto::jss_types::NotCommitted {
                    reason: Some(Self::convert_reason_to_proto(index, not_commit_reason)),
                })
            }
        }
    }

    fn convert_reason_to_proto(
        index: usize,
        reason: NotCommittedReason,
    ) -> jito_protos::proto::jss_types::not_committed::Reason {
        match reason {
            NotCommittedReason::PohTimeout => {
                jito_protos::proto::jss_types::not_committed::Reason::SchedulingError(
                    SchedulingError::PohTimeout as i32,
                )
            }
            // Should not happen, but just in case:
            NotCommittedReason::BatchRevert => {
                jito_protos::proto::jss_types::not_committed::Reason::GenericInvalid(
                    jito_protos::proto::jss_types::GenericInvalid {},
                )
            }
            NotCommittedReason::Error(err) => {
                jito_protos::proto::jss_types::not_committed::Reason::TransactionError(
                    jito_protos::proto::jss_types::TransactionError {
                        index: index as u32,
                        reason: convert_txn_error_to_proto(err) as i32,
                    },
                )
            }
        }
    }

    fn maybe_bank_boundary_actions(
        &mut self,
        decision: &BufferedPacketsDecision,
        container: &mut impl StateContainer<Tx>,
    ) {
        // Check if no bank or slot has changed
        let bank_start = decision.bank_start();
        if bank_start.map(|bs| bs.working_bank.slot()) == self.slot {
            return;
        }

        if let Some(bank_start) = bank_start {
            info!(
                "Bank boundary detected: slot changed from {:?} to {:?}",
                self.slot,
                bank_start.working_bank.slot()
            );
            self.slot = Some(bank_start.working_bank.slot());
        } else {
            info!("Bank boundary detected: slot changed to None");
            self.slot = None;
        }

        // Drain container and send back 'retryable'
        if self.slot.is_none() {
            while let Some(next_batch_id) = container.pop() {
                let seq_id = priority_to_seq_id(next_batch_id.priority);
                self.send_no_leader_slot_bundle_result(seq_id);
                container.remove_by_id(next_batch_id.id);
            }
        }

        // Unblock all transactions blocked by inflight batches
        // and then drain the prio-graph
        for (_, inflight_info) in self.inflight_batch_info.iter() {
            for priority_id in &inflight_info.priority_ids {
                self.prio_graph.unblock(priority_id);
            }
        }
        while let Some((next_batch_id, _)) = self.prio_graph.pop_and_unblock() {
            let seq_id = priority_to_seq_id(next_batch_id.priority);
            self.send_no_leader_slot_bundle_result(seq_id);
            container.remove_by_id(next_batch_id.id);
        }
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
        // Check if the slot/bank has changed; do what must be done
        // IMPORTANT: This must be called before the receiving code below
        self.maybe_bank_boundary_actions(decision, container);

        let mut num_transactions = 0;
        while let Ok(result) = self.finished_consume_work_receiver.try_recv() {
            num_transactions += result.work.ids.len();
            let batch_id = result.work.batch_id;
            let revert_on_error = result.work.revert_on_error;
            let Some(inflight_batch_info) = self.inflight_batch_info.remove(&batch_id) else {
                continue;
            };
            self.workers_scheduled_count[inflight_batch_info.worker_index] -= 1;

            let len = if revert_on_error {
                1
            } else {
                inflight_batch_info.priority_ids.len()
            };
            for (i, priority_id) in inflight_batch_info
                .priority_ids
                .iter()
                .enumerate()
                .take(len)
            {
                let seq_id = priority_to_seq_id(priority_id.priority);

                if let Some(extra_info) = result.extra_info.as_ref() {
                    let bundle_result = if revert_on_error {
                        Self::generate_revert_on_error_bundle_result(&extra_info.processed_results)
                    } else {
                        let Some(txn_result) = extra_info.processed_results.get(i) else {
                            warn!(
                                "Processed results for batch {} are missing for index {}",
                                batch_id.0, i
                            );
                            continue;
                        };
                        Self::generate_bundle_result(txn_result)
                    };
                    self.send_back_result(seq_id, bundle_result);
                }

                if Some(inflight_batch_info.slot) == self.slot {
                    self.prio_graph.unblock(priority_id);
                }

                container.remove_by_id(priority_id.id);
            }
        }

        Ok((num_transactions, 0))
    }
}
