/// A Scheduled implementation that pulls batches off the container, and then
/// schedules them to workers in a FIFO, account-aware manner. This is facilitated by the
/// `PrioGraph` data structure, which is a directed graph that tracks the dependencies.
use std::time::Instant;
use {
    super::{
        bam_receive_and_buffer::priority_to_seq_id,
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
        transaction_scheduler::bam_utils::convert_txn_error_to_proto,
    },
    ahash::HashMap,
    crossbeam_channel::{Receiver, Sender},
    jito_protos::proto::{
        bam_api::{start_scheduler_message_v0::Msg, StartSchedulerMessageV0},
        bam_types::{atomic_txn_batch_result, not_committed::Reason, SchedulingError},
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

const MAX_SCHEDULED_PER_WORKER: usize = 10;
const MAX_TXN_PER_BATCH: usize = 1;

pub struct BamScheduler<Tx: TransactionWithMeta> {
    workers_scheduled_count: Vec<usize>,
    consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    response_sender: Sender<StartSchedulerMessageV0>,

    next_batch_id: u64,
    inflight_batch_info: HashMap<TransactionBatchId, InflightBatchInfo>,
    prio_graph: SchedulerPrioGraph,
    slot: Option<Slot>,

    // Reusable objects to avoid allocations
    reusable_consume_work: Vec<ConsumeWork<Tx>>,
    reusable_priority_ids: Vec<Vec<TransactionPriorityId>>,
    reusable_batches_for_scheduling: Vec<(Vec<TransactionPriorityId>, bool)>,
}

// A structure to hold information about inflight batches.
// A batch can either be one 'revert_on_error' batch or multiple
// 'non-revert_on_error' batches that are scheduled together.
struct InflightBatchInfo {
    pub priority_ids: Vec<TransactionPriorityId>,
    pub worker_index: usize,
    pub slot: Slot,
}

impl<Tx: TransactionWithMeta> BamScheduler<Tx> {
    pub fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        response_sender: Sender<StartSchedulerMessageV0>,
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
            reusable_consume_work: Vec::new(),
            reusable_priority_ids: Vec::new(),
            reusable_batches_for_scheduling: Vec::new(),
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
            let Some((batch_ids, _, _)) = container.get_batch(next_batch_id.id) else {
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
            if best_worker_index.is_none() || *count < best_worker_count {
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
        let mut batches_for_scheduling = std::mem::take(&mut self.reusable_batches_for_scheduling);
        while let Some(worker_index) = self.get_best_available_worker() {
            self.get_batches_for_scheduling(&mut batches_for_scheduling, container, slot);
            if batches_for_scheduling.is_empty() {
                break;
            }
            for (priority_ids, revert_on_error) in batches_for_scheduling.drain(..) {
                let len = priority_ids.len();
                let batch_id = self.get_next_schedule_id();
                let mut work = self.get_or_create_work_object();
                Self::generate_work(
                    &mut work,
                    batch_id,
                    &priority_ids,
                    revert_on_error,
                    container,
                    slot,
                );
                self.send_to_worker(worker_index, priority_ids, work, slot);
                *num_scheduled += len;
            }
        }
        std::mem::swap(
            &mut self.reusable_batches_for_scheduling,
            &mut batches_for_scheduling,
        );
    }

    /// Get batches of transactions for scheduling.
    /// Build a normal txn batch up to a maximum of `MAX_TXN_PER_BATCH` transactions;
    /// but if a 'revert_on_error' batch is encountered, the WIP batch is finalized
    /// and the 'revert_on_error' batch is appended to the result.
    fn get_batches_for_scheduling(
        &mut self,
        result: &mut Vec<(Vec<TransactionPriorityId>, bool)>,
        container: &mut impl StateContainer<Tx>,
        current_slot: Slot,
    ) {
        let mut current_batch_ids = self.get_or_create_priority_ids();
        while let Some(next_batch_id) = self.prio_graph.pop() {
            let Some((_, revert_on_error, slot)) = container.get_batch(next_batch_id.id) else {
                continue;
            };

            // These should be cleared out earlier; but if not, we remove them here
            if slot != current_slot {
                container.remove_by_id(next_batch_id.id);
                self.prio_graph.unblock(&next_batch_id);
                self.send_no_leader_slot_bundle_result(priority_to_seq_id(next_batch_id.priority));
                continue;
            }

            if revert_on_error {
                if !current_batch_ids.is_empty() {
                    result.push((std::mem::take(&mut current_batch_ids), false));
                    current_batch_ids = self.get_or_create_priority_ids();
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

    fn get_or_create_work_object(&mut self) -> ConsumeWork<Tx> {
        if let Some(work) = self.reusable_consume_work.pop() {
            work
        } else {
            // These values will be overwritten by `generate_work`
            ConsumeWork {
                batch_id: TransactionBatchId::new(0),
                ids: Vec::with_capacity(MAX_TXN_PER_BATCH),
                transactions: Vec::with_capacity(MAX_TXN_PER_BATCH),
                max_ages: Vec::with_capacity(MAX_TXN_PER_BATCH),
                revert_on_error: false,
                respond_with_extra_info: false,
                schedulable_slot: None,
            }
        }
    }

    fn recycle_work_object(&mut self, mut work: ConsumeWork<Tx>) {
        // Just in case, clear the work object
        work.ids.clear();
        work.transactions.clear();
        work.max_ages.clear();
        self.reusable_consume_work.push(work);
    }

    fn get_or_create_priority_ids(&mut self) -> Vec<TransactionPriorityId> {
        if let Some(priority_ids) = self.reusable_priority_ids.pop() {
            priority_ids
        } else {
            Vec::with_capacity(MAX_TXN_PER_BATCH)
        }
    }

    fn recycle_priority_ids(&mut self, mut priority_ids: Vec<TransactionPriorityId>) {
        priority_ids.clear();
        self.reusable_priority_ids.push(priority_ids);
    }

    fn generate_work(
        output: &mut ConsumeWork<Tx>,
        batch_id: TransactionBatchId,
        priority_ids: &[TransactionPriorityId],
        revert_on_error: bool,
        container: &mut impl StateContainer<Tx>,
        slot: Slot,
    ) {
        output.ids.clear();
        output.ids.extend(
            priority_ids
                .iter()
                .filter_map(|priority_id| container.get_batch(priority_id.id))
                .flat_map(|(batch_ids, _, _)| batch_ids.into_iter())
                .cloned(),
        );

        output.transactions.clear();
        output.max_ages.clear();
        for txn in output.ids.iter().filter_map(|txn_id| {
            let result = container.get_mut_transaction_state(*txn_id)?;
            let result = result.transition_to_pending();
            Some(result)
        }) {
            output.transactions.push(txn.transaction);
            output.max_ages.push(txn.max_age);
        }

        output.batch_id = batch_id;
        output.revert_on_error = revert_on_error;
        output.schedulable_slot = Some(slot);
        output.respond_with_extra_info = true;
    }

    fn send_no_leader_slot_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessageV0 {
            msg: Some(Msg::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::OutsideLeaderSlot as i32,
                            )),
                        },
                    )),
                },
            )),
        });
    }

    fn send_back_result(&self, seq_id: u32, result: atomic_txn_batch_result::Result) {
        let _ = self.response_sender.try_send(StartSchedulerMessageV0 {
            msg: Some(Msg::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(result),
                },
            )),
        });
    }

    /// Generates a `bundle_result::Result` based on the processed results for 'revert_on_error' batches.
    fn generate_revert_on_error_bundle_result(
        processed_results: &[TransactionResult],
    ) -> atomic_txn_batch_result::Result {
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
            atomic_txn_batch_result::Result::Committed(jito_protos::proto::bam_types::Committed {
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

            atomic_txn_batch_result::Result::NotCommitted(
                jito_protos::proto::bam_types::NotCommitted {
                    reason: Some(Self::convert_reason_to_proto(index, not_commit_reason)),
                },
            )
        }
    }

    /// Generates a `bundle_result::Result` based on the processed result of a single transaction.
    fn generate_bundle_result(processed: &TransactionResult) -> atomic_txn_batch_result::Result {
        match processed {
            TransactionResult::Committed(result) => atomic_txn_batch_result::Result::Committed(
                jito_protos::proto::bam_types::Committed {
                    transaction_results: vec![result.clone()],
                },
            ),
            TransactionResult::NotCommitted(reason) => {
                let (index, not_commit_reason) = match reason {
                    NotCommittedReason::PohTimeout => (0, NotCommittedReason::PohTimeout),
                    NotCommittedReason::BatchRevert => (0, NotCommittedReason::BatchRevert),
                    NotCommittedReason::Error(err) => (0, NotCommittedReason::Error(err.clone())),
                };
                atomic_txn_batch_result::Result::NotCommitted(
                    jito_protos::proto::bam_types::NotCommitted {
                        reason: Some(Self::convert_reason_to_proto(index, not_commit_reason)),
                    },
                )
            }
        }
    }

    fn convert_reason_to_proto(
        index: usize,
        reason: NotCommittedReason,
    ) -> jito_protos::proto::bam_types::not_committed::Reason {
        match reason {
            NotCommittedReason::PohTimeout => {
                jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
                    SchedulingError::PohTimeout as i32,
                )
            }
            // Should not happen, but just in case:
            NotCommittedReason::BatchRevert => {
                jito_protos::proto::bam_types::not_committed::Reason::GenericInvalid(
                    jito_protos::proto::bam_types::GenericInvalid {},
                )
            }
            NotCommittedReason::Error(err) => {
                jito_protos::proto::bam_types::not_committed::Reason::TransactionError(
                    jito_protos::proto::bam_types::TransactionError {
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

impl<Tx: TransactionWithMeta> Scheduler<Tx> for BamScheduler<Tx> {
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
    /// back to BAM.
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
            self.recycle_work_object(result.work);

            let Some(inflight_batch_info) = self.inflight_batch_info.remove(&batch_id) else {
                continue;
            };
            self.workers_scheduled_count[inflight_batch_info.worker_index] -= 1;

            // Should never not be 1; but just in case
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
                // If we got extra info, we can send back the result
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
                    self.send_back_result(priority_to_seq_id(priority_id.priority), bundle_result);
                }

                // If in the same slot, unblock the transaction
                if Some(inflight_batch_info.slot) == self.slot {
                    self.prio_graph.unblock(priority_id);
                }

                // Remove the transaction from the container
                container.remove_by_id(priority_id.id);
            }
            self.recycle_priority_ids(inflight_batch_info.priority_ids);
        }

        Ok((num_transactions, 0))
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::banking_stage::{
            decision_maker::BufferedPacketsDecision,
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            scheduler_messages::{
                ConsumeWork, FinishedConsumeWork, MaxAge, NotCommittedReason, TransactionResult,
            },
            tests::create_slow_genesis_config,
            transaction_scheduler::{
                bam_receive_and_buffer::seq_id_to_priority,
                bam_scheduler::BamScheduler,
                scheduler::Scheduler,
                transaction_state::SanitizedTransactionTTL,
                transaction_state_container::{StateContainer, TransactionStateContainer},
            },
        },
        crossbeam_channel::unbounded,
        itertools::Itertools,
        jito_protos::proto::{
            bam_api::{start_scheduler_message_v0::Msg, StartSchedulerMessageV0},
            bam_types::{
                atomic_txn_batch_result::Result::{Committed, NotCommitted},
                TransactionCommittedResult,
            },
        },
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_perf::packet::Packet,
        solana_poh::poh_recorder::BankStart,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction,
            hash::Hash,
            message::Message,
            signature::Keypair,
            signer::Signer,
            system_instruction::transfer_many,
            transaction::{SanitizedTransaction, Transaction},
        },
        std::{
            borrow::Borrow,
            sync::{Arc, RwLock},
            time::Instant,
        },
    };

    struct TestScheduler {
        scheduler: BamScheduler<RuntimeTransaction<SanitizedTransaction>>,
        consume_work_receivers:
            Vec<crossbeam_channel::Receiver<ConsumeWork<RuntimeTransaction<SanitizedTransaction>>>>,
        finished_consume_work_sender: crossbeam_channel::Sender<
            FinishedConsumeWork<RuntimeTransaction<SanitizedTransaction>>,
        >,
        response_receiver: crossbeam_channel::Receiver<StartSchedulerMessageV0>,
    }

    fn create_test_scheduler(num_threads: usize) -> TestScheduler {
        let (consume_work_senders, consume_work_receivers) =
            (0..num_threads).map(|_| unbounded()).unzip();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
        let (response_sender, response_receiver) = unbounded();
        let scheduler = BamScheduler::new(
            consume_work_senders,
            finished_consume_work_receiver,
            response_sender,
        );
        TestScheduler {
            scheduler,
            consume_work_receivers,
            finished_consume_work_sender,
            response_receiver,
        }
    }

    fn prioritized_tranfers(
        from_keypair: &Keypair,
        to_pubkeys: impl IntoIterator<Item = impl Borrow<Pubkey>>,
        lamports: u64,
        priority: u64,
    ) -> RuntimeTransaction<SanitizedTransaction> {
        let to_pubkeys_lamports = to_pubkeys
            .into_iter()
            .map(|pubkey| *pubkey.borrow())
            .zip(std::iter::repeat(lamports))
            .collect_vec();
        let mut ixs = transfer_many(&from_keypair.pubkey(), &to_pubkeys_lamports);
        let prioritization = ComputeBudgetInstruction::set_compute_unit_price(priority);
        ixs.push(prioritization);
        let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
        let tx = Transaction::new(&[from_keypair], message, Hash::default());
        RuntimeTransaction::from_transaction_for_tests(tx)
    }

    fn create_container(
        tx_infos: impl IntoIterator<
            Item = (
                impl Borrow<Keypair>,
                impl IntoIterator<Item = impl Borrow<Pubkey>>,
                u64,
                u64,
            ),
        >,
    ) -> TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>> {
        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        for (from_keypair, to_pubkeys, lamports, compute_unit_price) in tx_infos.into_iter() {
            let transaction = prioritized_tranfers(
                from_keypair.borrow(),
                to_pubkeys,
                lamports,
                compute_unit_price,
            );
            let packet = Arc::new(
                ImmutableDeserializedPacket::new(
                    Packet::from_data(None, transaction.to_versioned_transaction()).unwrap(),
                )
                .unwrap(),
            );
            let transaction_ttl = SanitizedTransactionTTL {
                transaction,
                max_age: MaxAge::MAX,
            };
            const TEST_TRANSACTION_COST: u64 = 5000;
            container.insert_new_batch(
                vec![transaction_ttl],
                vec![packet],
                compute_unit_price,
                TEST_TRANSACTION_COST,
                false,
                0,
            );
        }

        container
    }

    fn test_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);

        let (_bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        (bank_forks, mint_keypair)
    }

    #[test]
    fn test_scheduler_empty() {
        let TestScheduler {
            mut scheduler,
            consume_work_receivers: _,
            finished_consume_work_sender: _,
            response_receiver: _,
        } = create_test_scheduler(4);

        let mut container = TransactionStateContainer::with_capacity(100);
        let result = scheduler
            .schedule(&mut container, |_, _| {}, |_| true)
            .unwrap();
        assert_eq!(result.num_scheduled, 0);
    }

    #[test]
    fn test_scheduler_basic() {
        let TestScheduler {
            mut scheduler,
            consume_work_receivers,
            finished_consume_work_sender,
            response_receiver,
        } = create_test_scheduler(4);

        let keypair_a = Keypair::new();

        let first_recipient = Pubkey::new_unique();

        let mut container = create_container(vec![
            (
                &keypair_a,
                vec![Pubkey::new_unique()],
                1000,
                seq_id_to_priority(1),
            ),
            (
                &keypair_a,
                vec![first_recipient],
                1500,
                seq_id_to_priority(0),
            ),
            (
                &keypair_a,
                vec![Pubkey::new_unique()],
                1500,
                seq_id_to_priority(2),
            ),
            (
                &Keypair::new(),
                vec![Pubkey::new_unique()],
                2000,
                seq_id_to_priority(3),
            ),
        ]);

        assert!(
            scheduler.slot.is_none(),
            "Scheduler slot should be None initially"
        );

        let (bank_forks, _) = test_bank_forks();

        let decision = BufferedPacketsDecision::Consume(BankStart {
            working_bank: bank_forks.read().unwrap().working_bank(),
            bank_creation_time: Arc::new(Instant::now()),
        });

        // Init scheduler with bank start info
        scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();

        assert!(
            scheduler.slot.is_some(),
            "Scheduler slot should be set after receiving bank start"
        );

        // Schedule the transactions
        let result = scheduler
            .schedule(&mut container, |_, _| {}, |_| true)
            .unwrap();

        // Only two should have been scheduled as one is blocked
        assert_eq!(result.num_scheduled, 2);

        // Since both are not bundles; should be scheduled together to first worker
        let work_1 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_1.ids.len(), 2);

        // Check that the first transaction is from keypair_a and first recipient is the first recipient
        assert_eq!(
            work_1.transactions[0].message().account_keys()[0],
            keypair_a.pubkey()
        );
        assert_eq!(
            work_1.transactions[0].message().account_keys()[1],
            first_recipient
        );

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler
            .schedule(&mut container, |_, _| {}, |_| true)
            .unwrap();
        assert_eq!(result.num_scheduled, 0);
        assert_eq!(scheduler.workers_scheduled_count[0], 1);

        // Respond with finsihed work
        let finished_work = FinishedConsumeWork {
            work: work_1,
            retryable_indexes: vec![],
            extra_info: Some(
                crate::banking_stage::scheduler_messages::FinishedConsumeWorkExtraInfo {
                    processed_results: vec![
                        TransactionResult::Committed(TransactionCommittedResult {
                            cus_consumed: 100,
                            feepayer_balance_lamports: 1000,
                            loaded_accounts_data_size: 10,
                        }),
                        TransactionResult::NotCommitted(NotCommittedReason::PohTimeout),
                    ],
                },
            ),
        };
        let _ = finished_consume_work_sender.send(finished_work);

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(num_transactions, 2);
        assert_eq!(scheduler.workers_scheduled_count[0], 0);

        // Check the response for the first transaction (committed)
        let response = response_receiver.try_recv().unwrap();
        assert!(response.msg.is_some(), "Response should contain a message");
        let msg = response.msg.unwrap();
        let Msg::AtomicTxnBatchResult(bundle_result) = msg else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 0);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(committed) => {
                assert_eq!(committed.transaction_results.len(), 1);
                assert_eq!(committed.transaction_results[0].cus_consumed, 100);
            }
            NotCommitted(not_committed) => {
                panic!(
                    "Expected Committed result, got NotCommitted: {:?}",
                    not_committed
                );
            }
        }

        // Check the response for the second transaction (not committed)
        let response = response_receiver.try_recv().unwrap();
        assert!(response.msg.is_some(), "Response should contain a message");
        let msg = response.msg.unwrap();
        let Msg::AtomicTxnBatchResult(bundle_result) = msg else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 3);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(_) => {
                panic!("Expected NotCommitted result, got Committed");
            }
            NotCommitted(not_committed) => {
                assert!(
                    not_committed.reason.is_some(),
                    "NotCommitted reason should be present"
                );
                let reason = not_committed.reason.unwrap();
                assert_eq!(
                    reason,
                    jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
                        jito_protos::proto::bam_types::SchedulingError::PohTimeout as i32
                    )
                );
            }
        }

        // Now try scheduling again; should schedule the remaining transaction
        let result = scheduler
            .schedule(&mut container, |_, _| {}, |_| true)
            .unwrap();
        assert_eq!(result.num_scheduled, 1);
        // Check that the remaining transaction is sent to the worker
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);
        assert_eq!(scheduler.workers_scheduled_count[0], 1);

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler
            .schedule(&mut container, |_, _| {}, |_| true)
            .unwrap();
        assert_eq!(result.num_scheduled, 0);

        // Send back the finished work for the second transaction
        let finished_work = FinishedConsumeWork {
            work: work_2,
            retryable_indexes: vec![],
            extra_info: Some(
                crate::banking_stage::scheduler_messages::FinishedConsumeWorkExtraInfo {
                    processed_results: vec![TransactionResult::Committed(
                        TransactionCommittedResult {
                            cus_consumed: 1500,
                            feepayer_balance_lamports: 1500,
                            loaded_accounts_data_size: 20,
                        },
                    )],
                },
            ),
        };
        let _ = finished_consume_work_sender.send(finished_work);

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(num_transactions, 1);
        assert_eq!(scheduler.workers_scheduled_count[0], 0);

        // Check the response for the next transaction
        let response = response_receiver.try_recv().unwrap();
        assert!(response.msg.is_some(), "Response should contain a message");
        let msg = response.msg.unwrap();
        let Msg::AtomicTxnBatchResult(bundle_result) = msg else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 1);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(committed) => {
                assert_eq!(committed.transaction_results.len(), 1);
                assert_eq!(committed.transaction_results[0].cus_consumed, 1500);
            }
            NotCommitted(not_committed) => {
                panic!(
                    "Expected Committed result, got NotCommitted: {:?}",
                    not_committed
                );
            }
        }

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &BufferedPacketsDecision::Forward)
            .unwrap();
        assert_eq!(num_transactions, 0);

        // Check that container + prio-graph are empty
        assert!(
            container.pop().is_none(),
            "Container should be empty after processing all transactions"
        );
        assert!(
            scheduler.prio_graph.is_empty(),
            "Prio-graph should be empty after processing all transactions"
        );

        // Receive the NotCommitted Result
        let response = response_receiver.try_recv().unwrap();
        assert!(response.msg.is_some(), "Response should contain a message");
        let msg = response.msg.unwrap();
        let Msg::AtomicTxnBatchResult(bundle_result) = msg else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 2);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(_) => {
                panic!("Expected NotCommitted result, got Committed");
            }
            NotCommitted(not_committed) => {
                assert!(
                    not_committed.reason.is_some(),
                    "NotCommitted reason should be present"
                );
                let reason = not_committed.reason.unwrap();
                assert_eq!(
                    reason,
                    jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
                        jito_protos::proto::bam_types::SchedulingError::OutsideLeaderSlot as i32
                    )
                );
            }
        }
    }
}
