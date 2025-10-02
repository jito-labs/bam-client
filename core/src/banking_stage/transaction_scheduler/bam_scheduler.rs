//! A Scheduler implementation that pulls batches off the container, and then
//! schedules them to workers in a FIFO, account-aware manner. This is facilitated by the
//! `PrioGraph` data structure, which is a directed graph that tracks the dependencies.
use {
    super::{
        bam_receive_and_buffer::priority_to_seq_id,
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_error::SchedulerError,
        transaction_priority_id::TransactionPriorityId,
        transaction_state_container::StateContainer,
    },
    crate::{
        bam_response_handle::BamResponseHandle,
        banking_stage::{
            consumer::Consumer,
            decision_maker::BufferedPacketsDecision,
            scheduler_messages::{ConsumeWork, FinishedConsumeWork, MaxAge, TransactionBatchId},
            transaction_scheduler::{
                scheduler::PreLockFilterAction, scheduler_common::SchedulingCommon,
                transaction_state::TransactionState, transaction_state_container::BatchInfo,
            },
        },
    },
    ahash::{HashSet, HashSetExt},
    arrayvec::ArrayVec,
    crossbeam_channel::{Receiver, Sender},
    itertools::izip,
    prio_graph::{AccessKind, GraphNode, PrioGraph},
    solana_clock::{Slot, MAX_PROCESSING_AGE},
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction_error::TransactionResult,
    std::{
        collections::HashMap,
        mem::replace,
        sync::{Arc, RwLock},
        time::Instant,
    },
};

type SchedulerPrioGraph = PrioGraph<
    TransactionPriorityId,
    Pubkey,
    TransactionPriorityId,
    fn(&TransactionPriorityId, &GraphNode<TransactionPriorityId>) -> TransactionPriorityId,
>;

// Inflight batches contain many batches
struct InflightBatchInfo {
    batch_priority_ids: Vec<(
        TransactionPriorityId,
        usize, /* number of txs in the batch */
    )>,
}

impl Default for InflightBatchInfo {
    fn default() -> Self {
        Self {
            batch_priority_ids: Vec::new(),
        }
    }
}

pub struct BamScheduler<Tx: TransactionWithMeta> {
    consume_work_sender: Sender<ConsumeWork<Tx>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    bam_response_handle: BamResponseHandle,

    inflight_batches: HashMap<u64, InflightBatchInfo>,

    prio_graph: SchedulerPrioGraph,
    slot: Option<Slot>,
    bank_forks: Arc<RwLock<BankForks>>,
    // pop_and_unblock() on the priority graph is slow at slot boundary, so we store the ids here to avoid the slow path.
    prio_graph_ids: HashSet<TransactionPriorityId>,

    batch_id: u64,
}

impl<Tx: TransactionWithMeta> BamScheduler<Tx> {
    pub fn new(
        consume_work_sender: Sender<ConsumeWork<Tx>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        bam_response_handle: BamResponseHandle,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        // Scheduler deadlock can occur when using bounded channels. The scheduler can enqueue too many tranasctions to the consumer, but
        // the consumer can get blocked on a bounded finished_consume_work channel since the loop uses a blocking send.
        // By using unbounded channels, we avoid this deadlock.
        // Alternatively, one could check to make sure the finished_consume_work_receiver isn't full in the loop.
        assert!(
            finished_consume_work_receiver.capacity().is_none(),
            "finished_consume_work_receiver must be unbounded to avoid scheduler deadlock"
        );
        assert!(
            consume_work_sender.capacity().is_none(),
            "consume_work_sender must be unbounded to avoid scheduler deadlock"
        );

        Self {
            consume_work_sender,
            finished_consume_work_receiver,
            bam_response_handle,
            inflight_batches: HashMap::with_capacity(10_000),
            prio_graph: PrioGraph::new(|id, _graph_node| *id),
            slot: None,
            bank_forks,
            prio_graph_ids: HashSet::with_capacity(10_000),
            batch_id: 0,
        }
    }

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_transactions_account_access<'a>(
        transactions: impl Iterator<Item = &'a (impl SVMMessage + 'a)> + 'a,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + 'a {
        transactions.flat_map(|txn| {
            txn.account_keys().iter().enumerate().map(|(index, key)| {
                if txn.is_writable(index) {
                    (*key, AccessKind::Write)
                } else {
                    (*key, AccessKind::Read)
                }
            })
        })
    }

    /// Pop the batch IDs from the container priority queue and insert each batch ID into the priority graph
    /// with each batch's account locks.
    fn pull_into_prio_graph<S: StateContainer<Tx>>(&mut self, container: &mut S) {
        while let Some(next_batch_id) = container.pop_batch() {
            let BatchInfo {
                transaction_ids, ..
            } = container
                .get_batch(next_batch_id.id)
                .expect("batch must exist");

            self.prio_graph.insert_transaction(
                next_batch_id,
                Self::get_transactions_account_access(transaction_ids.iter().map(|txn_id| {
                    container
                        .get_transaction(*txn_id)
                        .expect("transaction must exist")
                })),
            );
            self.prio_graph_ids.insert(next_batch_id);
        }
    }

    /// Pop batch IDs from the priority graph, grab the transactions, check them for validity, and send them to the workers.
    fn send_to_workers(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        num_scheduled: &mut usize,
        num_filtered_out: &mut usize,
    ) -> Result<(), SchedulerError> {
        const CHECK_TRANSACTIONS_BATCH_SIZE: usize = 256;

        let mut batch_priority_ids: ArrayVec<TransactionPriorityId, CHECK_TRANSACTIONS_BATCH_SIZE> =
            ArrayVec::new();
        let mut batch_lengths: ArrayVec<usize, CHECK_TRANSACTIONS_BATCH_SIZE> = ArrayVec::new();
        let mut revert_on_errors: ArrayVec<bool, CHECK_TRANSACTIONS_BATCH_SIZE> = ArrayVec::new();
        let mut transactions: ArrayVec<Tx, CHECK_TRANSACTIONS_BATCH_SIZE> = ArrayVec::new();
        let mut max_ages: ArrayVec<MaxAge, CHECK_TRANSACTIONS_BATCH_SIZE> = ArrayVec::new();

        let working_bank = self.bank_forks.read().unwrap().working_bank();

        // Accumulate work until we get to an appropriate spot to batch the operations
        while let Some(batch_priority_id) = self.prio_graph.pop() {
            let batch_info = container
                .get_batch(batch_priority_id.id)
                .expect("batch must exist");
            // don't overflow the ArrayVec
            if transactions.len() + batch_info.transaction_ids.len() > CHECK_TRANSACTIONS_BATCH_SIZE
            {
                let (scheduled, filtered) =
                    Self::check_transactions_and_send_work::<CHECK_TRANSACTIONS_BATCH_SIZE>(
                        container,
                        &mut self.prio_graph,
                        &working_bank,
                        &mut batch_priority_ids,
                        &mut batch_lengths,
                        &mut revert_on_errors,
                        &mut transactions,
                        &mut max_ages,
                        &mut self.batch_id,
                        &self.consume_work_sender,
                        &self.bam_response_handle,
                        &mut self.inflight_batches,
                        &mut self.prio_graph_ids,
                    )?;
                *num_scheduled += scheduled;
                *num_filtered_out += filtered;
            } else {
                batch_priority_ids.push(batch_priority_id);
                batch_lengths.push(batch_info.transaction_ids.len());
                revert_on_errors.push(batch_info.revert_on_error);
                let transaction_ids = batch_info.transaction_ids.clone();

                for txn_id in transaction_ids {
                    let tx = container
                        .get_mut_transaction_state(txn_id)
                        .expect("transaction must exist");
                    let (tx, max_age) = tx.take_transaction_for_scheduling();
                    transactions.push(tx);
                    max_ages.push(max_age);
                }
            }
        }

        if !transactions.is_empty() {
            let (scheduled, filtered) =
                Self::check_transactions_and_send_work::<CHECK_TRANSACTIONS_BATCH_SIZE>(
                    container,
                    &mut self.prio_graph,
                    &working_bank,
                    &mut batch_priority_ids,
                    &mut batch_lengths,
                    &mut revert_on_errors,
                    &mut transactions,
                    &mut max_ages,
                    &mut self.batch_id,
                    &self.consume_work_sender,
                    &self.bam_response_handle,
                    &mut self.inflight_batches,
                    &mut self.prio_graph_ids,
                )?;
            *num_scheduled += scheduled;
            *num_filtered_out += filtered;
        }

        Ok(())
    }

    fn check_transactions_and_send_work<const LEN: usize>(
        container: &mut impl StateContainer<Tx>,
        prio_graph: &mut SchedulerPrioGraph,
        working_bank: &Arc<Bank>,
        batch_priority_ids: &mut ArrayVec<TransactionPriorityId, LEN>,
        batch_lengths: &mut ArrayVec<usize, LEN>,
        revert_on_errors: &mut ArrayVec<bool, LEN>,
        transactions: &mut ArrayVec<Tx, LEN>,
        max_ages: &mut ArrayVec<MaxAge, LEN>,
        batch_id: &mut u64,
        consume_work_sender: &Sender<ConsumeWork<Tx>>,
        bam_response_handle: &BamResponseHandle,
        inflight_batches: &mut HashMap<u64, InflightBatchInfo>,
        prio_graph_ids: &mut HashSet<TransactionPriorityId>,
    ) -> Result<
        (
            usize, /* num scheduled */
            usize, /* num filtered out */
        ),
        SchedulerError,
    > {
        const CONSUME_WORK_BATCH_SIZE: usize = 4;

        let mut num_scheduled = 0;
        let mut num_filtered_out = 0;
        let mut error_counters = TransactionErrorMetrics::new();

        let lock_results: [_; LEN] = core::array::from_fn(|_| Ok(()));

        // check already processed, bad blockhash, or bad nonce
        let mut check_results: ArrayVec<TransactionResult<()>, LEN> = working_bank
            .check_transactions::<Tx>(
                &transactions,
                lock_results.as_slice(),
                MAX_PROCESSING_AGE,
                &mut error_counters,
            )
            .into_iter()
            .map(|r| match r {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            })
            .collect();

        // check to make sure fee payer has enough to process the transaction and remain rent-exempt
        for (tx, check_result) in transactions.iter().zip(check_results.iter_mut()) {
            match check_result {
                Ok(()) => {
                    *check_result =
                        Consumer::check_fee_payer_unlocked(&working_bank, tx, &mut error_counters);
                }
                Err(_) => {}
            }
        }

        let mut consume_work_batch_priority_ids = Vec::with_capacity(CONSUME_WORK_BATCH_SIZE);
        let mut consume_work_transactions = Vec::with_capacity(CONSUME_WORK_BATCH_SIZE);
        let mut consume_work_max_ages = Vec::with_capacity(CONSUME_WORK_BATCH_SIZE);

        let mut transactions_iter = transactions.drain(..);
        let mut max_ages_iter = max_ages.drain(..);
        let mut check_results_iter = check_results.drain(..);

        for (batch_priority_id, batch_length, is_revert_on_error) in izip!(
            batch_priority_ids.drain(..),
            batch_lengths.drain(..),
            revert_on_errors.drain(..)
        ) {
            // if any of the check results are bad, the entire batch can be dropped from the container and
            // the priority graph should be cleared out.
            if let Some((index, res)) = check_results_iter
                .by_ref()
                .take(batch_length)
                .enumerate()
                .find(|(_, res)| res.is_err())
            {
                transactions_iter.by_ref().take(batch_length).for_each(drop);
                max_ages_iter.by_ref().take(batch_length).for_each(drop);

                bam_response_handle.send_not_committed_result(
                    priority_to_seq_id(batch_priority_id.priority),
                    index,
                    res.unwrap_err(),
                );

                prio_graph.unblock(&batch_priority_id);
                prio_graph_ids.remove(&batch_priority_id);
                container.remove_batch_by_id(batch_priority_id.id);

                num_filtered_out += batch_length;
            } else if is_revert_on_error {
                // revert_on_error and normal transactions can't be scheduled in the same batch right now
                let transactions: Vec<Tx> = transactions_iter.by_ref().take(batch_length).collect();
                let max_ages = max_ages_iter.by_ref().take(batch_length).collect();
                let num_txs = transactions.len();

                *batch_id += 1;
                let work = ConsumeWork {
                    batch_id: TransactionBatchId::new(*batch_id),
                    ids: vec![batch_priority_id.id],
                    transactions,
                    max_ages,
                    revert_on_error: true,
                    respond_with_extra_info: true,
                    max_schedule_slot: Some(working_bank.slot()),
                };
                consume_work_sender
                    .send(work)
                    .map_err(|_| SchedulerError::DisconnectedSendChannel("consume_work_sender"))?;
                num_scheduled += num_txs;
                inflight_batches.insert(
                    *batch_id,
                    InflightBatchInfo {
                        batch_priority_ids: vec![(batch_priority_id, batch_length)],
                    },
                );
            } else {
                consume_work_batch_priority_ids.push((batch_priority_id, batch_length));
                consume_work_transactions.extend(transactions_iter.by_ref().take(batch_length));
                consume_work_max_ages.extend(max_ages_iter.by_ref().take(batch_length));

                let num_to_schedule = consume_work_transactions.len();
                if num_to_schedule >= CONSUME_WORK_BATCH_SIZE {
                    *batch_id += 1;
                    let work = ConsumeWork {
                        batch_id: TransactionBatchId::new(*batch_id),
                        ids: consume_work_batch_priority_ids
                            .iter()
                            .map(|(id, _)| id.id)
                            .collect(),
                        transactions: replace(
                            &mut consume_work_transactions,
                            Vec::with_capacity(CONSUME_WORK_BATCH_SIZE),
                        ),
                        max_ages: replace(
                            &mut consume_work_max_ages,
                            Vec::with_capacity(CONSUME_WORK_BATCH_SIZE),
                        ),
                        revert_on_error: false,
                        respond_with_extra_info: true,
                        max_schedule_slot: Some(working_bank.slot()),
                    };

                    consume_work_sender.send(work).map_err(|_| {
                        SchedulerError::DisconnectedSendChannel("consume_work_sender")
                    })?;
                    num_scheduled += num_to_schedule;
                    let batch_priority_ids = replace(
                        &mut consume_work_batch_priority_ids,
                        Vec::with_capacity(CONSUME_WORK_BATCH_SIZE),
                    );
                    inflight_batches.insert(*batch_id, InflightBatchInfo { batch_priority_ids });
                }
            }
        }

        // finish the rest
        if !consume_work_transactions.is_empty() {
            *batch_id += 1;
            let num_to_schedule = consume_work_transactions.len();

            let work = ConsumeWork {
                batch_id: TransactionBatchId::new(*batch_id),
                ids: consume_work_batch_priority_ids
                    .iter()
                    .map(|(id, _)| id.id)
                    .collect(),
                transactions: consume_work_transactions,
                max_ages: consume_work_max_ages,
                revert_on_error: false,
                respond_with_extra_info: true,
                max_schedule_slot: Some(working_bank.slot()),
            };
            consume_work_sender
                .send(work)
                .map_err(|_| SchedulerError::DisconnectedSendChannel("consume_work_sender"))?;
            num_scheduled += num_to_schedule;

            inflight_batches.insert(
                *batch_id,
                InflightBatchInfo {
                    batch_priority_ids: consume_work_batch_priority_ids,
                },
            );
        }

        Ok((num_scheduled, num_filtered_out))
    }

    /// On bank boundaries, the container is emptied and the priority graph is cleared
    fn maybe_bank_boundary_actions(
        &mut self,
        decision: &BufferedPacketsDecision,
        container: &mut impl StateContainer<Tx>,
    ) {
        // Check if no bank or slot has changed
        let maybe_bank = decision.bank();
        if maybe_bank.map(|bank| bank.slot()) == self.slot {
            return;
        }

        if let Some(bank) = maybe_bank {
            info!(
                "Bank boundary detected: slot changed from {:?} to {:?}",
                self.slot,
                bank.slot()
            );
            self.slot = Some(bank.slot());
        } else {
            info!("Bank boundary detected: slot changed to None");
            self.slot = None;
        }

        // First, wait for all the inflight batches to be completed.
        while !self.inflight_batches.is_empty() {
            let Ok(work) = self.finished_consume_work_receiver.recv() else {
                break;
            };
            let batch_info = self
                .inflight_batches
                .remove(&work.work.batch_id.0)
                .expect("batch must be inflight");
            let mut results = work
                .extra_info
                .expect("bam requires extra info")
                .processed_results
                .into_iter();

            for (batch_priority_id, batch_length) in batch_info.batch_priority_ids {
                self.prio_graph_ids.remove(&batch_priority_id);
                self.bam_response_handle.send_result(
                    priority_to_seq_id(batch_priority_id.priority),
                    container
                        .get_batch(batch_priority_id.id)
                        .expect("batch exists")
                        .revert_on_error,
                    results.by_ref().take(batch_length).collect(),
                );
            }
        }

        // Anything else left in prio_graph_ids is in the priority graph, but not scheduled yet. Send back 'retryable' results.
        for next_batch_id in self.prio_graph_ids.drain() {
            self.bam_response_handle
                .send_outside_leader_slot_bundle_result(priority_to_seq_id(next_batch_id.priority));
        }

        // Anything left in the container was pushed into the queue but hasn't been pulled into the priority graph yet.)
        while let Some(next_batch_id) = container.pop_batch() {
            self.bam_response_handle
                .send_outside_leader_slot_bundle_result(priority_to_seq_id(next_batch_id.priority));
        }

        self.prio_graph.clear();
        container.clear();
    }

    fn drain_finished_work<S: StateContainer<Tx>>(&mut self, container: &mut S) -> usize {
        let num_transactions = 0;
        while let Ok(result) = self.finished_consume_work_receiver.try_recv() {
            let inflight_batch_info = self
                .inflight_batches
                .remove(&result.work.batch_id.0)
                .expect("batch must be inflight to be removed");

            let mut results = result
                .extra_info
                .expect("bam requires extra info")
                .processed_results
                .into_iter();

            for (batch_priority_id, batch_length) in inflight_batch_info.batch_priority_ids {
                self.prio_graph.unblock(&batch_priority_id);
                self.prio_graph_ids.remove(&batch_priority_id);
                container.remove_batch_by_id(batch_priority_id.id);

                self.bam_response_handle.send_result(
                    priority_to_seq_id(batch_priority_id.priority),
                    result.work.revert_on_error,
                    results.by_ref().take(batch_length).collect(),
                );
            }
        }
        num_transactions
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx> for BamScheduler<Tx> {
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        _pre_graph_filter: impl Fn(&[&Tx], &mut [bool]),
        _pre_lock_filter: impl Fn(&TransactionState<Tx>) -> PreLockFilterAction,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let starting_queue_size = container.queue_size();
        let starting_buffer_size = container.buffer_size();

        let start_time = Instant::now();

        let mut num_scheduled = 0;
        let mut num_filtered_out = 0;

        self.pull_into_prio_graph(container);
        self.send_to_workers(container, &mut num_scheduled, &mut num_filtered_out)?;

        Ok(SchedulingSummary {
            starting_queue_size,
            starting_buffer_size,
            num_scheduled,
            num_unschedulable_conflicts: 0,
            num_filtered_out,
            filter_time_us: start_time.elapsed().as_micros() as u64,
            num_unschedulable_threads: 0,
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
        let num_transactions = self.drain_finished_work(container);

        Ok((num_transactions, 0))
    }

    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    // use crate::{
    //     bam_dependencies::BamOutboundMessage,
    //     bam_response_handle::BamResponseHandle,
    //     banking_stage::transaction_scheduler::{
    //         scheduler::PreLockFilterAction,
    //         transaction_state_container::TransactionViewStateContainer,
    //     },
    // };
    // use {
    //     crate::banking_stage::{
    //         decision_maker::BufferedPacketsDecision,
    //         scheduler_messages::{
    //             ConsumeWork, FinishedConsumeWork, MaxAge, NotCommittedReason, TransactionResult,
    //         },
    //         tests::create_slow_genesis_config,
    //         transaction_scheduler::{
    //             bam_receive_and_buffer::seq_id_to_priority,
    //             bam_scheduler::BamScheduler,
    //             scheduler::Scheduler,
    //             transaction_state_container::{StateContainer, TransactionStateContainer},
    //         },
    //     },
    //     crossbeam_channel::unbounded,
    //     itertools::Itertools,
    //     jito_protos::proto::bam_types::{
    //         atomic_txn_batch_result::Result::{Committed, NotCommitted},
    //         TransactionCommittedResult,
    //     },
    //     solana_compute_budget_interface::ComputeBudgetInstruction,
    //     solana_hash::Hash,
    //     solana_keypair::Keypair,
    //     solana_ledger::genesis_utils::GenesisConfigInfo,
    //     solana_message::Message,
    //     solana_pubkey::Pubkey,
    //     solana_runtime::{bank::Bank, bank_forks::BankForks},
    //     solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    //     solana_signer::Signer,
    //     solana_system_interface::instruction::transfer_many,
    //     solana_transaction::sanitized::SanitizedTransaction,
    //     solana_transaction::Transaction,
    //     std::{
    //         borrow::Borrow,
    //         sync::{Arc, RwLock},
    //     },
    // };

    // struct TestScheduler {
    //     scheduler: BamScheduler<RuntimeTransaction<SanitizedTransaction>>,
    //     consume_work_receivers:
    //         Vec<crossbeam_channel::Receiver<ConsumeWork<RuntimeTransaction<SanitizedTransaction>>>>,
    //     finished_consume_work_sender: crossbeam_channel::Sender<
    //         FinishedConsumeWork<RuntimeTransaction<SanitizedTransaction>>,
    //     >,
    //     response_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,
    // }

    // fn create_test_scheduler(
    //     num_threads: usize,
    //     bank_forks: &Arc<RwLock<BankForks>>,
    // ) -> TestScheduler {
    //     let (consume_work_sender, consume_work_receiver) = unbounded();
    //     let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
    //     let (response_sender, response_receiver) = unbounded();
    //     test_bank_forks();
    //     let scheduler = BamScheduler::new(
    //         consume_work_sender,
    //         finished_consume_work_receiver,
    //         BamResponseHandle::new(response_sender),
    //         bank_forks.clone(),
    //     );
    //     TestScheduler {
    //         scheduler,
    //         consume_work_receivers: (0..num_threads)
    //             .map(|_| consume_work_receiver.clone())
    //             .collect(),
    //         finished_consume_work_sender,
    //         response_receiver,
    //     }
    // }

    // fn prioritized_tranfers(
    //     from_keypair: &Keypair,
    //     to_pubkeys: impl IntoIterator<Item = impl Borrow<Pubkey>>,
    //     lamports: u64,
    //     priority: u64,
    // ) -> RuntimeTransaction<SanitizedTransaction> {
    //     let to_pubkeys_lamports = to_pubkeys
    //         .into_iter()
    //         .map(|pubkey| *pubkey.borrow())
    //         .zip(std::iter::repeat(lamports))
    //         .collect_vec();
    //     let mut ixs = transfer_many(&from_keypair.pubkey(), &to_pubkeys_lamports);
    //     let prioritization = ComputeBudgetInstruction::set_compute_unit_price(priority);
    //     ixs.push(prioritization);
    //     let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
    //     let tx = Transaction::new(&[from_keypair], message, Hash::default());
    //     RuntimeTransaction::from_transaction_for_tests(tx)
    // }

    // fn create_container(
    //     tx_infos: impl IntoIterator<
    //         Item = (
    //             impl Borrow<Keypair>,
    //             impl IntoIterator<Item = impl Borrow<Pubkey>>,
    //             u64,
    //             u64,
    //         ),
    //     >,
    // ) -> TransactionViewStateContainer {
    //     let mut container = TransactionViewStateContainer::with_capacity(10 * 1024, true);
    //     for (from_keypair, to_pubkeys, lamports, compute_unit_price) in tx_infos.into_iter() {
    //         let transaction = prioritized_tranfers(
    //             from_keypair.borrow(),
    //             to_pubkeys,
    //             lamports,
    //             compute_unit_price,
    //         );
    //         const TEST_TRANSACTION_COST: u64 = 5000;
    //         container.insert_new_batch(
    //             vec![(transaction, MaxAge::MAX)],
    //             compute_unit_price,
    //             TEST_TRANSACTION_COST,
    //             false,
    //             0,
    //         );
    //     }

    //     container
    // }

    // fn test_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair) {
    //     let GenesisConfigInfo {
    //         genesis_config,
    //         mint_keypair,
    //         ..
    //     } = create_slow_genesis_config(u64::MAX);

    //     let (_bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
    //     (bank_forks, mint_keypair)
    // }

    // #[test]
    // fn test_scheduler_empty() {
    //     let (bank_forks, _) = test_bank_forks();
    //     let TestScheduler {
    //         mut scheduler,
    //         consume_work_receivers: _,
    //         finished_consume_work_sender: _,
    //         response_receiver: _,
    //     } = create_test_scheduler(4, &bank_forks);

    //     let mut container = TransactionStateContainer::with_capacity(100, true);
    //     let result = scheduler
    //         .schedule(
    //             &mut container,
    //             |_, _| {},
    //             |_| PreLockFilterAction::AttemptToSchedule,
    //         )
    //         .unwrap();
    //     assert_eq!(result.num_scheduled, 0);
    // }

    // #[test]
    // fn test_scheduler_basic() {
    //     let (bank_forks, _) = test_bank_forks();
    //     let TestScheduler {
    //         mut scheduler,
    //         consume_work_receivers,
    //         finished_consume_work_sender,
    //         response_receiver,
    //     } = create_test_scheduler(4, &bank_forks);
    //     scheduler.extra_checks_enabled = false;

    //     let keypair_a = Keypair::new();

    //     let first_recipient = Pubkey::new_unique();
    //     let second_recipient = Pubkey::new_unique();

    //     let mut container = create_container(vec![
    //         (
    //             &keypair_a,
    //             vec![Pubkey::new_unique()],
    //             1000,
    //             seq_id_to_priority(1),
    //         ),
    //         (
    //             &keypair_a,
    //             vec![first_recipient],
    //             1500,
    //             seq_id_to_priority(0),
    //         ),
    //         (
    //             &keypair_a,
    //             vec![Pubkey::new_unique()],
    //             1500,
    //             seq_id_to_priority(2),
    //         ),
    //         (
    //             &Keypair::new(),
    //             vec![second_recipient],
    //             2000,
    //             seq_id_to_priority(3),
    //         ),
    //     ]);

    //     assert!(
    //         scheduler.slot.is_none(),
    //         "Scheduler slot should be None initially"
    //     );

    //     let decision = BufferedPacketsDecision::Consume(bank_forks.read().unwrap().working_bank());

    //     // Init scheduler with bank start info
    //     scheduler
    //         .receive_completed(&mut container, &decision)
    //         .unwrap();

    //     assert!(
    //         scheduler.slot.is_some(),
    //         "Scheduler slot should be set after receiving bank start"
    //     );

    //     // Schedule the transactions
    //     let result = scheduler
    //         .schedule(
    //             &mut container,
    //             |_, _| {},
    //             |_| PreLockFilterAction::AttemptToSchedule,
    //         )
    //         .unwrap();

    //     // Only two should have been scheduled as one is blocked
    //     assert_eq!(result.num_scheduled, 2);

    //     // Receive the scheduled work
    //     let work_1 = consume_work_receivers[0].try_recv().unwrap();
    //     assert_eq!(work_1.ids.len(), 1);
    //     let work_2 = consume_work_receivers[0].try_recv().unwrap();
    //     assert_eq!(work_2.ids.len(), 1);

    //     // Check that the first transaction is from keypair_a and first recipient is the first recipient
    //     assert_eq!(
    //         work_1.transactions[0].message().account_keys()[0],
    //         keypair_a.pubkey()
    //     );
    //     assert_eq!(
    //         work_1.transactions[0].message().account_keys()[1],
    //         first_recipient
    //     );

    //     // Check that the second transaction is from the other keypair
    //     assert_ne!(
    //         work_2.transactions[0].message().account_keys()[0],
    //         keypair_a.pubkey(),
    //     );
    //     assert_eq!(
    //         work_2.transactions[0].message().account_keys()[1],
    //         second_recipient
    //     );

    //     // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
    //     let result = scheduler
    //         .schedule(
    //             &mut container,
    //             |_, _| {},
    //             |_| PreLockFilterAction::AttemptToSchedule,
    //         )
    //         .unwrap();
    //     assert_eq!(result.num_scheduled, 0);

    //     // Respond with finished work
    //     let responses = [
    //         (
    //             work_1,
    //             TransactionResult::Committed(TransactionCommittedResult {
    //                 cus_consumed: 100,
    //                 feepayer_balance_lamports: 1000,
    //                 loaded_accounts_data_size: 10,
    //                 execution_success: true,
    //             }),
    //         ), // Committed
    //         (
    //             work_2,
    //             TransactionResult::NotCommitted(NotCommittedReason::PohTimeout),
    //         ), // Not committed
    //     ];
    //     for (work, response) in responses.into_iter() {
    //         let finished_work = FinishedConsumeWork {
    //             work,
    //             retryable_indexes: vec![],
    //             extra_info: Some(
    //                 crate::banking_stage::scheduler_messages::FinishedConsumeWorkExtraInfo {
    //                     processed_results: vec![response],
    //                 },
    //             ),
    //         };
    //         let _ = finished_consume_work_sender.send(finished_work);
    //     }

    //     // Receive the finished work
    //     let (num_transactions, _) = scheduler
    //         .receive_completed(&mut container, &decision)
    //         .unwrap();
    //     assert_eq!(num_transactions, 2);

    //     // Check the responses
    //     let response = response_receiver.try_recv().unwrap();
    //     let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
    //         panic!("Expected AtomicTxnBatchResult message");
    //     };
    //     assert_eq!(bundle_result.seq_id, 0);
    //     assert!(
    //         bundle_result.result.is_some(),
    //         "Bundle result should be present"
    //     );
    //     let result = bundle_result.result.unwrap();
    //     match result {
    //         Committed(committed) => {
    //             assert_eq!(committed.transaction_results.len(), 1);
    //             assert_eq!(committed.transaction_results[0].cus_consumed, 100);
    //         }
    //         NotCommitted(not_committed) => {
    //             panic!(
    //                 "Expected Committed result, got NotCommitted: {:?}",
    //                 not_committed
    //             );
    //         }
    //     }

    //     // Check the response for the second transaction (not committed)
    //     let response = response_receiver.try_recv().unwrap();
    //     let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
    //         panic!("Expected AtomicTxnBatchResult message");
    //     };
    //     assert_eq!(bundle_result.seq_id, 3);
    //     assert!(
    //         bundle_result.result.is_some(),
    //         "Bundle result should be present"
    //     );
    //     let result = bundle_result.result.unwrap();
    //     match result {
    //         Committed(_) => {
    //             panic!("Expected NotCommitted result, got Committed");
    //         }
    //         NotCommitted(not_committed) => {
    //             assert!(
    //                 not_committed.reason.is_some(),
    //                 "NotCommitted reason should be present"
    //             );
    //             let reason = not_committed.reason.unwrap();
    //             assert_eq!(
    //                 reason,
    //                 jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
    //                     jito_protos::proto::bam_types::SchedulingError::PohTimeout as i32
    //                 )
    //             );
    //         }
    //     }

    //     // Now try scheduling again; should schedule the remaining transaction
    //     let result = scheduler
    //         .schedule(
    //             &mut container,
    //             |_, _| {},
    //             |_| PreLockFilterAction::AttemptToSchedule,
    //         )
    //         .unwrap();
    //     assert_eq!(result.num_scheduled, 1);
    //     // Check that the remaining transaction is sent to the worker
    //     let work_2 = consume_work_receivers[0].try_recv().unwrap();
    //     assert_eq!(work_2.ids.len(), 1);

    //     // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
    //     let result = scheduler
    //         .schedule(
    //             &mut container,
    //             |_, _| {},
    //             |_| PreLockFilterAction::AttemptToSchedule,
    //         )
    //         .unwrap();
    //     assert_eq!(result.num_scheduled, 0);

    //     // Send back the finished work for the second transaction
    //     let finished_work = FinishedConsumeWork {
    //         work: work_2,
    //         retryable_indexes: vec![],
    //         extra_info: Some(
    //             crate::banking_stage::scheduler_messages::FinishedConsumeWorkExtraInfo {
    //                 processed_results: vec![TransactionResult::Committed(
    //                     TransactionCommittedResult {
    //                         cus_consumed: 1500,
    //                         feepayer_balance_lamports: 1500,
    //                         loaded_accounts_data_size: 20,
    //                         execution_success: true,
    //                     },
    //                 )],
    //             },
    //         ),
    //     };
    //     let _ = finished_consume_work_sender.send(finished_work);

    //     // Receive the finished work
    //     let (num_transactions, _) = scheduler
    //         .receive_completed(&mut container, &decision)
    //         .unwrap();
    //     assert_eq!(num_transactions, 1);

    //     // Check the response for the next transaction
    //     let response = response_receiver.try_recv().unwrap();
    //     let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
    //         panic!("Expected AtomicTxnBatchResult message");
    //     };
    //     assert_eq!(bundle_result.seq_id, 1);
    //     assert!(
    //         bundle_result.result.is_some(),
    //         "Bundle result should be present"
    //     );
    //     let result = bundle_result.result.unwrap();
    //     match result {
    //         Committed(committed) => {
    //             assert_eq!(committed.transaction_results.len(), 1);
    //             assert_eq!(committed.transaction_results[0].cus_consumed, 1500);
    //         }
    //         NotCommitted(not_committed) => {
    //             panic!(
    //                 "Expected Committed result, got NotCommitted: {:?}",
    //                 not_committed
    //             );
    //         }
    //     }

    //     // Receive the finished work
    //     let (num_transactions, _) = scheduler
    //         .receive_completed(&mut container, &BufferedPacketsDecision::Forward)
    //         .unwrap();
    //     assert_eq!(num_transactions, 0);

    //     // Check that container + prio-graph are empty
    //     assert!(
    //         container.pop().is_none(),
    //         "Container should be empty after processing all transactions"
    //     );
    //     assert!(
    //         scheduler.prio_graph.is_empty(),
    //         "Prio-graph should be empty after processing all transactions"
    //     );

    //     // Receive the NotCommitted Result
    //     let response = response_receiver.try_recv().unwrap();
    //     let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
    //         panic!("Expected AtomicTxnBatchResult message");
    //     };
    //     assert_eq!(bundle_result.seq_id, 2);
    //     assert!(
    //         bundle_result.result.is_some(),
    //         "Bundle result should be present"
    //     );
    //     let result = bundle_result.result.unwrap();
    //     match result {
    //         Committed(_) => {
    //             panic!("Expected NotCommitted result, got Committed");
    //         }
    //         NotCommitted(not_committed) => {
    //             assert!(
    //                 not_committed.reason.is_some(),
    //                 "NotCommitted reason should be present"
    //             );
    //             let reason = not_committed.reason.unwrap();
    //             assert_eq!(
    //                 reason,
    //                 jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
    //                     jito_protos::proto::bam_types::SchedulingError::OutsideLeaderSlot as i32
    //                 )
    //             );
    //         }
    //     }
    // }

    // #[test]
    // #[should_panic(expected = "node must exist")]
    // #[ignore]
    // fn test_prio_graph_clears_on_slot_boundary() {
    //     let (bank_forks, _) = test_bank_forks();
    //     let TestScheduler {
    //         mut scheduler,
    //         consume_work_receivers: _,
    //         finished_consume_work_sender: _,
    //         response_receiver: _,
    //     } = create_test_scheduler(4, &bank_forks);

    //     let keypair_a = Keypair::new();
    //     let keypair_b = Keypair::new();

    //     // Create container with some transactions
    //     let mut container = create_container(vec![
    //         (
    //             &keypair_a,
    //             vec![Pubkey::new_unique()],
    //             1000,
    //             seq_id_to_priority(0),
    //         ),
    //         (
    //             &keypair_b,
    //             vec![Pubkey::new_unique()],
    //             2000,
    //             seq_id_to_priority(1),
    //         ),
    //     ]);

    //     let bank = bank_forks.read().unwrap().working_bank();

    //     // Set initial slot with bank start
    //     let decision = BufferedPacketsDecision::Consume(bank.clone());

    //     scheduler
    //         .receive_completed(&mut container, &decision)
    //         .unwrap();
    //     assert_eq!(scheduler.slot, Some(bank.slot()));

    //     // Pull transactions into prio_graph
    //     scheduler.pull_into_prio_graph(&mut container);
    //     assert!(
    //         !scheduler.prio_graph.is_empty(),
    //         "Prio graph should have transactions"
    //     );

    //     // Store transaction IDs that are currently in the prio_graph
    //     let mut stored_txn_ids = Vec::new();
    //     while let Some(txn_id) = scheduler.prio_graph.pop() {
    //         stored_txn_ids.push(txn_id);
    //         // Unblock to allow the next transaction to be popped
    //         scheduler.prio_graph.unblock(&txn_id);
    //     }

    //     // Re-insert the transactions back into prio_graph for testing
    //     for txn_id in &stored_txn_ids {
    //         // Get transaction from container to re-insert
    //         if let Some((batch_ids, _, _)) = container.get_batch(txn_id.id) {
    //             let txns = batch_ids
    //                 .iter()
    //                 .filter_map(|id| container.get_transaction(*id));
    //             scheduler.prio_graph.insert_transaction(
    //                 *txn_id,
    //                 BamScheduler::<RuntimeTransaction<SanitizedTransaction>>::get_transactions_account_access(txns.into_iter()),
    //             );
    //         }
    //     }

    //     // Simulate slot boundary change by changing to no bank (None)
    //     let decision_no_bank = BufferedPacketsDecision::Forward;
    //     scheduler
    //         .receive_completed(&mut container, &decision_no_bank)
    //         .unwrap();

    //     assert_eq!(scheduler.slot, None);

    //     // This should panic because the prio_graph has been cleared
    //     // and the transaction ID no longer exists in the graph
    //     if let Some(first_id) = stored_txn_ids.first() {
    //         scheduler.prio_graph.unblock(first_id);
    //     }
    // }
}
