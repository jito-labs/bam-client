use crate::bam_response_handle::BamResponseHandle;
use crate::banking_stage::scheduler_messages::MaxAge;
/// A Scheduler implementation that pulls batches off the container, and then
/// schedules them to workers in a FIFO, account-aware manner. This is facilitated by the
/// `PrioGraph` data structure, which is a directed graph that tracks the dependencies.
///
use crate::banking_stage::transaction_scheduler::scheduler::PreLockFilterAction;
use crate::banking_stage::transaction_scheduler::scheduler_common::SchedulingCommon;
use crate::banking_stage::transaction_scheduler::transaction_state::TransactionState;
use crate::banking_stage::transaction_scheduler::transaction_state_container::BatchInfo;

use ahash::{HashSet, HashSetExt};
use itertools::Itertools;
use solana_clock::{Slot, MAX_PROCESSING_AGE};
use solana_runtime::bank_forks::BankForks;
use solana_svm::transaction_error_metrics::TransactionErrorMetrics;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Instant,
};

use {
    super::{
        bam_receive_and_buffer::priority_to_seq_id,
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_error::SchedulerError,
        transaction_priority_id::TransactionPriorityId,
        transaction_state_container::StateContainer,
    },
    crate::banking_stage::{
        decision_maker::BufferedPacketsDecision,
        scheduler_messages::{ConsumeWork, FinishedConsumeWork, TransactionBatchId},
    },
    crossbeam_channel::{Receiver, Sender},
    prio_graph::{AccessKind, GraphNode, PrioGraph},
    solana_pubkey::Pubkey,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm_transaction::svm_message::SVMMessage,
};

type SchedulerPrioGraph = PrioGraph<
    TransactionPriorityId,
    Pubkey,
    TransactionPriorityId,
    fn(&TransactionPriorityId, &GraphNode<TransactionPriorityId>) -> TransactionPriorityId,
>;

struct InflightBatchInfo {
    batch_priority_id: TransactionPriorityId,
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
}

impl<Tx: TransactionWithMeta> BamScheduler<Tx> {
    pub fn new(
        consume_work_sender: Sender<ConsumeWork<Tx>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        bam_response_handle: BamResponseHandle,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        let capacity = consume_work_sender.capacity().unwrap_or(1024);

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
            inflight_batches: HashMap::with_capacity(capacity),
            prio_graph: PrioGraph::new(|id, _graph_node| *id),
            slot: None,
            bank_forks,
            prio_graph_ids: HashSet::with_capacity(capacity),
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

            let txns = transaction_ids
                .iter()
                .map(|txn_id| {
                    container
                        .get_transaction(*txn_id)
                        .expect("transaction must exist")
                })
                .collect_vec();

            self.prio_graph.insert_transaction(
                next_batch_id,
                Self::get_transactions_account_access(txns.into_iter()),
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
    ) {
        const MAX_INFLIGHT_BATCHES: usize = 500;

        let working_bank = self.bank_forks.read().unwrap().working_bank();

        while let Some(batch_priority_id) = self.prio_graph.pop() {
            let batch_info = container
                .get_batch(batch_priority_id.id)
                .expect("batch must exist");

            let max_schedule_slot = batch_info.max_schedule_slot;
            let transaction_ids = batch_info.transaction_ids.to_vec();
            let revert_on_error = batch_info.revert_on_error;

            // Assumption: priority graph gets cleared on slot boundary
            // Otherwise would be worth checking the max_schedule_slot here

            let (transactions, max_ages): (Vec<Tx>, Vec<MaxAge>) = transaction_ids
                .iter()
                .map(|txn_id| {
                    let tx = container
                        .get_mut_transaction_state(*txn_id)
                        .expect("transaction must exist");
                    tx.take_transaction_for_scheduling()
                })
                .unzip();

            // Save bounce between between the coordinator, worker, and back for common errors.
            // It's expected that BAM will handle most of these errors, but durable nonces are annoying.
            let lock_results = (0..transactions.len())
                .map(|_| Ok(()))
                .collect::<Vec<solana_transaction_error::TransactionResult<()>>>();
            let check_result = working_bank.check_transactions::<Tx>(
                &transactions,
                lock_results.as_slice(),
                MAX_PROCESSING_AGE,
                &mut TransactionErrorMetrics::default(),
            );
            if let Some((index, err)) = check_result
                .iter()
                .find_position(|res| res.is_err())
                .map(|(i, res)| (i, res.as_ref().err().unwrap()))
            {
                container.remove_batch_by_id(batch_priority_id.id);
                self.prio_graph.unblock(&batch_priority_id);

                self.bam_response_handle.send_not_committed_result(
                    priority_to_seq_id(batch_priority_id.priority),
                    index,
                    err.clone(),
                );

                *num_filtered_out += transaction_ids.len();
                continue;
            }

            // Schedule it
            let work = ConsumeWork {
                batch_id: TransactionBatchId::new(batch_priority_id.id as u64),
                ids: transaction_ids.to_vec(),
                transactions,
                max_ages,
                revert_on_error,
                respond_with_extra_info: true,
                max_schedule_slot: Some(max_schedule_slot),
            };
            let _ = self.consume_work_sender.send(work);
            *num_scheduled += transaction_ids.len();

            self.inflight_batches.insert(
                batch_priority_id.id as u64,
                InflightBatchInfo { batch_priority_id },
            );

            // Limit how much work needs to come back from the workers, which is helpful at slot boundaries under high load.
            if self.inflight_batches.len() > MAX_INFLIGHT_BATCHES {
                break;
            }
        }
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

        let start = Instant::now();
        let mut num_drained = 0;
        while !self.inflight_batches.is_empty() {
            let Ok(work) = self.finished_consume_work_receiver.recv() else {
                break;
            };
            let batch_info = self
                .inflight_batches
                .remove(&work.work.batch_id.0)
                .expect("batch must be inflight");
            self.prio_graph_ids.remove(&batch_info.batch_priority_id);

            // the container and the priority graph will be cleared later
            // container.remove_batch_by_id(batch_info.batch_priority_id.id);
            // self.prio_graph.unblock(&batch_info.batch_priority_id);

            self.bam_response_handle
                .send_outside_leader_slot_bundle_result(priority_to_seq_id(
                    batch_info.batch_priority_id.priority,
                ));
            num_drained += 1;
        }
        println!(
            "inflight_batches: drained {num_drained} batches in {:?}us",
            start.elapsed().as_micros()
        );

        // On slot boundaries, all the transactions are removed from the container and the priority graph is cleared.
        // Anything left in the container at the end of the slot is outside the leader slot window.
        // The prio_graph_ids is used instead of the priority graph pop_and_unblock because its much faster.
        let start = Instant::now();
        let mut num_drained = 0;
        for next_batch_id in self.prio_graph_ids.drain() {
            // container.remove_batch_by_id(next_batch_id.id);
            self.bam_response_handle
                .send_outside_leader_slot_bundle_result(priority_to_seq_id(next_batch_id.priority));
            num_drained += 1;
        }
        println!(
            "prio_graph_ids: drained {num_drained} batches in {:?}us",
            start.elapsed().as_micros()
        );

        // Anything left in the container was pushed into the queue but hasn't been pulled into the priority graph yet
        let mut num_drained = 0;
        let start = Instant::now();
        while let Some(next_batch_id) = container.pop_batch() {
            // container.remove_batch_by_id(next_batch_id.id);
            self.bam_response_handle
                .send_outside_leader_slot_bundle_result(priority_to_seq_id(next_batch_id.priority));
            num_drained += 1;
        }
        println!(
            "container: drained {num_drained} batches in {:?}us",
            start.elapsed().as_micros()
        );

        debug!(
            "container size: {}, batch buffer size: {}, queue size: {}, batch queue size: {}",
            container.buffer_size(),
            container.batch_buffer_size(),
            container.queue_size(),
            container.batch_queue_size()
        );

        // assert_eq!(container.batch_buffer_size(), 0);
        // assert_eq!(container.batch_queue_size(), 0);
        // assert_eq!(container.buffer_size(), 0);
        // assert_eq!(container.queue_size(), 0);

        self.prio_graph.clear();
        container.clear();
        self.report_histogram_metrics();
    }

    fn report_histogram_metrics(&mut self) {
        // datapoint_info!(
        //     "bam_scheduler_bank_boundary-metrics",
        //     (
        //         "time_in_priograph_us_p50",
        //         self.time_in_priograph_us
        //             .percentile(50.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_priograph_us_p75",
        //         self.time_in_priograph_us
        //             .percentile(75.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_priograph_us_p90",
        //         self.time_in_priograph_us
        //             .percentile(90.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_priograph_us_p99",
        //         self.time_in_priograph_us
        //             .percentile(99.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_priograph_us_max",
        //         self.time_in_priograph_us.maximum().unwrap_or_default(),
        //         i64
        //     ),
        // );
        // self.time_in_priograph_us.clear();

        // datapoint_info!(
        //     "bam_scheduler_worker_time_metrics",
        //     (
        //         "time_in_worker_us_p50",
        //         self.time_in_worker_us.percentile(50.0).unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_worker_us_p75",
        //         self.time_in_worker_us.percentile(75.0).unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_worker_us_p90",
        //         self.time_in_worker_us.percentile(90.0).unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_worker_us_p99",
        //         self.time_in_worker_us.percentile(99.0).unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_in_worker_us_max",
        //         self.time_in_worker_us.maximum().unwrap_or_default(),
        //         i64
        //     ),
        // );
        // self.time_in_worker_us.clear();

        // datapoint_info!(
        //     "bam_scheduler_time_between_schedules_metrics",
        //     (
        //         "time_between_schedule_us_p50",
        //         self.time_between_schedule_us
        //             .percentile(50.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_between_schedule_us_p75",
        //         self.time_between_schedule_us
        //             .percentile(75.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_between_schedule_us_p90",
        //         self.time_between_schedule_us
        //             .percentile(90.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_between_schedule_us_p99",
        //         self.time_between_schedule_us
        //             .percentile(99.0)
        //             .unwrap_or_default(),
        //         i64
        //     ),
        //     (
        //         "time_between_schedule_us_max",
        //         self.time_between_schedule_us.maximum().unwrap_or_default(),
        //         i64
        //     ),
        // );
        // self.time_between_schedule_us.clear();
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
        self.send_to_workers(container, &mut num_scheduled, &mut num_filtered_out);

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

        let num_transactions = 0;
        while let Ok(result) = self.finished_consume_work_receiver.try_recv() {
            let inflight_batch_info = self
                .inflight_batches
                .remove(&result.work.batch_id.0)
                .expect("batch must be inflight to be removed");

            self.prio_graph
                .unblock(&inflight_batch_info.batch_priority_id);
            self.prio_graph_ids
                .remove(&inflight_batch_info.batch_priority_id);
            container.remove_batch_by_id(inflight_batch_info.batch_priority_id.id);

            self.bam_response_handle.send_result(
                priority_to_seq_id(inflight_batch_info.batch_priority_id.priority),
                result.work.revert_on_error,
                result
                    .extra_info
                    .expect("bam requires extra info")
                    .processed_results,
            );
        }

        Ok((num_transactions, 0))
    }

    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bam_dependencies::BamOutboundMessage,
        banking_stage::transaction_scheduler::scheduler::PreLockFilterAction,
    };
    use {
        crate::banking_stage::{
            decision_maker::BufferedPacketsDecision,
            scheduler_messages::{
                ConsumeWork, FinishedConsumeWork, MaxAge, NotCommittedReason, TransactionResult,
            },
            tests::create_slow_genesis_config,
            transaction_scheduler::{
                bam_receive_and_buffer::seq_id_to_priority,
                bam_scheduler::BamScheduler,
                scheduler::Scheduler,
                transaction_state_container::{StateContainer, TransactionStateContainer},
            },
        },
        crossbeam_channel::unbounded,
        itertools::Itertools,
        jito_protos::proto::bam_types::{
            atomic_txn_batch_result::Result::{Committed, NotCommitted},
            TransactionCommittedResult,
        },
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_system_interface::instruction::transfer_many,
        solana_transaction::sanitized::SanitizedTransaction,
        solana_transaction::Transaction,
        std::{
            borrow::Borrow,
            sync::{Arc, RwLock},
        },
    };

    struct TestScheduler {
        scheduler: BamScheduler<RuntimeTransaction<SanitizedTransaction>>,
        consume_work_receivers:
            Vec<crossbeam_channel::Receiver<ConsumeWork<RuntimeTransaction<SanitizedTransaction>>>>,
        finished_consume_work_sender: crossbeam_channel::Sender<
            FinishedConsumeWork<RuntimeTransaction<SanitizedTransaction>>,
        >,
        response_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,
    }

    fn create_test_scheduler(
        num_threads: usize,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> TestScheduler {
        let (consume_work_sender, consume_work_receiver) = unbounded();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
        let (response_sender, response_receiver) = unbounded();
        test_bank_forks();
        let scheduler = BamScheduler::new(
            consume_work_sender,
            finished_consume_work_receiver,
            response_sender,
            bank_forks.clone(),
        );
        TestScheduler {
            scheduler,
            consume_work_receivers: (0..num_threads)
                .map(|_| consume_work_receiver.clone())
                .collect(),
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
            const TEST_TRANSACTION_COST: u64 = 5000;
            container.insert_new_batch(
                vec![(transaction, MaxAge::MAX)],
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
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receivers: _,
            finished_consume_work_sender: _,
            response_receiver: _,
        } = create_test_scheduler(4, &bank_forks);

        let mut container = TransactionStateContainer::with_capacity(100);
        let result = scheduler
            .schedule(
                &mut container,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();
        assert_eq!(result.num_scheduled, 0);
    }

    #[test]
    fn test_scheduler_basic() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receivers,
            finished_consume_work_sender,
            response_receiver,
        } = create_test_scheduler(4, &bank_forks);
        scheduler.extra_checks_enabled = false;

        let keypair_a = Keypair::new();

        let first_recipient = Pubkey::new_unique();
        let second_recipient = Pubkey::new_unique();

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
                vec![second_recipient],
                2000,
                seq_id_to_priority(3),
            ),
        ]);

        assert!(
            scheduler.slot.is_none(),
            "Scheduler slot should be None initially"
        );

        let decision = BufferedPacketsDecision::Consume(bank_forks.read().unwrap().working_bank());

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
            .schedule(
                &mut container,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();

        // Only two should have been scheduled as one is blocked
        assert_eq!(result.num_scheduled, 2);

        // Receive the scheduled work
        let work_1 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_1.ids.len(), 1);
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);

        // Check that the first transaction is from keypair_a and first recipient is the first recipient
        assert_eq!(
            work_1.transactions[0].message().account_keys()[0],
            keypair_a.pubkey()
        );
        assert_eq!(
            work_1.transactions[0].message().account_keys()[1],
            first_recipient
        );

        // Check that the second transaction is from the other keypair
        assert_ne!(
            work_2.transactions[0].message().account_keys()[0],
            keypair_a.pubkey(),
        );
        assert_eq!(
            work_2.transactions[0].message().account_keys()[1],
            second_recipient
        );

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler
            .schedule(
                &mut container,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();
        assert_eq!(result.num_scheduled, 0);

        // Respond with finished work
        let responses = [
            (
                work_1,
                TransactionResult::Committed(TransactionCommittedResult {
                    cus_consumed: 100,
                    feepayer_balance_lamports: 1000,
                    loaded_accounts_data_size: 10,
                    execution_success: true,
                }),
            ), // Committed
            (
                work_2,
                TransactionResult::NotCommitted(NotCommittedReason::PohTimeout),
            ), // Not committed
        ];
        for (work, response) in responses.into_iter() {
            let finished_work = FinishedConsumeWork {
                work,
                retryable_indexes: vec![],
                extra_info: Some(
                    crate::banking_stage::scheduler_messages::FinishedConsumeWorkExtraInfo {
                        processed_results: vec![response],
                    },
                ),
            };
            let _ = finished_consume_work_sender.send(finished_work);
        }

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(num_transactions, 2);

        // Check the responses
        let response = response_receiver.try_recv().unwrap();
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
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
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
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
            .schedule(
                &mut container,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();
        assert_eq!(result.num_scheduled, 1);
        // Check that the remaining transaction is sent to the worker
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler
            .schedule(
                &mut container,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
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
                            execution_success: true,
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

        // Check the response for the next transaction
        let response = response_receiver.try_recv().unwrap();
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
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
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
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

    #[test]
    #[should_panic(expected = "node must exist")]
    #[ignore]
    fn test_prio_graph_clears_on_slot_boundary() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receivers: _,
            finished_consume_work_sender: _,
            response_receiver: _,
        } = create_test_scheduler(4, &bank_forks);

        let keypair_a = Keypair::new();
        let keypair_b = Keypair::new();

        // Create container with some transactions
        let mut container = create_container(vec![
            (
                &keypair_a,
                vec![Pubkey::new_unique()],
                1000,
                seq_id_to_priority(0),
            ),
            (
                &keypair_b,
                vec![Pubkey::new_unique()],
                2000,
                seq_id_to_priority(1),
            ),
        ]);

        let bank = bank_forks.read().unwrap().working_bank();

        // Set initial slot with bank start
        let decision = BufferedPacketsDecision::Consume(bank.clone());

        scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(scheduler.slot, Some(bank.slot()));

        // Pull transactions into prio_graph
        scheduler.pull_into_prio_graph(&mut container);
        assert!(
            !scheduler.prio_graph.is_empty(),
            "Prio graph should have transactions"
        );

        // Store transaction IDs that are currently in the prio_graph
        let mut stored_txn_ids = Vec::new();
        while let Some(txn_id) = scheduler.prio_graph.pop() {
            stored_txn_ids.push(txn_id);
            // Unblock to allow the next transaction to be popped
            scheduler.prio_graph.unblock(&txn_id);
        }

        // Re-insert the transactions back into prio_graph for testing
        for txn_id in &stored_txn_ids {
            // Get transaction from container to re-insert
            if let Some((batch_ids, _, _)) = container.get_batch(txn_id.id) {
                let txns = batch_ids
                    .iter()
                    .filter_map(|id| container.get_transaction(*id));
                scheduler.prio_graph.insert_transaction(
                    *txn_id,
                    BamScheduler::<RuntimeTransaction<SanitizedTransaction>>::get_transactions_account_access(txns.into_iter()),
                );
            }
        }

        // Simulate slot boundary change by changing to no bank (None)
        let decision_no_bank = BufferedPacketsDecision::Forward;
        scheduler
            .receive_completed(&mut container, &decision_no_bank)
            .unwrap();

        assert_eq!(scheduler.slot, None);

        // This should panic because the prio_graph has been cleared
        // and the transaction ID no longer exists in the graph
        if let Some(first_id) = stored_txn_ids.first() {
            scheduler.prio_graph.unblock(first_id);
        }
    }
}
