use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use agave_transaction_view::{
    resolved_transaction_view::ResolvedTransactionView, transaction_version::TransactionVersion,
    transaction_view::SanitizedTransactionView,
};
use anchor_lang::solana_program::example_mocks::solana_sdk::transaction;
use arrayvec::ArrayVec;
use prio_graph::{AccessKind, GraphNode, PrioGraph};
use solana_accounts_db::account_locks::validate_account_locks;
use solana_clock::MAX_PROCESSING_AGE;
use solana_fee_structure::FeeBudgetLimits;
use solana_pubkey::Pubkey;
use solana_runtime::bank::Bank;
use solana_runtime_transaction::{
    runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
};
use solana_svm::transaction_error_metrics::TransactionErrorMetrics;
use solana_svm_transaction::svm_message::SVMMessage;
use solana_transaction::sanitized::MessageHash;
use solana_transaction_error::TransactionError;

use crate::{
    banking_stage::{
        consumer::Consumer,
        scheduler_messages::{ConsumeWork, MaxAge, TransactionBatchId},
        transaction_scheduler::{
            bam_receive_and_buffer::{priority_to_seq_id, seq_id_to_priority},
            receive_and_buffer::{calculate_max_age, calculate_priority_and_cost, ReceivingStats},
            transaction_priority_id::TransactionPriorityId,
            transaction_state_container::{
                RuntimeTransactionView, SharedBytes, StateContainer, TransactionViewState,
                TransactionViewStateContainer, EXTRA_CAPACITY,
            },
        },
    },
    verified_bam_packet_batch::VerifiedBamPacketBatch,
};

pub(crate) type SchedulerPrioGraph = PrioGraph<
    TransactionPriorityId,
    Pubkey,
    TransactionPriorityId,
    fn(&TransactionPriorityId, &GraphNode<TransactionPriorityId>) -> TransactionPriorityId,
>;

struct Inner {
    priority_graph: SchedulerPrioGraph,
    container: TransactionViewStateContainer,
}

#[derive(Clone)]
pub struct BamPriorityGraphContainer {
    inner: Arc<Mutex<Inner>>,
}

pub enum InsertError {
    OutsideLeaderSlot,
    PacketBatchTooLong,
    BadSignature,
    Sanitization,
    LockValidation,
    ComputeBudget,
    Capacity,
    BlacklistedAccount,
    TransactionError(TransactionError),
}

pub struct InsertResult {
    pub(crate) result: Result<(), (usize /* tx index in batch */, InsertError)>,
    pub(crate) receiving_stats: ReceivingStats,
}

pub struct BamPriorityGraphWork {
    pub batch_priority_id: TransactionPriorityId,
    pub revert_on_error: bool,
    pub max_schedule_slot: u64,
    pub transactions: Vec<RuntimeTransactionView>,
    pub max_ages: Vec<MaxAge>,
}

impl BamPriorityGraphContainer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                priority_graph: SchedulerPrioGraph::new(|id, _graph_node| *id),
                container: TransactionViewStateContainer::with_capacity(200_000, true),
            })),
        }
    }

    pub fn drain_with_callback<F>(&self, mut callback: F)
    where
        F: FnMut(&TransactionPriorityId),
    {
        let mut inner = self.inner.lock().unwrap();
        while let Some((batch_priority_id, _ids)) = inner.priority_graph.pop_and_unblock() {
            inner.container.remove_batch_by_id(batch_priority_id.id);
            callback(&batch_priority_id);
        }
    }

    pub fn get_work(&self) -> Option<BamPriorityGraphWork> {
        let mut inner = self.inner.lock().unwrap();
        let Some(batch_priority_id) = inner.priority_graph.pop() else {
            return None;
        };

        let batch_info = inner.container.get_batch(batch_priority_id.id).unwrap();
        let ids = batch_info.transaction_ids.to_vec();
        let revert_on_error = batch_info.revert_on_error;
        let max_schedule_slot = batch_info.max_schedule_slot;

        let (transactions, max_ages): (Vec<_>, Vec<_>) = ids
            .iter()
            .map(|id| {
                inner
                    .container
                    .get_mut_transaction_state(*id)
                    .unwrap()
                    .take_transaction_for_scheduling()
            })
            .unzip();

        Some(BamPriorityGraphWork {
            batch_priority_id,
            revert_on_error,
            max_schedule_slot,
            transactions,
            max_ages,
        })
    }

    pub fn notify_worker_consumed(&self, batch_priority_id: TransactionPriorityId) {
        let mut inner = self.inner.lock().unwrap();
        inner.priority_graph.unblock(&batch_priority_id);
        inner.container.remove_batch_by_id(batch_priority_id.id);
    }

    /// Inserts a batch into the container and priority graph
    pub fn try_insert_and_notify_workers(
        &self,
        bam_packet_batch: VerifiedBamPacketBatch,
        root_bank: &Arc<Bank>,
        working_bank: &Arc<Bank>,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> InsertResult {
        let mut stats = ReceivingStats::default();

        // Discard packets that are marked as discard by failed signature verification in BamSigverifyStage
        if bam_packet_batch.meta().discard
            || bam_packet_batch
                .packet_batch()
                .iter()
                .any(|packet| packet.meta().discard())
        {
            stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
            return InsertResult {
                result: Err((0, InsertError::BadSignature)),
                receiving_stats: stats,
            };
        }

        if working_bank.slot() > bam_packet_batch.meta().max_schedule_slot {
            stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
            return InsertResult {
                result: Err((0, InsertError::OutsideLeaderSlot)),
                receiving_stats: stats,
            };
        }

        // info!(
        //     "inserting batch: {:?} with max_schedule_slot: {} for bank: {}",
        //     bam_packet_batch.meta().seq_id,
        //     bam_packet_batch.meta().max_schedule_slot,
        //     working_bank.slot()
        // );

        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();

        // The 5 packet check exists when creating the VerifiedBamPacketBatch, but might get removed in the future.
        // This check exists to ensure that we don't accidentally overflow the transactions ArrayVec below.
        if bam_packet_batch.packet_batch().len() > EXTRA_CAPACITY {
            stats.num_dropped_without_parsing += bam_packet_batch.packet_batch().len();
            return InsertResult {
                result: Err((0, InsertError::PacketBatchTooLong)),
                receiving_stats: stats,
            };
        }

        let mut packet_data = ArrayVec::<_, EXTRA_CAPACITY>::new();
        let lock_results: [_; EXTRA_CAPACITY] = core::array::from_fn(|_| Ok(()));
        packet_data.extend(
            bam_packet_batch
                .packet_batch()
                .iter()
                .map(|p| p.data(..).unwrap()),
        );

        let mut inner = self.inner.lock().unwrap();

        let mut packet_index = 0;
        let mut packet_handling_error = None;

        // Tries to insert the packets into the container.
        // The branching behavior here is a bit confusing:
        // - Ok(Some(batch_id)) means the batch and all of its transactions were inserted successfully
        // - Ok(None) means an error occurred during insertion, all of the transactions were removed from the container and insert_map_error is set
        // - Err(()) means the container is full, nothing was added
        match inner.container.try_insert_map_only_with_batch(
            packet_data.as_slice(),
            bam_packet_batch.meta().revert_on_error,
            bam_packet_batch.meta().max_schedule_slot,
            |bytes| match Self::try_handle_packet(
                bytes,
                root_bank,
                working_bank,
                transaction_account_lock_limit,
                blacklisted_accounts,
            ) {
                Ok(state) => {
                    packet_index += 1;
                    Ok(state)
                }
                Err(InsertError::Sanitization) => {
                    packet_handling_error = Some((packet_index, InsertError::Sanitization));
                    stats.num_dropped_on_parsing_and_sanitization += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(InsertError::LockValidation) => {
                    packet_handling_error = Some((packet_index, InsertError::LockValidation));
                    stats.num_dropped_on_lock_validation += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(InsertError::ComputeBudget) => {
                    packet_handling_error = Some((packet_index, InsertError::ComputeBudget));
                    stats.num_dropped_on_compute_budget += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(InsertError::BlacklistedAccount) => {
                    packet_handling_error = Some((packet_index, InsertError::BlacklistedAccount));
                    stats.num_dropped_on_blacklisted_account += 1;
                    packet_index += 1;
                    Err(())
                }
                Err(_) => Err(()),
            },
        ) {
            Ok(Some(batch_id)) => {
                let transaction_ids = {
                    let batch_info = inner
                        .container
                        .get_batch(batch_id)
                        .expect("batch must exist");
                    let mut transaction_ids = ArrayVec::<_, EXTRA_CAPACITY>::new();
                    transaction_ids.extend(batch_info.transaction_ids.iter().cloned());
                    transaction_ids
                };

                // Note: mega-batching these transaction checks would probably speed things up
                let mut transactions = ArrayVec::<_, EXTRA_CAPACITY>::new();
                transactions.extend(transaction_ids.iter().map(|id| {
                    inner
                        .container
                        .get_transaction(*id)
                        .expect("transaction must exist")
                }));
                let mut error_counters = TransactionErrorMetrics::default();
                let check_results = working_bank.check_transactions::<RuntimeTransaction<_>>(
                    &transactions,
                    &lock_results[..transactions.len()],
                    MAX_PROCESSING_AGE,
                    &mut error_counters,
                );

                for (index, (result, _transaction_id)) in
                    check_results.iter().zip(transaction_ids.iter()).enumerate()
                {
                    match result {
                        Ok(_) => {
                            if let Err(e) = Consumer::check_fee_payer_unlocked(
                                working_bank,
                                transactions[index],
                                &mut error_counters,
                            ) {
                                stats.num_dropped_on_fee_payer += transaction_ids.len();
                                packet_handling_error =
                                    Some((index, InsertError::TransactionError(e)));
                                break;
                            }
                        }
                        Err(TransactionError::BlockhashNotFound) => {
                            stats.num_dropped_on_age += 1;
                            packet_handling_error = Some((
                                index,
                                InsertError::TransactionError(TransactionError::BlockhashNotFound),
                            ));
                            break;
                        }
                        Err(TransactionError::AlreadyProcessed) => {
                            stats.num_dropped_on_already_processed += 1;
                            packet_handling_error = Some((
                                index,
                                InsertError::TransactionError(TransactionError::AlreadyProcessed),
                            ));
                            break;
                        }
                        _ => {}
                    }
                }

                if packet_handling_error.is_some() {
                    drop(transactions);
                    inner.container.remove_batch_by_id(batch_id);
                } else {
                    // try not to collect here; but by not collecting + keeping the iterator, we keep transactions
                    // which is borrwed from the container
                    let account_access: Vec<(Pubkey, AccessKind)> =
                        Self::get_transactions_account_access(transactions.into_iter()).collect();
                    // The normal consumer using this container will insert it into the priority queue, at which
                    // point it'd be popped off by the scheduler (while the txs stay in the container).
                    // However, we can just not use the priority queue and stick it straight into the priority graph.
                    // This API is called insert_transaction, but we're just using it to insert a batch.
                    inner.priority_graph.insert_transaction(
                        TransactionPriorityId::new(
                            seq_id_to_priority(bam_packet_batch.meta().seq_id),
                            batch_id,
                        ),
                        account_access.into_iter(),
                    );
                    stats.num_buffered += bam_packet_batch.packet_batch().len();
                }
            }
            // Ok(None) means an error occurred during insertion, all of the transactions were removed from the container and insert_map_error is set
            Ok(None) => {}
            // container is full, nothing was added
            Err(()) => {
                stats.num_dropped_on_capacity += bam_packet_batch.packet_batch().len();
                packet_handling_error = Some((0, InsertError::Capacity));
            }
        }

        return InsertResult {
            result: match packet_handling_error {
                Some(packet_handling_error) => Err(packet_handling_error),
                None => Ok(()),
            },
            receiving_stats: stats,
        };
    }

    // See TransactionViewReceiveAndBuffer::try_handle_packet
    fn try_handle_packet(
        bytes: SharedBytes,
        root_bank: &Arc<Bank>,
        working_bank: &Arc<Bank>,
        transaction_account_lock_limit: usize,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Result<TransactionViewState, InsertError> {
        let alt_bank = root_bank;
        let sanitized_epoch = root_bank.epoch();

        // Parsing and basic sanitization checks
        let Ok(view) = SanitizedTransactionView::try_new_sanitized(bytes) else {
            return Err(InsertError::Sanitization);
        };

        let Ok(view) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
            view,
            MessageHash::Compute,
            None,
        ) else {
            return Err(InsertError::Sanitization);
        };

        // Discard non-vote packets if in vote-only mode.
        if root_bank.vote_only_bank() && !view.is_simple_vote_transaction() {
            return Err(InsertError::Sanitization);
        }

        // Check if the transaction has too many account locks before loading ALTs
        if view.total_num_accounts() as usize > transaction_account_lock_limit {
            return Err(InsertError::LockValidation);
        }

        // Load addresses for transaction.
        let load_addresses_result = match view.version() {
            TransactionVersion::Legacy => Ok((None, u64::MAX)),
            TransactionVersion::V0 => alt_bank
                .load_addresses_from_ref(view.address_table_lookup_iter())
                .map(|(loaded_addresses, deactivation_slot)| {
                    (Some(loaded_addresses), deactivation_slot)
                }),
        };
        let Ok((loaded_addresses, deactivation_slot)) = load_addresses_result else {
            return Err(InsertError::Sanitization);
        };

        let Ok(view) = RuntimeTransaction::<ResolvedTransactionView<_>>::try_from(
            view,
            loaded_addresses,
            root_bank.get_reserved_account_keys(),
        ) else {
            return Err(InsertError::Sanitization);
        };

        if validate_account_locks(view.account_keys(), transaction_account_lock_limit).is_err() {
            return Err(InsertError::LockValidation);
        }

        if view
            .account_keys()
            .iter()
            .any(|account| blacklisted_accounts.contains(account))
        {
            return Err(InsertError::BlacklistedAccount);
        }

        let Ok(compute_budget_limits) = view
            .compute_budget_instruction_details()
            .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
        else {
            return Err(InsertError::ComputeBudget);
        };

        let max_age = calculate_max_age(sanitized_epoch, deactivation_slot, alt_bank.slot());
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let (priority, cost) = calculate_priority_and_cost(&view, &fee_budget_limits, working_bank);

        Ok(TransactionViewState::new(view, max_age, priority, cost))
    }

    // pub fn insert_transaction(
    //     &self,
    //     batch_priority_id: TransactionPriorityId,
    //     account_access: impl Iterator<Item = (Pubkey, AccessKind)>,
    // ) {
    //     self.inner
    //         .lock()
    //         .unwrap()
    //         .priority_graph
    //         .insert_transaction(batch_priority_id, account_access);
    // }

    // pub fn pop(&self) -> Option<TransactionPriorityId> {
    //     self.inner.lock().unwrap().priority_graph.pop()
    // }

    // pub fn unblock(&self, batch_priority_id: &TransactionPriorityId) {
    //     self.inner
    //         .lock()
    //         .unwrap()
    //         .priority_graph
    //         .unblock(batch_priority_id);
    // }

    // pub fn pop_and_unblock(&self) -> Option<(TransactionPriorityId, Vec<TransactionPriorityId>)> {
    //     self.inner.lock().unwrap().priority_graph.pop_and_unblock()
    // }

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
}
