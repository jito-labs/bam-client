use arrayvec::ArrayVec;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;

use crate::banking_stage::{
    scheduler_messages::{ConsumeWork, MaxAge, TransactionBatchId},
    transaction_scheduler::{
        transaction_priority_id::TransactionPriorityId,
        transaction_state_container::{BatchInfo, StateContainer, TransactionViewStateContainer},
    },
};

const MAX_ITEMS_PER_CHECK: usize = 64;
const MAX_ITEMS_PER_WORK: usize = 8;

/// Responsible for assembling batches of transactions for the BAM consumers.
pub struct BamWorkAssembler<Tx: TransactionWithMeta> {
    batch_ids: ArrayVec<TransactionPriorityId, MAX_ITEMS_PER_CHECK>,
    batch_lengths: ArrayVec<usize, MAX_ITEMS_PER_CHECK>,
    transactions: ArrayVec<Tx, MAX_ITEMS_PER_CHECK>,
    max_ages: ArrayVec<MaxAge, MAX_ITEMS_PER_CHECK>,
}

impl<Tx: TransactionWithMeta> BamWorkAssembler<Tx> {
    pub fn new() -> Self {
        Self {
            batch_ids: ArrayVec::new(),
            batch_lengths: ArrayVec::new(),
            transactions: ArrayVec::new(),
            max_ages: ArrayVec::new(),
        }
    }

    pub fn assemble_work(
        &mut self,
        container: &mut TransactionViewStateContainer<Tx>,
        max_schedule_slot: u64,
        transaction_ids: &[TransactionId],
        revert_on_error: bool,
        batch_id: &mut u64,
    ) -> Option<ConsumeWork<Tx>> {
        let (transactions, max_ages): (Vec<Tx>, Vec<MaxAge>) = transaction_ids
            .iter()
            .map(|txn_id| {
                let tx = container
                    .get_mut_transaction_state(*txn_id)
                    .expect("transaction must exist");
                tx.take_transaction_for_scheduling()
            })
            .unzip();

        if revert_on_error {
        } else {
        }

        todo!()
    }

    // pub fn get_consume_work(&self) -> ConsumeWork<Tx> {
    //     todo!()
    // }

    // fn can_add_to_consume_work(
    //     &self,
    //     consume_work: &ConsumeWork<Tx>,
    //     batch_info: &BatchInfo,
    // ) -> bool {
    //     consume_work.transactions.len() + batch_info.transaction_ids.len()
    //         <= self.max_transactions_per_batch
    //         && (consume_work.max_schedule_slot.is_none()
    //             || batch_info.max_schedule_slot == consume_work.max_schedule_slot.unwrap())
    //         && consume_work.revert_on_error == batch_info.revert_on_error
    // }

    // fn get_consume_work(&self) -> ConsumeWork<Tx> {
    //     ConsumeWork {
    //         batch_id: TransactionBatchId::new(self.batch_id),
    //         ids: Vec::with_capacity(self.max_transactions_per_batch),
    //         transactions: Vec::with_capacity(self.max_transactions_per_batch),
    //         max_ages: Vec::with_capacity(self.max_transactions_per_batch),
    //         revert_on_error: false,
    //         respond_with_extra_info: true,
    //         max_schedule_slot: None,
    //     }
    // }

    // fn add_to_consume_work(
    //     consume_work: &mut ConsumeWork<Tx>,
    //     inflight_batch: &mut InflightBatchInfo,
    //     container: &mut impl StateContainer<Tx>,
    //     batch_priority_id: TransactionPriorityId,
    //     max_schedule_slot: u64,
    //     transaction_ids: Vec<TransactionId>,
    //     revert_on_error: bool,
    // ) {
    //     inflight_batch
    //         .batch_priority_ids
    //         .push((batch_priority_id, transaction_ids.len()));
    //     for idx in transaction_ids {
    //         let (tx, max_age) = container
    //             .get_mut_transaction_state(*txn_id)
    //             .expect("transaction must exist")
    //             .take_transaction_for_scheduling();
    //         consume_work.transactions.push(tx);
    //         consume_work.max_ages.push(max_age);
    //         consume_work.ids.push(batch_priority_id);
    //         consume_work.max_schedule_slot = Some(max_schedule_slot);
    //         consume_work.revert_on_error = revert_on_error;
    //     }
    // }
}
