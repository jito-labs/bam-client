use crossbeam_channel::{Receiver, Sender};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;

use crate::banking_stage::scheduler_messages::{ConsumeWork, FinishedConsumeWork};

use super::{scheduler::{Scheduler, SchedulingSummary}, scheduler_error::SchedulerError, transaction_state_container::StateContainer};

pub struct FifoBatchScheduler<Tx: TransactionWithMeta> {
    consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
}

impl<Tx: TransactionWithMeta> FifoBatchScheduler<Tx> {
    pub fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    ) -> Self {
        Self {
            consume_work_senders,
            finished_consume_work_receiver,
        }
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx> for FifoBatchScheduler<Tx> {
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        pre_graph_filter: impl Fn(&[&Tx], &mut [bool]),
        pre_lock_filter: impl Fn(&Tx) -> bool,
    ) -> Result<SchedulingSummary, SchedulerError> {
        todo!()
    }

    fn receive_completed(
        &mut self,
        container: &mut impl StateContainer<Tx>,
    ) -> Result<(usize, usize), SchedulerError> {
        todo!()
    }
}