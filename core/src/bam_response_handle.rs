use std::sync::Arc;

use jito_protos::proto::bam_types::{
    atomic_txn_batch_result, not_committed::Reason, LeaderState, SchedulingError,
};
use solana_runtime::bank::Bank;

use crate::bam_dependencies::BamOutboundMessage;

#[derive(Clone)]
pub struct BamResponseHandle {
    sender: crossbeam_channel::Sender<BamOutboundMessage>,
}

/// The handle which abstracts sending messages to the BAM node.
impl BamResponseHandle {
    pub fn new(sender: crossbeam_channel::Sender<BamOutboundMessage>) -> Self {
        Self { sender }
    }

    /// Sends the leader state to the BAM node
    /// Returns true if the message was sent successfully and false otherwise
    pub fn send_leader_state(&self, bank: &Arc<Bank>) -> bool {
        let leader_state = Self::generate_leader_state(bank);
        self.sender
            .try_send(BamOutboundMessage::LeaderState(leader_state))
            .is_ok()
    }

    /// Sends a not committed result for a bundle that is outside the leader slot for this sequence id
    pub fn send_outside_leader_slot_bundle_result(&self, seq_id: u32) -> bool {
        self.sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
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
            ))
            .is_ok()
    }

    fn generate_leader_state(bank: &Bank) -> LeaderState {
        let max_block_cu = bank.read_cost_tracker().unwrap().block_cost_limit();
        let consumed_block_cu = bank.read_cost_tracker().unwrap().block_cost();
        let slot_cu_budget_remaining = max_block_cu.saturating_sub(consumed_block_cu) as u32;
        LeaderState {
            slot: bank.slot(),
            tick: (bank.tick_height() % bank.ticks_per_slot()) as u32,
            slot_cu_budget_remaining,
        }
    }
}
