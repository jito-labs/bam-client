use std::sync::Arc;

use jito_protos::proto::bam_types::{
    atomic_txn_batch_result, not_committed::Reason, DeserializationErrorReason, LeaderState,
    SchedulingError, TransactionErrorReason,
};
use solana_runtime::bank::Bank;
use solana_transaction_error::TransactionError;

use crate::{
    bam_dependencies::BamOutboundMessage,
    banking_stage::{
        immutable_deserialized_packet::DeserializedPacketError,
        scheduler_messages::NotCommittedReason,
    },
};

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

    pub fn send_container_full_txn_batch_result(&self, seq_id: u32) -> bool {
        self.sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::ContainerFull as i32,
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
            NotCommittedReason::Error(err) => {
                jito_protos::proto::bam_types::not_committed::Reason::TransactionError(
                    jito_protos::proto::bam_types::TransactionError {
                        index: index as u32,
                        reason: Self::convert_txn_error_to_proto(err) as i32,
                    },
                )
            }
        }
    }

    pub fn convert_txn_error_to_proto(err: TransactionError) -> TransactionErrorReason {
        match err {
            TransactionError::AccountInUse => TransactionErrorReason::AccountInUse,
            TransactionError::AccountLoadedTwice => TransactionErrorReason::AccountLoadedTwice,
            TransactionError::AccountNotFound => TransactionErrorReason::AccountNotFound,
            TransactionError::ProgramAccountNotFound => {
                TransactionErrorReason::ProgramAccountNotFound
            }
            TransactionError::InsufficientFundsForFee => {
                TransactionErrorReason::InsufficientFundsForFee
            }
            TransactionError::InvalidAccountForFee => TransactionErrorReason::InvalidAccountForFee,
            TransactionError::AlreadyProcessed => TransactionErrorReason::AlreadyProcessed,
            TransactionError::BlockhashNotFound => TransactionErrorReason::BlockhashNotFound,
            TransactionError::InstructionError(_, _) => TransactionErrorReason::InstructionError,
            TransactionError::CallChainTooDeep => TransactionErrorReason::CallChainTooDeep,
            TransactionError::MissingSignatureForFee => {
                TransactionErrorReason::MissingSignatureForFee
            }
            TransactionError::InvalidAccountIndex => TransactionErrorReason::InvalidAccountIndex,
            TransactionError::SignatureFailure => TransactionErrorReason::SignatureFailure,
            TransactionError::InvalidProgramForExecution => {
                TransactionErrorReason::InvalidProgramForExecution
            }
            TransactionError::SanitizeFailure => TransactionErrorReason::SanitizeFailure,
            TransactionError::ClusterMaintenance => TransactionErrorReason::ClusterMaintenance,
            TransactionError::AccountBorrowOutstanding => {
                TransactionErrorReason::AccountBorrowOutstanding
            }
            TransactionError::WouldExceedMaxBlockCostLimit => {
                TransactionErrorReason::WouldExceedMaxBlockCostLimit
            }
            TransactionError::UnsupportedVersion => TransactionErrorReason::UnsupportedVersion,
            TransactionError::InvalidWritableAccount => {
                TransactionErrorReason::InvalidWritableAccount
            }
            TransactionError::WouldExceedMaxAccountCostLimit => {
                TransactionErrorReason::WouldExceedMaxAccountCostLimit
            }
            TransactionError::WouldExceedAccountDataBlockLimit => {
                TransactionErrorReason::WouldExceedAccountDataBlockLimit
            }
            TransactionError::TooManyAccountLocks => TransactionErrorReason::TooManyAccountLocks,
            TransactionError::AddressLookupTableNotFound => {
                TransactionErrorReason::AddressLookupTableNotFound
            }
            TransactionError::InvalidAddressLookupTableOwner => {
                TransactionErrorReason::InvalidAddressLookupTableOwner
            }
            TransactionError::InvalidAddressLookupTableData => {
                TransactionErrorReason::InvalidAddressLookupTableData
            }
            TransactionError::InvalidAddressLookupTableIndex => {
                TransactionErrorReason::InvalidAddressLookupTableIndex
            }
            TransactionError::InvalidRentPayingAccount => {
                TransactionErrorReason::InvalidRentPayingAccount
            }
            TransactionError::WouldExceedMaxVoteCostLimit => {
                TransactionErrorReason::WouldExceedMaxVoteCostLimit
            }
            TransactionError::WouldExceedAccountDataTotalLimit => {
                TransactionErrorReason::WouldExceedAccountDataTotalLimit
            }
            TransactionError::DuplicateInstruction(_) => {
                TransactionErrorReason::DuplicateInstruction
            }
            TransactionError::InsufficientFundsForRent { .. } => {
                TransactionErrorReason::InsufficientFundsForRent
            }
            TransactionError::MaxLoadedAccountsDataSizeExceeded => {
                TransactionErrorReason::MaxLoadedAccountsDataSizeExceeded
            }
            TransactionError::InvalidLoadedAccountsDataSizeLimit => {
                TransactionErrorReason::InvalidLoadedAccountsDataSizeLimit
            }
            TransactionError::ResanitizationNeeded => TransactionErrorReason::ResanitizationNeeded,
            TransactionError::ProgramExecutionTemporarilyRestricted { .. } => {
                TransactionErrorReason::ProgramExecutionTemporarilyRestricted
            }
            TransactionError::UnbalancedTransaction => {
                TransactionErrorReason::UnbalancedTransaction
            }
            TransactionError::ProgramCacheHitMaxLimit => {
                TransactionErrorReason::ProgramCacheHitMaxLimit
            }
            TransactionError::CommitCancelled => TransactionErrorReason::CommitCancelled,
        }
    }

    pub fn convert_deserialize_error_to_proto(
        err: &DeserializedPacketError,
    ) -> DeserializationErrorReason {
        match err {
            DeserializedPacketError::ShortVecError(_) => DeserializationErrorReason::BincodeError,
            DeserializedPacketError::DeserializationError(_) => {
                DeserializationErrorReason::BincodeError
            }
            DeserializedPacketError::SignatureOverflowed(_) => {
                DeserializationErrorReason::SignatureOverflowed
            }
            DeserializedPacketError::SanitizeError(_) => DeserializationErrorReason::SanitizeError,
            DeserializedPacketError::PrioritizationFailure => {
                DeserializationErrorReason::PrioritizationFailure
            }
            DeserializedPacketError::VoteTransactionError => {
                DeserializationErrorReason::VoteTransactionFailure
            }
        }
    }
}
