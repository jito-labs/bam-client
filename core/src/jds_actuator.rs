use std::{borrow::Cow, sync::{Arc, RwLock}};

/// Receives pre-scheduled microblocks and attempts to 'actuate' them by applying the transactions to the state.

use jito_protos::proto::jds_types::{micro_block_packet::Data, Bundle, MicroBlock, Packet};
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::{bank::{Bank, ExecutedTransactionCounts, LoadAndExecuteTransactionsOutput}, prioritization_fee_cache::PrioritizationFeeCache, transaction_batch::TransactionBatch, vote_sender_types::ReplayVoteSender};
use solana_sdk::{clock::MAX_PROCESSING_AGE, packet::{Meta, PacketFlags}, transaction::{MessageHash, SanitizedTransaction, VersionedTransaction}};
use solana_svm::transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig};

use crate::banking_stage::{committer::Committer, immutable_deserialized_packet::ImmutableDeserializedPacket, leader_slot_timing_metrics::LeaderExecuteAndCommitTimings};

pub struct JdsActuator {
    poh_recorder: Arc<RwLock<PohRecorder>>,
    transaction_committer: Committer,
}

impl JdsActuator {
    pub fn new(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        replay_vote_sender: ReplayVoteSender,
    ) -> Self {
        Self {
            poh_recorder,
            transaction_committer: Committer::new(
                None, // TODO
                replay_vote_sender,
                Arc::new(PrioritizationFeeCache::default()), // TODO
            )
        }
    }

    fn parse_validate_execute_bundle(_bundle: Bundle) {
        todo!();
    }

    fn parse_validate_execute_transaction(
        bank: &Bank,
        packet: Packet)
    -> Option<TransactionExecutionResult>
    {
        // 1. Parse
        let mut solana_packet = solana_sdk::packet::Packet::default();
        solana_packet.buffer_mut().copy_from_slice(&packet.data);
        if let Some(meta) = packet.meta {
            solana_packet.meta_mut().size = meta.size as usize;
            solana_packet.meta_mut().addr = meta.addr.parse().ok()?;
            solana_packet.meta_mut().port = meta.port as u16;
            if let Some(flags) = meta.flags {
                if flags.simple_vote_tx {
                    solana_packet.meta_mut().flags.insert(PacketFlags::SIMPLE_VOTE_TX);
                }
                if flags.forwarded {
                    solana_packet.meta_mut().flags.insert(PacketFlags::FORWARDED);
                }
                if flags.tracer_packet {
                    solana_packet.meta_mut().flags.insert(PacketFlags::TRACER_PACKET);
                }
                if flags.repair {
                    solana_packet.meta_mut().flags.insert(PacketFlags::REPAIR);
                }
            }
        }
        let packet = ImmutableDeserializedPacket::new(solana_packet).ok()?;
        let sanitized_transaction = packet.build_sanitized_transaction(
            false,
            bank,
            bank.get_reserved_account_keys())?;
        let transaction = sanitized_transaction.to_versioned_transaction();

        // 2. Lock
        let txns = vec![sanitized_transaction];
        let batch = bank.prepare_sanitized_batch(&txns);

        // 2. Execute
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let execute_output = bank
            .load_and_execute_transactions(
                &batch,
                MAX_PROCESSING_AGE,
                &mut execute_and_commit_timings.execute_timings,
                TransactionProcessingConfig {
                    account_overrides: None,
                    check_program_modification_slot: bank.check_program_modification_slot(),
                    compute_budget: bank.compute_budget(),
                    log_messages_bytes_limit: None,
                    limit_to_load_programs: true,
                    recording_config: ExecutionRecordingConfig::new_single_setting(
                        false,
                    ),
                    transaction_account_lock_limit: Some(bank.get_transaction_account_lock_limit()),
                }
            );
        Some(TransactionExecutionResult{
            execute_output,
            transaction,
        })
    }

    pub fn execute_and_commit_micro_block(&mut self, micro_block: MicroBlock) {
        let bank = self.poh_recorder.read().unwrap().bank().unwrap();

        // Try to execute everything in the block
        let mut results = vec![];
        for packet in micro_block.packets {
            let Some(packet) = packet.data else {
                continue;
            };

            match packet {
                Data::Bundle(bundle) => {
                    Self::parse_validate_execute_bundle(bundle);
                }
                Data::Packet(packet) => {
                    let Some(result) = Self::parse_validate_execute_transaction(&bank, packet) else {
                        results.push(ExecutionResult::Failure);
                        continue;
                    };
                    results.push(ExecutionResult::TransactionSuccess(result));
                }
            }
        }

        // Record and commit successes
        let transaction_recorder = self.poh_recorder.read().unwrap().new_recorder();
        let freeze_lock = bank.freeze_lock();
        for result in results {
            match result {
                ExecutionResult::TransactionSuccess(TransactionExecutionResult{ execute_output, transaction} ) => {
                    let LoadAndExecuteTransactionsOutput {
                        mut loaded_transactions,
                        execution_results,
                        mut retryable_transaction_indexes,
                        executed_transactions_count,
                        executed_non_vote_transactions_count,
                        executed_with_successful_result_count,
                        signature_count,
                        error_counters,
                        ..
                    } = execute_output;
                    let (last_blockhash, lamports_per_signature) = bank.last_blockhash_and_lamports_per_signature();
                    if !execution_results.first().unwrap().was_executed() {
                        continue;
                    }
                    let record_transactions_summary =
                        transaction_recorder.record_transactions(bank.slot(), vec![vec![transaction]]);

                    // TODO: commit
                }
                ExecutionResult::Failure => {}
            }
        }
    }
}

struct TransactionExecutionResult {
    execute_output: LoadAndExecuteTransactionsOutput,
    transaction: VersionedTransaction,
}

enum ExecutionResult {
    TransactionSuccess(TransactionExecutionResult),
    Failure,
}