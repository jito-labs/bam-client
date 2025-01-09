use std::{borrow::Cow, sync::{Arc, RwLock}, time::Duration};

use itertools::Itertools;
use solana_bundle::bundle_execution::load_and_execute_bundle;
use solana_measure::measure_us;
/// Receives pre-scheduled microblocks and attempts to 'actuate' them by applying the transactions to the state.

use solana_svm::transaction_results::TransactionExecutionResult;
use jito_protos::proto::jds_types::{Bundle, MicroBlock, Packet};
use solana_poh::poh_recorder::{PohRecorder, RecordTransactionsSummary};
use solana_runtime::{bank::{Bank, ExecutedTransactionCounts, LoadAndExecuteTransactionsOutput}, prioritization_fee_cache::PrioritizationFeeCache, transaction_batch::TransactionBatch, vote_sender_types::ReplayVoteSender};
use solana_sdk::{bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle}, clock::MAX_PROCESSING_AGE, packet::{Meta, PacketFlags}, transaction::{MessageHash, SanitizedTransaction, TransactionError, VersionedTransaction}};
use solana_svm::{account_loader::LoadedTransaction, transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig}};
use solana_transaction_status::PreBalanceInfo;

use crate::{banking_stage::{committer::Committer, immutable_deserialized_packet::ImmutableDeserializedPacket, leader_slot_timing_metrics::LeaderExecuteAndCommitTimings}, bundle_stage};

pub struct JdsActuator {
    poh_recorder: Arc<RwLock<PohRecorder>>,
    committer: bundle_stage::committer::Committer,
}

impl JdsActuator {
    pub fn new(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        replay_vote_sender: ReplayVoteSender,
    ) -> Self {
        Self {
            poh_recorder,
            committer: bundle_stage::committer::Committer::new(
                None, // TODO
                replay_vote_sender,
                Arc::new(PrioritizationFeeCache::default()), // TODO
            )
        }
    }

    fn parse_transactions<'a>(
        bank: &Bank,
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Vec<SanitizedTransaction> {
        let txns = packets.map(|packet| {
            let mut solana_packet = solana_sdk::packet::Packet::default();
            solana_packet.buffer_mut().copy_from_slice(&packet.data);
            if let Some(meta) = &packet.meta {
                solana_packet.meta_mut().size = meta.size as usize;
                solana_packet.meta_mut().addr = meta.addr.parse().ok()?;
                solana_packet.meta_mut().port = meta.port as u16;
                if let Some(flags) = &meta.flags {
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
            Some(sanitized_transaction)
        }).collect_vec();
        if txns.iter().any(Option::is_none) {
            return vec![];
        }
        txns.into_iter().map(|x| x.unwrap()).collect_vec()
    }

    //fn parse_validate_execute_transactions<'a>(
    //    bank: &Bank,
    //    txns: Vec<SanitizedTransaction>,
    //) -> Option<JdsTransactionExecutionResult>
    //{
    //    let batch = bank.prepare_owned_sanitized_batch(txns);
    //    let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
    //    let execute_output = bank
    //        .load_and_execute_transactions(
    //            &batch,
    //            MAX_PROCESSING_AGE,
    //            &mut execute_and_commit_timings.execute_timings,
    //            TransactionProcessingConfig {
    //                account_overrides: None,
    //                check_program_modification_slot: bank.check_program_modification_slot(),
    //                compute_budget: bank.compute_budget(),
    //                log_messages_bytes_limit: None,
    //                limit_to_load_programs: true,
    //                recording_config: ExecutionRecordingConfig::new_single_setting(
    //                    false,
    //                ),
    //                transaction_account_lock_limit: Some(bank.get_transaction_account_lock_limit()),
    //            }
    //        );
    //    Some(JdsTransactionExecutionResult{
    //        execute_output,
    //        batch,
    //    })
    //}
//
    //fn record_and_commit_result<'a, 'b>(
    //    &mut self,
    //    result: JdsTransactionExecutionResult<'a, 'b>,
    //    bank: &Arc<Bank>,
    //    transaction_recorder: &solana_poh::poh_recorder::TransactionRecorder,
    //) -> bool {
    //    let JdsTransactionExecutionResult{ execute_output, batch} = result;
    //    let LoadAndExecuteTransactionsOutput {
    //        mut loaded_transactions,
    //        execution_results,
    //        retryable_transaction_indexes: _,
    //        executed_transactions_count,
    //        executed_non_vote_transactions_count,
    //        executed_with_successful_result_count,
    //        signature_count,
    //        error_counters: _,
    //        ..
    //    } = execute_output;
    //    let _freeze_lock = bank.freeze_lock();
    //    let (last_blockhash, lamports_per_signature) = bank.last_blockhash_and_lamports_per_signature();
//
    //    if executed_with_successful_result_count != batch.sanitized_transactions().len() {
    //        return false;
    //    }
//
    //    self.record_and_commit(
    //        &batch,
    //        &mut loaded_transactions,
    //        execution_results,
    //        last_blockhash,
    //        lamports_per_signature,
    //        &transaction_recorder,
    //        &bank,
    //        executed_transactions_count,
    //        executed_non_vote_transactions_count,
    //        executed_with_successful_result_count,
    //        signature_count,
    //    );
//
    //    true
    //}
//
    //fn record_and_commit(
    //    &mut self,
    //    batch: &TransactionBatch,
    //    loaded_transactions: &mut Vec<Result<LoadedTransaction, TransactionError>>,
    //    execution_results: Vec<TransactionExecutionResult>,
    //    last_blockhash: solana_sdk::hash::Hash,
    //    lamports_per_signature: u64,
    //    transaction_recorder: &solana_poh::poh_recorder::TransactionRecorder,
    //    bank: &Arc<solana_runtime::bank::Bank>,
    //    executed_transactions_count: usize,
    //    executed_non_vote_transactions_count: usize,
    //    executed_with_successful_result_count: usize,
    //    signature_count: u64,
    //) -> bool {
    //    let executed_transactions = execution_results
    //        .iter()
    //        .zip(batch.sanitized_transactions())
    //        .filter_map(|(execution_result, tx)| {
    //            if execution_result.was_executed() {
    //                Some(tx.to_versioned_transaction())
    //            } else {
    //                None
    //            }
    //        })
    //        .collect_vec();
//
    //    let record_transactions_summary =
    //        transaction_recorder.record_transactions(bank.slot(), vec![executed_transactions]);
//
    //    let RecordTransactionsSummary {
    //            result,
    //            record_transactions_timings: _,
    //            starting_transaction_index,
    //        } = record_transactions_summary;
    //    if result.is_err() {
    //        return false;
    //    }
//
    //    let mut pre_balance_info = PreBalanceInfo::default();
    //    let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
    //    self.transaction_committer.commit_transactions(
    //        &batch,
    //        loaded_transactions,
    //        execution_results,
    //        last_blockhash,
    //        lamports_per_signature,
    //        starting_transaction_index,
    //        bank,
    //        &mut pre_balance_info,
    //        &mut execute_and_commit_timings,
    //        signature_count,
    //        executed_transactions_count,
    //        executed_non_vote_transactions_count,
    //        executed_with_successful_result_count);
    //    true
    //}

    pub fn execute_and_commit_micro_block(&mut self, micro_block: MicroBlock) {
        let bank = self.poh_recorder.read().unwrap().bank().unwrap();
        let transaction_recorder = self.poh_recorder.read().unwrap().new_recorder();

        // Try to execute everything in the block
        for bundle in micro_block.bundles {
            let transactions = Self::parse_transactions(&bank, bundle.packets.iter());
            if transactions.is_empty() {
                continue;
            }

            let len = transactions.len();
            let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);
            let sanitized_bundle = SanitizedBundle{
                transactions,
                bundle_id,
            };

            let default_accounts = vec![None; len];
            let mut bundle_execution_results = load_and_execute_bundle(
                &bank,
                &sanitized_bundle,
                20,
                &Duration::from_secs(1),
                false,
                &None,
                false,
                None,
                &default_accounts,
                &default_accounts);

            if let Err(_) = bundle_execution_results.result() {
                continue;
            }

            let (executed_batches, execution_results_to_transactions_us) =
                measure_us!(bundle_execution_results.executed_transaction_batches());

            let freeze_lock = bank.freeze_lock();
            let (last_blockhash, lamports_per_signature) = bank.last_blockhash_and_lamports_per_signature();
            let RecordTransactionsSummary {
                    result: record_transactions_result,
                    record_transactions_timings,
                    starting_transaction_index,
                } =  transaction_recorder.record_transactions(bank.slot(), executed_batches);

            let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
            self.committer.commit_bundle(
                &mut bundle_execution_results,
                last_blockhash,
                lamports_per_signature,
                starting_transaction_index,
                &bank,
                &mut execute_and_commit_timings);
        }
    }
}

struct JdsTransactionExecutionResult<'a, 'b> {
    execute_output: LoadAndExecuteTransactionsOutput,
    batch: TransactionBatch<'a, 'b>,
}