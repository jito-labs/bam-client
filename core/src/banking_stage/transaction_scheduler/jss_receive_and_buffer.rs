/// An implementation of the `ReceiveAndBuffer` trait that receives messages from JSS
/// and buffers from into the the `TransactionStateContainer`. Key thing to note:
/// this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
/// to JSS with a `Retryable` result.
use std::{
    cmp::min,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use crossbeam_channel::Sender;
use itertools::Itertools;
use jito_protos::proto::{
    jss_api::{start_scheduler_message::Msg, StartSchedulerMessage},
    jss_types::{
        bundle_result, not_committed::Reason, Bundle, DeserializationErrorReason, Packet,
        PohTimeout,
    },
};
use solana_accounts_db::account_locks::validate_account_locks;
use solana_runtime::bank_forks::BankForks;
use solana_runtime_transaction::{
    runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
};
use solana_sdk::{
    clock::MAX_PROCESSING_AGE,
    packet::{PacketFlags, PACKET_DATA_SIZE},
    transaction::{SanitizedTransaction, TransactionError},
};
use solana_svm::transaction_error_metrics::TransactionErrorMetrics;

use crate::banking_stage::{
    decision_maker::BufferedPacketsDecision,
    immutable_deserialized_packet::{DeserializedPacketError, ImmutableDeserializedPacket},
    transaction_scheduler::{
        jss_utils::{convert_deserialize_error_to_proto, convert_txn_error_to_proto},
        receive_and_buffer::{calculate_max_age, calculate_priority_and_cost},
        transaction_state::SanitizedTransactionTTL,
    },
};

use super::{
    receive_and_buffer::ReceiveAndBuffer, transaction_state_container::TransactionStateContainer,
};

pub struct JssReceiveAndBuffer {
    jss_enabled: Arc<AtomicBool>,
    bundle_receiver: crossbeam_channel::Receiver<Bundle>,
    response_sender: Sender<StartSchedulerMessage>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl JssReceiveAndBuffer {
    pub fn new(
        jss_enabled: Arc<AtomicBool>,
        bundle_receiver: crossbeam_channel::Receiver<Bundle>,
        response_sender: Sender<StartSchedulerMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        Self {
            jss_enabled,
            bundle_receiver,
            response_sender,
            bank_forks,
        }
    }

    fn deserialize_jss_packets<'a>(
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Vec<Result<ImmutableDeserializedPacket, DeserializedPacketError>> {
        packets
            .map(|packet| {
                let mut solana_packet = solana_sdk::packet::Packet::default();
                solana_packet.meta_mut().size = packet.data.len();
                solana_packet.meta_mut().set_discard(false);
                let len_to_copy = min(packet.data.len(), PACKET_DATA_SIZE);
                solana_packet.buffer_mut()[0..len_to_copy]
                    .copy_from_slice(&packet.data[0..len_to_copy]);
                if let Some(meta) = &packet.meta {
                    solana_packet.meta_mut().size = meta.size as usize;
                    if let Some(addr) = &meta.addr.parse().ok() {
                        solana_packet.meta_mut().addr = *addr;
                    }
                    solana_packet.meta_mut().port = meta.port as u16;
                    if let Some(flags) = &meta.flags {
                        if flags.simple_vote_tx {
                            solana_packet
                                .meta_mut()
                                .flags
                                .insert(PacketFlags::SIMPLE_VOTE_TX);
                        }
                        if flags.forwarded {
                            solana_packet
                                .meta_mut()
                                .flags
                                .insert(PacketFlags::FORWARDED);
                        }
                        if flags.repair {
                            solana_packet.meta_mut().flags.insert(PacketFlags::REPAIR);
                        }
                    }
                }
                ImmutableDeserializedPacket::new(solana_packet)
            })
            .collect_vec()
    }

    fn send_txn_error_bundle_result(&self, seq_id: u32, index: usize, error: TransactionError) {
        let reason = convert_txn_error_to_proto(error);
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(Reason::TransactionError(
                                jito_protos::proto::jss_types::TransactionError {
                                    index: index as u32,
                                    reason: reason as i32,
                                },
                            )),
                        },
                    )),
                },
            )),
        });
    }

    fn send_deserialization_error_bundle_result(
        &self,
        seq_id: u32,
        index: usize,
        reason: DeserializationErrorReason,
    ) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(Reason::DeserializationError(
                                jito_protos::proto::jss_types::DeserializationError {
                                    index: index as u32,
                                    reason: reason as i32,
                                },
                            )),
                        },
                    )),
                },
            )),
        });
    }

    fn send_retryable_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(Reason::PohTimeout(PohTimeout {})),
                        },
                    )),
                },
            )),
        });
    }
}

impl ReceiveAndBuffer for JssReceiveAndBuffer {
    type Transaction = RuntimeTransaction<SanitizedTransaction>;
    type Container = TransactionStateContainer<Self::Transaction>;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        _: &mut super::scheduler_metrics::SchedulerTimingMetrics,
        _: &mut super::scheduler_metrics::SchedulerCountMetrics,
        decision: &crate::banking_stage::decision_maker::BufferedPacketsDecision,
    ) -> Result<usize, ()> {
        if !self.jss_enabled.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(5));
            return Ok(0);
        }

        let mut result = 0;
        const MAX_BUNDLES_PER_RECV: usize = 10;
        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => {
                while result < MAX_BUNDLES_PER_RECV {
                    let Ok(bundle) = self.bundle_receiver.try_recv() else {
                        break;
                    };
                    if bundle.packets.is_empty() {
                        self.send_deserialization_error_bundle_result(
                            bundle.seq_id,
                            0,
                            DeserializationErrorReason::Empty,
                        );
                        continue;
                    }

                    let mut parsed_packets = Self::deserialize_jss_packets(bundle.packets.iter());
                    if let Some(Err(err)) = parsed_packets.iter().find(|r| r.is_err()) {
                        let reason = convert_deserialize_error_to_proto(err);
                        self.send_deserialization_error_bundle_result(
                            bundle.seq_id,
                            parsed_packets.iter().position(|r| r.is_err()).unwrap_or(0),
                            reason,
                        );
                        continue;
                    }

                    let Ok(revert_on_error) = bundle
                        .packets
                        .iter()
                        .map(|p| {
                            p.meta
                                .as_ref()
                                .and_then(|meta| meta.flags.as_ref()).is_some_and(|flags| flags.revert_on_error)
                        })
                        .all_equal_value()
                    else {
                        self.send_deserialization_error_bundle_result(
                            bundle.seq_id,
                            0,
                            DeserializationErrorReason::InconsistentBundle,
                        );
                        continue;
                    };

                    let (root_bank, working_bank) = {
                        let bank_forks = self.bank_forks.read().unwrap();
                        let root_bank = bank_forks.root_bank();
                        let working_bank = bank_forks.working_bank();
                        (root_bank, working_bank)
                    };
                    let alt_resolved_slot = root_bank.slot();
                    let sanitized_epoch = root_bank.epoch();
                    let transaction_account_lock_limit =
                        working_bank.get_transaction_account_lock_limit();
                    let vote_only = working_bank.vote_only_bank();

                    let mut packets = vec![];
                    let mut cost: u64 = 0;
                    let mut transaction_ttls = vec![];
                    for (index, parsed_packet) in
                        parsed_packets.drain(..).filter_map(Result::ok).enumerate()
                    {
                        let Some((tx, deactivation_slot)) = parsed_packet
                            .build_sanitized_transaction(
                                vote_only,
                                root_bank.as_ref(),
                                root_bank.get_reserved_account_keys(),
                            )
                        else {
                            self.send_deserialization_error_bundle_result(
                                bundle.seq_id,
                                0,
                                DeserializationErrorReason::SanitizeError,
                            );
                            break;
                        };

                        if let Err(err) = validate_account_locks(
                            tx.message().account_keys(),
                            transaction_account_lock_limit,
                        ) {
                            self.send_txn_error_bundle_result(bundle.seq_id, index, err);
                            break;
                        }

                        let fee_budget_limits = match tx
                            .compute_budget_instruction_details()
                            .sanitize_and_convert_to_compute_budget_limits(
                                &working_bank.feature_set,
                            ) {
                            Ok(fee_budget_limits) => fee_budget_limits,
                            Err(err) => {
                                self.send_txn_error_bundle_result(bundle.seq_id, index, err);
                                break;
                            }
                        };

                        let lock_results: [_; 1] = core::array::from_fn(|_| Ok(()));
                        let check_results = working_bank.check_transactions(
                            std::slice::from_ref(&tx),
                            &lock_results,
                            MAX_PROCESSING_AGE,
                            &mut TransactionErrorMetrics::default(),
                        );
                        if let Some(Err(err)) = check_results.first() {
                            self.send_txn_error_bundle_result(bundle.seq_id, index, err.clone());
                            break;
                        }

                        let max_age = calculate_max_age(
                            sanitized_epoch,
                            deactivation_slot,
                            alt_resolved_slot,
                        );

                        let (_, txn_cost) = calculate_priority_and_cost(
                            &tx,
                            &fee_budget_limits.into(),
                            &working_bank,
                        );
                        cost = cost.saturating_add(txn_cost);
                        transaction_ttls.push(SanitizedTransactionTTL {
                            transaction: tx,
                            max_age,
                        });
                        packets.push(Arc::new(parsed_packet));
                    }
                    if packets.len() != bundle.packets.len() {
                        // Specific error would have been sent above for first 'invalid' packet/transaction
                        continue;
                    }

                    let priority = seq_id_to_priority(bundle.seq_id);
                    if container
                        .insert_new_batch(
                            transaction_ttls,
                            packets,
                            priority,
                            cost,
                            revert_on_error,
                        )
                        .is_none()
                    {
                        self.send_retryable_bundle_result(bundle.seq_id);
                        continue;
                    };

                    result += 1;
                }
            }
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Forward => {
                // Send back any bundles that were received while in Forward/Hold state
                let deadline = Instant::now() + Duration::from_millis(5);
                while let Ok(bundle) = self.bundle_receiver.recv_deadline(deadline) {
                    self.send_retryable_bundle_result(bundle.seq_id);
                }
            }
        }

        Ok(result)
    }
}

pub fn seq_id_to_priority(seq_id: u32) -> u64 {
    u64::MAX.saturating_sub(seq_id as u64)
}

pub fn priority_to_seq_id(priority: u64) -> u32 {
    u32::try_from(u64::MAX.saturating_sub(priority)).unwrap_or(u32::MAX)
}
