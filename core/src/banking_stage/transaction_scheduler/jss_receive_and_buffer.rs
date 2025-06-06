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

use crossbeam_channel::{unbounded, Sender};
use itertools::Itertools;
use jito_protos::proto::{
    jss_api::{start_scheduler_message::Msg, StartSchedulerMessage},
    jss_types::{bundle_result, not_committed::Reason, Bundle, GenericInvalid, Packet, PohTimeout},
};
use solana_runtime::bank_forks::BankForks;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::{
    packet::{PacketFlags, PACKET_DATA_SIZE},
    transaction::SanitizedTransaction,
};

use crate::banking_stage::{
    decision_maker::BufferedPacketsDecision,
    immutable_deserialized_packet::{DeserializedPacketError, ImmutableDeserializedPacket},
    packet_deserializer::PacketDeserializer,
};

use super::{
    receive_and_buffer::{ReceiveAndBuffer, SanitizedTransactionReceiveAndBuffer},
    transaction_state_container::TransactionStateContainer,
};

use crate::banking_stage::transaction_scheduler::transaction_state_container::StateContainer;

pub struct JssReceiveAndBuffer {
    jss_enabled: Arc<AtomicBool>,
    bundle_receiver: crossbeam_channel::Receiver<Bundle>,
    response_sender: Sender<StartSchedulerMessage>,
    internal_receive_and_buffer: SanitizedTransactionReceiveAndBuffer,
}

impl JssReceiveAndBuffer {
    pub fn new(
        jss_enabled: Arc<AtomicBool>,
        bundle_receiver: crossbeam_channel::Receiver<Bundle>,
        response_sender: Sender<StartSchedulerMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        let internal_receive_and_buffer = SanitizedTransactionReceiveAndBuffer::new(
            PacketDeserializer::new(unbounded().1),
            bank_forks.clone(),
            false,
            Arc::new(AtomicBool::new(false)),
        );
        Self {
            jss_enabled,
            bundle_receiver,
            response_sender,
            internal_receive_and_buffer,
        }
    }

    fn deserialize_jss_packets<'a>(
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Vec<Result<ImmutableDeserializedPacket, DeserializedPacketError>> {
        packets
            .map(|packet| {
                let mut solana_packet = solana_sdk::packet::Packet::default();
                solana_packet.meta_mut().size = packet.data.len() as usize;
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

    fn send_invalid_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id: seq_id,
                    result: Some(bundle_result::Result::NotCommitted(
                        jito_protos::proto::jss_types::NotCommitted {
                            reason: Some(Reason::GenericInvalid(GenericInvalid {})),
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
                    seq_id: seq_id,
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
        timing_metrics: &mut super::scheduler_metrics::SchedulerTimingMetrics,
        count_metrics: &mut super::scheduler_metrics::SchedulerCountMetrics,
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
                    if bundle.packets.len() == 0 {
                        self.send_invalid_bundle_result(bundle.seq_id);
                        continue;
                    }

                    let mut parsed_packets = Self::deserialize_jss_packets(bundle.packets.iter());
                    if let Some(Err(_)) = parsed_packets.iter().find(|r| r.is_err()) {
                        // TODO_DG: report specific error
                        self.send_invalid_bundle_result(bundle.seq_id);
                        continue;
                    }

                    let Ok(revert_on_error) = bundle
                        .packets
                        .iter()
                        .map(|p| {
                            p.meta
                                .as_ref()
                                .and_then(|meta| meta.flags.as_ref())
                                .map_or(false, |flags| flags.revert_on_error)
                        })
                        .all_equal_value()
                    else {
                        self.send_invalid_bundle_result(bundle.seq_id);
                        continue;
                    };

                    // Hacky way to leverage existing receive_and_buffer logic
                    let mut packets = vec![];
                    let mut cost: u64 = 0;
                    let mut transaction_ttls = vec![];
                    for parsed_packet in parsed_packets.drain(..).filter_map(Result::ok) {
                        let mut tmp_container = Self::Container::with_capacity(1);
                        self.internal_receive_and_buffer.buffer_packets(
                            &mut tmp_container,
                            timing_metrics,
                            count_metrics,
                            vec![parsed_packet],
                        );

                        let Some(id) = tmp_container.pop() else {
                            break;
                        };
                        let Some(entry) = tmp_container.get_mut_transaction_state(id.id) else {
                            break;
                        };
                        let Some(packet) = entry.packet().cloned() else {
                            break;
                        };
                        cost = cost.saturating_add(entry.cost());
                        transaction_ttls.push(entry.transition_to_pending());
                        packets.push(packet);
                    }
                    if packets.len() != bundle.packets.len() {
                        self.send_invalid_bundle_result(bundle.seq_id);
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
