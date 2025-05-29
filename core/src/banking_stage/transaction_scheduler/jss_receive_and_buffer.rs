/// An implementation of the `ReceiveAndBuffer` trait that receives messages from JSS
/// and buffers from into the the `TransactionStateContainer`. Key thing to note:
/// this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
/// to JSS with a `Retryable` result.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use crossbeam_channel::{unbounded, Sender};
use itertools::Itertools;
use jito_protos::proto::{
    jss_api::{start_scheduler_message::Msg, StartSchedulerMessage},
    jss_types::{bundle_result, Bundle, Packet},
};
use solana_runtime::bank_forks::BankForks;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::{
    packet::{PacketFlags, PACKET_DATA_SIZE},
    transaction::SanitizedTransaction,
};

use crate::banking_stage::{
    decision_maker::BufferedPacketsDecision,
    immutable_deserialized_packet::ImmutableDeserializedPacket,
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

    fn parse_transactions<'a>(
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Vec<ImmutableDeserializedPacket> {
        packets
            .filter_map(|packet| {
                if packet.data.len() > PACKET_DATA_SIZE {
                    return None;
                }
                let mut solana_packet = solana_sdk::packet::Packet::default();
                solana_packet.meta_mut().size = packet.data.len() as usize;
                solana_packet.meta_mut().set_discard(false);
                solana_packet.buffer_mut()[0..packet.data.len()].copy_from_slice(&packet.data);
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
                Some(ImmutableDeserializedPacket::new(solana_packet).ok()?)
            })
            .collect_vec()
    }

    fn send_invalid_bundle_result(&self, seq_id: u32) {
        let _ = self.response_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::BundleResult(
                jito_protos::proto::jss_types::BundleResult {
                    seq_id: seq_id,
                    result: Some(bundle_result::Result::Invalid(
                        jito_protos::proto::jss_types::Invalid {},
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
                    result: Some(bundle_result::Result::Retryable(
                        jito_protos::proto::jss_types::Retryable {},
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
            return Ok(0);
        }

        let mut result = 0;
        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => {
                while let Ok(bundle) = self.bundle_receiver.try_recv() {
                    if bundle.packets.len() == 0 {
                        continue;
                    }

                    let packets = Self::parse_transactions(bundle.packets.iter());
                    if packets.len() != bundle.packets.len() {
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

                    let mut tmp_container = Self::Container::with_capacity(bundle.packets.len());
                    self.internal_receive_and_buffer.buffer_packets(
                        &mut tmp_container,
                        timing_metrics,
                        count_metrics,
                        packets,
                    );

                    let mut packets = vec![];
                    let mut cost: u64 = 0;
                    let mut transaction_ttls = vec![];
                    for _ in 0..bundle.packets.len() {
                        let Some(id) = tmp_container.pop() else {
                            self.send_invalid_bundle_result(bundle.seq_id);
                            break;
                        };
                        let Some(entry) = tmp_container.get_mut_transaction_state(id.id) else {
                            self.send_invalid_bundle_result(bundle.seq_id);
                            break;
                        };
                        let Some(packet) = entry.packet().cloned() else {
                            self.send_invalid_bundle_result(bundle.seq_id);
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

                    container.insert_new_batch(
                        transaction_ttls,
                        packets,
                        u64::MAX.saturating_sub(bundle.seq_id as u64),
                        cost,
                        revert_on_error,
                    );

                    result += 1;
                }
            }
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Forward => {
                // Send back any bundles that were received while in Forward/Hold state
                while let Ok(bundle) = self.bundle_receiver.try_recv() {
                    self.send_retryable_bundle_result(bundle.seq_id);
                }

                // Send back all packets in container
                while let Some(id) = container.pop() {
                    let seq_id = id.priority;
                    self.send_retryable_bundle_result(seq_id as u32);
                    container.remove_by_id(id.id);
                }
            }
        }

        Ok(result)
    }
}
