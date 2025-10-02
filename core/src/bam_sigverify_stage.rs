use std::{
    ops::AddAssign,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, spawn, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender, TryRecvError};
use solana_measure::measure_us;
use solana_perf::{packet::PacketBatch, sigverify::ed25519_verify_cpu};
use std::num::Saturating;

use crate::bam_packet_batch::{BamPacketBatch, BamPacketBatchMeta};

struct BamSigverifyStageMetrics {
    last_update: Instant,

    total_batches: Saturating<u64>,

    total_packets: Saturating<u64>,

    total_discard_batches: Saturating<u64>,
    total_discard_packets: Saturating<u64>,

    total_non_discard_batches: Saturating<u64>,
    total_non_discard_packets: Saturating<u64>,

    total_verify_elapsed_us: Saturating<u64>,

    receiver_watermark: u64,
}

impl Default for BamSigverifyStageMetrics {
    fn default() -> Self {
        Self {
            last_update: Instant::now(),

            total_batches: Saturating(0),

            total_packets: Saturating(0),

            total_discard_batches: Saturating(0),
            total_discard_packets: Saturating(0),

            total_non_discard_batches: Saturating(0),
            total_non_discard_packets: Saturating(0),

            total_verify_elapsed_us: Saturating(0),

            receiver_watermark: 0,
        }
    }
}

impl BamSigverifyStageMetrics {
    fn maybe_report(&mut self, report_interval: Duration) {
        if self.total_packets.0 > 0 && self.last_update.elapsed() > report_interval {
            datapoint_info!(
                "bam_sigverify_stage",
                ("total_batches", self.total_batches.0 as i64, i64),
                ("total_packets", self.total_packets.0 as i64, i64),
                (
                    "total_discard_batches",
                    self.total_discard_batches.0 as i64,
                    i64
                ),
                (
                    "total_discard_packets",
                    self.total_discard_packets.0 as i64,
                    i64
                ),
                (
                    "total_non_discard_batches",
                    self.total_non_discard_batches.0 as i64,
                    i64
                ),
                (
                    "total_non_discard_packets",
                    self.total_non_discard_packets.0 as i64,
                    i64
                ),
                (
                    "total_verify_elapsed_us",
                    self.total_verify_elapsed_us.0 as i64,
                    i64
                ),
                ("receiver_watermark", self.receiver_watermark, i64),
            );
            self.last_update = Instant::now();
        }
    }
}

pub struct BamSigverifyStage {
    thread: JoinHandle<()>,
}

/// The BamSigverifyStage is responsible for verifying the signatures of the packets in the
/// batch and sending the verified packets to the next stage.
///
/// One can make the following assumptions about BAM when looking at this code:
/// - BAM performs packet deduplication and doesn't expose erroneous deduplication from downstream consumers.
///   Therefore, deduplication is not needed in this stage.
/// - BAM performs sigverify on the packets. The number of packets marked as discard will be zero majority of the time,
///   unless a bug exists in BAM.
/// - BAM cares more about latency than throughput for sigverify since it's doing filtering on the BAM node side. Therefore,
///   one should choose small batch sizes to minimize latency.
impl BamSigverifyStage {
    const MAX_PACKET_BATCHES_PER_SIGVERIFY: usize = 128;

    pub fn new(
        receiver: Receiver<(PacketBatch, BamPacketBatchMeta)>,
        sender: Sender<BamPacketBatch>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let thread = spawn(move || {
            Self::run(receiver, sender, exit);
        });

        Self { thread }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread.join()
    }

    fn run(
        receiver: Receiver<(PacketBatch, BamPacketBatchMeta)>,
        sender: Sender<BamPacketBatch>,
        exit: Arc<AtomicBool>,
    ) {
        let mut metrics = BamSigverifyStageMetrics::default();

        let mut batches: Vec<PacketBatch> =
            Vec::with_capacity(Self::MAX_PACKET_BATCHES_PER_SIGVERIFY);
        let mut atomic_tx_batch_metadata: Vec<BamPacketBatchMeta> =
            Vec::with_capacity(Self::MAX_PACKET_BATCHES_PER_SIGVERIFY);

        while !exit.load(Ordering::Relaxed) {
            metrics.receiver_watermark = metrics.receiver_watermark.max(receiver.len() as u64);

            metrics.maybe_report(Duration::from_millis(20));

            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok((packet_batch, atomic_tx_batch_meta)) => {
                    batches.push(packet_batch);
                    atomic_tx_batch_metadata.push(atomic_tx_batch_meta);
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    warn!("BamSigverifyStage channel disconnected, exiting...");
                    return;
                }
            }

            while batches.len() < Self::MAX_PACKET_BATCHES_PER_SIGVERIFY {
                let (packet_batch, atomic_tx_batch_meta) = match receiver.try_recv() {
                    Ok((packet_batch, atomic_tx_batch_meta)) => {
                        (packet_batch, atomic_tx_batch_meta)
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                };
                batches.push(packet_batch);
                atomic_tx_batch_metadata.push(atomic_tx_batch_meta);
            }

            let packet_count = batches.iter().map(|batch| batch.len()).sum::<usize>();

            metrics.total_packets.add_assign(packet_count as u64);
            metrics.total_batches.add_assign(batches.len() as u64);

            // packets will be marked as discard if they fail verification
            let (_, verify_elapsed_us) =
                measure_us!(ed25519_verify_cpu(&mut batches, false, packet_count));
            metrics
                .total_verify_elapsed_us
                .add_assign(verify_elapsed_us);

            for (packet_batch, mut bam_packet_batch_meta) in
                batches.drain(..).zip(atomic_tx_batch_metadata.drain(..))
            {
                // The entire batch is marked as discard if any packet is marked as discard
                if packet_batch.iter().any(|packet| packet.meta().discard()) {
                    bam_packet_batch_meta.discard = true;
                    metrics.total_discard_batches.add_assign(1);
                    metrics
                        .total_discard_packets
                        .add_assign(packet_batch.len() as u64);
                } else {
                    metrics.total_non_discard_batches.add_assign(1);
                    metrics
                        .total_non_discard_packets
                        .add_assign(packet_batch.len() as u64);
                }

                let bam_packet_batch = BamPacketBatch::new(packet_batch, bam_packet_batch_meta);
                if let Err(SendError(_)) = sender.send(bam_packet_batch) {
                    error!("BamSigverifyStage channel disconnected, exiting...");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::bounded;

    use super::*;

    #[test]
    fn test_bam_sigverify_stage_exits() {
        let (receiver, sender) = bounded(10);
        let exit = Arc::<AtomicBool>::default();
        let stage = BamSigverifyStage::new(receiver, sender, exit.clone());
        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }

    #[test]
    fn test_bam_sigverify_stage_sends_batches() {
        panic!("Not implemented");
    }

    #[test]
    fn test_multi_tx_batch_with_bad_tx_discards_entire_batch() {
        panic!("Not implemented");
    }

    #[test]
    fn test_single_packet_bad_signature_discards_entire_batch() {
        panic!("Not implemented");
    }
}
