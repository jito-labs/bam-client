use {
    crate::verified_bam_packet_batch::{BamPacketBatchMeta, VerifiedBamPacketBatch},
    crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender, TryRecvError},
    solana_measure::measure_us,
    solana_perf::{packet::PacketBatch, sigverify::ed25519_verify_cpu},
    std::{
        num::Saturating,
        ops::AddAssign,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, spawn, JoinHandle},
        time::{Duration, Instant},
    },
};

struct BamSigverifyStageMetrics {
    last_update: Instant,

    num_batches: Saturating<u64>,

    num_packets: Saturating<u64>,

    num_discard_batches: Saturating<u64>,
    num_discard_packets: Saturating<u64>,

    num_non_discard_batches: Saturating<u64>,
    num_non_discard_packets: Saturating<u64>,

    verify_elapsed_us: Saturating<u64>,

    receiver_watermark: u64,
}

impl Default for BamSigverifyStageMetrics {
    fn default() -> Self {
        Self {
            last_update: Instant::now(),

            num_batches: Saturating(0),

            num_packets: Saturating(0),

            num_discard_batches: Saturating(0),
            num_discard_packets: Saturating(0),

            num_non_discard_batches: Saturating(0),
            num_non_discard_packets: Saturating(0),

            verify_elapsed_us: Saturating(0),

            receiver_watermark: 0,
        }
    }
}

impl BamSigverifyStageMetrics {
    fn maybe_report(&mut self, report_interval: Duration) {
        if self.num_packets.0 > 0 && self.last_update.elapsed() >= report_interval {
            datapoint_info!(
                "bam_sigverify_stage",
                ("num_batches", self.num_batches.0 as i64, i64),
                ("num_packets", self.num_packets.0 as i64, i64),
                (
                    "num_discard_batches",
                    self.num_discard_batches.0 as i64,
                    i64
                ),
                (
                    "num_discard_packets",
                    self.num_discard_packets.0 as i64,
                    i64
                ),
                (
                    "num_non_discard_batches",
                    self.num_non_discard_batches.0 as i64,
                    i64
                ),
                (
                    "num_non_discard_packets",
                    self.num_non_discard_packets.0 as i64,
                    i64
                ),
                ("verify_elapsed_us", self.verify_elapsed_us.0 as i64, i64),
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
        sender: Sender<VerifiedBamPacketBatch>,
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
        sender: Sender<VerifiedBamPacketBatch>,
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

            let mut packet_count = 0;
            for batch in &batches {
                packet_count += batch.len();
            }

            metrics.num_packets.add_assign(packet_count as u64);
            metrics.num_batches.add_assign(batches.len() as u64);

            // packets will be marked as discard if they fail verification
            let (_, verify_elapsed_us) =
                measure_us!(ed25519_verify_cpu(&mut batches, false, packet_count));
            metrics.verify_elapsed_us.add_assign(verify_elapsed_us);

            for (packet_batch, mut bam_packet_batch_meta) in
                batches.drain(..).zip(atomic_tx_batch_metadata.drain(..))
            {
                // The entire batch is marked as discard if any packet is marked as discard
                if packet_batch.iter().any(|packet| packet.meta().discard()) {
                    bam_packet_batch_meta.discard = true;
                    metrics.num_discard_batches.add_assign(1);
                    metrics
                        .num_discard_packets
                        .add_assign(packet_batch.len() as u64);
                } else {
                    metrics.num_non_discard_batches.add_assign(1);
                    metrics
                        .num_non_discard_packets
                        .add_assign(packet_batch.len() as u64);
                }

                let bam_packet_batch =
                    VerifiedBamPacketBatch::new(packet_batch, bam_packet_batch_meta);
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
    use {
        super::*,
        crossbeam_channel::{bounded, unbounded},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_perf::packet::BytesPacket,
        solana_signer::Signer,
        solana_transaction::Transaction,
    };

    #[test]
    fn test_bam_sigverify_stage_exits() {
        let (_unverified_sender, unverified_receiver) = bounded(10);
        let (verified_sender, _verified_receiver) = unbounded();
        let exit = Arc::<AtomicBool>::default();
        let stage = BamSigverifyStage::new(unverified_receiver, verified_sender, exit.clone());
        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }

    fn packets_to_packet_batch(txs: Vec<Transaction>) -> (PacketBatch, BamPacketBatchMeta) {
        let packets: Vec<BytesPacket> = txs
            .iter()
            .map(|tx| BytesPacket::from_data(None, tx).unwrap())
            .collect();
        let packet_batch = PacketBatch::from(packets);
        let bam_packet_batch_meta = BamPacketBatchMeta {
            seq_id: 0,
            max_schedule_slot: 0,
            revert_on_error: false,
            discard: false,
        };
        (packet_batch, bam_packet_batch_meta)
    }

    pub fn test_tx() -> Transaction {
        let keypair1 = Keypair::new();
        let pubkey1 = keypair1.pubkey();
        let zero = Hash::default();
        solana_system_transaction::transfer(&keypair1, &pubkey1, 42, zero)
    }

    pub fn test_invalid_tx() -> Transaction {
        let mut tx = test_tx();
        tx.signatures = vec![Transaction::get_invalid_signature()];
        tx
    }

    #[test]
    fn test_bam_sigverify_stage_sends_batches() {
        let (unverified_sender, unverified_receiver) = bounded(10);
        let (verified_sender, verified_receiver) = unbounded();
        let exit = Arc::<AtomicBool>::default();
        let stage = BamSigverifyStage::new(unverified_receiver, verified_sender, exit.clone());

        let (packet_batch, bam_packet_batch_meta) = packets_to_packet_batch(vec![test_tx()]);
        unverified_sender
            .send((packet_batch, bam_packet_batch_meta))
            .unwrap();

        let verified_batch = verified_receiver.recv().unwrap();
        assert_eq!(verified_batch.packet_batch().len(), 1);
        assert!(!verified_batch
            .packet_batch()
            .first()
            .unwrap()
            .meta()
            .discard());
        assert!(!verified_batch.meta().discard);

        exit.store(true, Ordering::Relaxed);
        drop(unverified_sender);
        stage.join().unwrap();
    }

    #[test]
    fn test_multi_tx_batch_with_bad_tx_discards_entire_batch() {
        let (unverified_sender, unverified_receiver) = bounded(10);
        let (verified_sender, verified_receiver) = unbounded();
        let exit = Arc::<AtomicBool>::default();
        let stage = BamSigverifyStage::new(unverified_receiver, verified_sender, exit.clone());

        let (packet_batch, bam_packet_batch_meta) =
            packets_to_packet_batch(vec![test_tx(), test_invalid_tx()]);
        unverified_sender
            .send((packet_batch, bam_packet_batch_meta))
            .unwrap();

        let verified_batch = verified_receiver.recv().unwrap();
        assert_eq!(verified_batch.packet_batch().len(), 2);
        assert!(!verified_batch
            .packet_batch()
            .get(0)
            .unwrap()
            .meta()
            .discard());
        assert!(verified_batch
            .packet_batch()
            .get(1)
            .unwrap()
            .meta()
            .discard());
        assert!(verified_batch.meta().discard);

        exit.store(true, Ordering::Relaxed);
        drop(unverified_sender);
        stage.join().unwrap();
    }

    #[test]
    fn test_single_packet_bad_signature_discards_entire_batch() {
        let (unverified_sender, unverified_receiver) = bounded(10);
        let (verified_sender, verified_receiver) = unbounded();
        let exit = Arc::<AtomicBool>::default();
        let stage = BamSigverifyStage::new(unverified_receiver, verified_sender, exit.clone());

        let (packet_batch, bam_packet_batch_meta) =
            packets_to_packet_batch(vec![test_invalid_tx()]);
        unverified_sender
            .send((packet_batch, bam_packet_batch_meta))
            .unwrap();

        let verified_batch = verified_receiver.recv().unwrap();
        assert_eq!(verified_batch.packet_batch().len(), 1);
        assert!(verified_batch
            .packet_batch()
            .first()
            .unwrap()
            .meta()
            .discard());
        assert!(verified_batch.meta().discard);

        exit.store(true, Ordering::Relaxed);
        drop(unverified_sender);
        stage.join().unwrap();
    }
}
