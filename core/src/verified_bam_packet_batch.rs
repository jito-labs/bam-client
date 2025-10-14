use {
    itertools::Itertools,
    jito_protos::proto::bam_types::AtomicTxnBatch,
    solana_packet::{Meta, PACKET_DATA_SIZE},
    solana_perf::packet::{BytesPacket, PacketBatch},
};

pub enum BamPacketBatchError {
    OutsideLeaderSlot,
    EmptyBatch,
    TooManyPackets,
    MissingMeta,
    InconsistentRevertOnError,
    PacketTooLarge,
}

pub struct BamPacketBatchMeta {
    // Discard marked true by BamSigverifyStage
    pub discard: bool,

    pub seq_id: u32,
    pub max_schedule_slot: u64,
    pub revert_on_error: bool,
}

pub struct VerifiedBamPacketBatch {
    packet_batch: PacketBatch,
    meta: BamPacketBatchMeta,
}

impl VerifiedBamPacketBatch {
    pub fn new(packet_batch: PacketBatch, meta: BamPacketBatchMeta) -> Self {
        Self { packet_batch, meta }
    }

    pub fn meta(&self) -> &BamPacketBatchMeta {
        &self.meta
    }

    pub fn packet_batch(&self) -> &PacketBatch {
        &self.packet_batch
    }

    pub fn take(self) -> (PacketBatch, BamPacketBatchMeta) {
        (self.packet_batch, self.meta)
    }

    /// Validates the AtomicTxnBatch and splits it into a PacketBatch and a BamPacketBatchMeta.
    /// ed25519_verify_cpu needs a list of PacketBatches, not an AtomicTxnBatch or BamPacketBatch.
    pub fn validate_and_split(
        txn_batch: AtomicTxnBatch,
        current_slot: u64,
    ) -> Result<(PacketBatch, BamPacketBatchMeta), BamPacketBatchError> {
        let revert_on_error = Self::validate(&txn_batch, current_slot)?;

        let meta = BamPacketBatchMeta {
            seq_id: txn_batch.seq_id,
            max_schedule_slot: txn_batch.max_schedule_slot,
            revert_on_error,
            discard: false,
        };

        let packet_batch = Self::to_packet_batch(txn_batch);

        Ok((packet_batch, meta))
    }

    /// Converts the AtomicTxnBatch to a PacketBatch.
    fn to_packet_batch(atomic_txn_batch: AtomicTxnBatch) -> PacketBatch {
        let mut packets = Vec::with_capacity(atomic_txn_batch.packets.len());
        for packet in atomic_txn_batch.packets {
            let meta = Meta {
                size: packet.data.len(),
                ..Meta::default()
            };
            let packet = BytesPacket::new(packet.data.into(), meta);
            packets.push(packet);
        }
        PacketBatch::from(packets)
    }

    /// Validates the AtomicTxnBatch and returns the revert_on_error flag.
    fn validate(
        atomic_txn_batch: &AtomicTxnBatch,
        current_slot: u64,
    ) -> Result<bool, BamPacketBatchError> {
        if atomic_txn_batch.max_schedule_slot < current_slot {
            return Err(BamPacketBatchError::OutsideLeaderSlot);
        }

        if atomic_txn_batch.packets.is_empty() {
            return Err(BamPacketBatchError::EmptyBatch);
        }

        if atomic_txn_batch.packets.len() > 5 {
            return Err(BamPacketBatchError::TooManyPackets);
        }

        if atomic_txn_batch.packets.iter().any(|p| p.meta.is_none()) {
            return Err(BamPacketBatchError::MissingMeta);
        }

        if atomic_txn_batch.packets.iter().any(|p| {
            p.data.len() > PACKET_DATA_SIZE
                || p.meta
                    .as_ref()
                    .map_or(false, |m| m.size > PACKET_DATA_SIZE as u64)
        }) {
            return Err(BamPacketBatchError::PacketTooLarge);
        }

        let Ok(revert_on_error) = atomic_txn_batch
            .packets
            .iter()
            .map(|p| {
                p.meta
                    .as_ref()
                    .and_then(|meta| meta.flags.as_ref())
                    .is_some_and(|flags| flags.revert_on_error)
            })
            .all_equal_value()
        else {
            return Err(BamPacketBatchError::InconsistentRevertOnError);
        };

        Ok(revert_on_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_schedule_slot_out_of_range_returns_error() {}

    #[test]
    fn test_empty_batch_returns_error() {}

    #[test]
    fn test_too_many_packets_returns_error() {}

    #[test]
    fn test_packet_too_large_returns_error() {}

    #[test]
    fn test_packet_meta_size_too_large_returns_error() {}

    #[test]
    fn test_packet_meta_missing_returns_error() {}

    #[test]
    fn test_inconsistent_revert_on_error_returns_error() {}
}
