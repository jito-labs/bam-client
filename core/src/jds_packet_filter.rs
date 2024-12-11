// Simple channel filter that drops packets if JDS is enabled

use std::{sync::{atomic::AtomicBool, Arc}, thread};

use crossbeam_channel::{unbounded, Sender};
use solana_perf::packet::PacketBatch;

use crate::packet_bundle::PacketBundle;

pub struct JdsPacketFilter {
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl JdsPacketFilter {
    pub fn new(
        jds_enabled: Arc<AtomicBool>,
        txn_sender_original: Sender<PacketBatch>,
        bundle_sender_original: Sender<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
    ) -> (Self, Sender<PacketBatch>, Sender<Vec<PacketBundle>>)
    {
        let (txn_sender, txn_receiver) = unbounded();
        let jds_enabled_txn = jds_enabled.clone();
        let txn_exit = exit.clone();
        let txn_thread = std::thread::Builder::new()
            .name("solana-jds-transaction-processor".to_string())
            .spawn(move || {
                while !txn_exit.load(std::sync::atomic::Ordering::Relaxed) {
                    if jds_enabled_txn.load(std::sync::atomic::Ordering::Relaxed) {
                        // If JDS is enabled, send packets to the transaction processor
                        let packet_batch = txn_receiver.recv().unwrap();
                        txn_sender_original.send(packet_batch).unwrap();
                    } else {
                        // If JDS is disabled, drop packets
                        txn_receiver.recv().unwrap();
                    }
                }
            })
            .unwrap();
            
        let (bundle_sender, bundle_receiver) = unbounded();
        let jds_enabled_bundle = jds_enabled.clone();
        let bundle_exit = exit.clone();
        let bundle_thread = std::thread::Builder::new()
            .name("solana-jds-bundle-processor".to_string())
            .spawn(move || {
                while !bundle_exit.load(std::sync::atomic::Ordering::Relaxed) {
                    if jds_enabled_bundle.load(std::sync::atomic::Ordering::Relaxed) {
                        // If JDS is enabled, send packets to the bundle processor
                        let packet_bundle = bundle_receiver.recv().unwrap();
                        bundle_sender_original.send(packet_bundle).unwrap();
                    } else {
                        // If JDS is disabled, drop packets
                        bundle_receiver.recv().unwrap();
                    }
                }
            })
            .unwrap();

        (JdsPacketFilter { handles: vec![txn_thread, bundle_thread] }, txn_sender, bundle_sender)
    }

    pub fn join(self) -> thread::Result<()> {
        for handle in self.handles {
            handle.join()?;
        }
        Ok(())
    }
}