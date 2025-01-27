use std::{collections::{HashSet, VecDeque}, sync::{atomic::AtomicBool, Arc, RwLock}};

use ahash::HashMap;
use itertools::Itertools;
use jito_protos::proto::jss_types::{MicroBlock, Packet};
use prio_graph::{AccessKind, TopLevelId};
use rand::seq::IteratorRandom;
use solana_bundle::bundle_execution::load_and_execute_bundle;
use solana_entry::poh;
use solana_ledger::blockstore_processor::TransactionStatusSender;
use solana_measure::measure_us;
use solana_poh::poh_recorder::{PohRecorder, RecordTransactionsSummary, TransactionRecorder};
use solana_runtime::{
    bank::Bank, prioritization_fee_cache::PrioritizationFeeCache,
    vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{
    bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle},
    clock::MAX_PROCESSING_AGE,
    packet::PacketFlags,
    pubkey::Pubkey,
    transaction::SanitizedTransaction,
};

use crate::{
    banking_stage::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
    },
    bundle_stage::{self, MAX_BUNDLE_RETRY_DURATION},
};

pub struct JssExecutor2 {
    microblock_sender: crossbeam_channel::Sender<MicroBlock>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

const WORKER_THREAD_COUNT: usize = 4;

impl JssExecutor2 {
    pub fn new(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let committer = bundle_stage::committer::Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
        );

        let (microblock_sender, microblock_receiver) = crossbeam_channel::bounded(50);

        let mut threads = Vec::new();

        let mut workers = Vec::new();
        for _ in 0..WORKER_THREAD_COUNT {
            let (sender, receiver) = crossbeam_channel::bounded(50);
            workers.push(Worker::new(sender));
            let poh_recorder = poh_recorder.clone();
            let committer = committer.clone();
            let exit = exit.clone();
            threads.push(std::thread::spawn(move || {
                Self::spawn_worker(poh_recorder, committer.clone(), receiver, exit);
            }));
        }

        threads.push(std::thread::spawn(|| {
            Self::spawn_management_thread(microblock_receiver, poh_recorder, workers, exit);
        }));

        Self {
            microblock_sender,
            threads,
        }
    }

    fn parse_transactions<'a>(
        bank: &Bank,
        packets: impl Iterator<Item = &'a Packet>,
    ) -> Vec<SanitizedTransaction> {
        let txns = packets
            .map(|packet| {
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
                        if flags.tracer_packet {
                            solana_packet
                                .meta_mut()
                                .flags
                                .insert(PacketFlags::TRACER_PACKET);
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
                    bank.get_reserved_account_keys(),
                )?;
                Some(sanitized_transaction)
            })
            .collect_vec();
        if txns.iter().any(Option::is_none) {
            return vec![];
        }
        txns.into_iter().map(|x| x.unwrap()).collect_vec()
    }

    pub fn execute_and_commit_and_record_micro_block(&mut self, micro_block: MicroBlock) -> bool {
        self.microblock_sender.try_send(micro_block).is_ok()
    }

    fn spawn_management_thread(
        microblock_receiver: crossbeam_channel::Receiver<MicroBlock>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut workers: Vec<Worker>,
        exit: Arc<AtomicBool>,
    ) {
        let mut bundles_scheduled = 0;
        let mut last_metrics = std::time::Instant::now();

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
                continue;
            };
            if !bank_start.should_working_bank_still_be_processing_txs() {
                continue;
            }

            let mut bundles = HashMap::default();
            let mut prio_graph = prio_graph::PrioGraph::new(|id: &BundleId, _graph_node| *id);
            let mut next_bundle_id: u64 = 0;

            while bank_start.should_working_bank_still_be_processing_txs() {
                if let Ok(micro_block) = microblock_receiver.try_recv() {
                    let start = std::time::Instant::now();
                    let len = micro_block.bundles.len();
                    for bundle in micro_block.bundles {
                        let transactions = Self::parse_transactions(
                            &bank_start.working_bank,
                            bundle.packets.iter(),
                        );
                        let id = next_bundle_id;
                        let bundle_id = BundleId { id };
                        next_bundle_id += 1;
                        prio_graph.insert_transaction(
                            bundle_id,
                            transactions
                                .iter()
                                .map(|tx| Self::get_transaction_account_access(tx))
                                .flatten(),
                        );
                        bundles.insert(bundle_id, transactions);
                    }
                    info!(
                        "Received micro block with {} bundles; ingestion_time={}",
                        len,
                        start.elapsed().as_millis()
                    );
                }

                if last_metrics.elapsed().as_secs() > 1 {
                    info!(
                        "mempool_size={} scheduled={}",
                        bundles.len(),
                        bundles_scheduled
                    );
                    bundles_scheduled = 0;
                    last_metrics = std::time::Instant::now();
                }

                // Fill queues with bundles
                let Some(worker) = workers.iter_mut().find(|w| !w.is_full())
                else {
                    info!("All workers are full");
                    continue;
                };

                for bundle_id in worker.get_unblocked() {
                    prio_graph.unblock(&bundle_id);
                }

                let (bundle_id, txns) = match prio_graph.pop() {
                    Some(bundle) => (bundle, bundles.remove(&bundle).unwrap()),
                    None => {
                        continue;
                    }
                };
                if worker.send(bundle_id, txns) {
                    bundles_scheduled += 1;
                }
            }

            for worker in workers.iter_mut() {
                worker.clear();
            }

            info!("unscheduled={}", bundles.len());
        }
    }

    fn spawn_worker(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        mut committer: bundle_stage::committer::Committer,
        receiver: crossbeam_channel::Receiver<(BundleId, Vec<SanitizedTransaction>)>,
        exit: Arc<AtomicBool>,
    ) {
        let recorder = poh_recorder.read().unwrap().new_recorder();
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let Ok((_, txns)) = receiver.try_recv() else {
                continue;
            };
            let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
                continue;
            };
            if !bank_start.should_working_bank_still_be_processing_txs() {
                continue;
            }
            Self::execute_commit_record_bundle(
                &bank_start.working_bank,
                &recorder,
                &mut committer,
                txns,
            );
        }
    }

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_transaction_account_access(
        transaction: &SanitizedTransaction,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + '_ {
        let message = transaction.message();
        message
            .account_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| {
                if message.is_writable(index) {
                    (*key, AccessKind::Write)
                } else {
                    (*key, AccessKind::Read)
                }
            })
    }

    pub fn execute_commit_record_bundle(
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        committer: &mut bundle_stage::committer::Committer,
        txns: Vec<SanitizedTransaction>,
    ) -> bool {
        let len = txns.len();
        let bundle_id = derive_bundle_id_from_sanitized_transactions(&txns);
        let sanitized_bundle = SanitizedBundle {
            transactions: txns,
            bundle_id: bundle_id.clone(),
        };

        let default_accounts = vec![None; len];
        let transaction_status_sender_enabled = committer.transaction_status_sender_enabled();
        let mut bundle_execution_results = load_and_execute_bundle(
            &bank,
            &sanitized_bundle,
            MAX_PROCESSING_AGE,
            &MAX_BUNDLE_RETRY_DURATION,
            transaction_status_sender_enabled,
            &None,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        if let Err(err) = bundle_execution_results.result() {
            error!("Error executing bundle {}: {:?}", bundle_id, err);
            return false;
        }

        let (executed_batches, _execution_results_to_transactions_us) =
            measure_us!(bundle_execution_results.executed_transaction_batches());

        let _freeze_lock = bank.freeze_lock();
        let (last_blockhash, lamports_per_signature) =
            bank.last_blockhash_and_lamports_per_signature();
        let RecordTransactionsSummary {
            result: record_transactions_result,
            record_transactions_timings: _,
            starting_transaction_index,
        } = recorder.record_transactions(bank.slot(), executed_batches);
        if record_transactions_result.is_err() {
            return false;
        }

        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        committer.commit_bundle(
            &mut bundle_execution_results,
            last_blockhash,
            lamports_per_signature,
            starting_transaction_index,
            &bank,
            &mut execute_and_commit_timings,
        );

        true
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
struct BundleId {
    id: u64,
}

impl TopLevelId<Self> for BundleId {
    fn id(&self) -> Self {
        *self
    }
}

struct Worker {
    sender: crossbeam_channel::Sender<(BundleId, Vec<SanitizedTransaction>)>,
    queue: VecDeque<BundleId>,
}

impl Worker {
    fn new(sender: crossbeam_channel::Sender<(BundleId, Vec<SanitizedTransaction>)>) -> Self {
        Self {
            sender,
            queue: VecDeque::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    fn send(&mut self, bundle_id: BundleId, txns: Vec<SanitizedTransaction>) -> bool {
        if self.sender.try_send((bundle_id, txns)).is_ok() {
            self.queue.push_back(bundle_id);
            true
        } else {
            false
        }
    }

    fn get_unblocked(&mut self) -> Vec<BundleId> {
        let mut unblocked = Vec::new();
        let diff = self.queue.len().saturating_sub(self.sender.len());
        for _ in 0..diff {
            let Some(blocking) = self.queue.pop_front() else {
                break;
            };
            unblocked.push(blocking);
        }

        unblocked
    }

    fn clear(&mut self) {
        self.queue.clear();
    }
}