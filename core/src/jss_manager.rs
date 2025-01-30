/// Manages majority of the JSS related functionality:
/// - Connecting to the JSS block engine via GRPC service
/// - Sending signed slot ticks + Receive microblocks
/// - Executing the received microblocks
/// - Disabling JSS and re-enabling standard txn processing when health check fails
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, RwLock},
    thread::Builder,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use jito_protos::proto::{
    jss_api::TpuConfigResp,
    jss_types::{LeaderState, MicroBlock, Socket},
};
use solana_gossip::cluster_info::ClusterInfo;
use solana_ledger::blockstore_processor::TransactionStatusSender;
use solana_poh::poh_recorder::{BankStart, PohRecorder};
use solana_runtime::{
    bank::Bank, prioritization_fee_cache::PrioritizationFeeCache,
    vote_sender_types::ReplayVoteSender,
};
use solana_sdk::signer::Signer;

use crate::{
    jss_connection::JssConnection, jss_executor::JssExecutor,
};

pub(crate) struct JssManager {
    threads: Vec<std::thread::JoinHandle<()>>,
}

// The (woah)man of the hour; the JSS Manager
// Runs based on timeouts and messages received from the JSS block engine
impl JssManager {
    // Create and run a new instance of the JSS Manager
    pub fn new(
        jss_url: String,
        jss_enabled: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        exit: Arc<AtomicBool>,
        cluster_info: Arc<ClusterInfo>,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) -> Self {
        let (micro_block_sender, micro_block_receiver) = std::sync::mpsc::channel();
        let exit_micro_block_execution_thread = exit.clone();
        let poh_recorder_micro_block_execution_thread = poh_recorder.clone();
        let micro_block_execution_thread = Builder::new()
            .name("micro_block_execution_thread".to_string())
            .spawn(move || {
                const WORKER_THREAD_COUNT: usize = 4;
                let mut executor = JssExecutor::new(
                    WORKER_THREAD_COUNT,
                    poh_recorder_micro_block_execution_thread.clone(),
                    replay_vote_sender,
                    transaction_status_sender,
                    prioritization_fee_cache,
                    exit_micro_block_execution_thread.clone(),
                );

                info!("Micro block execution thread started");

                while !exit_micro_block_execution_thread.load(std::sync::atomic::Ordering::Relaxed)
                {
                    let Some((micro_block, slot)): Option<(MicroBlock, u64)> =
                        micro_block_receiver.recv().ok()
                    else {
                        continue;
                    };
                    let Some(current_slot) = poh_recorder_micro_block_execution_thread
                        .read()
                        .unwrap()
                        .working_slot()
                    else {
                        continue;
                    };
                    if slot != current_slot {
                        info!(
                            "Received micro block for slot={} but current slot is={}; skipping",
                            slot, current_slot
                        );
                        continue;
                    }
                    let poh = poh_recorder_micro_block_execution_thread.read().unwrap();
                    let current_tick = poh.tick_height() % poh.ticks_per_slot();
                    info!(
                        "Received micro block; slot={}, tick={}, bundle_count: {}",
                        current_slot,
                        current_tick,
                        micro_block.bundles.len()
                    );
                    let Some(bank) = poh.bank_start() else {
                        continue;
                    };
                    if !bank.should_working_bank_still_be_processing_txs() {
                        continue;
                    }
                    executor.schedule_microblock(&bank.working_bank, micro_block);
                }
            })
            .unwrap();

        let api_connection_thread = Builder::new()
            .name("jss-manager".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(Self::start_manager(
                    jss_url,
                    jss_enabled,
                    exit,
                    poh_recorder,
                    cluster_info,
                    micro_block_sender,
                ));
            })
            .unwrap();

        Self {
            threads: vec![api_connection_thread, micro_block_execution_thread],
        }
    }

    // The main loop for the JSS Manager running inside an async environment
    async fn start_manager(
        jss_url: String,
        jss_enabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        micro_block_sender: std::sync::mpsc::Sender<(MicroBlock, u64)>,
    ) {
        let mut jss_connection = None;
        let mut tpu_info = None;

        info!("JSS Manager started");

        // Run until (our) world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            // Init and/or check health of connection
            if !Self::get_or_init_connection(jss_url.clone(), &mut jss_connection).await {
                jss_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                continue;
            } else {
                jss_enabled.store(true, std::sync::atomic::Ordering::Relaxed);
            }

            let Some(mut jss_connection) = jss_connection.as_mut() else {
                continue;
            };

            // Update TPU config
            let new_tpu_info = jss_connection.get_tpu_config();
            if new_tpu_info != tpu_info {
                tpu_info = new_tpu_info;
                info!("TPU config updated: {:?}", tpu_info);
                Self::update_tpu_config(tpu_info.as_ref(), &cluster_info).await;
            }

            if Self::is_within_leader_slot_with_lookahead(&poh_recorder.read().unwrap(), 0) {
                let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
                    continue;
                };
                if !bank_start.should_working_bank_still_be_processing_txs() {
                    continue;
                }

                info!(
                    "Entering leader slot mode slot={} tick={}",
                    bank_start.working_bank.slot(),
                    bank_start.working_bank.tick_height() as u64
                        % bank_start.working_bank.ticks_per_slot()
                );
                Self::run_leader_slot_mode(
                    &mut jss_connection,
                    &cluster_info,
                    &bank_start,
                    &micro_block_sender,
                );
            }
        }

        info!("JSS Manager exiting");
    }

    // Run the leader slot mode
    fn run_leader_slot_mode(
        jss_connection: &mut JssConnection,
        cluster_info: &Arc<ClusterInfo>,
        bank_start: &BankStart,
        micro_block_sender: &std::sync::mpsc::Sender<(MicroBlock, u64)>,
    ) {
        let send_leader_state = |jss_connection: &mut JssConnection, bank: &Bank| {
            let max_block_cu = bank.read_cost_tracker().unwrap().block_cost_limit();
            let consumed_block_cu = bank.read_cost_tracker().unwrap().block_cost();
            let slot_cu_budget = max_block_cu.saturating_sub(consumed_block_cu) as u32;
            let slot_account_cu_budget = vec![]; // TODO fill this
            let leader_state = LeaderState {
                pubkey: cluster_info.keypair().pubkey().to_bytes().to_vec(),
                slot: bank.slot(),
                tick: bank.tick_height() as u32,
                slot_account_cu_budget,
                slot_cu_budget,
                recently_executed_txn_signatures: vec![], // TODO; fill this (Maybe not needed for POC)
            };
            //info!("Sending leader state slot={} tick={}", leader_state.slot, leader_state.tick as u64 % poh.ticks_per_slot());
            jss_connection.send_leader_state(leader_state);
        };

        // Start off by sending the leader state

        let mut prev_tick = bank_start.working_bank.tick_height();
        while bank_start.should_working_bank_still_be_processing_txs() {
            while let Some(micro_block) = jss_connection.try_recv_microblock() {
                let current_slot = bank_start.working_bank.slot();
                micro_block_sender.send((micro_block, current_slot)).ok();
            }

            // If tick has increased, send leader state
            let tick = bank_start.working_bank.tick_height();
            if prev_tick != tick && tick % 8 == 0 {
                prev_tick = tick;
                send_leader_state(jss_connection, &bank_start.working_bank);
            }
        }
    }

    // Returns true if connection is created and healthy
    async fn get_or_init_connection(
        jss_url: String,
        jss_connection: &mut Option<JssConnection>,
    ) -> bool {
        if jss_connection.is_none() {
            *jss_connection = JssConnection::try_init(jss_url.clone()).await;
            if jss_connection.is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                return false;
            }
            info!("JSS connection initialized to url={}", jss_url);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        if !jss_connection.as_mut().unwrap().is_healthy().await {
            info!("JSS connection is unhealthy; closing connection");
            *jss_connection = None;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            return false;
        }

        true
    }

    fn get_sockaddr(info: Option<&Socket>) -> Option<SocketAddr> {
        let info = info?;
        let Socket { ip, port } = info;
        Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(&ip).ok()?,
            *port as u16,
        )))
    }

    pub async fn update_tpu_config(
        tpu_info: Option<&TpuConfigResp>,
        cluster_info: &Arc<ClusterInfo>,
    ) {
        if let Some(tpu_info) = tpu_info {
            if let Some(tpu) = Self::get_sockaddr(tpu_info.tpu_sock.as_ref()) {
                let _ = cluster_info.set_tpu(tpu);
            }

            if let Some(tpu_fwd) = Self::get_sockaddr(tpu_info.tpu_fwd_sock.as_ref()) {
                let _ = cluster_info.set_tpu_forwards(tpu_fwd);
            }
        }
    }

    // Join all threads that the manager owns
    pub fn join(self) -> std::thread::Result<()> {
        for thread in self.threads {
            thread.join()?;
        }
        Ok(())
    }

    // Check if it's time for an auction
    // This is decided based on the PohRecorder's current slot and the lookahead
    pub fn is_within_leader_slot_with_lookahead(
        poh_recorder: &PohRecorder,
        tick_lookahead: u64,
    ) -> bool {
        poh_recorder.would_be_leader(tick_lookahead) || poh_recorder.would_be_leader(0)
    }
}

pub fn instant_to_prost_timestamp(instant: Instant) -> prost_types::Timestamp {
    let now = Instant::now();
    let system_now = SystemTime::now();

    let duration_since_now = if instant >= now {
        instant - now
    } else {
        now - instant
    };

    let target_time = if instant >= now {
        system_now + duration_since_now
    } else {
        system_now - duration_since_now
    };

    let duration_since_epoch = target_time.duration_since(UNIX_EPOCH).unwrap_or_default();

    prost_types::Timestamp {
        seconds: duration_since_epoch.as_secs() as i64,
        nanos: duration_since_epoch.subsec_nanos() as i32,
    }
}
