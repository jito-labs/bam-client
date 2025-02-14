/// Manages majority of the JSS related functionality:
/// - Connecting to the JSS block engine via GRPC service
/// - Sending signed slot ticks + Receive microblocks
/// - Executing the received microblocks
/// - Disabling JSS and re-enabling standard txn processing when health check fails
use std::{
    collections::VecDeque,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
    thread::Builder,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use jito_protos::proto::{
    jss_api::TpuConfigResp,
    jss_types::{AccountComputeUnitBudget, LeaderState, Socket},
};
use solana_gossip::{
    cluster_info::ClusterInfo,
    contact_info::{ContactInfo, Protocol},
};
use solana_ledger::blockstore_processor::TransactionStatusSender;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::{
    prioritization_fee_cache::PrioritizationFeeCache, vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{pubkey::Pubkey, signer::Signer};

use crate::{
    bundle_stage::bundle_account_locker::BundleAccountLocker, jss_connection::JssConnection,
    jss_executor::JssExecutor, proxy::block_engine_stage::BlockBuilderFeeInfo,
    tip_manager::TipManager,
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
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
    ) -> Self {
        // TODO: Remove hardcoded values
        let block_builder_fee_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: Pubkey::from_str("feeywn2ffX8DivmRvBJ9i9YZnss7WBouTmujfQcEdeY").unwrap(),
            block_builder_commission: 5,
        }));

        let api_connection_thread = Builder::new()
            .name("jss-manager".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                Self::start_manager(
                    runtime,
                    jss_url,
                    jss_enabled,
                    exit,
                    poh_recorder,
                    cluster_info,
                    block_builder_fee_info,
                    tip_manager,
                    bundle_account_locker,
                    replay_vote_sender,
                    transaction_status_sender,
                    prioritization_fee_cache,
                );
            })
            .unwrap();

        Self {
            threads: vec![api_connection_thread],
        }
    }

    // The main loop for the JSS Manager running inside an async environment
    fn start_manager(
        runtime: tokio::runtime::Runtime,
        jss_url: String,
        jss_enabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) {
        const WORKER_THREAD_COUNT: usize = 4;
        let mut executor = JssExecutor::new(
            WORKER_THREAD_COUNT,
            poh_recorder.clone(),
            replay_vote_sender,
            transaction_status_sender,
            prioritization_fee_cache,
            tip_manager,
            exit.clone(),
            cluster_info.keypair().to_owned(),
            block_builder_fee_info,
            bundle_account_locker,
        );

        let mut jss_connection: Option<JssConnection> = None;
        let mut tpu_info = None;
        let local_contact_info = cluster_info.my_contact_info();

        info!("JSS Manager started");

        // Run until (our) world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            // Init and/or check health of connection
            let pubkey = cluster_info.keypair().pubkey();
            if !Self::get_or_init_connection(&runtime, jss_url.clone(), &mut jss_connection, pubkey)
            {
                if jss_enabled.load(std::sync::atomic::Ordering::Relaxed) {
                    jss_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                    Self::revert_tpu_config(&cluster_info, &local_contact_info);
                }
                continue;
            } else {
                if !jss_enabled.load(std::sync::atomic::Ordering::Relaxed) {
                    jss_enabled.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Grab the connection object
            let Some(mut jss_connection) = jss_connection.as_mut() else {
                continue;
            };

            // Update TPU config (if new config is available)
            let new_tpu_info = jss_connection.get_tpu_config();
            if new_tpu_info != tpu_info {
                tpu_info = new_tpu_info;
                info!("TPU config updated: {:?}", tpu_info);
                Self::update_tpu_config(tpu_info.as_ref(), &cluster_info);
            }

            const TICK_LOOKAHEAD: u64 = 8;
            if Self::is_within_leader_slot_with_lookahead(
                &poh_recorder.read().unwrap(),
                TICK_LOOKAHEAD,
            ) {
                Self::run_leader_slot_mode(
                    &mut jss_connection,
                    &cluster_info,
                    &poh_recorder,
                    &mut executor,
                );
            }
        }

        info!("JSS Manager exiting");
    }

    // Run the leader slot mode
    fn run_leader_slot_mode(
        jss_connection: &mut JssConnection,
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        executor: &mut JssExecutor,
    ) {
        let current_slot = poh_recorder.read().unwrap().get_current_slot();
        let current_tick = poh_recorder.read().unwrap().tick_height()
            % poh_recorder.read().unwrap().ticks_per_slot();
        info!(
            "Running leader slot mode for slot={} current_slot={} current_tick={}",
            current_slot + 1,
            current_slot,
            current_tick
        );

        let mut buffered_micro_blocks = VecDeque::new();
        let mut prev_tick = 0;
        const TICK_LOOKAHEAD: u64 = 8;
        while Self::is_within_leader_slot_with_lookahead(
            &poh_recorder.read().unwrap(),
            TICK_LOOKAHEAD,
        ) && jss_connection.is_healthy()
        {
            // Send leader state every tick
            let current_tick = poh_recorder.read().unwrap().tick_height();
            if current_tick != prev_tick {
                prev_tick = current_tick;
                let leader_state = Self::generate_leader_state(cluster_info, poh_recorder);
                jss_connection.send_leader_state(leader_state);
            }

            // Receive micro-blocks
            while let Some(micro_block) = jss_connection.try_recv_microblock() {
                buffered_micro_blocks.push_back(micro_block);
            }

            // If possible; schedule the micro-blocks
            let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
                continue;
            };
            if !bank_start.should_working_bank_still_be_processing_txs() {
                continue;
            }
            while let Some(micro_block) = buffered_micro_blocks.pop_front() {
                info!("micro_block bundles={}", micro_block.bundles.len());
                executor.schedule_microblock(&bank_start.working_bank, micro_block);
            }
        }
    }

    fn generate_leader_state(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) -> LeaderState {
        if let Some(bank_start) = poh_recorder.read().unwrap().bank_start() {
            let bank = bank_start.working_bank;
            let max_block_cu = bank.read_cost_tracker().unwrap().block_cost_limit();
            let consumed_block_cu = bank.read_cost_tracker().unwrap().block_cost();
            let slot_cu_budget = max_block_cu.saturating_sub(consumed_block_cu) as u32;

            let account_cost_limit = bank.read_cost_tracker().unwrap().account_cost_limit;
            let slot_account_cu_budget = bank
                .read_cost_tracker()
                .unwrap()
                .cost_by_writable_accounts
                .iter()
                .filter_map(|(pubkey, cost)| {
                    let pubkey = pubkey.to_bytes().to_vec();
                    let available_cus = account_cost_limit.saturating_sub(*cost);

                    // If available_cus is within 90% of the account_cost_limit, skip it
                    // (Efficiency optimization)
                    if available_cus > ((account_cost_limit / 10) * 9) {
                        return None;
                    }

                    Some(AccountComputeUnitBudget{
                        pubkey,
                        available_cus,
                    })
                })
                .collect();

            return LeaderState {
                pubkey: cluster_info.keypair().pubkey().to_bytes().to_vec(),
                slot: bank.slot(),
                tick: bank.tick_height() as u32,
                slot_cu_budget,
                slot_account_cu_budget,
                recently_executed_txn_signatures: vec![],
            };
        } else {
            let current_slot = poh_recorder.read().unwrap().get_current_slot();
            return LeaderState {
                pubkey: cluster_info.keypair().pubkey().to_bytes().to_vec(),
                slot: current_slot + 1,
                tick: 0,
                slot_cu_budget: 48_000_000,
                slot_account_cu_budget: vec![],
                recently_executed_txn_signatures: vec![],
            };
        }
    }

    // Returns true if connection is created and healthy
    fn get_or_init_connection(
        runtime: &tokio::runtime::Runtime,
        jss_url: String,
        jss_connection: &mut Option<JssConnection>,
        pubkey: Pubkey,
    ) -> bool {
        // If we have no connection object; start from scratch
        if jss_connection.is_none() {
            *jss_connection = runtime.block_on(JssConnection::try_init(jss_url.clone(), pubkey));
            if jss_connection.is_none() {
                return false;
            }
            info!("JSS connection initialized to url={}", jss_url);
        }

        // If the object is unhealthy; delete it
        if !jss_connection.as_mut().unwrap().is_healthy() {
            info!("JSS connection is unhealthy; closing connection");
            *jss_connection = None;
            return false;
        }

        true
    }

    fn revert_tpu_config(cluster_info: &Arc<ClusterInfo>, local_contact_info: &ContactInfo) {
        let _ = cluster_info
            .set_tpu(local_contact_info.tpu(Protocol::UDP).unwrap())
            .inspect_err(|e| {
                warn!("Failed to set TPU: {:?}", e);
            });
        let _ = cluster_info
            .set_tpu_forwards(local_contact_info.tpu_forwards(Protocol::UDP).unwrap())
            .inspect_err(|e| {
                warn!("Failed to set TPU forwards: {:?}", e);
            });
    }

    fn get_sockaddr(info: Option<&Socket>) -> Option<SocketAddr> {
        let info = info?;
        let Socket { ip, port } = info;
        Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(&ip).ok()?,
            *port as u16,
        )))
    }

    /// Update the TPU config in the cluster info
    pub fn update_tpu_config(tpu_info: Option<&TpuConfigResp>, cluster_info: &Arc<ClusterInfo>) {
        if let Some(tpu_info) = tpu_info {
            if let Some(tpu) = Self::get_sockaddr(tpu_info.tpu_sock.as_ref()) {
                let _ = cluster_info.set_tpu(tpu);
            }

            if let Some(tpu_fwd) = Self::get_sockaddr(tpu_info.tpu_fwd_sock.as_ref()) {
                let _ = cluster_info.set_tpu_forwards(tpu_fwd);
            }
        }
    }

    /// Join all threads that the manager owns
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

/// Convert an Instant to a prost Timestamp
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
