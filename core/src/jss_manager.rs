/// Manages majority of the JSS related functionality:
/// - Connecting to the JSS block engine via GRPC service
/// - Sending signed slot ticks + Receive bundles
/// - Executing the received bundles
/// - Disabling JSS and re-enabling standard txn processing when health check fails
use std::{
    collections::VecDeque,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
    thread::Builder,
};

use jito_protos::proto::{
    jss_api::BuilderConfigResp,
    jss_types::{AccountComputeUnitBudget, LeaderState, Socket},
};
use solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS;
use solana_gossip::{
    cluster_info::ClusterInfo,
    contact_info::{ContactInfo, Protocol},
};
use solana_ledger::blockstore_processor::TransactionStatusSender;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::{
    prioritization_fee_cache::PrioritizationFeeCache, vote_sender_types::ReplayVoteSender,
};
use solana_sdk::pubkey::Pubkey;

use crate::{
    bundle_stage::bundle_account_locker::BundleAccountLocker, jss_connection::JssConnection,
    jss_executor::JssExecutor, proxy::block_engine_stage::BlockBuilderFeeInfo,
    tip_manager::TipManager,
};

pub(crate) struct JssManager {
    threads: Vec<std::thread::JoinHandle<()>>,
}

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
        let block_builder_fee_info = Arc::new(Mutex::new(BlockBuilderFeeInfo::default()));
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
        let (retry_bundle_sender, retry_bundle_receiver) = crossbeam_channel::bounded(10_000);
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
            block_builder_fee_info.clone(),
            bundle_account_locker,
            retry_bundle_sender,
        );

        let mut jss_connection: Option<JssConnection> = None;
        let mut builder_info = None;
        let local_contact_info = cluster_info.my_contact_info();

        info!("JSS Manager started");

        // Run until (our) world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            // See if we are connected correctly to a JSS instance
            if !Self::get_or_init_connection(
                &runtime,
                jss_url.clone(),
                &mut jss_connection,
                &poh_recorder,
                &cluster_info,
            ) {
                if jss_enabled.swap(false, std::sync::atomic::Ordering::Relaxed) {
                    Self::revert_tpu_config(&cluster_info, &local_contact_info);
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
                continue;
            } else {
                jss_enabled.store(true, std::sync::atomic::Ordering::Relaxed)
            }

            // Grab the connection object
            let Some(mut jss_connection) = jss_connection.as_mut() else {
                continue;
            };

            // Update TPU config (if new config is available)
            let new_builder_info = jss_connection.get_builder_config();
            if new_builder_info != builder_info {
                builder_info = new_builder_info;
                info!("Builder config updated: {:?}", builder_info);
                Self::update_tpu_config(builder_info.as_ref(), &cluster_info);
                Self::update_key_and_commission(builder_info.as_ref(), &block_builder_fee_info);
            }

            const TICK_LOOKAHEAD: u64 = 8;
            if poh_recorder.read().unwrap().would_be_leader(TICK_LOOKAHEAD) {
                Self::run_leader_slot_mode(
                    &mut jss_connection,
                    &poh_recorder,
                    &mut executor,
                    TICK_LOOKAHEAD,
                    &retry_bundle_receiver,
                );
            } else {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }

        jss_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
        info!("JSS Manager exiting");
    }

    // Run the leader slot mode
    fn run_leader_slot_mode(
        jss_connection: &mut JssConnection,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        executor: &mut JssExecutor,
        tick_lookahead: u64,
        retry_bundle_receiver: &crossbeam_channel::Receiver<[u8; 32]>,
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

        // Drain out the old micro-blocks and retryable bundle IDs
        // One of the annoying side-effects of re-using the channel between slots
        jss_connection.drain_bundles();
        retry_bundle_receiver.try_iter().for_each(|_| ());

        let mut buffered_bundles = VecDeque::new();
        let mut retryable_bundle_ids = Vec::new();
        let mut prev_tick = 0;
        while poh_recorder.read().unwrap().would_be_leader(tick_lookahead)
            && jss_connection.is_healthy()
        {
            // Send leader state every tick
            let current_tick = poh_recorder.read().unwrap().tick_height();
            if current_tick != prev_tick {
                prev_tick = current_tick;
                let leader_state =
                    Self::generate_leader_state(poh_recorder, &mut retryable_bundle_ids);
                jss_connection.send_leader_state(leader_state);
            }

            // Receive micro-blocks
            while let Some(bundle) = jss_connection.try_recv_bundle() {
                buffered_bundles.push_back(bundle);
            }

            // Receive retryable bundle IDs
            while let Ok(bundle_id) = retry_bundle_receiver.try_recv() {
                retryable_bundle_ids.push(bundle_id);
            }

            // If possible; schedule the micro-blocks
            let Some(bank_start) = poh_recorder.read().unwrap().bank_start() else {
                continue;
            };
            if !bank_start.should_working_bank_still_be_processing_txs() {
                continue;
            }
            while let Some(bundle) = buffered_bundles.pop_front() {
                executor.schedule_bundle(&bank_start.working_bank, bundle);
            }
        }
    }

    fn generate_leader_state(
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        retryable_bundle_ids: &mut Vec<[u8; 32]>,
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

                    Some(AccountComputeUnitBudget {
                        pubkey,
                        available_cus,
                    })
                })
                .collect();

            return LeaderState {
                slot: bank.slot(),
                tick: bank.tick_height() as u32,
                slot_cu_budget,
                slot_account_cu_budget,
                retryable_bundle_ids: retryable_bundle_ids
                    .drain(..)
                    .map(|id| id.to_vec())
                    .collect(),
            };
        } else {
            let current_slot = poh_recorder.read().unwrap().get_current_slot();
            let in_leader_slot = poh_recorder.read().unwrap().would_be_leader(0);
            return LeaderState {
                slot: current_slot + if in_leader_slot { 0 } else { 1 },
                tick: 0,
                slot_cu_budget: MAX_BLOCK_UNITS as u32,
                slot_account_cu_budget: vec![],
                retryable_bundle_ids: vec![],
            };
        }
    }

    // Returns true if connection is created and healthy
    fn get_or_init_connection(
        runtime: &tokio::runtime::Runtime,
        jss_url: String,
        jss_connection: &mut Option<JssConnection>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        cluster_info: &Arc<ClusterInfo>,
    ) -> bool {
        // If we have no connection object; start from scratch
        if jss_connection.is_none() {
            *jss_connection = runtime.block_on(JssConnection::try_init(
                jss_url.clone(),
                poh_recorder.clone(),
                cluster_info.clone(),
            ));
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
    fn update_tpu_config(tpu_info: Option<&BuilderConfigResp>, cluster_info: &Arc<ClusterInfo>) {
        if let Some(tpu_info) = tpu_info {
            if let Some(tpu) = Self::get_sockaddr(tpu_info.tpu_sock.as_ref()) {
                let _ = cluster_info.set_tpu(tpu);
            }

            if let Some(tpu_fwd) = Self::get_sockaddr(tpu_info.tpu_fwd_sock.as_ref()) {
                let _ = cluster_info.set_tpu_forwards(tpu_fwd);
            }
        }
    }

    fn update_key_and_commission(
        builder_info: Option<&BuilderConfigResp>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) {
        if let Some(builder_info) = builder_info {
            let pubkey = Pubkey::from_str(&builder_info.builder_pubkey).unwrap();
            let commission = builder_info.builder_commission as u64;
            let mut block_builder_fee_info = block_builder_fee_info.lock().unwrap();
            block_builder_fee_info.block_builder = pubkey;
            block_builder_fee_info.block_builder_commission = commission;
        }
    }

    /// Join all threads that the manager owns
    pub fn join(self) -> std::thread::Result<()> {
        for thread in self.threads {
            thread.join()?;
        }
        Ok(())
    }
}
