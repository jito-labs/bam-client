/// Manages majority of the JSS related functionality:
/// - Connecting to the JSS block engine via GRPC service
/// - Sending signed slot ticks + Receive bundles
/// - Executing the received bundles
/// - Disabling JSS and re-enabling standard txn processing when health check fails
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
    thread::Builder,
};

use jito_protos::proto::{
    jss_api::BuilderConfigResp,
    jss_types::{BundleResult, LeaderState, Socket},
};
use solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS;
use solana_gossip::cluster_info::ClusterInfo;
use solana_ledger::blockstore_processor::TransactionStatusSender;
use solana_poh::poh_recorder::{BankStart, PohRecorder};
use solana_runtime::{
    prioritization_fee_cache::PrioritizationFeeCache, vote_sender_types::ReplayVoteSender,
};
use solana_sdk::pubkey::Pubkey;

use crate::{
    bundle_stage::bundle_account_locker::BundleAccountLocker, jss_connection::JssConnection,
    jss_executor::JssExecutor, proxy::block_engine_stage::BlockBuilderFeeInfo,
    tip_manager::TipManager,
};

pub(crate) struct JssStage {
    threads: Vec<std::thread::JoinHandle<()>>,
}

// Runs based on timeouts and messages received from the JSS block engine
impl JssStage {
    // Create and run a new instance of the JSS Manager
    pub fn new(
        jss_url: Arc<Mutex<Option<String>>>,
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
        let api_connection_thread = Builder::new()
            .name("jss-manager".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                Self::loiter_and_start(
                    runtime,
                    jss_url,
                    jss_enabled,
                    exit,
                    poh_recorder,
                    cluster_info,
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

    fn loiter_and_start(
        runtime: tokio::runtime::Runtime,
        jss_url: Arc<Mutex<Option<String>>>,
        jss_enabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) {
        info!("JSS Manager loitering for JSS URL");

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            let current_jss_url = jss_url.lock().unwrap().clone();
            if let Some(current_jss_url) = current_jss_url {
                Self::start(
                    &runtime,
                    current_jss_url,
                    &jss_url,
                    jss_enabled.clone(),
                    exit.clone(),
                    poh_recorder.clone(),
                    cluster_info.clone(),
                    &tip_manager,
                    &bundle_account_locker,
                    &replay_vote_sender,
                    transaction_status_sender.clone(),
                    prioritization_fee_cache.clone(),
                );
            } else {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }

    // The main loop for the JSS Manager running inside an async environment
    fn start(
        runtime: &tokio::runtime::Runtime,
        current_jss_url: String,
        jss_url: &Mutex<Option<String>>,
        jss_enabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        tip_manager: &TipManager,
        bundle_account_locker: &BundleAccountLocker,
        replay_vote_sender: &ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) {
        let executor_exit = Arc::new(AtomicBool::new(false));
        let block_builder_fee_info = Arc::new(Mutex::new(BlockBuilderFeeInfo::default()));
        let (bundle_result_sender, bundle_result_receiver) = crossbeam_channel::bounded(10_000);
        const WORKER_THREAD_COUNT: usize = 4;
        let mut executor = JssExecutor::new(
            WORKER_THREAD_COUNT,
            poh_recorder.clone(),
            replay_vote_sender.clone(),
            transaction_status_sender,
            prioritization_fee_cache,
            tip_manager.clone(),
            executor_exit.clone(),
            cluster_info.clone(),
            block_builder_fee_info.clone(),
            bundle_account_locker.clone(),
            bundle_result_sender,
        );

        let mut jss_connection: Option<JssConnection> = None;
        let mut builder_info = None;
        let mut last_jss_url_check_time = std::time::Instant::now();
        let mut last_leader_slot = 0;

        info!("JSS Manager started");

        // Run until (our) world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if last_jss_url_check_time.elapsed().as_secs() > 1 {
                last_jss_url_check_time = std::time::Instant::now();
                if let Some(current_jss_url) = jss_url.lock().unwrap().clone() {
                    if current_jss_url != current_jss_url {
                        info!("JSS URL changed to {}", current_jss_url);
                        break;
                    }
                } else {
                    info!("JSS URL is None; stopping JSS Manager");
                    break;
                }
            }

            // See if we are connected correctly to a JSS instance
            if !Self::get_or_init_connection(
                &runtime,
                current_jss_url.clone(),
                &mut jss_connection,
                &poh_recorder,
                &cluster_info,
            ) {
                if !jss_enabled.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(400));
                    continue;
                }

                jss_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                builder_info = None;
                std::thread::sleep(std::time::Duration::from_millis(10));
                // FetchStageManager will revert the right TPU config
                continue;
            } else {
                jss_enabled.store(true, std::sync::atomic::Ordering::Relaxed)
            }

            // Grab the connection object
            let Some(mut jss_connection) = jss_connection.as_mut() else {
                unreachable!("JSS connection should be available");
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
            if poh_recorder.read().unwrap().would_be_leader(TICK_LOOKAHEAD)
                && poh_recorder.read().unwrap().get_current_slot() != last_leader_slot
            {
                last_leader_slot = poh_recorder.read().unwrap().get_current_slot();
                Self::run_leader_slot_mode(
                    &mut jss_connection,
                    &poh_recorder,
                    &mut executor,
                    &bundle_result_receiver,
                );
            } else {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }

        executor_exit.store(true, std::sync::atomic::Ordering::Relaxed);
        jss_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
        info!("JSS Manager exiting");
    }

    /// Invariants:
    /// - Only called when validator would be leader for the upcoming slot
    /// - bundle queue is cleared before processing begins
    /// - All received bundle are processed in order
    /// - Leader state is sent on every tick change
    /// - Processing stops if JSS connection becomes unhealthy
    fn run_leader_slot_mode(
        jss_connection: &mut JssConnection,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        executor: &mut JssExecutor,
        bundle_result_receiver: &crossbeam_channel::Receiver<BundleResult>,
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
        while let Ok(bundle_result) = bundle_result_receiver.try_recv() {
            jss_connection.send_bundle_result(bundle_result);
        }

        // Since this function is triggered ahead of the leader slot
        // Let's send an initial leader state (that might exclude information from a real bank
        // since a bank may not be available yet)
        let leader_state = Self::generate_leader_state(current_slot, None);
        jss_connection.send_leader_state(leader_state);

        // Wait up to 400ms for a bank_start
        let mut bank_start = None;
        let start = std::time::Instant::now();
        while start.elapsed().as_millis() < 400 {
            if let Some(acquired_bank_start) = poh_recorder.read().unwrap().bank_start() {
                bank_start = Some(acquired_bank_start);
                break;
            }
        }
        let Some(bank_start) = bank_start else {
            info!("No bank start available after wait; skipping leader slot mode");
            return;
        };

        let mut last_tick_send_time = std::time::Instant::now();
        let mut prev_tick = 0;
        while bank_start.should_working_bank_still_be_processing_txs()
            && jss_connection.is_healthy()
        {
            // Receive micro-blocks
            while let Some(bundle) = jss_connection.try_recv_bundle() {
                executor.schedule_bundle(&bank_start.working_bank, bundle);
            }

            // Receive retryable bundle IDs
            while let Ok(bundle_result) = bundle_result_receiver.try_recv() {
                jss_connection.send_bundle_result(bundle_result);
            }

            if last_tick_send_time.elapsed().as_millis() < 5 {
                continue;
            }

            // Send leader state every tick
            let current_tick = poh_recorder.read().unwrap().tick_height();
            if current_tick != prev_tick {
                prev_tick = current_tick;
                let current_slot = poh_recorder.read().unwrap().get_current_slot();
                let leader_state = Self::generate_leader_state(current_slot, Some(&bank_start));
                jss_connection.send_leader_state(leader_state);
                last_tick_send_time = std::time::Instant::now();
            }
        }
    }

    /// Invariants:
    /// - Returns valid slot and tick information even if no bank is available
    /// - Account CU budgets are only included for accounts approaching limits
    /// - Retryable bundle IDs are cleared after being included in a leader state
    /// - Slot CU budget accurately reflects remaining compute units
    fn generate_leader_state(current_slot: u64, bank_start: Option<&BankStart>) -> LeaderState {
        if let Some(bank_start) = bank_start {
            let bank = bank_start.working_bank.as_ref();
            let max_block_cu = bank.read_cost_tracker().unwrap().block_cost_limit();
            let consumed_block_cu = bank.read_cost_tracker().unwrap().block_cost();
            let slot_cu_budget = max_block_cu.saturating_sub(consumed_block_cu) as u32;

            return LeaderState {
                slot: bank.slot(),
                tick: bank.tick_height() as u32,
                slot_cu_budget,
            };
        } else {
            return LeaderState {
                slot: current_slot + 1,
                tick: 0,
                slot_cu_budget: MAX_BLOCK_UNITS as u32,
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
        if let Some(mut current_jss_connection) = jss_connection.take() {
            if current_jss_connection.is_healthy() {
                jss_connection.replace(current_jss_connection);
                true
            } else {
                *jss_connection = None;
                info!("JSS connection is unhealthy; closing connection");
                false
            }
        } else {
            match runtime.block_on(JssConnection::try_init(
                jss_url.clone(),
                poh_recorder.clone(),
                cluster_info.clone(),
            )) {
                Ok(current_jss_connection) => {
                    jss_connection.replace(current_jss_connection);
                    info!("JSS connection initialized to url={}", jss_url);
                    true
                }
                Err(err) => {
                    info!("Failed to connect to JSS: {}", err);
                    *jss_connection = None;
                    false
                }
            }
        }
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
