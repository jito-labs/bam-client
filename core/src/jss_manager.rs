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
    jss_types::{
        AccountComputeUnitBudget, MicroBlock, Socket,
    },
};
use solana_gossip::cluster_info::ClusterInfo;
use solana_ledger::blockstore_processor::TransactionStatusSender;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::{
    bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
    vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{pubkey::Pubkey, signer::Signer};
use tokio::task::spawn_blocking;

use crate::{
    jss_connection::JssConnection,
    jss_executor::JssExecutor,
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
        jss_is_executing: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
        cluster_info: Arc<ClusterInfo>,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) -> Self {
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
                    jss_is_executing,
                    exit,
                    poh_recorder,
                    bank_forks,
                    cluster_info,
                    replay_vote_sender,
                    transaction_status_sender,
                    prioritization_fee_cache,
                ));
            })
            .unwrap();

        Self {
            threads: vec![api_connection_thread],
        }
    }

    // The main loop for the JSS Manager running inside an async environment
    async fn start_manager(
        jss_url: String,
        jss_enabled: Arc<AtomicBool>,
        jss_is_executing: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        _bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        replay_vote_sender: ReplayVoteSender,
        transaction_status_sender: Option<TransactionStatusSender>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) {
        let mut jss_connection = None;
        let mut jss_executor = JssExecutor::new(
            poh_recorder.clone(),
            replay_vote_sender,
            transaction_status_sender,
            prioritization_fee_cache,
        );
        let mut tpu_info = None;

        // Run until (our) world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            // Init and/or check health of connection
            if !Self::get_or_init_connection(jss_url.clone(), &mut jss_connection).await {
                jss_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                continue;
            }

            // Update TPU config
            let new_tpu_info = jss_connection.as_ref().unwrap().get_tpu_config();
            if new_tpu_info != tpu_info {
                tpu_info = new_tpu_info;
                Self::update_tpu_config(tpu_info.as_ref(), &cluster_info).await;
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
        }

        if !jss_connection.as_mut().unwrap().is_healthy().await {
            *jss_connection = None;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            return false;
        }

        true
    }

    // Returns the total CUS that failed to execute
    async fn execute_micro_block(
        jss_connection: &mut JssConnection,
        jss_executor: &mut JssExecutor,
        jss_is_executing: &Arc<AtomicBool>,
        micro_block: MicroBlock,
    ) -> u32 {
        jss_is_executing.store(true, std::sync::atomic::Ordering::Relaxed);
        let (executed_sender, executed_receiver) = std::sync::mpsc::channel();
        let mut jss_executor = jss_executor.clone(); // TODO: why are we cloning?
        let actuation_task = spawn_blocking(move || {
            jss_executor.execute_and_commit_and_record_micro_block(micro_block, executed_sender);
            jss_executor
        });
        let mut failed_cus = 0;
        while !actuation_task.is_finished() {
            if let Ok(execution_result) = executed_receiver.try_recv() {
                todo!();
            }
        }

        jss_is_executing.store(false, std::sync::atomic::Ordering::Relaxed);

        failed_cus
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
    pub fn is_within_leader_slot_with_lookahead(poh_recorder: &PohRecorder) -> bool {
        const TICK_LOOKAHEAD: u64 = 8;
        poh_recorder.would_be_leader(TICK_LOOKAHEAD) || poh_recorder.would_be_leader(0)
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
