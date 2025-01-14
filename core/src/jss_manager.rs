/// Manages majority of the JSS related functionality:
/// - Connecting to the JSS block engine via GRPC service
/// - Sending signed slot ticks + Receive microblocks
/// - Actuating the received microblocks
/// - Disabling JSS and re-enabling standard txn processing when health check fails

use std::{net::{Ipv4Addr, SocketAddr, SocketAddrV4}, str::FromStr, sync::{atomic::AtomicBool, Arc, RwLock}, thread::Builder, time::{Instant, SystemTime, UNIX_EPOCH}};

use ahash::HashMap;
use jito_protos::proto::{ jds_api::TpuConfigResp, jds_types::{AccountComputeUnitBudget, ExecutionPreConfirmation, MicroBlock, MicroBlockRequest, SignedSlotTick, SlotTick, Socket} };
use solana_gossip::cluster_info::ClusterInfo;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::{bank_forks::BankForks, vote_sender_types::ReplayVoteSender};
use solana_sdk::{pubkey::Pubkey, signer::Signer};
use tokio::task::spawn_blocking;

use crate::{jss_executor::{JssExecutor, JssExecutorExecutionResult}, jss_connection::JssConnection};

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
        jss_is_actuating: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
        cluster_info: Arc<ClusterInfo>,
        replay_vote_sender: ReplayVoteSender,
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
                    jss_is_actuating,
                    exit,
                    poh_recorder,
                    bank_forks,
                    cluster_info,
                    replay_vote_sender,
                ));
            })
            .unwrap();

        Self {
            threads: vec![
                api_connection_thread
            ],
        }
    }

    // The main loop for the JSS Manager running inside an async environment
    async fn start_manager(
        jss_url: String,
        jss_enabled: Arc<AtomicBool>,
        jss_is_actuating: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        _bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        replay_vote_sender: ReplayVoteSender,
    ) {
        let mut jss_connection = None;
        let mut jss_executor = JssExecutor::new(poh_recorder.clone(), replay_vote_sender);
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

            // If we are within the leader slot, or within lookahead, request and process micro blocks
            if Self::is_within_leader_slot_with_lookahead(&poh_recorder.read().unwrap()) {
                Self::request_and_process_microblocks(
                    jss_connection.as_mut().unwrap(),
                    &mut jss_executor,
                    &jss_is_actuating,
                    &poh_recorder,
                    &cluster_info,
                ).await;
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

    async fn request_and_process_microblocks(
        jss_connection: &mut JssConnection,
        jss_executor: &mut JssExecutor,
        jss_is_actuating: &Arc<AtomicBool>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        cluster_info: &Arc<ClusterInfo>,
    ) {
        // Per slot:
        // - Aim to request 1/4 of block CUs per micro-block (48 million / 4 = 12 million each)
        // - For each subsequent micro-block, add the failed CUS to the next micro-block request
        //
        // In addition:
        // - Keep track of CUs used per account and send it in the micro-block request
        //   so that JSS knows how much more CUS each account can still use 

        let slot = poh_recorder.read().unwrap().bank().map(|bank| bank.slot()).unwrap_or_default();
        let mut failed_cus = 0;
        let keep_going = ||{
            slot == poh_recorder.read().unwrap().bank().map(|bank| bank.slot()).unwrap_or_default()
        };
        while keep_going() {
            // Send signed slot tick to JSS
            let Some(micro_block_request) = Self::build_micro_block_request(
                &poh_recorder.read().unwrap(),
                &cluster_info,
                Instant::now() + std::time::Duration::from_millis(50),
                12_000_000 + failed_cus,
                HashMap::default(), // TODO
            )
            else {
                return;
            };
            jss_connection.send_micro_block_request(micro_block_request);
            
            const TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);
            let Some(micro_block) = jss_connection.recv_microblock_with_timeout(TIMEOUT).await else {
                return;
            };

            failed_cus = Self::execute_micro_block(
                jss_connection,
                jss_executor,
                jss_is_actuating,
                micro_block,
            ).await;
        }
    }

    // Returns the total CUS that failed to execute
    async fn execute_micro_block(
        jss_connection: &mut JssConnection,
        jss_executor: &mut JssExecutor,
        jss_is_actuating: &Arc<AtomicBool>,
        micro_block: MicroBlock,
    ) -> u32 {
        jss_is_actuating.store(true, std::sync::atomic::Ordering::Relaxed);
        let (executed_sender, executed_receiver) = std::sync::mpsc::channel();
        let mut jss_executor = jss_executor.clone(); // TODO: why are we cloning?
        let actuation_task = spawn_blocking(move || {
            jss_executor.execute_and_commit_and_record_micro_block(micro_block, executed_sender);
            jss_executor
        });
        let mut failed_cus = 0;
        while !actuation_task.is_finished() {
            if let Ok(execution_result) = executed_receiver.try_recv() {
                match execution_result {
                    JssExecutorExecutionResult::Success(bundle_id) => {
                        jss_connection.send_bundle_execution_confirmation(ExecutionPreConfirmation{
                            bundle_id: bundle_id.bytes().into_iter().collect(),
                        });
                    },
                    JssExecutorExecutionResult::Failure { bundle_id: _, cus } => {
                        failed_cus += cus;
                    }
                }
            }
        }

        jss_is_actuating.store(false, std::sync::atomic::Ordering::Relaxed);

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

    pub async fn update_tpu_config(tpu_info: Option<&TpuConfigResp>, cluster_info: &Arc<ClusterInfo>) {
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
        poh_recorder.would_be_leader(TICK_LOOKAHEAD) ||
        poh_recorder.would_be_leader(0)
    }

    // Get the special signed slot tick message to send to the JSS
    // This signed message is used to verify the authenticity of the sender (us)
    // so that JSS knows we are allowed to receive potentially juicy microblocks
    pub fn build_micro_block_request(
        poh_recorder: &PohRecorder,
        cluster_info: &ClusterInfo,
        deadline: std::time::Instant,
        requested_compute_units: u32,
        account_cu_budget: HashMap<Pubkey, u64>,
    ) -> Option<MicroBlockRequest> {
        let Some(current_slot) = poh_recorder.bank().and_then(|bank| Some(bank.slot())) else {
            return None;
        };
        let Some(parent_slot) = poh_recorder.bank().and_then(|bank| Some(bank.parent_slot())) else {
            return None;
        };
        let slot_tick = SlotTick{ current_slot, parent_slot };
        let mut message = Vec::new();
        message.extend_from_slice(&slot_tick.current_slot.to_le_bytes());
        message.extend_from_slice(&slot_tick.parent_slot.to_le_bytes());
        let signature = cluster_info.keypair().sign_message(&message).to_string().into_bytes();
        let signed_slot_tick = SignedSlotTick { slot_tick: Some(slot_tick), signature };

        Some(MicroBlockRequest{
            leader_identity_pubkey: cluster_info.keypair().pubkey().to_bytes().to_vec(),
            signed_slot_tick: Some(signed_slot_tick),
            deadline: Some(instant_to_prost_timestamp(deadline)),
            requested_compute_units,
            account_cu_budget: account_cu_budget.into_iter().map(|(pubkey, available_cus)| {
                AccountComputeUnitBudget{ pubkey: pubkey.to_bytes().to_vec(), available_cus }
            }).collect(),
        })
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