/// Manages majority of the JDS related functionality:
/// - Connecting to the JDS block engine via GRPC service
/// - Sending signed slot ticks + Receive microblocks
/// - Actuating the received microblocks
/// - Disabling JDS and re-enabling standard txn processing when health check fails

use std::{sync::{atomic::AtomicBool, Arc, RwLock}, thread::Builder};

use jito_protos::proto::{jds_api::validator_api_client::ValidatorApiClient, jds_types::{MicroBlock, SignedSlotTick, SlotTick}};
use solana_gossip::cluster_info::ClusterInfo;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::bank_forks::BankForks;
use solana_sdk::signer::Signer;
use tokio::task::spawn_blocking;

use crate::jds_actuator::JdsActuator;

pub(crate) struct JdsManager {
    threads: Vec<std::thread::JoinHandle<()>>,
}

// The (woah)man of the hour; the JDS Manager
// Run based on timeouts and messages received from the JDS block engine
impl JdsManager {
    // Create and run a new instance of the JDS Manager
    pub fn new(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        jds_is_actuating: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Self {
        let api_connection_thread = Builder::new()
            .name("block-engine-stage".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(Self::start_manager(
                    jds_url,
                    jds_enabled,
                    jds_is_actuating,
                    exit,
                    poh_recorder,
                    bank_forks,
                    cluster_info,
                ));
            })
            .unwrap();

        Self {
            threads: vec![
                api_connection_thread
            ],
        }
    }

    // The main loop for the JDS Manager running inside an async environment
    async fn start_manager(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        jds_is_actuating: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
    ) {
        let mut jds_connection = None;
        let mut jds_actuator = JdsActuator::new();

        // Run until (our) world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {

            // If no connection exists; create one
            let Some(current_jds_connection) = jds_connection.as_mut() else {
                jds_connection = JdsConnection::try_init(jds_url.clone());
                if jds_connection.is_none() {
                    jds_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                continue;
            };

            // If jds connection is not ready (hasn't received first heartbeat); wait and loop again
            if !current_jds_connection.is_ready() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                continue;
            }

            // If the jds_connection is not healthy; disable jds
            if !current_jds_connection.is_healthy() {
                jds_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                jds_connection = None;
                continue;
            }

            // If not time for auction; wait and loop again
            if !Self::time_for_auction(&poh_recorder.read().unwrap()) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            // Send slot tick
            let Some(signed_slot_tick) = Self::get_signed_slot_tick(
                &poh_recorder.read().unwrap(), &cluster_info)
            else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };
            current_jds_connection.send_signed_tick(signed_slot_tick);

            // Wait til in leader slot (TODO: breakout for error)
            while !Self::inside_leader_slot(&poh_recorder.read().unwrap()) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // Keep receiving microblocks and actuating them as long as within the existing slot
            while Self::inside_leader_slot(&poh_recorder.read().unwrap()) {
                let micro_block = current_jds_connection.recv_next();
                if micro_block.is_none() {
                    tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                    continue;
                }
                jds_is_actuating.store(true, std::sync::atomic::Ordering::Relaxed);
                jds_actuator = spawn_blocking(move || {
                    jds_actuator.execute_and_commit_micro_block(micro_block.unwrap());
                    jds_actuator
                }).await.unwrap();
                jds_is_actuating.store(false, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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
    pub fn time_for_auction(poh_recorder: &PohRecorder) -> bool {
        const TICK_LOOKAHEAD: u64 = 8;
        poh_recorder.would_be_leader(TICK_LOOKAHEAD)
    }

    // Check if we are currently inside the leader slot
    // and therefore can keep waiting for microblocks
    pub fn inside_leader_slot(poh_recorder: &PohRecorder) -> bool {
        poh_recorder.would_be_leader(0)
    }

    // Get the special signed slot tick message to send to the JDS block engine
    // This signed message is used to verify the authenticity of the sender (us)
    // so that JDS knows we are allowed to receive potentially juicy microblocks
    pub fn get_signed_slot_tick(poh_recorder: &PohRecorder, cluster_info: &ClusterInfo) -> Option<SignedSlotTick> {
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
        Some(SignedSlotTick { slot_tick: Some(slot_tick), signature })
    }
}

struct JdsConnection {
}

impl JdsConnection {
    fn try_init(url: String) -> Option<Self> {
        // let backend_endpoint = tonic::transport::Endpoint::from_shared(jds_url).unwrap();
        // let connection_timeout = std::time::Duration::from_secs(5);
        // let block_engine_channel = timeout(connection_timeout, backend_endpoint.connect()).await.unwrap().unwrap();
        // let validator_client = ValidatorApiClient::new(block_engine_channel);
        // let sent_tick_for_slot: Option<Slot> = None;

        //let init_request = jito_protos::proto::jds_types::SignedSlotTick{ slot_tick: todo!(), signature: todo!() };
        //let stream = validator_client.start_scheduler_stream(init_request).await.unwrap();


        None
    }

    fn recv_next(&mut self) -> Option<MicroBlock> {
        todo!()
    }

    fn send_signed_tick(&mut self, _signed_slot_tick: SignedSlotTick) {
        todo!()
    }

    fn is_healthy(&self) -> bool {
        todo!()
    }

    fn is_ready(&self) -> bool {
        todo!()
    }
}