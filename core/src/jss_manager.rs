/// Manages majority of the JDS related functionality:
/// - Connecting to the JDS block engine via GRPC service
/// - Sending signed slot ticks + Receive microblocks
/// - Actuating the received microblocks
/// - Disabling JDS and re-enabling standard txn processing when health check fails

use std::{sync::{atomic::AtomicBool, Arc, RwLock}, thread::Builder};

use futures::channel::mpsc;
use futures::{FutureExt, StreamExt};
use jito_protos::proto::{jds_api::{jss_node_api_client::JssNodeApiClient, start_scheduler_message::Msg, start_scheduler_response::Resp, StartSchedulerMessage}, jds_types::{MicroBlock, SignedSlotTick, SlotTick}};
use solana_gossip::cluster_info::ClusterInfo;
use solana_poh::poh_recorder::PohRecorder;
use solana_runtime::{bank_forks::BankForks, vote_sender_types::ReplayVoteSender};
use solana_sdk::signer::Signer;
use tokio::{task::spawn_blocking, time::timeout};

use crate::jss_actuator::JssActuator;

pub(crate) struct JssManager {
    threads: Vec<std::thread::JoinHandle<()>>,
}

// The (woah)man of the hour; the JDS Manager
// Run based on timeouts and messages received from the JDS block engine
impl JssManager {
    // Create and run a new instance of the JDS Manager
    pub fn new(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
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
                    jds_url,
                    jds_enabled,
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

    // The main loop for the JDS Manager running inside an async environment
    async fn start_manager(
        jds_url: String,
        jds_enabled: Arc<AtomicBool>,
        jss_is_actuating: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        _bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        replay_vote_sender: ReplayVoteSender,
    ) {
        let mut jss_connection = None;
        let mut jss_actuator = JssActuator::new(poh_recorder.clone(), replay_vote_sender);

        // Run until (our) world ends
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {

            // If no connection exists; create one
            let Some(current_jss_connection) = jss_connection.as_mut() else {
                jss_connection = JssConnection::try_init(jds_url.clone()).await;
                if jss_connection.is_none() {
                    jds_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                continue;
            };

            // If the jss_connection is not healthy; disable jds
            if !current_jss_connection.is_healthy() {
                jds_enabled.store(false, std::sync::atomic::Ordering::Relaxed);
                jss_connection = None;
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
            current_jss_connection.send_signed_tick(signed_slot_tick);

            // Wait til in leader slot (TODO: breakout for error)
            while !Self::inside_leader_slot(&poh_recorder.read().unwrap()) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            let poh_recorder = poh_recorder.read().unwrap();
            let current_slot = poh_recorder.bank().unwrap().slot();

            // Keep receiving microblocks and actuating them as long as within the existing slot
            while current_slot == poh_recorder.bank().unwrap().slot() {
                if poh_recorder.tick_height() > 62 &&  poh_recorder.next_slot_leader() == Some(cluster_info.id()) {
                    break;
                }


                let micro_block = current_jss_connection.try_recv();
                if micro_block.is_none() {
                    tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                    continue;
                }
                jss_is_actuating.store(true, std::sync::atomic::Ordering::Relaxed);
                let actuation_task = spawn_blocking(move || {
                    jss_actuator.execute_and_commit_and_record_micro_block(micro_block.unwrap());
                    jss_actuator
                });
                jss_actuator = actuation_task.await.unwrap();


                jss_is_actuating.store(false, std::sync::atomic::Ordering::Relaxed);
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


// Maintains a connection to the JDS block engine and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received
struct JssConnection {
    inbound_stream: tonic::Streaming<jito_protos::proto::jds_api::StartSchedulerResponse>,
    outbound_sender: mpsc::UnboundedSender<StartSchedulerMessage>,
    its_over: bool,
    last_heartbeat: Option<std::time::Instant>,
}

impl JssConnection {
    async fn try_init(url: String) -> Option<Self> {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url).ok()?;
        let connection_timeout = std::time::Duration::from_secs(5);
        let block_engine_channel = timeout(connection_timeout, backend_endpoint.connect()).await.ok()?.ok()?;
        let mut validator_client = JssNodeApiClient::new(block_engine_channel);

        let (outbound_sender, outbound_receiver) = mpsc::unbounded();
        // TODO: send initial message to start the scheduler
        let outbound_stream = tonic::Request::new(outbound_receiver.map(|req: StartSchedulerMessage| req));
        let inbound_stream = validator_client.start_scheduler_stream(outbound_stream).await.ok()?.into_inner();
        Some(Self {
            inbound_stream,
            outbound_sender,
            its_over: false,
            last_heartbeat: None,
        })
    }

    fn try_recv(&mut self) -> Option<MicroBlock> {
        let next = self.inbound_stream.next().now_or_never()?;
        match next {
            Some(Ok(response)) => {
                self.last_heartbeat = Some(std::time::Instant::now());
                if let Some(Resp::MicroBlock(micro_block)) = response.resp {
                    Some(micro_block)
                } else {
                    None
                }
            }
            Some(Err(_)) => {
                self.its_over = true;
                None
            }
            None => None
        }
    }

    // Send a signed slot tick to the JDS block engine
    fn send_signed_tick(&mut self, signed_slot_tick: SignedSlotTick) {
        let _ = self.outbound_sender.unbounded_send(StartSchedulerMessage {
            msg: Some(Msg::SchedulerTick(todo!())),
        });
    }

    // Check if the connection is healthy
    fn is_healthy(&mut self) -> bool {
        // Will update last_heartbeat if a new one is received
        let _ = self.try_recv();

        !self.its_over
            && self.last_heartbeat.map_or(true, 
                |heartbeat| heartbeat.elapsed().as_secs() < 5)
    }
}