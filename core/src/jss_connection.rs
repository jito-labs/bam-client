use std::sync::{Arc, Mutex, RwLock};

use crossbeam_channel::Receiver;
use futures::{channel::mpsc, StreamExt};
use jito_protos::proto::{
    jss_api::{
        jss_node_api_client::JssNodeApiClient, start_scheduler_message::Msg,
        start_scheduler_response::Resp, BuilderConfigResp, GetBuilderConfigRequest,
        StartSchedulerMessage, StartSchedulerResponse,
    },
    jss_types::{LeaderState, MicroBlock, ValidatorHeartBeat},
};
use solana_gossip::cluster_info::ClusterInfo;
use solana_poh::poh_recorder::PohRecorder;
use solana_sdk::{signature::Keypair, signer::Signer};
use tokio::time::{interval, timeout};

// Maintains a connection to the JSS Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received
pub struct JssConnection {
    outbound_sender: mpsc::Sender<StartSchedulerMessage>,
    last_heartbeat: Arc<Mutex<std::time::Instant>>,
    builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
    microblock_receiver: Receiver<MicroBlock>,
    background_task: tokio::task::JoinHandle<()>,
}

impl JssConnection {
    pub async fn try_init(
        url: String,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Option<Self> {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url).ok()?;
        let connection_timeout = std::time::Duration::from_secs(5);
        let builder_config = Arc::new(Mutex::new(None));

        let channel = timeout(connection_timeout, backend_endpoint.connect())
            .await
            .ok()?
            .ok()?;

        let mut validator_client = JssNodeApiClient::new(channel);

        let (outbound_sender, outbound_receiver) = mpsc::channel(100_000);
        let outbound_stream =
            tonic::Request::new(outbound_receiver.map(|req: StartSchedulerMessage| req));
        let inbound_stream = validator_client
            .start_scheduler_stream(outbound_stream)
            .await
            .ok()?
            .into_inner();

        let last_heartbeat = Arc::new(Mutex::new(std::time::Instant::now()));
        let (microblock_sender, microblock_receiver) = crossbeam_channel::bounded(100_000);
        let background_task = tokio::spawn(Self::background_task(
            inbound_stream,
            outbound_sender.clone(),
            validator_client,
            last_heartbeat.clone(),
            builder_config.clone(),
            microblock_sender,
            poh_recorder,
            cluster_info,
        ));

        Some(Self {
            outbound_sender,
            last_heartbeat,
            builder_config,
            background_task,
            microblock_receiver,
        })
    }

    async fn background_task(
        mut inbound_stream: tonic::Streaming<StartSchedulerResponse>,
        mut sender: mpsc::Sender<StartSchedulerMessage>,
        mut validator_client: JssNodeApiClient<tonic::transport::channel::Channel>,
        last_heartbeat_clone: Arc<Mutex<std::time::Instant>>,
        builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
        microblock_sender: crossbeam_channel::Sender<MicroBlock>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
    ) {
        let mut heartbeat_interval = interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let Some(signed_heartbeat) = Self::create_signed_heartbeat(poh_recorder.read().unwrap().get_current_slot(), cluster_info.as_ref()) else {
                        error!("Failed to create signed heartbeat");
                        break;
                    };
                    let _ = sender.try_send(StartSchedulerMessage {
                        msg: Some(Msg::HeartBeat(signed_heartbeat)),
                    });
                    let Ok(resp) = validator_client.get_builder_config(GetBuilderConfigRequest {}).await else {
                        break;
                    };
                    *builder_config.lock().unwrap() = Some(resp.into_inner());
                }
                inbound = inbound_stream.message() => {
                    let Ok(Some(inbound)) = inbound else {
                        break;
                    };

                    match inbound {
                        StartSchedulerResponse { resp: Some(Resp::HeartBeat(_)), .. } => {
                            *last_heartbeat_clone.lock().unwrap() = std::time::Instant::now();
                        }
                        StartSchedulerResponse { resp: Some(Resp::MicroBlock(microblock)), .. } => {
                            let _ = microblock_sender.send(microblock);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn create_signed_heartbeat(
        slot: u64,
        cluster_info: &ClusterInfo,
    ) -> Option<ValidatorHeartBeat> {
        let slot_signature = Self::sign_slot(slot, cluster_info.keypair().as_ref())?;
        Some(ValidatorHeartBeat {
            pubkey: cluster_info.keypair().pubkey().to_string(),
            slot,
            slot_signature,
        })
    }

    fn sign_slot(slot: u64, keypair: &Keypair) -> Option<String> {
        let slot_signature = keypair.try_sign_message(&slot.to_le_bytes()).ok()?;
        let slot_signature = slot_signature.to_string();
        Some(slot_signature)
    }

    pub fn try_recv_microblock(&mut self) -> Option<MicroBlock> {
        self.microblock_receiver.try_recv().ok()
    }

    pub fn drain_microblocks(&mut self) {
        while let Ok(_) = self.microblock_receiver.try_recv() {}
    }

    // Send a signed slot tick to the JSS instance
    pub fn send_leader_state(&mut self, leader_state: LeaderState) {
        let _ = self.outbound_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::LeaderState(leader_state)),
        });
    }

    // Check if the connection is healthy
    pub fn is_healthy(&mut self) -> bool {
        let is_healthy =
            self.last_heartbeat.lock().unwrap().elapsed() < std::time::Duration::from_secs(6);

        if !is_healthy {
            info!(
                "dead_heartbeat={}",
                self.last_heartbeat.lock().unwrap().elapsed().as_secs()
            );
        }

        is_healthy
    }

    pub fn get_builder_config(&self) -> Option<BuilderConfigResp> {
        self.builder_config.lock().unwrap().clone()
    }
}

impl Drop for JssConnection {
    fn drop(&mut self) {
        self.background_task.abort();
    }
}
