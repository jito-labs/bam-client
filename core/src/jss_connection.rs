use std::sync::{Arc, Mutex};

use crossbeam_channel::Receiver;
use futures::{channel::mpsc, StreamExt};
use jito_protos::proto::{
    jss_api::{
        jss_node_api_client::JssNodeApiClient, start_scheduler_message::Msg,
        start_scheduler_response::Resp, GetTpuConfigRequest, StartSchedulerMessage,
        StartSchedulerResponse, TpuConfigResp,
    },
    jss_types::{HeartBeat, LeaderState, MicroBlock},
};
use solana_sdk::pubkey::Pubkey;
use tokio::time::{interval, timeout};

// Maintains a connection to the JSS Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received
pub struct JssConnection {
    outbound_sender: mpsc::UnboundedSender<StartSchedulerMessage>,
    last_heartbeat: Arc<Mutex<std::time::Instant>>,
    tpu_config: Arc<Mutex<Option<TpuConfigResp>>>,
    microblock_receiver: Receiver<MicroBlock>,
    background_task: tokio::task::JoinHandle<()>,
}

impl JssConnection {
    pub async fn try_init(url: String, pubkey: Pubkey) -> Option<Self> {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url).ok()?;
        let connection_timeout = std::time::Duration::from_secs(5);
        let tpu_config = Arc::new(Mutex::new(None));

        let channel = timeout(connection_timeout, backend_endpoint.connect())
            .await
            .ok()?
            .ok()?;

        let mut validator_client = JssNodeApiClient::new(channel);

        let (outbound_sender, outbound_receiver) = mpsc::unbounded();
        let outbound_stream =
            tonic::Request::new(outbound_receiver.map(|req: StartSchedulerMessage| req));
        let inbound_stream = validator_client
            .start_scheduler_stream(outbound_stream)
            .await
            .ok()?
            .into_inner();

        let last_heartbeat = Arc::new(Mutex::new(std::time::Instant::now()));
        let (microblock_sender, microblock_receiver) = crossbeam_channel::unbounded();
        let background_task = tokio::spawn(Self::background_task(
            inbound_stream,
            outbound_sender.clone(),
            validator_client,
            pubkey,
            last_heartbeat.clone(),
            tpu_config.clone(),
            microblock_sender,
        ));

        Some(Self {
            outbound_sender,
            last_heartbeat,
            tpu_config,
            background_task,
            microblock_receiver,
        })
    }

    async fn background_task(
        mut inbound_stream: tonic::Streaming<StartSchedulerResponse>,
        sender_clone: mpsc::UnboundedSender<StartSchedulerMessage>,
        mut validator_client: JssNodeApiClient<tonic::transport::channel::Channel>,
        pubkey: Pubkey,
        last_heartbeat_clone: Arc<Mutex<std::time::Instant>>,
        tpu_config_clone: Arc<Mutex<Option<TpuConfigResp>>>,
        microblock_sender: crossbeam_channel::Sender<MicroBlock>,
    ) {
        let mut heartbeat_interval = interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let _ = sender_clone.unbounded_send(StartSchedulerMessage {
                        msg: Some(Msg::HeartBeat(HeartBeat {
                            pubkey: pubkey.to_string(),
                        })),
                    });
                    let Ok(resp) = validator_client.get_tpu_config(GetTpuConfigRequest {}).await else {
                        break;
                    };
                    *tpu_config_clone.lock().unwrap() = Some(resp.into_inner());
                }
                inbound = inbound_stream.message() => {
                    let Ok(inbound) = inbound else {
                        break;
                    };
                    let Some(inbound) = inbound else {
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

    pub fn try_recv_microblock(&mut self) -> Option<MicroBlock> {
        self.microblock_receiver.try_recv().ok()
    }

    // Send a signed slot tick to the JSS instance
    pub fn send_leader_state(&mut self, leader_state: LeaderState) {
        let _ = self.outbound_sender.unbounded_send(StartSchedulerMessage {
            msg: Some(Msg::LeaderState(leader_state)),
        });
    }

    // Check if the connection is healthy
    pub fn is_healthy(&mut self) -> bool {
        let is_healthy = self.last_heartbeat.lock().unwrap().elapsed() < std::time::Duration::from_secs(6);

        if !is_healthy {
            info!("dead_heartbeat={}", self.last_heartbeat.lock().unwrap().elapsed().as_secs());
        }

        is_healthy
    }

    pub fn get_tpu_config(&self) -> Option<TpuConfigResp> {
        self.tpu_config.lock().unwrap().clone()
    }
}

impl Drop for JssConnection {
    fn drop(&mut self) {
        self.background_task.abort();
    }
}