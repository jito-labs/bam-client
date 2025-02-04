use futures::{channel::mpsc, FutureExt, StreamExt, TryStreamExt};
use jito_protos::proto::{
    jss_api::{
        jss_node_api_client::JssNodeApiClient, start_scheduler_message::Msg,
        start_scheduler_response::Resp, GetTpuConfigRequest, StartSchedulerMessage,
        StartSchedulerResponse, TpuConfigResp,
    },
    jss_types::{HeartBeat, LeaderState, MicroBlock},
};
use solana_sdk::pubkey::Pubkey;
use tokio::time::timeout;

// Maintains a connection to the JSS Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received
pub struct JssConnection {
    validator_client: JssNodeApiClient<tonic::transport::Channel>,
    inbound_stream: tonic::Streaming<StartSchedulerResponse>,
    outbound_sender: mpsc::UnboundedSender<StartSchedulerMessage>,
    its_over: bool,
    last_heartbeat: Option<std::time::Instant>,

    last_tpu_update: std::time::Instant,
    tpu_config: Option<TpuConfigResp>,
    
    heartbeat_task: tokio::task::JoinHandle<()>,
}

impl JssConnection {
    pub async fn try_init(url: String, pubkey: Pubkey) -> Option<Self> {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url).ok()?;
        let connection_timeout = std::time::Duration::from_secs(5);

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

        let sender_clone = outbound_sender.clone();
        let heartbeat_task = tokio::spawn(async move {
            loop {
                let _ = sender_clone
                    .unbounded_send(StartSchedulerMessage {
                        msg: Some(Msg::HeartBeat(HeartBeat{
                            pubkey: pubkey.to_string(),
                        })),
                    });
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });

        Some(Self {
            validator_client,
            inbound_stream,
            outbound_sender,
            its_over: false,
            last_heartbeat: None,
            last_tpu_update: std::time::Instant::now(),
            tpu_config: None,
            heartbeat_task,
        })
    }

    pub fn try_recv_microblock(&mut self) -> Option<MicroBlock> {
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
            None => None,
        }
    }

    pub async fn recv_microblock_with_timeout(
        &mut self,
        timeout_duration: std::time::Duration,
    ) -> Option<MicroBlock> {
        let next = timeout(timeout_duration, self.inbound_stream.next())
            .await
            .ok()?;
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
            None => None,
        }
    }

    // Send a signed slot tick to the JSS instance
    pub fn send_leader_state(&mut self, leader_state: LeaderState) {
        let _ = self.outbound_sender.unbounded_send(StartSchedulerMessage {
            msg: Some(Msg::LeaderState(leader_state)),
        });
    }

    // Check if the connection is healthy
    pub async fn is_healthy(&mut self) -> bool {
        // Will update last_heartbeat if a new one is received
        self.housekeeping().await;

        !self.its_over
            && self
                .last_heartbeat
                .map_or(true, |heartbeat| heartbeat.elapsed().as_secs() < 10)
    }

    async fn housekeeping(&mut self) {
        // Update TPU config
        let now = std::time::Instant::now();
        if now.duration_since(self.last_tpu_update).as_secs() > 5 {
            if let Ok(tpu_config_response) = self
                .validator_client
                .get_tpu_config(GetTpuConfigRequest {})
                .await
            {
                info!("Received TPU config");
                self.tpu_config = Some(tpu_config_response.into_inner());
            }
            self.last_tpu_update = now;
        }

        // Pickup heartbeats and drain stream
        while let Some(Ok(Some(msg))) = self.inbound_stream.try_next().now_or_never() {
            // Update last_heartbeat if a new one is received
            if let Some(Resp::HeartBeat(_)) = msg.resp {
                self.last_heartbeat = Some(std::time::Instant::now());
            }
        }
    }

    pub fn get_tpu_config(&self) -> Option<TpuConfigResp> {
        self.tpu_config.clone()
    }
}
