use futures::{channel::mpsc, FutureExt, StreamExt};
use jito_protos::proto::{jds_api::{jss_node_api_client::JssNodeApiClient, start_scheduler_message::Msg, start_scheduler_response::Resp, StartSchedulerMessage}, jds_types::{ExecutionPreConfirmation, MicroBlock, SignedSlotTick}};
use tokio::time::timeout;

// Maintains a connection to the JSS Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received
pub struct JssConnection {
    inbound_stream: tonic::Streaming<jito_protos::proto::jds_api::StartSchedulerResponse>,
    outbound_sender: mpsc::UnboundedSender<StartSchedulerMessage>,
    its_over: bool,
    last_heartbeat: Option<std::time::Instant>,
}

impl JssConnection {
    pub async fn try_init(url: String) -> Option<Self> {
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

    pub fn try_recv(&mut self) -> Option<MicroBlock> {
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

    pub async fn recv_with_timeout(&mut self, timeout_duration: std::time::Duration) -> Option<MicroBlock> {
        let next = timeout(timeout_duration, self.inbound_stream.next()).await.ok()?;
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

    // Send a signed slot tick to the JSS instance
    pub fn send_signed_tick(&mut self, _signed_slot_tick: SignedSlotTick) {
        let _ = self.outbound_sender.unbounded_send(StartSchedulerMessage {
            msg: Some(Msg::SchedulerTick(todo!())),
        });
    }

    // Send a bundle execution confirmation to the JSS instance
    pub fn send_bundle_execution_confirmation(&mut self, msg: ExecutionPreConfirmation) {
        let _ = self.outbound_sender.unbounded_send(StartSchedulerMessage {
            msg: Some(Msg::ExecutionPreConfirmation(msg)),
        });
    }

    // Check if the connection is healthy
    pub fn is_healthy(&mut self) -> bool {
        // Will update last_heartbeat if a new one is received
        let _ = self.try_recv();

        !self.its_over
            && self.last_heartbeat.map_or(true, 
                |heartbeat| heartbeat.elapsed().as_secs() < 5)
    }
}