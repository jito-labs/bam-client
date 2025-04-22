// Maintains a connection to the JSS Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use std::sync::{
    atomic::{AtomicBool, AtomicU64},
    Arc, Mutex, RwLock,
};

use crossbeam_channel::Receiver;
use futures::{channel::mpsc, StreamExt};
use jito_protos::proto::{
    jss_api::{
        jss_node_api_client::JssNodeApiClient, start_scheduler_message::Msg,
        start_scheduler_response::Resp, BuilderConfigResp, GetBuilderConfigRequest,
        StartSchedulerMessage, StartSchedulerResponse,
    },
    jss_types::{Bundle, LeaderState, ValidatorHeartBeat},
};
use solana_gossip::cluster_info::ClusterInfo;
use solana_poh::poh_recorder::PohRecorder;
use solana_sdk::{signature::Keypair, signer::Signer};
use std::sync::atomic::Ordering::Relaxed;
use thiserror::Error;
use tokio::time::{interval, timeout};

const TICKS_PER_SLOT: u64 = 64;

pub struct JssConnection {
    outbound_sender: mpsc::Sender<StartSchedulerMessage>,
    builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
    bundle_receiver: Receiver<Bundle>,
    background_task: tokio::task::JoinHandle<()>,
    metrics: Arc<JssConnectionMetrics>,
    is_healthy: Arc<AtomicBool>,
}

impl JssConnection {
    pub async fn try_init(
        url: String,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Result<Self, TryInitError> {
        // If a leader slot is coming up; we don't want to enable jss
        if poh_recorder.read().unwrap().would_be_leader(TICKS_PER_SLOT) {
            return Err(TryInitError::MidLeaderSlotError);
        };

        let backend_endpoint = tonic::transport::Endpoint::from_shared(url)?;
        let connection_timeout = std::time::Duration::from_secs(5);
        let builder_config = Arc::new(Mutex::new(None));

        let channel = timeout(connection_timeout, backend_endpoint.connect()).await??;

        let mut validator_client = JssNodeApiClient::new(channel);

        let (outbound_sender, outbound_receiver) = mpsc::channel(100_000);
        let outbound_stream =
            tonic::Request::new(outbound_receiver.map(|req: StartSchedulerMessage| req));
        let inbound_stream = validator_client
            .start_scheduler_stream(outbound_stream)
            .await?
            .into_inner();

        let metrics = Arc::new(JssConnectionMetrics::default());
        let is_healthy = Arc::new(AtomicBool::new(false));

        let (bundle_sender, bundle_receiver) = crossbeam_channel::bounded(100_000);
        let background_task = tokio::spawn(Self::background_task(
            inbound_stream,
            outbound_sender.clone(),
            validator_client,
            builder_config.clone(),
            bundle_sender,
            poh_recorder,
            cluster_info,
            metrics.clone(),
            is_healthy.clone(),
        ));

        Ok(Self {
            outbound_sender,
            builder_config,
            background_task,
            bundle_receiver,
            metrics,
            is_healthy,
        })
    }

    async fn background_task(
        mut inbound_stream: tonic::Streaming<StartSchedulerResponse>,
        mut outbound_sender: mpsc::Sender<StartSchedulerMessage>,
        mut validator_client: JssNodeApiClient<tonic::transport::channel::Channel>,
        builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
        bundle_sender: crossbeam_channel::Sender<Bundle>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        metrics: Arc<JssConnectionMetrics>,
        is_healthy: Arc<AtomicBool>,
    ) {
        let mut last_heartbeat = std::time::Instant::now();
        let mut heartbeat_interval = interval(std::time::Duration::from_secs(5));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut metrics_and_health_check_interval = interval(std::time::Duration::from_secs(1));
        metrics_and_health_check_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let Some(signed_heartbeat) = Self::create_signed_heartbeat(poh_recorder.read().unwrap().get_current_slot(), cluster_info.as_ref()) else {
                        error!("Failed to create signed heartbeat");
                        break;
                    };
                    let _ = outbound_sender.try_send(StartSchedulerMessage {
                        msg: Some(Msg::HeartBeat(signed_heartbeat)),
                    });
                    metrics.heartbeat_sent.fetch_add(1, Relaxed);

                    // If a leader slot is coming up; we don't want to block the worker
                    if poh_recorder.read().unwrap().would_be_leader(TICKS_PER_SLOT) {
                        continue;
                    };

                    let Ok(resp) = validator_client.get_builder_config(GetBuilderConfigRequest {}).await else {
                        break;
                    };
                    *builder_config.lock().unwrap() = Some(resp.into_inner());
                    metrics.builder_config_received.fetch_add(1, Relaxed);
                }
                _ = metrics_and_health_check_interval.tick() => {
                    const TIMEOUT_DURATION: std::time::Duration = std::time::Duration::from_secs(6);
                    let is_healthy_now = last_heartbeat.elapsed() < TIMEOUT_DURATION;
                    is_healthy.store(is_healthy_now, Relaxed);
                    if !is_healthy_now {
                        metrics
                            .unhealthy_connection_count
                            .fetch_add(1, Relaxed);
                    }

                    metrics.report();
                }
                inbound = inbound_stream.message() => {
                    let Ok(Some(inbound)) = inbound else {
                        error!("Failed to receive message from inbound stream");
                        is_healthy.store(false, Relaxed);
                        break;
                    };

                    match inbound {
                        StartSchedulerResponse { resp: Some(Resp::HeartBeat(_)), .. } => {
                            last_heartbeat = std::time::Instant::now();
                            metrics.heartbeat_received.fetch_add(1, Relaxed);
                        }
                        StartSchedulerResponse { resp: Some(Resp::Bundle(bundle)), .. } => {
                            let _ = bundle_sender.try_send(bundle).inspect_err(|_| {
                                error!("Failed to send bundle to receiver");
                            });
                            metrics.bundle_received.fetch_add(1, Relaxed);
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

    pub fn try_recv_bundle(&mut self) -> Option<Bundle> {
        self.metrics.bundle_polls.fetch_add(1, Relaxed);
        self.bundle_receiver.try_recv().ok()
    }

    pub fn drain_bundles(&mut self) {
        while let Ok(_) = self.bundle_receiver.try_recv() {
            self.metrics.bundle_received.fetch_add(1, Relaxed);
        }
    }

    pub fn send_leader_state(&mut self, leader_state: LeaderState) {
        let _ = self.outbound_sender.try_send(StartSchedulerMessage {
            msg: Some(Msg::LeaderState(leader_state)),
        });
        self.metrics.leaderstate_sent.fetch_add(1, Relaxed);
    }

    pub fn is_healthy(&mut self) -> bool {
        self.is_healthy.load(Relaxed)
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

#[derive(Default)]
struct JssConnectionMetrics {
    bundle_received: AtomicU64,
    heartbeat_received: AtomicU64,
    builder_config_received: AtomicU64,

    unhealthy_connection_count: AtomicU64,

    leaderstate_sent: AtomicU64,
    heartbeat_sent: AtomicU64,
    bundle_polls: AtomicU64,
}

impl JssConnectionMetrics {
    pub fn report(&self) {
        datapoint_info!(
            "jss_connection-metrics",
            (
                "bundle_received",
                self.bundle_received.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "heartbeat_received",
                self.heartbeat_received.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "builder_config_received",
                self.builder_config_received.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "unhealthy_connection_count",
                self.unhealthy_connection_count.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "leaderstate_sent",
                self.leaderstate_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "heartbeat_sent",
                self.heartbeat_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "bundle_polls",
                self.bundle_polls.swap(0, Relaxed) as i64,
                i64
            ),
        );
    }
}

#[derive(Error, Debug)]
pub enum TryInitError {
    #[error("In leader slot")]
    MidLeaderSlotError,
    #[error("Invalid URI")]
    EndpointConnectError(#[from] tonic::transport::Error),
    #[error("Connection timeout")]
    ConnectionTimeout(#[from] tokio::time::error::Elapsed),
    #[error("Stream start error")]
    StreamStartError(#[from] tonic::Status),
}
