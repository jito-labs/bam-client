// Maintains a connection to the JSS Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use {
    futures::{channel::mpsc, StreamExt},
    jito_protos::proto::{
        jss_api::{
            jss_node_api_client::JssNodeApiClient, start_scheduler_message::Msg,
            start_scheduler_response::Resp, BuilderConfigResp, GetBuilderConfigRequest,
            StartSchedulerMessage, StartSchedulerResponse,
        },
        jss_types::{Bundle, ValidatorHeartBeat},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{signature::Keypair, signer::Signer},
    std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc, Mutex, RwLock,
    },
    thiserror::Error,
    tokio::time::{interval, timeout},
};

const TICKS_PER_SLOT: u64 = 64;

pub struct JssConnection {
    builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
    background_task: tokio::task::JoinHandle<()>,
    is_healthy: Arc<AtomicBool>,
    url: String,
    exit: Arc<AtomicBool>,
}

impl JssConnection {
    /// Try to initialize a connection to the JSS Node; if it is not possible to connect, it will return an error.
    pub async fn try_init(
        url: String,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        bundle_sender: crossbeam_channel::Sender<Bundle>,
        outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessage>,
    ) -> Result<Self, TryInitError> {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url.clone())?;
        let connection_timeout = std::time::Duration::from_secs(5);

        let channel = timeout(connection_timeout, backend_endpoint.connect()).await??;

        let mut validator_client = JssNodeApiClient::new(channel);

        let (outbound_sender, outbound_receiver_internal) = mpsc::channel(100_000);
        let outbound_stream =
            tonic::Request::new(outbound_receiver_internal.map(|req: StartSchedulerMessage| req));
        let inbound_stream = validator_client
            .start_scheduler_stream(outbound_stream)
            .await
            .map_err(|e| {
                error!("Failed to start scheduler stream: {:?}", e);
                TryInitError::StreamStartError(e)
            })?
            .into_inner();

        let metrics = Arc::new(JssConnectionMetrics::default());
        let is_healthy = Arc::new(AtomicBool::new(true));
        let builder_config = Arc::new(Mutex::new(None));

        let exit = Arc::new(AtomicBool::new(false));
        let background_task = tokio::spawn(Self::connection_task(
            exit.clone(),
            inbound_stream,
            outbound_sender,
            validator_client,
            builder_config.clone(),
            bundle_sender,
            poh_recorder,
            cluster_info,
            metrics.clone(),
            is_healthy.clone(),
            outbound_receiver,
        ));

        Ok(Self {
            builder_config,
            background_task,
            is_healthy,
            url,
            exit,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn connection_task(
        exit: Arc<AtomicBool>,
        mut inbound_stream: tonic::Streaming<StartSchedulerResponse>,
        mut outbound_sender: mpsc::Sender<StartSchedulerMessage>,
        validator_client: JssNodeApiClient<tonic::transport::channel::Channel>,
        builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
        bundle_sender: crossbeam_channel::Sender<Bundle>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        metrics: Arc<JssConnectionMetrics>,
        is_healthy: Arc<AtomicBool>,
        outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessage>,
    ) {
        let mut last_heartbeat = std::time::Instant::now();
        let mut heartbeat_interval = interval(std::time::Duration::from_secs(5));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut metrics_and_health_check_interval = interval(std::time::Duration::from_secs(1));
        metrics_and_health_check_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut outbound_tick_interval = interval(std::time::Duration::from_millis(1));
        outbound_tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

        let builder_config_task = tokio::spawn(Self::refresh_builder_config_task(
            exit.clone(),
            builder_config.clone(),
            validator_client.clone(),
            metrics.clone(),
        ));
        while !exit.load(Relaxed) {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let Some(signed_heartbeat) = Self::create_signed_heartbeat(
                        poh_recorder.read().unwrap().get_current_slot(),
                        cluster_info.as_ref()) else
                    {
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
                _ = outbound_tick_interval.tick() => {
                    while let Ok(outbound) = outbound_receiver.try_recv() {
                        match outbound.msg.as_ref() {
                            Some(Msg::LeaderState(_)) => {
                                metrics.leaderstate_sent.fetch_add(1, Relaxed);
                            }
                            Some(Msg::BundleResult(_)) => {
                                metrics.bundleresult_sent.fetch_add(1, Relaxed);
                            }
                            _ => {}
                        }
                        let _ = outbound_sender.try_send(outbound).inspect_err(|_| {
                            error!("Failed to send outbound message");
                        });
                        metrics.outbound_sent.fetch_add(1, Relaxed);
                    }
                }
            }
        }
        is_healthy.store(false, Relaxed);
        let _ = builder_config_task.await.ok();
    }

    async fn refresh_builder_config_task(
        exit: Arc<AtomicBool>,
        builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
        mut validator_client: JssNodeApiClient<tonic::transport::channel::Channel>,
        metrics: Arc<JssConnectionMetrics>,
    ) {
        let mut interval = interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while !exit.load(Relaxed) {
            tokio::select! {
                _ = interval.tick() => {
                    let request = tonic::Request::new(GetBuilderConfigRequest {});
                    match validator_client.get_builder_config(request).await {
                        Ok(response) => {
                            let config = response.into_inner();
                            *builder_config.lock().unwrap() = Some(config);
                            metrics.builder_config_received.fetch_add(1, Relaxed);
                        }
                        Err(e) => {
                            error!("Failed to get builder config: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    /// Create a signed heartbeat message for the given slot.
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

    pub fn is_healthy(&mut self) -> bool {
        self.is_healthy.load(Relaxed)
    }

    pub fn get_builder_config(&self) -> Option<BuilderConfigResp> {
        self.builder_config.lock().unwrap().clone()
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for JssConnection {
    fn drop(&mut self) {
        self.is_healthy.store(false, Relaxed);
        self.exit.store(true, Relaxed);
        std::thread::sleep(std::time::Duration::from_millis(10));
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
    bundleresult_sent: AtomicU64,
    heartbeat_sent: AtomicU64,
    outbound_sent: AtomicU64,
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
                "bundleresult_sent",
                self.bundleresult_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "heartbeat_sent",
                self.heartbeat_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "outbound_sent",
                self.outbound_sent.swap(0, Relaxed) as i64,
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
