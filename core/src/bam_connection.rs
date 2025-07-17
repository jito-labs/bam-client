// Maintains a connection to the BAM Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use {
    crate::bam_dependencies::v0_to_versioned_proto,
    futures::{channel::mpsc, SinkExt, StreamExt},
    jito_protos::proto::{
        bam_api::{
            bam_node_api_client::BamNodeApiClient, start_scheduler_message_v0::Msg,
            start_scheduler_response::VersionedMsg, start_scheduler_response_v0::Resp,
            AuthChallengeRequest, ConfigRequest, ConfigResponse, StartSchedulerMessage,
            StartSchedulerMessageV0, StartSchedulerResponse, StartSchedulerResponseV0,
        },
        bam_types::{AtomicTxnBatch, AuthProof, ValidatorHeartBeat},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_sdk::{signature::Keypair, signer::Signer},
    std::{os::unix::process, sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    }, time::Duration},
    thiserror::Error,
    tokio::time::{interval, timeout}, tokio_metrics::TaskMonitor,
};

pub struct BamConnection {
    config: Arc<Mutex<Option<ConfigResponse>>>,
    background_task: tokio::task::JoinHandle<()>,
    is_healthy: Arc<AtomicBool>,
    url: String,
    exit: Arc<AtomicBool>,
}

impl BamConnection {
    /// Try to initialize a connection to the BAM Node; if it is not possible to connect, it will return an error.
    pub async fn try_init(
        url: String,
        cluster_info: Arc<ClusterInfo>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessageV0>,
    ) -> Result<Self, TryInitError> {
        let mut validator_client = Self::build_client(&url).await?;
        let (outbound_sender, outbound_receiver_internal) = mpsc::channel(1);
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

        let metrics = Arc::new(BamConnectionMetrics::default());
        let is_healthy = Arc::new(AtomicBool::new(true));
        let config = Arc::new(Mutex::new(None));

        let exit = Arc::new(AtomicBool::new(false));
        let background_task = tokio::spawn(Self::connection_task(
            exit.clone(),
            inbound_stream,
            outbound_sender,
            validator_client,
            config.clone(),
            batch_sender,
            cluster_info,
            metrics.clone(),
            is_healthy.clone(),
            outbound_receiver,
            url.clone(),
        ));

        Ok(Self {
            config,
            background_task,
            is_healthy,
            url,
            exit,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn connection_task(
        exit: Arc<AtomicBool>,
        inbound_stream: tonic::Streaming<StartSchedulerResponse>, // GRPC
        mut outbound_sender: mpsc::Sender<StartSchedulerMessage>, // GRPC
        mut validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        config: Arc<Mutex<Option<ConfigResponse>>>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        cluster_info: Arc<ClusterInfo>,
        metrics: Arc<BamConnectionMetrics>,
        is_healthy: Arc<AtomicBool>,
        outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessageV0>,
        url: String,
    ) {
        let last_heartbeat = Arc::new(Mutex::new(std::time::Instant::now()));
        let mut heartbeat_interval = interval(std::time::Duration::from_secs(5));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut metrics_and_health_check_interval = interval(std::time::Duration::from_millis(25));
        metrics_and_health_check_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Create auth proof
        let Some(auth_proof) = Self::prepare_auth_proof(&mut validator_client, cluster_info).await
        else {
            error!("Failed to prepare auth response");
            is_healthy.store(false, Relaxed);
            return;
        };

        // Send it as first message
        let start_message = StartSchedulerMessageV0 {
            msg: Some(Msg::AuthProof(auth_proof)),
        };
        if outbound_sender
            .send(v0_to_versioned_proto(start_message))
            .await
            .inspect_err(|_| {
                error!("Failed to send initial auth proof message");
            })
            .is_err()
        {
            error!("Outbound sender channel closed before sending initial auth proof message");
            return;
        }

        let Ok(config_client) = Self::build_client(&url).await else {
            error!("Failed to build client for config task");
            is_healthy.store(false, Relaxed);
            return;
        };
        let builder_config_task = tokio::spawn(Self::refresh_config_task(
            exit.clone(),
            config.clone(),
            config_client,
            metrics.clone(),
        ));

        let exit_clone = exit.clone();
        let outbound_sender_clone = outbound_sender.clone();
        let metrics_clone = metrics.clone();
        let outbound_forwarder_task = std::thread::Builder::new()
            .name("bam-outbound-forwarder".to_string())
            .spawn(move || {
                Self::outbound_forwarder_task(
                    exit_clone,
                    outbound_receiver,
                    outbound_sender_clone,
                    metrics_clone,
                )
            })
            .expect("Failed to spawn outbound forwarder thread");

        let inbound_task_monitor = TaskMonitor::new();

        let inbound_forwarder_task = tokio::spawn(inbound_task_monitor.instrument(Self::inbound_forwarder_task(
            exit.clone(),
            inbound_stream,
            batch_sender,
            metrics.clone(),
            last_heartbeat.clone(),
        )));
        let mut inbound_task_metrics_interval = inbound_task_monitor.intervals();
        while !exit.load(Relaxed) {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let _ = outbound_sender.try_send(v0_to_versioned_proto(StartSchedulerMessageV0 {
                        msg: Some(Msg::HeartBeat(ValidatorHeartBeat {})),
                    }));
                    metrics.heartbeat_sent.fetch_add(1, Relaxed);
                }
                _ = metrics_and_health_check_interval.tick() => {
                    const TIMEOUT_DURATION: std::time::Duration = std::time::Duration::from_secs(6);
                    let is_healthy_now = last_heartbeat.lock().unwrap().elapsed() < TIMEOUT_DURATION;
                    is_healthy.store(is_healthy_now, Relaxed);
                    if !is_healthy_now {
                        metrics
                            .unhealthy_connection_count
                            .fetch_add(1, Relaxed);
                    }

                    // Report inbound_task metrics
                    let inbound_task_metrics = inbound_task_metrics_interval.next().unwrap_or_default();
                    Self::report_task_metrics(
                        "bam-inbound-forwarder",
                        inbound_task_metrics,
                    );

                    metrics.report();
                }
            }
        }
        is_healthy.store(false, Relaxed);
        let _ = builder_config_task.await.ok();
        let _ = outbound_forwarder_task.join();
        let _ = inbound_forwarder_task.await.ok();
    }

    async fn refresh_config_task(
        exit: Arc<AtomicBool>,
        config: Arc<Mutex<Option<ConfigResponse>>>,
        mut validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        metrics: Arc<BamConnectionMetrics>,
    ) {
        let mut interval = interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while !exit.load(Relaxed) {
            tokio::select! {
                _ = interval.tick() => {
                    let request = tonic::Request::new(ConfigRequest {});
                    match validator_client.get_builder_config(request).await {
                        Ok(response) => {
                            let resp_config = response.into_inner();
                            *config.lock().unwrap() = Some(resp_config);
                            metrics.builder_config_received.fetch_add(1, Relaxed);
                        }
                        Err(e) => {
                            error!("Failed to get config: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    fn report_task_metrics(task_name: &str, interval: tokio_metrics::TaskMetrics) {
        datapoint_info!(
            "tokio_task_metrics",
            "task_name" => task_name,
            ("instrumented_count", interval.instrumented_count, i64),
            ("dropped_count", interval.dropped_count, i64),
            ("first_poll_count", interval.first_poll_count, i64),
            ("total_poll_count", interval.total_poll_count, i64),
            ("total_idled_count", interval.total_idled_count, i64),
            ("total_scheduled_count", interval.total_scheduled_count, i64),
            ("total_fast_poll_count", interval.total_fast_poll_count, i64),
            ("total_slow_poll_count", interval.total_slow_poll_count, i64),
            (
                "total_short_delay_count",
                interval.total_short_delay_count,
                i64
            ),
            (
                "total_long_delay_count",
                interval.total_long_delay_count,
                i64
            ),
            (
                "total_first_poll_delay_us",
                interval.total_first_poll_delay.as_micros(),
                i64
            ),
            (
                "total_idle_duration_us",
                interval.total_idle_duration.as_micros(),
                i64
            ),
            (
                "total_scheduled_duration_us",
                interval.total_scheduled_duration.as_micros(),
                i64
            ),
            (
                "total_poll_duration_us",
                interval.total_poll_duration.as_micros(),
                i64
            ),
            (
                "total_fast_poll_duration_us",
                interval.total_fast_poll_duration.as_micros(),
                i64
            ),
            (
                "total_slow_poll_duration_us",
                interval.total_slow_poll_duration.as_micros(),
                i64
            ),
            (
                "total_short_delay_duration_us",
                interval.total_short_delay_duration.as_micros(),
                i64
            ),
            (
                "total_long_delay_duration_us",
                interval.total_long_delay_duration.as_micros(),
                i64
            ),
            (
                "mean_first_poll_delay_us",
                interval.mean_first_poll_delay().as_micros(),
                i64
            ),
            (
                "mean_idle_duration_us",
                interval.mean_idle_duration().as_micros(),
                i64
            ),
            (
                "mean_scheduled_duration_us",
                interval.mean_scheduled_duration().as_micros(),
                i64
            ),
            (
                "mean_poll_duration_us",
                interval.mean_poll_duration().as_micros(),
                i64
            ),
            (
                "mean_fast_poll_duration_us",
                interval.mean_fast_poll_duration().as_micros(),
                i64
            ),
            (
                "mean_slow_poll_duration_us",
                interval.mean_slow_poll_duration().as_micros(),
                i64
            ),
            (
                "mean_short_delay_duration_us",
                interval.mean_short_delay_duration().as_micros(),
                i64
            ),
            (
                "mean_long_delay_duration_us",
                interval.mean_long_delay_duration().as_micros(),
                i64
            ),
            ("slow_poll_ratio", if interval.slow_poll_ratio().is_nan() { 0.0 } else { interval.slow_poll_ratio() }, f64),
            ("long_delay_ratio", if interval.long_delay_ratio().is_nan() { 0.0 } else { interval.long_delay_ratio() }, f64),
        );
    }

    fn outbound_forwarder_task(
        exit: Arc<AtomicBool>,
        outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessageV0>,
        mut outbound_sender: mpsc::Sender<StartSchedulerMessage>,
        metrics: Arc<BamConnectionMetrics>,
    ) {
        while !exit.load(Relaxed) {
            let Ok(outbound) = outbound_receiver.recv_timeout(std::time::Duration::from_millis(10))
            else {
                continue;
            };
            match outbound.msg.as_ref() {
                Some(Msg::LeaderState(_)) => {
                    metrics.leaderstate_sent.fetch_add(1, Relaxed);
                }
                Some(Msg::AtomicTxnBatchResult(_)) => {
                    metrics.bundleresult_sent.fetch_add(1, Relaxed);
                }
                _ => {}
            }
            let _ = outbound_sender
                .send(v0_to_versioned_proto(outbound));
            metrics.outbound_sent.fetch_add(1, Relaxed);
        }
    }

    async fn inbound_forwarder_task(
        exit: Arc<AtomicBool>,
        mut inbound_stream: tonic::Streaming<StartSchedulerResponse>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        metrics: Arc<BamConnectionMetrics>,
        last_heartbeat: Arc<Mutex<std::time::Instant>>,
    ) {
        while !exit.load(Relaxed) {
            let start = std::time::Instant::now();

            match inbound_stream.message().await {
                Ok(Some(msg)) => {
                    let processing_start = std::time::Instant::now();
                    let polling_duration = processing_start.duration_since(start);
                    info!("Received inbound message in {:?}", polling_duration);

                    let Some(VersionedMsg::V0(inbound)) = msg.versioned_msg else {
                        error!("Received unsupported versioned message: {:?}", msg);
                        continue;
                    };

                    match inbound {
                        StartSchedulerResponseV0 { resp: Some(Resp::HeartBeat(_)), .. } => {
                            *last_heartbeat.lock().unwrap() = std::time::Instant::now();
                            metrics.heartbeat_received.fetch_add(1, Relaxed);
                        }
                        StartSchedulerResponseV0 { resp: Some(Resp::AtomicTxnBatch(batch)), .. } => {
                            let _ = batch_sender.try_send(batch).inspect_err(|_| {
                                error!("Failed to send bundle to receiver");
                            });
                            metrics.bundle_received.fetch_add(1, Relaxed);
                        }
                        _ => {}
                    }
                    let processing_duration = processing_start.elapsed();
                    info!("Processed inbound message in {:?}", processing_duration);
                    tokio::time::sleep(Duration::from_micros(1)).await; // Yield to allow other tasks to run
                }
                Ok(None) => {
                    info!("Inbound stream closed");
                    break;
                }
                Err(e) => {
                    error!("Failed to receive message from inbound stream: {:?}", e);
                    break;
                }
            }
        }
        exit.store(true, Relaxed);
    }

    fn sign_message(keypair: &Keypair, message: &[u8]) -> Option<String> {
        let slot_signature = keypair.try_sign_message(message).ok()?;
        let slot_signature = slot_signature.to_string();
        Some(slot_signature)
    }

    pub fn is_healthy(&mut self) -> bool {
        self.is_healthy.load(Relaxed)
    }

    pub fn get_latest_config(&self) -> Option<ConfigResponse> {
        self.config.lock().unwrap().clone()
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    async fn prepare_auth_proof(
        validator_client: &mut BamNodeApiClient<tonic::transport::channel::Channel>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Option<AuthProof> {
        let request = tonic::Request::new(AuthChallengeRequest {});
        let Ok(resp) = validator_client.get_auth_challenge(request).await else {
            error!("Failed to get auth challenge");
            return None;
        };

        let resp = resp.into_inner();
        let challenge_to_sign = resp.challenge_to_sign;
        let challenge_bytes = challenge_to_sign.as_bytes();

        let signature = Self::sign_message(cluster_info.keypair().as_ref(), challenge_bytes)?;

        Some(AuthProof {
            challenge_to_sign,
            validator_pubkey: cluster_info.keypair().pubkey().to_string(),
            signature,
        })
    }

    async fn build_client(
        url: &str,
    ) -> Result<BamNodeApiClient<tonic::transport::channel::Channel>, TryInitError> {
        let endpoint = tonic::transport::Endpoint::from_shared(url.to_string())
            .map_err(TryInitError::EndpointConnectError)?
            .http2_keep_alive_interval(Duration::from_secs(20))
            .http2_adaptive_window(true)
            .initial_stream_window_size(Some(1024 * 1024))
            .tcp_nodelay(true)
            .initial_connection_window_size(Some(10 * 1024 * 1024));
        let channel = timeout(std::time::Duration::from_secs(5), endpoint.connect())
            .await
            .map_err(TryInitError::ConnectionTimeout)??;
        Ok(BamNodeApiClient::new(channel))
    }
}

impl Drop for BamConnection {
    fn drop(&mut self) {
        self.is_healthy.store(false, Relaxed);
        self.exit.store(true, Relaxed);
        std::thread::sleep(std::time::Duration::from_millis(10));
        self.background_task.abort();
    }
}

#[derive(Default)]
struct BamConnectionMetrics {
    bundle_received: AtomicU64,
    heartbeat_received: AtomicU64,
    builder_config_received: AtomicU64,

    unhealthy_connection_count: AtomicU64,

    leaderstate_sent: AtomicU64,
    bundleresult_sent: AtomicU64,
    heartbeat_sent: AtomicU64,
    outbound_sent: AtomicU64,
}

impl BamConnectionMetrics {
    pub fn report(&self) {
        datapoint_info!(
            "bam_connection-metrics",
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
