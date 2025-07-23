// Maintains a connection to the BAM Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use {
    crate::bam_dependencies::v0_to_versioned_proto, futures::{channel::mpsc, SinkExt, StreamExt}, jito_protos::proto::{
        bam_api::{
            bam_node_api_client::BamNodeApiClient, start_scheduler_message_v0::Msg,
            start_scheduler_response::VersionedMsg, start_scheduler_response_v0::Resp,
            AuthChallengeRequest, ConfigRequest, ConfigResponse, StartSchedulerMessage,
            StartSchedulerMessageV0, StartSchedulerResponse, StartSchedulerResponseV0,
        },
        bam_types::{AtomicTxnBatch, AuthProof, ValidatorHeartBeat},
    }, prost::Message, solana_gossip::cluster_info::ClusterInfo, solana_sdk::{signature::Keypair, signer::Signer}, std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    }, thiserror::Error, tokio::time::{interval, timeout}, tokio_tungstenite::connect_async,
};

pub struct BamConnection {
    websocket_thread: tokio::task::JoinHandle<()>,
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
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url.clone())?;
        let connection_timeout = std::time::Duration::from_secs(5);

        let channel = timeout(connection_timeout, backend_endpoint.connect()).await??;

        let validator_client = BamNodeApiClient::new(channel);

        let (outbound_sender, outbound_receiver_internal) = mpsc::channel(100_000);
        let (inbound_sender, inbound_receiver) = mpsc::channel(100_000);
        let inbound_stream = inbound_receiver;

        let exit = Arc::new(AtomicBool::new(false));

        let exit_clone = exit.clone();
        let url_clone = url.clone();
        let websocket_thread = tokio::spawn(Self::run_websocket(
                exit_clone,
                url_clone,
                outbound_receiver_internal,
                inbound_sender,
            )
        );


        let metrics = Arc::new(BamConnectionMetrics::default());
        let is_healthy = Arc::new(AtomicBool::new(true));
        let config = Arc::new(Mutex::new(None));

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
        ));

        Ok(Self {
            websocket_thread,
            config,
            background_task,
            is_healthy,
            url,
            exit,
        })
    }

    async fn run_websocket(
        exit: Arc<AtomicBool>,
        url: String,
        mut outbound_receiver: mpsc::Receiver<StartSchedulerMessage>,
        mut inbound_sender: mpsc::Sender<StartSchedulerResponse>,
    ) {
        // Add one to the port
        let url = if let Some(port) = url.split(':').last() {
            if let Ok(port_num) = port.parse::<u16>() {
                let new_port = port_num + 1;
                url.replace(&format!(":{}", port_num), &format!(":{}", new_port))

            } else {
                panic!("Invalid port number in URL");
            }
        } else {
            panic!("Invalid URL format, missing port");
        };

        // Replace http with ws
        let url = url.replace("http://", "ws://").replace("https://", "wss://");

         // Connect to a WebSocket server
        let (ws_stream, _) = connect_async(url.as_str()).await.expect("Failed to connect to WebSocket");
        let (mut write, mut read) = ws_stream.split();

        info!("WEBSOCK connected at {}", url);

        // IGRESS
        let exit_clone = exit.clone();
        tokio::spawn(async move {
            while !exit_clone.load(Relaxed) {
                if let Some(outbound) = outbound_receiver.next().await {
                    let msg = outbound.encode_to_vec();
                    if let Err(_) = write.send(tokio_tungstenite::tungstenite::Message::Binary(msg)).await {
                        break;
                    }
                }
            }
        });

        // INGEST
        let exit_clone = exit.clone();
        tokio::spawn(async move {
            while !exit_clone.load(Relaxed) {
                let Some(Ok(message)) = read.next().await else {
                    break;
                };
                let tokio_tungstenite::tungstenite::Message::Binary(data) = message else {
                    continue;
                };

                let Ok(msg) = StartSchedulerResponse::decode(data.as_slice()) else {
                        continue;
                    };
                let _ = inbound_sender.try_send(msg);
            }
        });

        info!("WEBSOCK WebSocket thread exiting");

    }

    #[allow(clippy::too_many_arguments)]
    async fn connection_task(
        exit: Arc<AtomicBool>,
        mut inbound_stream: mpsc::Receiver<StartSchedulerResponse>,
        mut outbound_sender: mpsc::Sender<StartSchedulerMessage>,
        mut validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        config: Arc<Mutex<Option<ConfigResponse>>>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        cluster_info: Arc<ClusterInfo>,
        metrics: Arc<BamConnectionMetrics>,
        is_healthy: Arc<AtomicBool>,
        outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessageV0>,
    ) {
        let mut last_heartbeat = std::time::Instant::now();
        let mut heartbeat_interval = interval(std::time::Duration::from_secs(5));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut metrics_and_health_check_interval = interval(std::time::Duration::from_secs(1));
        metrics_and_health_check_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut outbound_tick_interval = interval(std::time::Duration::from_millis(1));
        outbound_tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

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

        let builder_config_task = tokio::spawn(Self::refresh_config_task(
            exit.clone(),
            config.clone(),
            validator_client.clone(),
            metrics.clone(),
        ));
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
                    let is_healthy_now = last_heartbeat.elapsed() < TIMEOUT_DURATION;
                    is_healthy.store(is_healthy_now, Relaxed);
                    if !is_healthy_now {
                        metrics
                            .unhealthy_connection_count
                            .fetch_add(1, Relaxed);
                    }

                    metrics.report();
                }
                inbound = inbound_stream.next() => {
                    let Some(inbound) = inbound else {
                        error!("Inbound stream closed unexpectedly");
                        is_healthy.store(false, Relaxed);
                        break;
                    };

                    let Some(VersionedMsg::V0(inbound)) = inbound.versioned_msg else {
                        error!("Received unsupported versioned message: {:?}", inbound);
                        break;
                    };

                    match inbound {
                        StartSchedulerResponseV0 { resp: Some(Resp::HeartBeat(_)), .. } => {
                            last_heartbeat = std::time::Instant::now();
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
                }
                _ = outbound_tick_interval.tick() => {
                    while let Ok(outbound) = outbound_receiver.try_recv() {
                        match outbound.msg.as_ref() {
                            Some(Msg::LeaderState(_)) => {
                                metrics.leaderstate_sent.fetch_add(1, Relaxed);
                            }
                            Some(Msg::AtomicTxnBatchResult(_)) => {
                                metrics.bundleresult_sent.fetch_add(1, Relaxed);
                            }
                            _ => {}
                        }
                        let _ = outbound_sender.try_send(v0_to_versioned_proto(outbound)).inspect_err(|_| {
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
}

impl Drop for BamConnection {
    fn drop(&mut self) {
        self.is_healthy.store(false, Relaxed);
        self.exit.store(true, Relaxed);
        std::thread::sleep(std::time::Duration::from_millis(10));
        self.background_task.abort();
        self.websocket_thread.abort();
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
