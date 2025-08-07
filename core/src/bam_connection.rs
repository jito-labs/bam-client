// Maintains a connection to the BAM Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use {
    crate::bam_dependencies::v0_to_versioned_proto,
    h3_util::quinn::H3QuinnConnector,
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
    solana_keypair::Keypair,
    solana_signer::Signer,
    std::{
        io,
        io::ErrorKind,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
            Arc, Mutex,
        },
    },
    thiserror::Error,
    tokio::{sync::mpsc, time::interval},
    tokio_stream::wrappers::ReceiverStream,
    tonic_h3::H3Channel,
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
        let uri = http_1_1_0::uri::Uri::from_str(&url).map_err(|e| {
            io::Error::new(
                ErrorKind::Other,
                format!("failed to resolve block engine IP {e}"),
            )
        })?;

        let mut tls_config = rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous() // Do not verify server certs
        .with_custom_certificate_verifier(Arc::new(danger::NoCertificateVerification::new(
            rustls::crypto::ring::default_provider(),
        )))
        .with_no_client_auth();

        tls_config.enable_early_data = true;
        tls_config.alpn_protocols = vec![b"h3".to_vec()];
        let mut client_endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap())?;
        let client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls_config).unwrap(),
        ));
        client_endpoint.set_default_client_config(client_config);

        let connector = h3_util::quinn::H3QuinnConnector::new(
            uri.clone(),
            "localhost".to_owned(),
            client_endpoint.clone(),
        );

        let mut validator_client =
            BamNodeApiClient::new(H3Channel::new(connector.clone(), uri.clone()));
        let config_refresh_client = BamNodeApiClient::new(H3Channel::new(connector, uri.clone()));

        let (outbound_sender, outbound_receiver_internal) = mpsc::channel(100_000);
        let outbound_stream = tonic::Request::new(ReceiverStream::new(outbound_receiver_internal));
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
            config_refresh_client,
            config.clone(),
            batch_sender,
            cluster_info,
            metrics.clone(),
            is_healthy.clone(),
            outbound_receiver,
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
        mut inbound_stream: tonic::Streaming<StartSchedulerResponse>,
        outbound_sender: mpsc::Sender<StartSchedulerMessage>,
        mut validator_client: BamNodeApiClient<H3Channel<H3QuinnConnector>>,
        config_refresh_client: BamNodeApiClient<H3Channel<H3QuinnConnector>>,
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
            config_refresh_client,
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
                inbound = inbound_stream.message() => {
                    let inbound = match inbound {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            error!("Inbound stream closed");
                            break;
                        }
                        Err(e) => {
                            error!("Failed to receive message from inbound stream: {:?}", e);
                            break;
                        }
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
        mut validator_client: BamNodeApiClient<H3Channel<H3QuinnConnector>>,
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
        validator_client: &mut BamNodeApiClient<H3Channel<H3QuinnConnector>>,
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
    UriError(#[from] http_1_1_0::Error),
    #[error("Invalid URI")]
    EndpointConnectError(#[from] tonic::transport::Error),
    #[error("Connect Error")]
    H3EndpointConnectError(#[from] std::io::Error),
    #[error("Connection timeout")]
    ConnectionTimeout(#[from] tokio::time::error::Elapsed),
    #[error("Stream start error")]
    StreamStartError(#[from] tonic::Status),
}
// copied from https://github.com/rustls/rustls/blob/f98484bdbd57a57bafdd459db594e21c531f1b4a/examples/src/bin/tlsclient-mio.rs#L331
mod danger {
    use rustls::{
        client::danger::HandshakeSignatureValid,
        crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
        pki_types::{CertificateDer, ServerName, UnixTime},
        DigitallySignedStruct,
    };

    #[derive(Debug)]
    pub struct NoCertificateVerification(CryptoProvider);

    impl NoCertificateVerification {
        pub fn new(provider: CryptoProvider) -> Self {
            Self(provider)
        }
    }

    impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}
