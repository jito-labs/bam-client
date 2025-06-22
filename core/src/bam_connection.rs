// Maintains a connection to the BAM Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use {
    crate::bam_dependencies::v0_to_versioned_proto,
    futures::{channel::mpsc, StreamExt},
    jito_protos::proto::{
        bam_api::{
            bam_node_api_client::BamNodeApiClient, start_scheduler_message_v0::Msg,
            start_scheduler_response::VersionedMsg, start_scheduler_response_v0::Resp,
            BuilderConfigResp, GetBuilderConfigRequest, StartSchedulerMessage,
            StartSchedulerMessageV0, StartSchedulerResponse, StartSchedulerResponseV0,
        },
        bam_types::{AtomicTxnBatch, ValidatorHeartBeat, FeeCollectionRequest, FeeCollectionResponse, Packet, Meta},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{
        packet::PACKET_DATA_SIZE, signature::Keypair, signer::Signer,
        transaction::VersionedTransaction,
    },
    std::{
        cmp::min,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
            Arc, Mutex, RwLock,
        },
    },
    thiserror::Error,
    tokio::time::{interval, timeout},
};

pub struct BamConnection {
    builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
    background_task: tokio::task::JoinHandle<()>,
    is_healthy: Arc<AtomicBool>,
    url: String,
    exit: Arc<AtomicBool>,
}

impl BamConnection {
    /// Try to initialize a connection to the BAM Node; if it is not possible to connect, it will return an error.
    pub async fn try_init(
        url: String,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessageV0>,
    ) -> Result<Self, TryInitError> {
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url.clone())?;
        let connection_timeout = std::time::Duration::from_secs(5);

        let channel = timeout(connection_timeout, backend_endpoint.connect()).await??;

        let mut validator_client = BamNodeApiClient::new(channel);

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

        let metrics = Arc::new(BamConnectionMetrics::default());
        let is_healthy = Arc::new(AtomicBool::new(true));
        let builder_config = Arc::new(Mutex::new(None));

        let exit = Arc::new(AtomicBool::new(false));
        let background_task = tokio::spawn(Self::connection_task(
            exit.clone(),
            inbound_stream,
            outbound_sender,
            validator_client,
            builder_config.clone(),
            batch_sender,
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
        validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
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
                    let _ = outbound_sender.try_send(v0_to_versioned_proto(StartSchedulerMessageV0 {
                        msg: Some(Msg::HeartBeat(signed_heartbeat)),
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
                        StartSchedulerResponseV0 { resp: Some(Resp::FeeCollectionRequest(fee_collection_request)), .. } => {
                            Self::handle_fee_collection_request(
                                fee_collection_request,
                                &mut outbound_sender,
                                &poh_recorder,
                                &cluster_info,
                                &metrics,
                            );
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

    async fn refresh_builder_config_task(
        exit: Arc<AtomicBool>,
        builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
        mut validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        metrics: Arc<BamConnectionMetrics>,
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

    fn handle_fee_collection_request(
        fee_collection_request: FeeCollectionRequest,
        outbound_sender: &mut mpsc::Sender<StartSchedulerMessage>,
        poh_recorder: &RwLock<PohRecorder>,
        cluster_info: &ClusterInfo,
        metrics: &Arc<BamConnectionMetrics>,
    ) {
        // Update metrics
        metrics
            .fee_collection_request_received
            .fetch_add(1, Relaxed);

        // Get values
        let FeeCollectionRequest {
            slot,
            destination_pubkey,
            lamports,
        } = fee_collection_request;
        let Ok(destination_pubkey) = solana_sdk::pubkey::Pubkey::from_str(&destination_pubkey)
        else {
            return;
        };

        // Confirm fee amounts (and ensure this is not a duplicate request)
        // TODO_DG

        // Create transfer transaction and sign
        let transfer_txn = solana_sdk::system_transaction::transfer(
            &cluster_info.keypair(),
            &destination_pubkey,
            lamports,
            poh_recorder
                .read()
                .unwrap()
                .get_poh_recorder_bank()
                .bank()
                .last_blockhash(),
        );
        let versioned_tx = VersionedTransaction::from(transfer_txn);

        info!(
            "Sending fee of {} for slot {} to {}",
            lamports, slot, destination_pubkey
        );

        // Send the transaction to the JSS Node
        let packet = Self::jss_packet_from_versioned_tx(&versioned_tx);
        let msg = StartSchedulerMessageV0 {
            msg: Some(Msg::FeeCollectionResponse(FeeCollectionResponse {
                slot,
                packet: Some(packet),
            })),
        };
        if let Err(e) = outbound_sender.try_send(v0_to_versioned_proto(msg)) {
            error!("Failed to send fee collection response: {:?}", e);
        } else {
            metrics
                .fee_collection_request_received
                .fetch_add(1, Relaxed);
        }
    }

    fn jss_packet_from_versioned_tx(tx: &VersionedTransaction) -> Packet {
        let tx_data = bincode::serialize(tx).expect("serializes");
        let mut data = [0; PACKET_DATA_SIZE];
        let copy_len = min(tx_data.len(), data.len());
        data[..copy_len].copy_from_slice(&tx_data[..copy_len]);
        Packet {
            meta: Some(Meta {
                size: copy_len as u64,
                ..Default::default()
            }),
            data: data.into(),
        }
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
    fee_collection_request_received: AtomicU64,

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
                "fee_collection_request_received",
                self.fee_collection_request_received.swap(0, Relaxed) as i64,
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
