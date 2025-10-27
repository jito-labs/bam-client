/// Facilitates the BAM sub-system in the validator:
/// - Tries to connect to BAM
/// - Sends leader state to BAM
/// - Updates TPU config
/// - Updates block builder fee info
/// - Sets `bam_enabled` flag that is used everywhere
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
};
use {
    crate::{
        admin_rpc_post_init::{KeyUpdaterType, KeyUpdaters},
        bam_connection::BamConnection,
        bam_dependencies::BamDependencies,
        bam_fallback_manager::BamFallbackManager,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
    },
    jito_protos::proto::{
        bam_api::ConfigResponse,
        bam_types::{LeaderState, Socket},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_quic_definitions::NotifyKeyUpdate,
    solana_runtime::bank::Bank,
    solana_signer::Signer,
};

pub struct BamConnectionIdentityUpdater {
    bam_url: Arc<Mutex<Option<String>>>,
    new_identity: Arc<Mutex<Option<Pubkey>>>,
    identity_changed_force_reconnect: Arc<AtomicBool>,
}

impl NotifyKeyUpdate for BamConnectionIdentityUpdater {
    fn update_key(&self, key: &solana_keypair::Keypair) -> Result<(), Box<dyn core::error::Error>> {
        let disconnect_url = self
            .bam_url
            .lock()
            .unwrap()
            .as_ref()
            .map_or("None".to_string(), |u| u.clone());

        datapoint_warn!(
            "bam-manager_identity-changed",
            ("count", 1, i64),
            ("identity_changed_to", key.pubkey().to_string(), String),
            ("bam_url", disconnect_url, String)
        );
        warn!(
            "BAM Manager: validator identity changed! Reconnecting to BAM at url {:?} from new identity {}",
            disconnect_url,
            key.pubkey(),
        );
        *self.new_identity.lock().unwrap() = Some(key.pubkey());
        self.identity_changed_force_reconnect
            .store(true, Ordering::Relaxed);
        Ok(())
    }
}

pub struct BamManager {
    thread: std::thread::JoinHandle<()>,
}

impl BamManager {
    pub fn new(
        exit: Arc<AtomicBool>,
        bam_url: Arc<Mutex<Option<String>>>,
        bam_txns_per_slot_threshold: Arc<RwLock<u64>>,
        dependencies: BamDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        identity_notifiers: Arc<RwLock<KeyUpdaters>>,
    ) -> Self {
        Self {
            thread: std::thread::spawn(move || {
                Self::run(
                    exit,
                    bam_url,
                    bam_txns_per_slot_threshold,
                    dependencies,
                    poh_recorder,
                    identity_notifiers,
                )
            }),
        }
    }

    fn run(
        exit: Arc<AtomicBool>,
        bam_url: Arc<Mutex<Option<String>>>,
        bam_txns_per_slot_threshold: Arc<RwLock<u64>>,
        dependencies: BamDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        identity_notifiers: Arc<RwLock<KeyUpdaters>>,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();
        let shared_working_bank = poh_recorder.read().unwrap().shared_working_bank();

        let mut current_connection = None;
        let mut cached_builder_config = None;
        let mut fallback_manager = BamFallbackManager::new(
            exit.clone(),
            poh_recorder.clone(),
            bam_txns_per_slot_threshold,
            bam_url.clone(),
            dependencies.clone(),
        );

        let identity_changed = Arc::new(AtomicBool::new(false));
        let new_identity = Arc::new(Mutex::new(None));

        let identity_updater = Arc::new(BamConnectionIdentityUpdater {
            bam_url: bam_url.clone(),
            new_identity: new_identity.clone(),
            identity_changed_force_reconnect: identity_changed.clone(),
        }) as Arc<dyn NotifyKeyUpdate + Sync + Send>;

        let mut identity_notifiers = identity_notifiers.write().unwrap();
        identity_notifiers.add(KeyUpdaterType::BamConnection, identity_updater);
        drop(identity_notifiers);
        info!("BAM Manager: Added BAM connection key updater");
        let mut prev_bam_url = bam_url.lock().unwrap().clone();

        while !exit.load(Ordering::Relaxed) {
            // Update if bam is enabled
            dependencies.bam_enabled.store(
                current_connection.is_some() && cached_builder_config.is_some(),
                Ordering::Relaxed,
            );

            // If no connection then try to create a new one
            if current_connection.is_none() {
                let url = bam_url.lock().unwrap().clone();
                if let Some(url) = url {
                    let result = runtime.block_on(BamConnection::try_init(
                        url.clone(),
                        dependencies.cluster_info.clone(),
                        dependencies.batch_sender.clone(),
                        dependencies.outbound_receiver.clone(),
                    ));
                    match result {
                        Ok(connection) => {
                            current_connection = Some(connection);
                            let slot = poh_recorder.read().unwrap().current_poh_slot();
                            info!("BAM connection established");
                            datapoint_info!(
                                "bam_manager-connected",
                                ("count", 1, i64),
                                ("slot", slot, i64),
                                (
                                    "prev_bam_url",
                                    prev_bam_url.as_deref().unwrap_or("None"),
                                    String
                                ),
                                ("bam_url", url.clone(), String)
                            );
                            prev_bam_url = Some(url);
                            // Sleep to let heartbeat come in
                            std::thread::sleep(std::time::Duration::from_secs(2));
                        }
                        Err(e) => {
                            error!("Failed to connect to BAM: {}", e);
                        }
                    }
                }
            }

            let Some(connection) = current_connection.as_mut() else {
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            };

            // Check if connection is healthy or if the identity changed; if no then disconnect
            // Disconnecting will cause a reconnect attempt, with the new identity if it changed
            if !connection.is_healthy() || identity_changed.load(Ordering::Relaxed) {
                current_connection = None;
                cached_builder_config = None;
                if identity_changed.load(Ordering::Relaxed) {
                    // Wait until the new identity is set in cluster info as to avoid race conditions
                    // with sending an auth proof w/ the old identity
                    let identity = new_identity.lock().unwrap().take();
                    let timeout = std::time::Duration::from_secs(180);
                    Self::wait_for_identity_in_cluster_info(
                        identity,
                        &dependencies.cluster_info,
                        timeout,
                    );
                    identity_changed.store(false, Ordering::Relaxed);
                }
                warn!("BAM connection lost");
                continue;
            }

            // Check if url changed; if yes then disconnect
            let url = bam_url.lock().unwrap().clone();
            if Some(connection.url().to_string()) != url {
                current_connection = None;
                cached_builder_config = None;
                if let Some(prev_url) = prev_bam_url.as_ref() {
                    info!(
                        "BAM URL changed from {} to {:?}",
                        prev_url,
                        url.as_deref().unwrap_or("None")
                    );
                } else {
                    info!(
                        "BAM URL set to {:?} from None",
                        url.as_deref().unwrap_or("None")
                    );
                }
                continue;
            }

            // Check if block builder info has changed
            if let Some(builder_config) = connection.get_latest_config() {
                if Some(&builder_config) != cached_builder_config.as_ref() {
                    Self::update_tpu_config(Some(&builder_config), &dependencies.cluster_info);
                    Self::update_block_engine_key_and_commission(
                        Some(&builder_config),
                        &dependencies.block_builder_fee_info,
                    );
                    Self::update_bam_recipient_and_commission(
                        &builder_config,
                        &dependencies.bam_node_pubkey,
                    );
                    cached_builder_config = Some(builder_config);
                }
            }

            // Send leader state to BamFallbackManager if we are in a leader slot
            if let Some(bank) = shared_working_bank.load() {
                if !bank.is_frozen() {
                    let leader_state = Self::generate_leader_state(&bank);
                    match fallback_manager.send_slot(leader_state.slot) {
                        Ok(()) => {}
                        Err(crossbeam_channel::TrySendError::Full(_)) => {
                            error!("Failed to send slot to fallback manager: channel full");
                        }
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                            error!("Failed to send slot to fallback manager: channel disconnected. Exiting...");
                            break;
                        }
                    }

                    let _ = dependencies.outbound_sender.try_send(
                        crate::bam_dependencies::BamOutboundMessage::LeaderState(leader_state),
                    );
                }
            }

            // Sleep for a short duration to avoid busy-waiting
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        fallback_manager
            .join()
            .expect("Failed to join fallback manager thread");
        info!("BAM Manager thread exiting");
    }

    fn generate_leader_state(bank: &Bank) -> LeaderState {
        let max_block_cu = bank.read_cost_tracker().unwrap().block_cost_limit();
        let consumed_block_cu = bank.read_cost_tracker().unwrap().block_cost();
        let slot_cu_budget_remaining = max_block_cu.saturating_sub(consumed_block_cu) as u32;
        LeaderState {
            slot: bank.slot(),
            tick: (bank.tick_height() % bank.ticks_per_slot()) as u32,
            slot_cu_budget_remaining,
        }
    }

    fn get_sockaddr(info: Option<&Socket>) -> Option<SocketAddr> {
        let info = info?;
        let Socket { ip, port } = info;
        Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(ip).ok()?,
            *port as u16,
        )))
    }

    fn update_tpu_config(config: Option<&ConfigResponse>, cluster_info: &Arc<ClusterInfo>) {
        let Some(tpu_info) = config.and_then(|c| c.bam_config.as_ref()) else {
            return;
        };

        if let Some(tpu) = Self::get_sockaddr(tpu_info.tpu_sock.as_ref()) {
            info!("Setting TPU: {:?}", tpu);
            let _ = cluster_info.set_tpu(tpu);
        }
        if let Some(tpu_fwd) = Self::get_sockaddr(tpu_info.tpu_fwd_sock.as_ref()) {
            info!("Setting TPU forward: {:?}", tpu_fwd);
            let _ = cluster_info.set_tpu_forwards(tpu_fwd);
        }
    }

    fn update_block_engine_key_and_commission(
        config: Option<&ConfigResponse>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) {
        let Some(builder_info) = config.and_then(|c| c.block_engine_config.as_ref()) else {
            return;
        };

        let Some(pubkey) = Pubkey::from_str(&builder_info.builder_pubkey).ok() else {
            error!(
                "Failed to parse builder pubkey: {}",
                builder_info.builder_pubkey
            );
            block_builder_fee_info.lock().unwrap().block_builder = Pubkey::default();
            return;
        };

        let commission = builder_info.builder_commission;
        if commission > 100 {
            error!("Block builder commission must be <= 100");
            return;
        }
        let mut block_builder_fee_info = block_builder_fee_info.lock().unwrap();
        block_builder_fee_info.block_builder = pubkey;
        block_builder_fee_info.block_builder_commission = commission as u64;
    }

    fn update_bam_recipient_and_commission(
        config: &ConfigResponse,
        prio_fee_recipient_pubkey: &Arc<Mutex<Pubkey>>,
    ) -> bool {
        let Some(bam_info) = config.bam_config.as_ref() else {
            return false;
        };

        let Some(pubkey) = Pubkey::from_str(&bam_info.prio_fee_recipient_pubkey).ok() else {
            return false;
        };

        prio_fee_recipient_pubkey
            .lock()
            .unwrap()
            .clone_from(&pubkey);
        true
    }

    fn wait_for_identity_in_cluster_info(
        new_identity: Option<Pubkey>,
        cluster_info: &Arc<ClusterInfo>,
        timeout: std::time::Duration,
    ) -> bool {
        let Some(new_identity) = new_identity else {
            return false;
        };

        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if cluster_info.keypair().pubkey() == new_identity {
                info!(
                    "BAM Manager: detected new identity {} in cluster info",
                    new_identity
                );
                return true;
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        warn!(
            "BAM Manager: timed out waiting for new identity {} to appear in cluster info after {:?}",
            new_identity,
            start.elapsed()
        );
        datapoint_warn!(
            "bam-manager_identity-wait-timeout",
            ("waited_for_identity", new_identity.to_string(), String),
            ("timeout_secs", timeout.as_secs() as i64, i64)
        );
        false
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}
