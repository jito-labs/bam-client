/// Facilitates the JSS sub-system in the validator:
/// - Tries to connect to JSS
/// - Sends leader state to JSS
/// - Updates TPU config
/// - Updates block builder fee info
/// - Sets `jss_enabled` flag that is used everywhere
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
        jss_connection::JssConnection, jss_dependencies::JssDependencies,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
    },
    jito_protos::proto::{
        jss_api::{start_scheduler_message::Msg, BuilderConfigResp, StartSchedulerMessage},
        jss_types::{LeaderState, Socket},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
};

pub struct JssManager {
    thread: std::thread::JoinHandle<()>,
}

impl JssManager {
    pub fn new(
        exit: Arc<AtomicBool>,
        jss_url: Arc<Mutex<Option<String>>>,
        dependencies: JssDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) -> Self {
        Self {
            thread: std::thread::spawn(move || {
                Self::run(exit, jss_url, dependencies, poh_recorder)
            }),
        }
    }

    fn run(
        exit: Arc<AtomicBool>,
        jss_url: Arc<Mutex<Option<String>>>,
        dependencies: JssDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();

        let start = std::time::Instant::now();
        const GRACE_PERIOD_DURATION: std::time::Duration = std::time::Duration::from_secs(10);
        let mut in_startup_grace_period = true;

        let mut current_connection = None;
        let mut cached_builder_config = None;
        while !exit.load(Ordering::Relaxed) {
            // Check if we are in the startup grace period
            if in_startup_grace_period {
                if start.elapsed() > GRACE_PERIOD_DURATION {
                    in_startup_grace_period = false;
                }
            }

            // Update if jss is enabled and sleep for a while before checking again
            // While in grace period, we allow JSS to be enabled even if no connection is established
            dependencies.jss_enabled.store(
                in_startup_grace_period || (current_connection.is_some() && cached_builder_config.is_some()),
                Ordering::Relaxed,
            );

            // If no connection then try to create a new one
            if current_connection.is_none() {
                let url = jss_url.lock().unwrap().clone();
                if let Some(url) = url {
                    let result = runtime.block_on(JssConnection::try_init(
                        url,
                        poh_recorder.clone(),
                        dependencies.cluster_info.clone(),
                        dependencies.bundle_sender.clone(),
                        dependencies.outbound_receiver.clone(),
                    ));
                    match result {
                        Ok(connection) => {
                            current_connection = Some(connection);
                            info!("JSS connection established");
                            // Sleep to let heartbeat come in
                            std::thread::sleep(std::time::Duration::from_secs(2));
                        }
                        Err(e) => {
                            error!("Failed to connect to JSS: {}", e);
                        }
                    }
                }
            }

            let Some(connection) = current_connection.as_mut() else {
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            };

            // Check if connection is healthy; if no then disconnect
            if !connection.is_healthy() {
                current_connection = None;
                cached_builder_config = None;
                info!("JSS connection lost");
                continue;
            }

            // Check if url changed; if yes then disconnect
            let url = jss_url.lock().unwrap().clone();
            if Some(connection.url().to_string()) != url {
                current_connection = None;
                cached_builder_config = None;
                info!("JSS URL changed");
                continue;
            }

            // Check if block builder info has changed
            if let Some(builder_config) = connection.get_builder_config() {
                if Some(&builder_config) != cached_builder_config.as_ref() {
                    Self::update_tpu_config(Some(&builder_config), &dependencies.cluster_info);
                    Self::update_key_and_commission(
                        Some(&builder_config),
                        &dependencies.block_builder_fee_info,
                    );
                    cached_builder_config = Some(builder_config);
                }
            }

            // Send leader state if we are in a leader slot
            if let Some(bank_start) = poh_recorder.read().unwrap().bank_start() {
                if bank_start.should_working_bank_still_be_processing_txs() {
                    let leader_state = Self::generate_leader_state(&bank_start.working_bank);
                    let _ = dependencies
                        .outbound_sender
                        .try_send(StartSchedulerMessage {
                            msg: Some(Msg::LeaderState(leader_state)),
                        });
                }
            }

            // Sleep for a short duration to avoid busy-waiting
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn generate_leader_state(bank: &Bank) -> LeaderState {
        let max_block_cu = bank.read_cost_tracker().unwrap().block_cost_limit();
        let consumed_block_cu = bank.read_cost_tracker().unwrap().block_cost();
        let slot_cu_budget = max_block_cu.saturating_sub(consumed_block_cu) as u32;
        LeaderState {
            slot: bank.slot(),
            tick: (bank.tick_height() % bank.ticks_per_slot()) as u32,
            slot_cu_budget,
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

    fn update_tpu_config(tpu_info: Option<&BuilderConfigResp>, cluster_info: &Arc<ClusterInfo>) {
        if let Some(tpu_info) = tpu_info {
            if let Some(tpu) = Self::get_sockaddr(tpu_info.tpu_sock.as_ref()) {
                let _ = cluster_info.set_tpu(tpu);
            }

            if let Some(tpu_fwd) = Self::get_sockaddr(tpu_info.tpu_fwd_sock.as_ref()) {
                let _ = cluster_info.set_tpu_forwards(tpu_fwd);
            }
        }
    }

    fn update_key_and_commission(
        builder_info: Option<&BuilderConfigResp>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) {
        if let Some(builder_info) = builder_info {
            let pubkey = Pubkey::from_str(&builder_info.builder_pubkey).unwrap();
            let commission = builder_info.builder_commission;
            let mut block_builder_fee_info = block_builder_fee_info.lock().unwrap();
            block_builder_fee_info.block_builder = pubkey;
            block_builder_fee_info.block_builder_commission = commission;
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}
