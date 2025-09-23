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
        bam_connection::BamConnection, bam_dependencies::BamDependencies,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
    },
    jito_protos::proto::{
        bam_api::ConfigResponse,
        bam_types::{LeaderState, Socket},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_poh::poh_recorder::PohRecorder,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
};

pub struct BamManager {
    thread: std::thread::JoinHandle<()>,
}

impl BamManager {
    pub fn new(
        exit: Arc<AtomicBool>,
        bam_url: Arc<Mutex<Option<String>>>,
        dependencies: BamDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) -> Self {
        Self {
            thread: std::thread::spawn(move || {
                Self::run(exit, bam_url, dependencies, poh_recorder)
            }),
        }
    }

    fn run(
        exit: Arc<AtomicBool>,
        bam_url: Arc<Mutex<Option<String>>>,
        dependencies: BamDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();
        let shared_working_bank = poh_recorder.read().unwrap().shared_working_bank();

        let mut current_connection = None;
        let mut cached_builder_config = None;

        let mut circuit_breaker_count: u32 = 0;
        let mut last_checked_slot: Option<u64> = None;
        let mut our_leader_slots: std::collections::VecDeque<u64> = std::collections::VecDeque::new();
        const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
        const MAX_TRACKED_SLOTS: usize = 10;

        // get our pubkey for leader slot checks
        let my_pubkey = dependencies.cluster_info.id();

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
                        url,
                        dependencies.cluster_info.clone(),
                        dependencies.batch_sender.clone(),
                        dependencies.outbound_receiver.clone(),
                    ));
                    match result {
                        Ok(connection) => {
                            current_connection = Some(connection);
                            info!("BAM connection established");
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

            // Check if connection is healthy; if no then disconnect
            if !connection.is_healthy() {
                current_connection = None;
                cached_builder_config = None;
                warn!("BAM connection lost");
                continue;
            }

            // Check if url changed; if yes then disconnect
            let url = bam_url.lock().unwrap().clone();
            if Some(connection.url().to_string()) != url {
                current_connection = None;
                cached_builder_config = None;
                info!("BAM URL changed");
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

            // Send leader state if we are in a leader slot
            if let Some(bank) = shared_working_bank.load() {
                let current_slot = bank.slot();
    
                if bank.collector_id() == &my_pubkey && !bank.is_frozen() {
                    // track our leader slots
                    if !our_leader_slots.contains(&current_slot) {
                        our_leader_slots.push_back(current_slot);
                        if our_leader_slots.len() > MAX_TRACKED_SLOTS {
                            our_leader_slots.pop_front();
                        }
                    }
                }
                
                // Check frozen banks for non-vote transaction counts (?)
                // maybe can do this at an earlier commitment stage
                if bank.is_frozen() && last_checked_slot != Some(current_slot) {
                    last_checked_slot = Some(current_slot);
                    
                    if our_leader_slots.contains(&current_slot) {
                        let non_vote_tx_count = bank.non_vote_transaction_count_since_restart();
                        
                        let parent_non_vote_count = bank.parent()
                            .map(|p| p.non_vote_transaction_count_since_restart())
                            .unwrap_or(0);
                        
                        let slot_non_vote_txs = non_vote_tx_count.saturating_sub(parent_non_vote_count);
                        
                        if slot_non_vote_txs == 0 {
                            circuit_breaker_count += 1;
                            warn!("Circuit breaker: No non-vote transactions in our leader slot {}. Count: {}", 
                                current_slot, circuit_breaker_count);
                            
                            if circuit_breaker_count >= CIRCUIT_BREAKER_THRESHOLD {
                                error!("Circuit breaker triggered! Disconnecting from BAM");
                                current_connection = None;
                                cached_builder_config = None;
                                circuit_breaker_count = 0;
                            }
                        } else {
                            if circuit_breaker_count > 0 {
                                info!("Circuit breaker reset. Had {} non-vote txs in slot {}", 
                                    slot_non_vote_txs, current_slot);
                                circuit_breaker_count = 0;
                            }
                        }
                        our_leader_slots.retain(|&s| s != current_slot);
                    }
                }

                if !bank.is_frozen() {
                    let leader_state = Self::generate_leader_state(&bank);
                    let _ = dependencies.outbound_sender.try_send(
                        crate::bam_dependencies::BamOutboundMessage::LeaderState(leader_state),
                    );
                }
            }

            // Sleep for a short duration to avoid busy-waiting
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
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
        block_builder_fee_info.block_builder_commission = commission;
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

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}
