use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
};

use jito_protos::proto::{jss_api::BuilderConfigResp, jss_types::Socket};
use solana_gossip::cluster_info::ClusterInfo;
use solana_poh::poh_recorder::PohRecorder;
use solana_pubkey::Pubkey;

use crate::{
    jss_connection::JssConnection, jss_dependencies::JssDependencies,
    proxy::block_engine_stage::BlockBuilderFeeInfo,
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
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut current_connection = None;
        while !exit.load(Ordering::Relaxed) {
            // Update if jss is enabled and sleep for a while before checking again
            dependencies
                .jss_enabled
                .store(current_connection.is_some(), Ordering::Relaxed);

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
                info!("JSS connection lost");
                continue;
            }

            // Check if url changed; if yes then disconnect
            let url = jss_url.lock().unwrap().clone();
            if Some(connection.url().to_string()) != url {
                current_connection = None;
                info!("JSS URL changed");
                continue;
            }

            // Check if block builder info has changed
            if let Some(builder_config) = connection.get_builder_config() {
                // Update tpu if needed
                Self::update_tpu_config(Some(&builder_config), &dependencies.cluster_info);

                // Update commission if needed
                Self::update_key_and_commission(
                    Some(&builder_config),
                    &dependencies.block_builder_fee_info,
                );
            }

            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    fn get_sockaddr(info: Option<&Socket>) -> Option<SocketAddr> {
        let info = info?;
        let Socket { ip, port } = info;
        Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(&ip).ok()?,
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
            let commission = builder_info.builder_commission as u64;
            let mut block_builder_fee_info = block_builder_fee_info.lock().unwrap();
            block_builder_fee_info.block_builder = pubkey;
            block_builder_fee_info.block_builder_commission = commission;
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}
