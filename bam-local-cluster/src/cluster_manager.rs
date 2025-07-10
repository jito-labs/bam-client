use {
    crate::{config::{LocalClusterConfig, ClusterInfo}, http_server::{AppState, create_app}},
    log::info,
    solana_core::{
        tip_manager::{TipDistributionAccountConfig, TipManagerConfig},
        validator::ValidatorConfig,
    },
    solana_faucet::faucet::{run_faucet, Faucet},
    solana_local_cluster::{
        integration_tests::DEFAULT_NODE_STAKE,
        local_cluster::{ClusterConfig, LocalCluster, DEFAULT_MINT_LAMPORTS},
    },
    solana_program_test::{
        programs::spl_programs,
        tokio::{self, runtime::Runtime},
    },
    solana_rpc::rpc::JsonRpcConfig,
    solana_sdk::{
        pubkey::Pubkey,
        rent::Rent,
        signature::{Keypair, Signer},
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        iter,
        net::SocketAddr,
        str::FromStr,
        sync::{Arc, Mutex},
    },
    tokio::{signal, sync::Notify},
};

pub struct BamLocalCluster {
    cluster: LocalCluster,
    runtime: Runtime,
}

impl BamLocalCluster {
    pub fn new(config: LocalClusterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let faucet_address = SocketAddr::from_str(&config.faucet_address)?;

        let all_configs = config.validators.iter().map(|v| {
            let identity_keypair = Keypair::new();
            let vote_account_keypair = Keypair::new();

            let mut validator_config = ValidatorConfig::default_for_test();
            validator_config.blockstore_options.enforce_ulimit_nofile = false;

            validator_config.tip_manager_config = TipManagerConfig {
                tip_payment_program_id: Pubkey::from_str(&config.tip_payment_program_id).unwrap(),
                tip_distribution_program_id: Pubkey::from_str(&config.tip_distribution_program_id)
                    .unwrap(),
                tip_distribution_account_config: TipDistributionAccountConfig {
                    merkle_root_upload_authority: Pubkey::new_unique(),
                    vote_account: vote_account_keypair.pubkey(),
                    commission_bps: 100,
                },
            };

            // setup RPC to forward requests to faucet for airdrops
            validator_config.rpc_config = JsonRpcConfig {
                enable_rpc_transaction_history: true,
                enable_extended_tx_metadata_storage: true,
                faucet_addr: Some(faucet_address),
                full_api: true,
                disable_health_check: true,
                ..JsonRpcConfig::default()
            };

            // apply the geyser files if provided
            validator_config.on_start_geyser_plugin_config_files =
                v.geyser_config.as_ref().map(|geyser| vec![geyser.clone()]);

            validator_config.bam_url = Arc::new(Mutex::new(Some(config.bam_url.clone())));

            (validator_config, identity_keypair, vote_account_keypair)
        });

        let mut validator_configs = Vec::new();
        let mut identity_keys = Vec::new();
        let mut vote_keys = Vec::new();
        for (cfg, identity, vote) in all_configs {
            validator_configs.push(cfg);
            identity_keys.push(Arc::new(identity));
            vote_keys.push(Arc::new(vote));
        }

        let rent = Rent::default();
        let mut cluster_config = ClusterConfig {
            mint_lamports: DEFAULT_MINT_LAMPORTS + DEFAULT_NODE_STAKE * 100,
            node_stakes: vec![DEFAULT_NODE_STAKE; config.validators.len()],
            validator_configs,
            validator_keys: Some(
                identity_keys
                    .into_iter()
                    .zip(iter::repeat_with(|| true))
                    .collect(),
            ),
            node_vote_keys: Some(vote_keys),
            skip_warmup_slots: true,
            additional_accounts: spl_programs(&rent),
            ..ClusterConfig::default()
        };

        // Start the cluster and the faucet for airdrops
        let cluster = LocalCluster::new(&mut cluster_config, SocketAddrSpace::new(true));
        let faucet = Arc::new(Mutex::new(Faucet::new(
            cluster.funding_keypair.insecure_clone(),
            None,
            None,
            None,
        )));
        let runtime = Runtime::new().expect("Could not create Tokio runtime");
        runtime.spawn(run_faucet(faucet, faucet_address, None));

        Ok(Self { cluster, runtime })
    }

    pub fn get_rpc_endpoint(&self) -> String {
        format!("http://{}", self.cluster.entry_point_info.rpc().unwrap())
    }

    pub fn run_http_server(&self, config: &LocalClusterConfig) -> Result<(), Box<dyn std::error::Error>> {
        let rpc_endpoint = self.get_rpc_endpoint();
        let cluster_info = ClusterInfo { rpc_endpoint };
        let shutdown_notify = Arc::new(Notify::new());
        let app_state = Arc::new(AppState {
            cluster_info,
            shutdown_notify: shutdown_notify.clone(),
        });

        let app = create_app(app_state);
        let server_addr = SocketAddr::from_str(&config.info_address)?;
        info!("Starting HTTP server on {}", server_addr);
        
        let server_handle = self.runtime.spawn(async move {
            let listener = tokio::net::TcpListener::bind(server_addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        // Set up signal handler
        let shutdown_notify_ctrl_c = shutdown_notify.clone();
        let ctrl_c_signal = self.runtime.spawn(async move {
            signal::ctrl_c().await.unwrap();
            info!("Received Ctrl+C, shutting down...");
            shutdown_notify_ctrl_c.notify_one();
        });

        // Wait for either the server to exit, a signal, or HTTP shutdown request
        self.runtime.block_on(async {
            tokio::select! {
                _ = server_handle => {
                    info!("HTTP server exited");
                }
                _ = ctrl_c_signal => {
                    info!("Ctrl+C signal received");
                }
                _ = shutdown_notify.notified() => {
                    info!("Shutdown requested via HTTP /exit endpoint");
                }
            }
        });

        Ok(())
    }

    pub fn shutdown(&mut self) {
        info!("Shutting down cluster...");
        self.cluster.exit();
    }
} 