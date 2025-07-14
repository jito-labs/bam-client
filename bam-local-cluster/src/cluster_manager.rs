use {
    crate::config::{CustomValidatorConfig, LocalClusterConfig},
    anyhow::{Context, Result},
    log::{debug, error, info, warn},
    solana_faucet::faucet::{run_faucet, Faucet},
    solana_ledger::{blockstore::create_new_ledger, blockstore_options::LedgerColumnOptions},
    solana_local_cluster::integration_tests::DEFAULT_NODE_STAKE,
    solana_program_test::programs::spl_programs,
    solana_runtime::genesis_utils::{
        create_genesis_config_with_vote_accounts_and_cluster_type, ValidatorVoteKeypairs,
    },
    solana_sdk::{
        genesis_config::{ClusterType, GenesisConfig},
        shred_version::compute_shred_version,
        signature::Keypair,
        signer::Signer,
    },
    std::{
        fs,
        net::SocketAddr,
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        str::FromStr,
        sync::{Arc, Mutex},
    },
    tokio::{runtime::Runtime, signal, task},
};

pub struct BamLocalCluster {
    processes: Arc<Mutex<Vec<Child>>>,
    runtime: Runtime,
}

impl BamLocalCluster {
    pub fn new(config: LocalClusterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let faucet_address = SocketAddr::from_str(&config.faucet_address)?;

        // Generate keypairs for all validators first
        let mut validator_keypairs = Vec::new();
        for _ in 0..config.validators.len() {
            let validator_identity = Keypair::new();
            let validator_vote = Keypair::new();
            validator_keypairs.push((validator_identity, validator_vote));
        }

        // Create genesis configuration with the validator keypairs
        let voting_keypairs = validator_keypairs
            .iter()
            .map(|(identity_keypair, vote_keypair)| {
                let stake_keypair = Keypair::new();
                ValidatorVoteKeypairs::new(
                    identity_keypair.insecure_clone(),
                    vote_keypair.insecure_clone(),
                    stake_keypair,
                )
            })
            .collect::<Vec<_>>();
        let stakes = vec![DEFAULT_NODE_STAKE; config.validators.len()];
        let mut genesis_config_info = create_genesis_config_with_vote_accounts_and_cluster_type(
            solana_local_cluster::local_cluster::DEFAULT_MINT_LAMPORTS,
            &voting_keypairs,
            stakes,
            ClusterType::MainnetBeta,
        );

        // Add SPL programs
        for (pubkey, account) in spl_programs(&genesis_config_info.genesis_config.rent) {
            genesis_config_info
                .genesis_config
                .add_account(pubkey, account);
        }

        let runtime = Runtime::new().expect("Could not create Tokio runtime");

        // Start faucet
        let faucet_keypair = Keypair::new();
        let faucet = Arc::new(Mutex::new(Faucet::new(faucet_keypair, None, None, None)));
        runtime.spawn(run_faucet(faucet, faucet_address, None));

        let mut processes = Vec::new();
        let mut bootstrap_gossip = String::new();

        // Process all validators
        for (i, validator_config) in config.validators.iter().enumerate() {
            let is_bootstrap = i == 0; // First validator is always bootstrap

            // Auto-generate ledger path based on index
            let ledger_subdir = if is_bootstrap {
                "bootstrap-ledger".to_string()
            } else {
                format!("validator-{}", i)
            };

            let validator_ledger_path =
                Self::create_ledger_directory(&config.ledger_base_directory, &ledger_subdir)?;

            // Use pre-generated keypairs for validator
            let (validator_identity, validator_vote) = &validator_keypairs[i];

            // Save validator keypairs in the ledger directory
            let validator_identity_path = validator_ledger_path.join("identity.json");
            let validator_vote_path = validator_ledger_path.join("vote.json");
            fs::write(
                &validator_identity_path,
                serde_json::to_string_pretty(&validator_identity.to_bytes().to_vec())?,
            )?;
            fs::write(
                &validator_vote_path,
                serde_json::to_string_pretty(&validator_vote.to_bytes().to_vec())?,
            )?;

            // Copy genesis to validator ledger
            genesis_config_info
                .genesis_config
                .write(&validator_ledger_path)?;

            create_new_ledger(
                &validator_ledger_path,
                &genesis_config_info.genesis_config,
                10737418240,
                LedgerColumnOptions::default(),
            )?;

            // Assign gossip port: 8001 for bootstrap, random for others
            let gossip_port = is_bootstrap.then_some(8001);
            let rpc_port = is_bootstrap.then_some(8899);

            let node_name = if is_bootstrap {
                "bootstrap".to_string()
            } else {
                format!("validator-{}", i)
            };

            let validator_process = Self::start_validator_node(
                validator_config,
                &config,
                &genesis_config_info.genesis_config,
                &validator_ledger_path,
                &validator_identity_path,
                &validator_vote_path,
                if is_bootstrap {
                    None
                } else {
                    Some(&bootstrap_gossip)
                },
                gossip_port,
                rpc_port,
                &runtime,
                &node_name,
            )?;

            if is_bootstrap {
                bootstrap_gossip = format!("127.0.0.1:{}", gossip_port.unwrap());
            }

            processes.push(validator_process);
        }

        Ok(Self {
            processes: Arc::new(Mutex::new(processes)),
            runtime,
        })
    }

    fn create_ledger_directory(
        base_path: &str,
        name: &str,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let ledger_path = PathBuf::from(base_path).join(name);
        fs::create_dir_all(&ledger_path).context(format!(
            "Failed to create ledger directory: {:?}",
            ledger_path
        ))?;
        Ok(ledger_path)
    }

    fn start_validator_node(
        validator_config: &CustomValidatorConfig,
        config: &LocalClusterConfig,
        genesis_config: &GenesisConfig,
        ledger_path: &Path,
        identity_path: &Path,
        vote_path: &Path,
        bootstrap_gossip: Option<&str>,
        gossip_port: Option<u16>,
        rpc_port: Option<u16>,
        runtime: &Runtime,
        node_name: &str,
    ) -> Result<Child, Box<dyn std::error::Error>> {
        // Determine validator binary path
        let validator_binary = config.validator_binary_path.as_deref().unwrap_or_else(|| {
            // Try to find the validator binary in common locations
            let possible_paths = [
                "target/debug/agave-validator",
                "target/release/agave-validator",
                "agave-validator",
            ];

            for path in &possible_paths {
                if Path::new(path).exists() {
                    return path;
                }
            }

            // Fallback to the hardcoded path if nothing else works
            "/Users/lucasbruder/jito/jito-solana-jds/target/debug/agave-validator"
        });

        let mut cmd = Command::new(validator_binary);

        cmd.env("RUST_LOG", "info")
            .arg("--log")
            .arg("-")
            .arg("--ledger")
            .arg(ledger_path)
            .arg("--identity")
            .arg(identity_path)
            .arg("--vote-account")
            .arg(vote_path)
            .arg("--bind-address")
            .arg("0.0.0.0")
            // .arg("--wait-for-supermajority")
            // .arg("0")
            .arg("--no-wait-for-vote-to-start-leader")
            .arg("--allow-private-addr")
            .arg("--expected-bank-hash")
            .arg(genesis_config.hash().to_string())
            .arg("--expected-shred-version")
            .arg(compute_shred_version(&genesis_config.hash(), None).to_string())
            .arg("--bam-url")
            .arg(config.bam_url.to_string())
            .arg("--tip-distribution-program-pubkey")
            .arg(config.tip_distribution_program_id.to_string())
            .arg("--tip-payment-program-pubkey")
            .arg(config.tip_payment_program_id.to_string())
            .arg("--merkle-root-upload-authority")
            .arg("11111111111111111111111111111111")
            .arg("--commission-bps")
            .arg("100")
            .arg("--full-rpc-api");

        if let Some(gossip_port) = gossip_port {
            cmd.arg("--gossip-port").arg(gossip_port.to_string());
        }

        if let Some(rpc_port) = rpc_port {
            cmd.arg("--rpc-port").arg(rpc_port.to_string());
        }

        // Only add entrypoint for non-bootstrap nodes
        if let Some(bootstrap_gossip) = bootstrap_gossip {
            if !bootstrap_gossip.is_empty() {
                cmd.arg("--entrypoint").arg(bootstrap_gossip);
            }
        }

        if let Some(ref geyser_config) = validator_config.geyser_config {
            cmd.arg("--geyser-plugin-config").arg(geyser_config);
        }

        cmd.arg("run");
        info!("Starting {} node with command: {:?}", node_name, cmd);

        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context(format!("Failed to start {} node", node_name))?;

        // Spawn a task to stream the output
        let child_stdout = child.stdout.take();
        let child_stderr = child.stderr.take();
        let node_name_clone = node_name.to_string();

        if let Some(stdout) = child_stdout {
            let node_name = node_name_clone.clone();
            runtime.spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let reader = BufReader::new(tokio::process::ChildStdout::from_std(stdout).unwrap());
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    info!("[{}] {}", node_name, line);
                }
            });
        }

        if let Some(stderr) = child_stderr {
            let node_name = node_name_clone;
            runtime.spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let reader = BufReader::new(tokio::process::ChildStderr::from_std(stderr).unwrap());
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    warn!("[{}] {}", node_name, line);
                }
            });
        }

        Ok(child)
    }

    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Start process monitoring
        let processes_clone = Arc::clone(&self.processes);
        self.runtime.spawn(async move {
            Self::monitor_processes(processes_clone).await;
        });

        // Wait for Ctrl+C or server to exit
        self.runtime.block_on(async {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("Received Ctrl+C, shutting down...");
                }
            }
        });

        Ok(())
    }

    async fn monitor_processes(processes: Arc<Mutex<Vec<Child>>>) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            if let Ok(mut processes_guard) = processes.lock() {
                for (i, process) in processes_guard.iter_mut().enumerate() {
                    match process.try_wait() {
                        Ok(Some(status)) => {
                            error!("Validator process {} exited with status: {:?}", i, status);
                        }
                        Ok(None) => {
                            // Process is still running
                            debug!("Validator process {} (PID: {}) is running", i, process.id());
                        }
                        Err(e) => {
                            error!("Error checking validator process {}: {}", i, e);
                        }
                    }
                }
            }
        }
    }

    pub fn shutdown(self) {
        info!("Shutting down cluster...");

        // Terminate all child processes
        if let Ok(mut processes) = self.processes.lock() {
            for mut process in processes.drain(..) {
                if let Err(e) = process.kill() {
                    error!("Failed to kill process: {}", e);
                }
                if let Err(e) = process.wait() {
                    error!("Failed to wait for process: {}", e);
                }
            }
        }
    }
}
