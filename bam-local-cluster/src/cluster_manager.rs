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
    },
    std::{
        fs,
        net::SocketAddr,
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        str::FromStr,
        sync::{Arc, Mutex},
        thread::sleep,
        time::Duration,
    },
    tokio::{runtime::Runtime, signal},
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

            let base_dir = Path::new(&config.ledger_base_directory).join(&ledger_subdir);
            if base_dir.exists() {
                fs::remove_dir_all(&base_dir)?;
            }

            let validator_ledger_path =
                Self::create_ledger_directory(&config.ledger_base_directory, &ledger_subdir)?;

            if is_bootstrap {
                create_new_ledger(
                    &validator_ledger_path,
                    &genesis_config_info.genesis_config,
                    10737418240,
                    LedgerColumnOptions::default(),
                )?;
                Self::create_snapshot(&validator_ledger_path)?;
            }

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

            // Assign gossip port: 8001 for bootstrap, random for others
            let gossip_port = is_bootstrap.then_some(8001);
            let rpc_port = is_bootstrap.then_some(8899);

            let node_name = if is_bootstrap {
                "bootstrap".to_string()
            } else {
                format!("validator-{}", i)
            };
            let dynamic_port_range_start = 10_000 + (i * 1000) as u64;
            let dynamic_port_range_end = 10_000 + ((i + 1) * 1000) as u64;

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
                (dynamic_port_range_start, dynamic_port_range_end),
            )?;

            if is_bootstrap {
                bootstrap_gossip = format!("127.0.0.1:{}", gossip_port.unwrap());
                sleep(Duration::from_secs(5)); // TODO: need smarter test here
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
        dynamic_port_range: (u64, u64),
    ) -> Result<Child, Box<dyn std::error::Error>> {
        // Determine validator binary path
        let validator_binary =
            "/Users/lucasbruder/jito/jito-solana-jds/target/debug/agave-validator";

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
            .arg("--dynamic-port-range")
            .arg(format!("{}-{}", dynamic_port_range.0, dynamic_port_range.1))
            .arg("--no-wait-for-vote-to-start-leader")
            .arg("--allow-private-addr")
            .arg("--full-rpc-api")
            .arg("--enable-rpc-transaction-history")
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
            .arg("100");

        if let Some(gossip_port) = gossip_port {
            cmd.arg("--gossip-port").arg(gossip_port.to_string());
        }

        if let Some(rpc_port) = rpc_port {
            cmd.arg("--rpc-port").arg(rpc_port.to_string());
        }

        if let Some(ref geyser_config) = validator_config.geyser_config {
            cmd.arg("--geyser-plugin-config").arg(geyser_config);
        }

        if let Some(bootstrap_gossip) = bootstrap_gossip {
            cmd.arg("--entrypoint").arg(bootstrap_gossip);
        }

        info!("Starting {} node with command: {:?}", node_name, cmd);
        // Print the command as it would appear on the CLI
        use std::ffi::OsStr;
        let cmd_str = std::iter::once(cmd.get_program())
            .chain(cmd.get_args())
            .map(|s| s.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        println!("CLI Command: {}", cmd_str);

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
                    println!("[{}] {}", node_name, line);
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
                    println!("[{}] {}", node_name, line);
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

    fn create_snapshot(validator_ledger_path: &PathBuf) -> anyhow::Result<()> {
        // Determine validator binary path
        let ledger_tool_binary =
            "/Users/lucasbruder/jito/jito-solana-jds/target/debug/agave-ledger-tool";

        let mut cmd = Command::new(ledger_tool_binary);

        cmd.env("RUST_LOG", "info")
            .arg("--ledger")
            .arg(validator_ledger_path.to_str().unwrap())
            .arg("create-snapshot")
            .arg("0");

        let status = cmd.status()?;
        if !status.success() {
            return Err(anyhow::anyhow!(
                "Failed to create snapshot: process exited with status {}",
                status
            ));
        }
        Ok(())
    }
}
