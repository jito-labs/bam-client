use {
    crate::config::{CustomValidatorConfig, LocalClusterConfig},
    anyhow::{Context, Result},
    log::{debug, error, info},
    solana_faucet::faucet::{run_faucet, Faucet},
    solana_ledger::{
        blockstore::create_new_ledger, blockstore_options::LedgerColumnOptions,
        genesis_utils::GenesisConfigInfo,
    },
    solana_local_cluster::integration_tests::DEFAULT_NODE_STAKE,
    solana_program_test::programs::spl_programs,
    solana_runtime::genesis_utils::{create_genesis_config_with_leader_ex, ValidatorVoteKeypairs},
    solana_sdk::{
        account::Account,
        fee_calculator::FeeRateGovernor,
        genesis_config::{ClusterType, GenesisConfig},
        native_token::LAMPORTS_PER_SOL,
        rent::Rent,
        shred_version::compute_shred_version,
        signature::Keypair,
        signer::Signer,
        system_program,
    },
    solana_stake_program::stake_state,
    solana_vote_program::vote_state,
    std::{
        borrow::Borrow,
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

pub struct BamValidator {
    process: Child,
    node_name: String,
}

impl BamValidator {
    fn start_process(
        ledger_path: &PathBuf,
        log_file_path: &PathBuf,
        node_name: &str,
        gossip_port: Option<u16>,
        rpc_port: u16,
        dynamic_port_range_start: u64,
        dynamic_port_range_end: u64,
        cluster_config: &LocalClusterConfig,
        genesis_config: &GenesisConfig,
        bootstrap_gossip: Option<&str>,
        expected_bank_hash: Option<String>,
        identity_path: &PathBuf,
        vote_path: &PathBuf,
        runtime: &Runtime,
        config: &CustomValidatorConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let validator_binary = format!("{}/agave-validator", cluster_config.validator_build_path);

        let mut cmd = Command::new(validator_binary);

        cmd.env("RUST_LOG", "info")
            .arg("--log")
            .arg(log_file_path)
            .arg("--ledger")
            .arg(ledger_path)
            .arg("--identity")
            .arg(&identity_path)
            .arg("--vote-account")
            .arg(&vote_path)
            .arg("--authorized-voter")
            .arg(&vote_path)
            .arg("--bind-address")
            .arg("0.0.0.0")
            .arg("--dynamic-port-range")
            .arg(format!(
                "{}-{}",
                dynamic_port_range_start, dynamic_port_range_end
            ))
            .arg("--no-wait-for-vote-to-start-leader")
            .arg("--wait-for-supermajority")
            .arg("0")
            .arg("--rpc-port")
            .arg(rpc_port.to_string());
            .arg("--rpc-faucet-address")
            .arg(cluster_config.faucet_address.to_string())
            .arg("--rpc-pubsub-enable-block-subscription")
            .arg("--rpc-pubsub-enable-vote-subscription")
            .arg("--account-index")
            .arg("program-id")
            .arg("--allow-private-addr")
            .arg("--full-rpc-api")
            .arg("--enable-rpc-transaction-history")
            .arg("--enable-extended-tx-metadata-storage")
            .arg("--expected-shred-version")
            .arg(compute_shred_version(&genesis_config.hash(), None).to_string())
            .arg("--bam-url")
            .arg(cluster_config.bam_url.to_string())
            .arg("--tip-distribution-program-pubkey")
            .arg(cluster_config.tip_distribution_program_id.to_string())
            .arg("--tip-payment-program-pubkey")
            .arg(cluster_config.tip_payment_program_id.to_string())
            .arg("--merkle-root-upload-authority")
            .arg("11111111111111111111111111111111")
            .arg("--commission-bps")
            .arg("100");

        if let Some(expected_bank_hash) = expected_bank_hash {
            cmd.arg("--expected-bank-hash").arg(expected_bank_hash);
        }

        if let Some(gossip_port) = gossip_port {
            cmd.arg("--gossip-port").arg(gossip_port.to_string());
        }

        if let Some(bootstrap_gossip) = bootstrap_gossip {
            cmd.arg("--entrypoint").arg(bootstrap_gossip);
        }

        if let Some(geyser_config) = &config.geyser_config {
            cmd.arg("--geyser-plugin-config").arg(geyser_config);
        }

        info!("Starting {} node with command: {:?}", node_name, cmd);

        // Print the command as it would appear on the CLI
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

        // Spawn tasks to stream the output and tail the log file
        let child_stdout = child.stdout.take();
        let child_stderr = child.stderr.take();
        let log_file_path = log_file_path.clone();
        let node_name = node_name.to_string();

        if let Some(stdout) = child_stdout {
            let node_name = node_name.clone();
            runtime.spawn(async move {
                Self::stream_output_stdout(stdout, &node_name).await;
            });
        }

        if let Some(stderr) = child_stderr {
            let node_name = node_name.clone();
            runtime.spawn(async move {
                Self::stream_output_stderr(stderr, &node_name).await;
            });
        }

        // Spawn log file tailing
        {
            let node_name = node_name.clone();
            runtime.spawn(async move {
                Self::tail_log_file(&log_file_path, &node_name).await;
            });
        }

        Ok(Self {
            process: child,
            node_name: node_name.to_string(),
        })
    }

    pub fn get_bank_hash(ledger_path: &PathBuf, build_path: &str) -> Result<String, anyhow::Error> {
        let ledger_tool_binary = format!("{}/agave-ledger-tool", build_path);

        let mut cmd = std::process::Command::new(ledger_tool_binary);

        cmd.arg("-l")
            .arg(ledger_path.to_str().unwrap())
            .arg("verify")
            .arg("--print-bank-hash");

        let output = cmd.output()?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            error!("stdout: {}", stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("stderr: {}", stderr);
            return Err(anyhow::anyhow!(
                "Failed to run agave-ledger-tool: process exited with status {}",
                output.status
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if let Some(hash) = line.strip_prefix("Bank hash for slot 0: ") {
                return Ok(hash.trim().to_string());
            }
        }

        Err(anyhow::anyhow!(
            "Bank hash for slot 0 not found in agave-ledger-tool output"
        ))
    }

    async fn stream_output_stdout(stdout: std::process::ChildStdout, node_name: &str) {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let reader = BufReader::new(tokio::process::ChildStdout::from_std(stdout).unwrap());
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            println!("[\x1b[35m{}\x1b[0m] stdout: {}", node_name, line);
        }
    }

    async fn stream_output_stderr(stderr: std::process::ChildStderr, node_name: &str) {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let reader = BufReader::new(tokio::process::ChildStderr::from_std(stderr).unwrap());
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            println!("[\x1b[35m{}\x1b[0m] stderr: {}", node_name, line);
        }
    }

    async fn tail_log_file(log_file_path: &PathBuf, node_name: &str) {
        use std::io;
        use tokio::fs::File;
        use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom};

        // Wait a bit for the file to be created
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        loop {
            match File::open(log_file_path).await {
                Ok(mut file) => {
                    // Seek to the end of the file to only show new lines
                    if let Err(e) = file.seek(SeekFrom::End(0)).await {
                        eprintln!("Failed to seek log file: {}", e);
                        return;
                    }
                    let mut reader = BufReader::new(file);
                    let mut buf = String::new();

                    loop {
                        buf.clear();
                        match reader.read_line(&mut buf).await {
                            Ok(0) => {
                                // No new line, wait and try again
                                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                            }
                            Ok(_) => {
                                // Print the new log line
                                print!("[\x1b[35m{}\x1b[0m] LOG: {}", node_name, buf);
                            }
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            Err(e) => {
                                eprintln!("Error reading log file: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(_) => {
                    // File doesn't exist yet, wait and retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub fn is_running(&mut self) -> bool {
        self.process
            .try_wait()
            .map(|status| status.is_none())
            .unwrap_or(false)
    }

    pub fn kill(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.process.kill()?;
        self.process.wait()?;
        Ok(())
    }

    fn create_snapshot(validator_ledger_path: &PathBuf, build_path: &str) -> anyhow::Result<()> {
        let ledger_tool_binary = format!("{}/agave-ledger-tool", build_path);

        let mut cmd = Command::new(ledger_tool_binary);

        cmd.env("RUST_LOG", "info")
            .arg("--ledger")
            .arg(validator_ledger_path.to_str().unwrap())
            .arg("create-snapshot")
            .arg("0");

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

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

pub struct BamLocalCluster {
    validators: Arc<Mutex<Vec<BamValidator>>>,
    runtime: Runtime,
}

impl BamLocalCluster {
    pub fn new(config: LocalClusterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        const BOOTSTRAP_GOSSIP: &'static str = "127.0.0.1:8001";
        const BOOTSTRAP_GOSSIP_PORT: u16 = 8001;
        const BOOTSTRAP_RPC_PORT: u16 = 8899;

        // Generate keypairs for all validators
        let num_validators = config.validators.len();
        let vote_keypairs: Vec<ValidatorVoteKeypairs> = (0..num_validators)
            .map(|_| ValidatorVoteKeypairs {
                node_keypair: Keypair::new(),
                vote_keypair: Keypair::new(),
                stake_keypair: Keypair::new(),
            })
            .collect();
        let stakes = vec![DEFAULT_NODE_STAKE; num_validators];
        let genesis_config_info = Self::create_genesis_config_with_vote_accounts_and_cluster_type(
            solana_local_cluster::local_cluster::DEFAULT_MINT_LAMPORTS,
            &vote_keypairs,
            stakes,
            ClusterType::MainnetBeta,
        );

        let runtime = Runtime::new().expect("Could not create Tokio runtime");

        // Start faucet
        let faucet_address = SocketAddr::from_str(&config.faucet_address)?;
        let faucet = Arc::new(Mutex::new(Faucet::new(
            genesis_config_info.mint_keypair.insecure_clone(),
            None,
            None,
            None,
        )));
        runtime.spawn(run_faucet(faucet, faucet_address, None));

        let mut validators = Vec::new();
        let mut expected_bank_hash = None;

        // Process all validators
        for (i, validator_config) in config.validators.iter().enumerate() {
            let is_bootstrap = i == 0; // First validator is always bootstrap
            let ValidatorVoteKeypairs {
                node_keypair,
                vote_keypair,
                stake_keypair: _,
            } = &vote_keypairs[i];

            // Setup ledger before creating validator
            let node_name = if is_bootstrap {
                "bootstrap-ledger".to_string()
            } else {
                format!("validator-{}", i)
            };
            let ledger_path = Path::new(&config.ledger_base_directory).join(&node_name);
            if ledger_path.exists() {
                fs::remove_dir_all(&ledger_path)?;
            }
            fs::create_dir_all(&ledger_path).context(format!(
                "Failed to create ledger directory: {:?}",
                ledger_path
            ))?;

            // Create ledger and snapshot if bootstrap; other validators need to have snapshot
            // to download from the bootstrap validator to start
            if is_bootstrap {
                create_new_ledger(
                    &ledger_path,
                    &genesis_config_info.genesis_config,
                    10737418240,
                    LedgerColumnOptions::default(),
                )?;
                BamValidator::create_snapshot(&ledger_path, &config.validator_build_path)?;

                info!("Getting bank hash for bootstrap validator");
                let bank_hash =
                    BamValidator::get_bank_hash(&ledger_path, &config.validator_build_path)?;
                info!("Bank hash for slot 0: {:?}", bank_hash);
                expected_bank_hash = Some(bank_hash);
            }

            let identity_path = ledger_path.join("identity.json");
            let vote_path = ledger_path.join("vote.json");

            fs::write(
                &identity_path,
                serde_json::to_string_pretty(&node_keypair.to_bytes().to_vec())?,
            )?;
            fs::write(
                &vote_path,
                serde_json::to_string_pretty(&vote_keypair.to_bytes().to_vec())?,
            )?;

            let log_file_path = ledger_path.join("validator.log");

            let dynamic_port_range_start = 8_000 + (i * 1000) as u64;
            let dynamic_port_range_end = 8_000 + ((i + 1) * 1000) as u64;

            let validator = BamValidator::start_process(
                &ledger_path,
                &log_file_path,
                &node_name,
                is_bootstrap.then_some(BOOTSTRAP_GOSSIP_PORT),
                if is_bootstrap {
                    BOOTSTRAP_RPC_PORT
                } else {
                    dynamic_port_range_start as u16 + 100
                },
                dynamic_port_range_start,
                dynamic_port_range_end,
                &config,
                &genesis_config_info.genesis_config,
                if !is_bootstrap {
                    Some(BOOTSTRAP_GOSSIP)
                } else {
                    None
                },
                expected_bank_hash.clone(),
                &identity_path,
                &vote_path,
                &runtime,
                &validator_config,
            )?;
            validators.push(validator);

            // Need to wait for gossip to start on bootstrap validator
            if is_bootstrap {
                sleep(Duration::from_secs(5));
            }
        }

        let validators = std::sync::Arc::new(std::sync::Mutex::new(validators));

        Ok(Self {
            validators,
            runtime,
        })
    }

    /// Similar to `create_genesis_config_with_vote_accounts_and_cluster_type` but with
    /// real rent and real fees.
    pub fn create_genesis_config_with_vote_accounts_and_cluster_type(
        mint_lamports: u64,
        voting_keypairs: &[impl Borrow<ValidatorVoteKeypairs>],
        stakes: Vec<u64>,
        cluster_type: ClusterType,
    ) -> GenesisConfigInfo {
        const VALIDATOR_LAMPORTS: u64 = 100000 * LAMPORTS_PER_SOL;

        assert!(!voting_keypairs.is_empty());
        assert_eq!(voting_keypairs.len(), stakes.len());

        let mint_keypair = Keypair::new();
        let voting_keypair = voting_keypairs[0].borrow().vote_keypair.insecure_clone();

        let validator_pubkey = voting_keypairs[0].borrow().node_keypair.pubkey();
        let genesis_config = create_genesis_config_with_leader_ex(
            mint_lamports,
            &mint_keypair.pubkey(),
            &validator_pubkey,
            &voting_keypairs[0].borrow().vote_keypair.pubkey(),
            &voting_keypairs[0].borrow().stake_keypair.pubkey(),
            stakes[0],
            VALIDATOR_LAMPORTS,
            FeeRateGovernor::default(),
            Rent::default(),
            cluster_type,
            vec![],
        );

        let mut genesis_config_info = GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            voting_keypair,
            validator_pubkey,
        };

        for (validator_voting_keypairs, stake) in voting_keypairs[1..].iter().zip(&stakes[1..]) {
            let node_pubkey = validator_voting_keypairs.borrow().node_keypair.pubkey();
            let vote_pubkey = validator_voting_keypairs.borrow().vote_keypair.pubkey();
            let stake_pubkey = validator_voting_keypairs.borrow().stake_keypair.pubkey();

            // Create accounts
            let node_account = Account::new(VALIDATOR_LAMPORTS, 0, &system_program::id());
            let vote_account = vote_state::create_account(&vote_pubkey, &node_pubkey, 0, *stake);
            let stake_account = Account::from(stake_state::create_account(
                &stake_pubkey,
                &vote_pubkey,
                &vote_account,
                &genesis_config_info.genesis_config.rent,
                *stake,
            ));

            let vote_account = Account::from(vote_account);

            // Put newly created accounts into genesis
            genesis_config_info.genesis_config.accounts.extend(vec![
                (node_pubkey, node_account),
                (vote_pubkey, vote_account),
                (stake_pubkey, stake_account),
            ]);
        }

        genesis_config_info.genesis_config.fee_rate_governor = FeeRateGovernor::default();

        // Add SPL programs
        for (pubkey, account) in spl_programs(&genesis_config_info.genesis_config.rent) {
            genesis_config_info
                .genesis_config
                .add_account(pubkey, account);
        }

        genesis_config_info
    }

    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Start process monitoring
        let validators_ptr = self.validators.clone();
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        self.runtime.spawn(async move {
            Self::monitor_validators(validators_ptr, tx).await;
        });

        // Wait for Ctrl+C or validator death
        self.runtime.block_on(async {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("Received Ctrl+C, shutting down...");
                }
                _ = &mut rx => {
                    error!("A validator died, shutting down cluster...");
                }
            }
        });

        Ok(())
    }

    async fn monitor_validators(
        validators: std::sync::Arc<std::sync::Mutex<Vec<BamValidator>>>,
        exit_tx: tokio::sync::oneshot::Sender<()>,
    ) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            if let Ok(mut validators_guard) = validators.lock() {
                for (i, validator) in validators_guard.iter_mut().enumerate() {
                    if !validator.is_running() {
                        error!(
                            "Validator process {} ({}) is not running - exiting cluster",
                            i, validator.node_name
                        );
                        // Signal exit and return
                        let _ = exit_tx.send(());
                        return;
                    } else {
                        debug!(
                            "Validator process {} ({}) is running",
                            i, validator.node_name
                        );
                    }
                }
            }
        }
    }

    pub fn shutdown(self) {
        info!("Shutting down cluster...");

        // Terminate all validator processes
        for mut validator in self.validators.lock().unwrap().drain(..) {
            if let Err(e) = validator.kill() {
                error!("Failed to kill validator {}: {}", validator.node_name, e);
            }
        }
    }
}
