use solana_faucet::faucet::{run_faucet, Faucet};
use solana_program_test::programs::spl_programs;
use solana_program_test::tokio;
use solana_program_test::tokio::runtime::Runtime;
use solana_rpc::rpc::JsonRpcConfig;
use solana_sdk::rent::Rent;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Mutex;
use {
    clap::{App, Arg},
    log::info,
    serde::Deserialize,
    solana_core::{
        tip_manager::{TipDistributionAccountConfig, TipManagerConfig},
        validator::ValidatorConfig,
    },
    solana_local_cluster::{
        integration_tests::DEFAULT_NODE_STAKE,
        local_cluster::{ClusterConfig, LocalCluster, DEFAULT_MINT_LAMPORTS},
    },
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{fs, iter, path::PathBuf, sync::Arc, thread::sleep, time::Duration},
};

#[derive(Debug, Deserialize)]
struct LocalClusterConfig {
    // bam_url: String,
    tip_payment_program_id: String,
    tip_distribution_program_id: String,
    faucet_address: String,
    validators: Vec<CustomValidatorConfig>,
}

#[derive(Debug, Deserialize)]
struct CustomValidatorConfig {
    geyser_config: Option<PathBuf>,
}

fn main() {
    solana_logger::setup();

    let matches = App::new("BAM LocalCluster bootstrapper")
        .version("0.1")
        .about("Spins up a local Solana cluster from a TOML config")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("TOML configuration file path")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let config_str = fs::read_to_string(matches.value_of("config").unwrap())
        .expect("Unable to read config file");
    let config: LocalClusterConfig = toml::from_str(&config_str).expect("Failed to parse TOML");

    info!("Loaded config: {:?}", config);

    let faucet_address =
        SocketAddr::from_str(&config.faucet_address).expect("Invalid faucet address");

    let all_configs = config.validators.iter().map(|v| {
        let identity_keypair = Keypair::new();
        let vote_account_keypair = Keypair::new();

        let mut validator_config = ValidatorConfig::default_for_test();

        validator_config.blockstore_options.enforce_ulimit_nofile = false;
        validator_config.on_start_geyser_plugin_config_files =
            v.geyser_config.as_ref().map(|geyser| vec![geyser.clone()]);
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
        // setup RPC to forward requests to faucet
        validator_config.rpc_config = JsonRpcConfig {
            enable_rpc_transaction_history: true,
            enable_extended_tx_metadata_storage: true,
            faucet_addr: Some(faucet_address),
            full_api: true,
            ..JsonRpcConfig::default()
        };

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

    // Start the cluster
    info!("Starting local cluster with Geyser plugins...");
    let mut cluster = LocalCluster::new(&mut cluster_config, SocketAddrSpace::new(true));
    info!("Started cluster");

    let faucet = Arc::new(Mutex::new(Faucet::new(
        cluster.funding_keypair.insecure_clone(),
        None,
        None,
        None,
    )));
    let runtime = Runtime::new().expect("Could not create Tokio runtime");
    runtime.spawn(run_faucet(faucet, faucet_address, None));

    info!("Stopping cluster");
    cluster.exit();
    info!("Stopped cluster");
}
