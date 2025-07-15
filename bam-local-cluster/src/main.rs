use {
    bam_local_cluster::{BamLocalCluster, LocalClusterConfig},
    clap::{App, Arg},
    log::{error, info},
    solana_logger::setup,
};

fn main() {
    setup();

    let matches = App::new("BAM LocalCluster bootstrapper")
        .version("0.1")
        .about("Spins up a local Solana cluster from a TOML config for BAM testing")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("TOML configuration file path")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    let config = LocalClusterConfig::from_file(config_path).expect("Failed to parse TOML");

    info!("Starting cluster with config: {:?}", config);
    let cluster = BamLocalCluster::new(config.clone()).expect("Failed to start cluster");

    // Run the cluster (this will block until shutdown is requested)
    if let Err(e) = cluster.run() {
        error!("Cluster error: {}", e);
    }

    // Graceful shutdown
    cluster.shutdown();
}
