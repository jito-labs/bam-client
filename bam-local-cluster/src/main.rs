use bam_local_cluster::{BamLocalCluster, LocalClusterConfig};
use clap::{App, Arg};
use solana_logger::setup;

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
    let mut cluster = BamLocalCluster::new(config.clone()).expect("Failed to start cluster");
    cluster.run_http_server(&config).expect("Failed to run HTTP server");
    cluster.shutdown();
} 