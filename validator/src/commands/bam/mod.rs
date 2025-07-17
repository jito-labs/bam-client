use {
    crate::{admin_rpc_service, cli::DefaultArgs},
    clap::{value_t_or_exit, App, Arg, ArgMatches, SubCommand},
    std::path::Path,
};

pub fn command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-bam-config")
        .about("Set configuration for connection to a BAM node")
        .arg(
            Arg::with_name("bam_url")
                .long("bam-url")
                .help("URL of BAM Node; leave empty to disable BAM")
                .takes_value(true),
        )
}

pub fn execute(subcommand_matches: &ArgMatches, ledger_path: &Path) -> crate::commands::Result<()> {
    let bam_url = value_t_or_exit!(subcommand_matches, "bam_url", String);
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move { admin_client.await?.set_bam_url(Some(bam_url)).await })?;
    Ok(())
}
