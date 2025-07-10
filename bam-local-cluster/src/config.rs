use {
    serde::{Deserialize, Serialize},
    std::path::PathBuf,
};

#[derive(Debug, Deserialize, Clone)]
pub struct LocalClusterConfig {
    pub bam_url: String,
    pub info_address: String,
    pub tip_payment_program_id: String,
    pub tip_distribution_program_id: String,
    pub faucet_address: String,
    pub validators: Vec<CustomValidatorConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CustomValidatorConfig {
    pub geyser_config: Option<PathBuf>,
    /// Optional hard-coded RPC port for this validator
    pub rpc_port: Option<u16>,
    /// Optional hard-coded RPC pubsub port for this validator
    pub rpc_pubsub_port: Option<u16>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterInfo {
    pub rpc_endpoint: String,
}

impl LocalClusterConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: LocalClusterConfig = toml::from_str(&config_str)?;
        Ok(config)
    }
} 