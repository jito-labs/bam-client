# BAM Local Cluster

A tool for spinning up local Solana clusters with BAM (Block Assembly Marketplace) support for testing purposes. This tool uses subprocess-based execution to spawn `agave-validator` instances.

## Features

- **Subprocess-based execution**: Uses `agave-validator` to spawn validator processes
- **Auto-detection of validator binary**: Automatically finds the validator binary in common locations
- **Real-time output streaming**: Captures and displays validator stdout/stderr in real-time
- **Unified validator configuration**: Bootstrap node is treated as the first validator
- **Multiple validators**: Supports running multiple validator nodes
- **Dynamic keypair generation**: Automatically generates and manages identity and vote keypairs
- **Dynamic ledger paths**: Creates ledger directories automatically (bootstrap-ledger, validator-1-ledger, etc.)
- **Geyser plugin support**: Optional geyser plugin configuration for each validator
- **HTTP API**: Provides cluster information and shutdown endpoints
- **Faucet integration**: Built-in faucet for airdrops
- **Super majority waiting**: Uses `--wait-for-supermajority` for proper cluster startup
- **Process monitoring**: Continuous monitoring of validator process health with real-time status updates

## Quick Start

1. **Build the binaries**:
   ```bash
   # Build agave-validator
   cargo build --release --bin agave-validator
   
   # Build bam-local-cluster
   cargo build --release --bin bam-local-cluster
   ```

2. **Create a configuration file** (see `examples/example_config.toml`):
   ```toml
   # BAM service URL
   bam_url = "http://127.0.0.1:45678"
   
   # Tip manager configuration
   tip_payment_program_id = "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt"
   tip_distribution_program_id = "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7"
   
   # Faucet address for airdrops
   faucet_address = "127.0.0.1:12345"
   
   # HTTP server address for cluster info
   info_address = "127.0.0.1:8080"
   
   # Base directory for ledger storage
   ledger_base_directory = "config"
   
   # Optional: Path to the validator binary (if not specified, will auto-detect)
   # validator_binary_path = "./target/debug/agave-validator"
   
   # Validator configurations
   # The first validator is the bootstrap node
   [[validators]]
   # Optional: Path to geyser plugin configuration
   # geyser_config = "./geyser-config.json"
   
   [[validators]]
   # Second validator (optional)
   # geyser_config = "./geyser-config-2.json"
   ```

3. **Run the cluster**:
   ```bash
   # Using the provided script
   ./scripts/run_cluster.sh examples/example_config.toml
   
   # Or directly
   ./target/release/bam-local-cluster --config examples/example_config.toml
   ```

## Configuration

### Validators

All validators (including the bootstrap node) use the same configuration structure:

- `rpc_port`: RPC port for the validator
- `ledger_path`: Name of the ledger directory (will be created under `ledger_base_directory`)
- `gossip_port`: Gossip port for the validator
- `is_bootstrap`: Set to `true` for the bootstrap node, `false` for regular validators
- `geyser_config`: Optional path to geyser plugin configuration

### Bootstrap Node

The bootstrap node is simply the first validator with `is_bootstrap = true`. It serves as the entrypoint for the cluster and provides a known gossip address that other validators connect to.

### Dynamic Generation

The cluster automatically:

- **Generates keypairs**: Creates identity and vote keypairs for all nodes
- **Creates ledger directories**: Builds ledger paths like `config/bootstrap-ledger`, `config/validator-1-ledger`, etc.
- **Manages temporary files**: Uses temporary directories for keypair storage
- **Copies genesis**: Ensures all nodes have the same genesis configuration
- **Starts in order**: Bootstrap node starts first, then other validators connect to it

## HTTP API

The cluster provides an HTTP API for monitoring and control:

- `GET /cluster-info`: Get cluster information (RPC endpoint, bootstrap gossip address)
- `GET /exit`: Gracefully shutdown the cluster

## Architecture

The BAM Local Cluster uses a subprocess-based architecture:

1. **Genesis Creation**: Creates a genesis configuration with SPL programs
2. **Directory Setup**: Creates ledger directories under the base directory
3. **Keypair Generation**: Generates identity and vote keypairs for all nodes
4. **Bootstrap Node**: Starts the first validator as bootstrap node
5. **Validator Nodes**: Spawns remaining validator processes that connect to the bootstrap node
6. **Faucet**: Runs a faucet service for airdrops
7. **HTTP Server**: Provides cluster information and control endpoints

## Process Management

- All validator processes are spawned as child processes
- Bootstrap node starts first with a 3-second delay
- Other validators start with 1-second delays between them
- Graceful shutdown terminates all child processes
- Process output is captured for debugging
- Ctrl+C triggers graceful shutdown
- Temporary files are automatically cleaned up

## File Structure

When running with `ledger_base_directory = "config"`, the following structure is created:

```
config/
├── bootstrap-ledger/     # Bootstrap node ledger
├── validator-1-ledger/   # First validator ledger
├── validator-2-ledger/   # Second validator ledger
└── validator-3-ledger/   # Third validator ledger
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure all ports in the configuration are available
2. **Permission errors**: Make sure the ledger base directory is writable
3. **Binary not found**: Ensure `agave-validator` is built and available at `./target/release/agave-validator`
4. **Startup timing**: The bootstrap node needs time to start before other validators connect

### Debugging

- Check process output for error messages
- Verify network connectivity between nodes
- Ensure genesis configuration is properly written
- Check that ledger directories are created successfully
- Monitor startup timing - validators need the bootstrap node to be ready

## Development

To modify the cluster behavior:

1. Update `src/cluster_manager.rs` for process spawning logic
2. Modify `src/config.rs` for configuration structure
3. Update `src/http_server.rs` for API endpoints

## License

This project is part of the Jito Solana JDS repository and follows the same license terms. 