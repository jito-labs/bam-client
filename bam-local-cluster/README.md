# BAM Local Cluster

This package provides a local Solana cluster bootstrapper specifically designed for BAM (Block Assembly Marketplace) testing.

## Overview

The `bam-local-cluster` package contains a binary that spins up a local Solana cluster from a TOML configuration file. It's designed to be used for BAM (Block Assembly Marketplace) testing scenarios and includes:

- Local cluster setup with configurable validators
- HTTP server for cluster information and health checks
- Faucet integration for airdrops
- Tip manager configuration for BAM-specific features

## Usage

```bash
# Run with info logging (from top-level directory)
RUST_LOG=info cargo run --bin bam-local-cluster -- --config bam-local-cluster/examples/example_config.toml

# Run with debug logging (more verbose)
RUST_LOG=debug cargo run --bin bam-local-cluster -- --config bam-local-cluster/examples/example_config.toml

# Run from within the bam-local-cluster directory
cd bam-local-cluster
RUST_LOG=info cargo run -- --config examples/example_config.toml
```

## Configuration

The configuration file should be in TOML format and include:

- `bam_url`: URL for BAM service (currently unused but required)
- `info_address`: HTTP server address for cluster info
- `tip_payment_program_id`: Program ID for tip payments
- `tip_distribution_program_id`: Program ID for tip distribution
- `faucet_address`: Address for the faucet service
- `validators`: Array of validator configurations

### Validator Configuration

Each validator in the `validators` array can have:

- `geyser_config`: Optional path to geyser plugin configuration file

RPC ports are assigned dynamically by the local cluster system.

See `examples/example_config.toml` for a complete example.

## RPC Port Discovery

**Important Gotcha**: RPC ports are assigned dynamically by the local cluster system and cannot be hard-coded in the configuration. The ports you specify in config files will be overwritten.

To discover the actual RPC endpoint at runtime, use the HTTP server endpoint:

```bash
# Get cluster info including the RPC endpoint
curl http://127.0.0.1:12346/cluster-info

# Example response:
# {
#   "rpc_endpoint": "http://127.0.0.1:12345"
# }
```

You can then use the returned RPC endpoint in your applications:

```bash
# Use the discovered RPC endpoint
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
  http://127.0.0.1:12345
```

## HTTP Endpoints

- `/cluster-info`: Returns one of the RPC endpoints that can be used to discover the rest of the cluster
- `/exit`: Graceful shutdown endpoint

## Building

```bash
# Build the binary
cargo build

# Build and run with logging
RUST_LOG=info cargo run -- --config examples/example_config.toml
```

## Development

The code is organized into modules:
- `src/config.rs`: Configuration parsing and structures
- `src/http_server.rs`: HTTP server and endpoints
- `src/cluster_manager.rs`: Main cluster management logic
- `src/main.rs`: Binary entry point 