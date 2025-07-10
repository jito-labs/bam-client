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
# Run with info logging
RUST_LOG=info cargo run -- --config examples/example_config.toml

# Run with debug logging (more verbose)
RUST_LOG=debug cargo run -- --config examples/example_config.toml
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

## HTTP Endpoints

- `/cluster-info`: Returns cluster information including RPC endpoint
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