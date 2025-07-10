# BAM Local Cluster

This package provides a local Solana cluster bootstrapper specifically designed for BAM (Blockchain Application Manager) testing.

## Overview

The `bam-local-cluster` package contains a binary that spins up a local Solana cluster from a TOML configuration file. It's designed to be used for BAM testing scenarios and includes:

- Local cluster setup with configurable validators
- HTTP server for cluster information
- Faucet integration for airdrops
- Tip manager configuration for BAM-specific features

## Usage

```bash
cargo run --bin bam-local-cluster -- --config examples/example_config.toml
```

## Configuration

The configuration file should be in TOML format and include:

- `bam_url`: URL for BAM service
- `info_address`: HTTP server address for cluster info
- `tip_payment_program_id`: Program ID for tip payments
- `tip_distribution_program_id`: Program ID for tip distribution
- `faucet_address`: Address for the faucet service
- `validators`: Array of validator configurations

See `examples/example_config.toml` for a complete example.

## HTTP Endpoints

- `/cluster-info`: Returns cluster information including RPC endpoint
- `/exit`: Graceful shutdown endpoint

## Building

```bash
cargo build --bin bam-local-cluster
``` 