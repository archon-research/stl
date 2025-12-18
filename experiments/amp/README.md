# Amp Evaluation Demo

This folder contains a demo project to evaluate [Amp](https://github.com/edgeandnode/amp) - the blockchain native database from Edge & Node.

## What is Amp?

Amp is a tool for building and managing blockchain datasets. It provides:
- SQL interface for querying blockchain data
- JSON Lines over HTTP server
- Arrow Flight (gRPC) server for high-performance data access
- OpenTelemetry integration for metrics and traces

## Prerequisites

1. **Docker & Docker Compose** - For running PostgreSQL metadata database
2. **Rust** - For building from source (optional)
3. **ampup** - The official version manager and installer

## Installation

### Option 1: Using ampup (Recommended)

```bash
# Install ampup
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh

# Restart terminal or source your shell config
source ~/.zshenv  # or ~/.bashrc, ~/.zshrc

# Install latest version
ampup install
```

### Option 2: Build from Source

```bash
cargo build --release -p ampd
# Binary will be at target/release/ampd
```

## Quick Start

### 1. Start the metadata database

```bash
docker-compose up -d
```

This starts PostgreSQL at `postgresql://postgres:postgres@localhost:5432/amp`

### 2. Run the Amp server

```bash
ampd server
```

This starts:
- HTTP server (JSON Lines) on port 1603
- Arrow Flight (gRPC) server

### 3. Query data

```bash
# Example query
curl -X POST http://localhost:1603 --data 'select * from "my_namespace/eth_rpc".logs limit 10'
```

## Configuration

Create an `amp.toml` file to configure Amp:

```toml
metadata_db_url = "postgresql://postgres:postgres@localhost:5432/amp"
```

See [config documentation](https://github.com/edgeandnode/amp/blob/main/docs/config.md) for more options.

## Python Client

For Python integration, see [amp-python](https://github.com/edgeandnode/amp-python).

## Telemetry

Amp includes OpenTelemetry support. The docker-compose setup includes Grafana LGTM stack for local testing.

## Evaluation Goals

- [ ] Install and run Amp locally
- [ ] Connect to Ethereum RPC data source
- [ ] Query blockchain data (logs, transactions, blocks)
- [ ] Evaluate query performance
- [ ] Test Python client integration
- [ ] Explore custom dataset creation

## Resources

- [GitHub Repository](https://github.com/edgeandnode/amp)
- [Configuration Docs](https://github.com/edgeandnode/amp/blob/main/docs/config.md)
- [Telemetry Docs](https://github.com/edgeandnode/amp/blob/main/docs/telemetry.md)
- [Python Client](https://github.com/edgeandnode/amp-python)
