# STL Verify - Block Watcher

A real-time Ethereum block watcher service that subscribes to new blocks via WebSocket, handles chain reorganizations, and publishes block data to SNS for downstream processing.

## Architecture

The watcher consists of two main services:

- **Live Data Service**: Subscribes to new block headers via Alchemy WebSocket, fetches full block data (receipts, traces, blobs), detects and handles chain reorgs, and publishes events to SNS
- **Backfill Service** (optional): Fills gaps in block data by polling the database for missing blocks and fetching them via HTTP

## Prerequisites

- Go 1.26+
- Docker
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) — Kubernetes IN Docker
- [kubectl](https://kubernetes.io/docs/tasks/tools/) — Kubernetes CLI

## Quick Start

```bash
make dev-up
```

This creates a local `kind` Kubernetes cluster and deploys the full pipeline. See [k8s/README.md](k8s/README.md) for service ports, lifecycle commands, and configuration details.

## Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-disable-blobs` | Disable fetching blob sidecars | `false` |
| `-pprof` | Enable pprof profiling server (e.g., `:6060`) | disabled |

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `ALCHEMY_API_KEY` | Alchemy API key for Ethereum mainnet |

### Optional

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable` |
| `REDIS_ADDR` | Redis server address | `localhost:6379` |
| `REDIS_PASSWORD` | Redis password | (empty) |
| `AWS_SNS_ENDPOINT` | SNS endpoint URL (for LocalStack) | `http://localhost:4566` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `test` |
| `JAEGER_ENDPOINT` | Jaeger OTLP endpoint | `localhost:4317` |
| `ENVIRONMENT` | Environment name for tracing | `development` |
| `ENABLE_BACKFILL` | Enable gap backfill service | `false` |

## Development

### Run Tests

```bash
# Run all tests with race detector
make test-race

# Run tests with coverage
make cover
```

### Run CI Checks

```bash
# Run full CI suite (tests, vet, format check, staticcheck, vulncheck, golangci-lint)
make ci
```

### Install Development Tools

```bash
make tools
```
### Changes to services in Infrastructure

When you add a new service (e.g., a new worker or cmd entrypoint) that needs to run in AWS, provision the required AWS resources in the [Infrastructure repo](https://github.com/archon-research/infrastructure).

### View Traces

Open the Jaeger UI at http://localhost:16686 to view distributed traces.

## Cache Key Convention

Block data is cached in Redis with the following key format:

```
stl:{chainId}:{blockNumber}:{version}:{dataType}
```

Where:
- `chainId` - The blockchain network ID (e.g., `1` for Ethereum mainnet)
- `blockNumber` - The block height
- `version` - Incremented on chain reorgs (starts at `0`)
- `dataType` - One of: `block`, `receipts`, `traces`, `blobs`

**Examples:**
```
stl:1:12345:0:block     # Block 12345, first version, block data
stl:1:12345:0:receipts  # Block 12345, first version, transaction receipts
stl:1:12345:1:block     # Block 12345, second version (after reorg)
```

Consumers receive `BlockEvent` messages via SNS/SQS and derive cache keys using this convention.

## Graceful Shutdown

The watcher handles `SIGINT` and `SIGTERM` signals gracefully:

1. Stops accepting new blocks
2. Completes in-flight processing
3. Closes database connections
4. Shuts down within 25 seconds (compatible with Fargate's 30s default)

Press `Ctrl+C` to stop the watcher.
