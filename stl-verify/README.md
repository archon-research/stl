# STL Verify - Block Watcher

A real-time Ethereum block watcher service that subscribes to new blocks via WebSocket, handles chain reorganizations, and publishes block data to SNS for downstream processing.

## Architecture

The watcher consists of two main services:

- **Live Data Service**: Subscribes to new block headers via Alchemy WebSocket, fetches full block data (receipts, traces, blobs), detects and handles chain reorgs, and publishes events to SNS
- **Backfill Service** (optional): Fills gaps in block data by polling the database for missing blocks and fetching them via HTTP

## Prerequisites

- Go 1.25+
- Docker and Docker Compose
- Alchemy API key (for Ethereum mainnet access)

## Quick Start

### 1. Start Infrastructure

Start the required infrastructure services (PostgreSQL, Redis, Jaeger, LocalStack):

```bash
docker compose up -d
```

This starts:
- **PostgreSQL** (port 5432) - Block state persistence
- **Redis** (port 6379) - Block data cache
- **Jaeger** (port 16686) - Distributed tracing UI
- **LocalStack** (port 4566) - Local AWS SNS/SQS for development

### 2. Configure Environment

```bash
# Required
ALCHEMY_API_KEY=your_alchemy_api_key_here

# Optional (defaults shown)
ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2
DATABASE_URL=postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=

# AWS/LocalStack
AWS_SNS_ENDPOINT=http://localhost:4566
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test

# SNS FIFO Topic (single topic for all event types)
AWS_SNS_TOPIC_ARN=arn:aws:sns:us-east-1:000000000000:stl-ethereum-blocks.fifo

# Tracing
JAEGER_ENDPOINT=localhost:4317
ENVIRONMENT=development

# Features
ENABLE_BACKFILL=false
```

### 3. Run the Watcher

```bash
go run ./cmd/watcher -disable-blobs
```

Or with flags:

```bash
# Enable pprof profiling server
go run ./cmd/watcher -disable-blobs -pprof :6060
```

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

### View Traces

Open the Jaeger UI at http://localhost:16686 to view distributed traces.

## Graceful Shutdown

The watcher handles `SIGINT` and `SIGTERM` signals gracefully:

1. Stops accepting new blocks
2. Completes in-flight processing
3. Closes database connections
4. Shuts down within 25 seconds (compatible with Fargate's 30s default)

Press `Ctrl+C` to stop the watcher.
