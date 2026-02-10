# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

This is a monorepo containing:
- **stl-verify/** - Main Go service (block watcher, backfill, backup worker)
- **infra/** - Terraform/OpenTofu infrastructure (AWS: ECS, RDS, SNS, SQS, S3, Redis)
- **experiments/** - Exploration projects
- **docs/** - Architecture diagrams and entity relations

## Common Commands

All commands run from `stl-verify/`:

```bash
# Development
make dev-up              # Start Docker services (PostgreSQL, Redis, Jaeger, LocalStack)
make dev-down            # Stop services
make run-watcher         # Run watcher (loads .env from cmd/watcher/)

# Testing
make test               # Unit tests only
make test-race          # Unit tests with race detector (CI default)
make test-integration   # Integration tests (requires Docker, 5m timeout)
make e2e                # End-to-end tests with testcontainers
make cover              # Generate coverage report

# CI (runs all checks)
make ci                 # test-race, vet, fmt-check, tidy-check, staticcheck, vulncheck, golangci-lint

# Formatting & Linting
make fmt                # Format code
make tools              # Install dev tools (staticcheck, golangci-lint, govulncheck, etc.)

# Docker (ARM64 for Fargate Graviton)
make docker-release ENV=sentinelstaging    # Build and push watcher image
make docker-release-backup ENV=sentinelstaging  # Build and push backup worker image

# Terraform
make tf-check-staging   # Init, validate, plan for sentinelstaging
make tf-apply-staging   # Apply infrastructure changes

# Erigon node management (requires ERIGON_USER, ERIGON_IP)
make erigon-status ERIGON_USER=<user> ERIGON_IP=<ip>
make deploy-bulk-download ERIGON_USER=<user> ERIGON_IP=<ip>
```

## Architecture

### Hexagonal (Ports and Adapters)

```
stl-verify/
├── cmd/                    # Entry points (watcher, bulk-download, raw_data_backup, event-persister, migrate)
├── internal/
│   ├── domain/entity/      # Core business entities (no external dependencies)
│   ├── ports/
│   │   ├── inbound/        # Use case interfaces
│   │   └── outbound/       # Infrastructure interfaces
│   ├── adapters/
│   │   ├── inbound/        # HTTP, gRPC, CLI handlers
│   │   └── outbound/       # Implementations: alchemy, postgres, redis, sns, sqs, s3, telemetry
│   └── services/           # Use case implementations (live_data, backfill_gaps, raw_data_backup)
└── db/migrations/          # SQL migrations (auto-applied)
```

**Dependency Rule**: Dependencies flow inward only. Domain has no dependencies; adapters depend on ports; ports depend on domain.

### Core Services

1. **Live Data Service** - WebSocket subscription to Alchemy for new blocks, handles chain reorgs, publishes to SNS FIFO
2. **Backfill Service** - Fills gaps in block data via HTTP polling
3. **Raw Data Backup** - Backs up block data to S3

### Data Flow

```
Alchemy WebSocket → Live Data Service → PostgreSQL (TimescaleDB) + Redis (cache) + SNS FIFO → SQS consumers
```

### Cache Key Convention

```
stl:{chainId}:{blockNumber}:{version}:{dataType}
```
- version increments on chain reorgs
- dataType: block, receipts, traces, blobs

## Code Conventions

- **Interfaces**: Use `-er` suffix (Reader, Publisher)
- **Constructors**: Use `New` prefix
- **Files**: snake_case
- **Errors**: Wrap with context: `fmt.Errorf("doing X: %w", err)`
- **Testing**: 
    Table-driven tests, mock outbound ports for unit tests.
    Services and main.go files should have 100% coverage. Think very hard about edge cases, it is mission critical that code is correct and robust.
    In services, ONLY test the public api. Dont test internals if you can avoid it.
    You can move the main.go code into a function and only call that from main() so that you can test it properly.
    For main.go files, only create integration tests.
    For services, create both unit and integration tests.
    Integration tests are only allowed to mock our data sources that we cannot control, e.g. Alchemy
- **Binaries/Building**: Output to `stl/dist`
- **Code structure**: In main.go files, keep main() at the top of the file.
- **Function composition**: Compose large functions from smaller functions. Large functions should read like prose, with each step delegated to a well-named helper function.
- **Testing**: Table-driven tests, mock outbound ports for unit tests
- **Libraries**: Use the standard library as much as possible

## Do NOT

- Import adapters in domain or application layer
- Add business logic to adapters
- Use global state or singletons
- Skip error handling
- Commit generated files or binaries

## Environment

- Go 1.25+
- Docker for local development (PostgreSQL, Redis, Jaeger, LocalStack)
- AWS for production (ECS Fargate ARM64, RDS Aurora, ElastiCache, SNS/SQS, S3)
- Alchemy API key required for Ethereum mainnet access
