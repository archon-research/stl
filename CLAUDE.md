# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

This repository contains the application code:
- **stl-verify/** - Main Go service (block watcher, backfill, backup worker)
- **k8s/** - Kubernetes manifests (Kustomize) for EKS deployment
  - `k8s/base/` — one subdirectory per service: `Deployment`, `ServiceAccount`
  - `k8s/overlays/prod/` — prod-specific patches (namespace, images/image tags)
  - `k8s/overlays/staging/` — staging-specific patches (namespace, images/image tags)
- **experiments/** - Exploration projects
- **docs/** - Architecture diagrams and entity relations

Infrastructure code (Terraform/OpenTofu) lives in a separate repository for security reasons.

## Common Commands

All commands run from `stl-verify/`:

```bash
# Development
make dev-up              # Start kind cluster with full pipeline (mock blockchain server by default)
make dev-down            # Stop services
make dev-env             # Generate .env files for all services (fetches secrets from AWS)
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

# Erigon node management (requires ERIGON_USER, ERIGON_IP)
make erigon-status ERIGON_USER=<user> ERIGON_IP=<ip>
make deploy-bulk-download ERIGON_USER=<user> ERIGON_IP=<ip>
```

See [stl-verify/Makefile](stl-verify/Makefile) for the complete list of targets.

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
- **Errors**: 
Wrap with context: `fmt.Errorf("doing X: %w", err)`.
Never ignore errors.
Lean towards returning errors instead of continuing, unless there is an extremely good reason to continue instead.
- **Testing**: 
    Table-driven tests, mock outbound ports for unit tests.
    Services and main.go files should have 100% coverage. Think very hard about edge cases, it is mission critical that code is correct and robust.
    In services, ONLY test the public api. Dont test internals if you can avoid it.
    You can move the main.go code into a function and only call that from main() so that you can test it properly.
    For main.go files, only create integration tests.
    For services, create both unit and integration tests.
    Integration tests are only allowed to mock our data sources that we cannot control, e.g. Alchemy
- **Binaries/Building**: When building binaries using `go build`, output to `stl/dist`
- **Code structure**: In main.go files, keep main() at the top of the file.
- **Function composition**: 
    Compose large functions from smaller functions.
    Large functions should read like prose, with each step delegated to a well-named helper function.
- **Libraries**: 
    Use the standard library as much as possible.
    Instead of duplicating code, create a function containing the shared functionality, and re-use it.
- **Database**:
    Always think hard and carefully about how the wrong data could be written to the database.
    Always think hard and carefully about schema design.
    For timeseries tables, use Tigerdata primitives, and make sure they support distributed tables.
    NEVER modify an existing migration file in `stl-verify/db/migrations/`. Migrations are immutable once applied — the migrator tracks checksums and will reject modified files. Always create a new migration file for fixes or additions.

## Do NOT

- Import adapters in domain or application layer
- Add business logic to adapters
- Use global state or singletons
- Skip error handling
- Commit generated files or binaries

## Environment

- Go 1.26+
- Docker for local development (PostgreSQL, Redis, Jaeger, LocalStack)
- AWS for production (EKS on Graviton arm64 — migrating from ECS Fargate. RDS Aurora (TimescaleDB via TigerData), ElastiCache Redis, SNS/SQS, S3)
- Alchemy API key required for Ethereum mainnet access
