# STL Project

This repository contains the `stl` system, a modular monolith designed for blockchain risk analysis and verification.

## Architecture

The project follows a **Modular Monolith** architecture. Each sub-system (like `stl-verify` or `stl-trade`) is designed using **Ports & Adapters** (Hexagonal Architecture) to ensure decoupling between business logic and infrastructure.

### Directory Structure

```
stl/
├── cmd/                        # Application Entry Points
│   ├── stl-trade/              # Trade execution service
│   └── stl-verify/             # Verification CLI tool
│
├── internal/                   # Private Application Code
│   ├── common/                 # Shared utilities (logging, config, types)
│   │
│   ├── stl-verify/             # === VERIFY SUB-SYSTEM ===
│   │   ├── domain/             # Core Business Logic
│   │   ├── ports/              # Interfaces defining the boundaries
│   │   ├── services/           # Application Services
│   │   └── adapters/           # Subsystem-specific adapters
│   │       ├── primary/        # Driving Adapters
│   │       │   ├── cli/        # CLI commands
│   │       │   └── http/       # HTTP handlers
│   │       └── secondary/      # Driven Adapters
│   │           └── postgres/   # Verify-specific Repositories & Migrations
│   │               ├── migrations/
│   │               └── models/
│   │
│   ├── stl-trade/              # === TRADE SUB-SYSTEM ===
│   │   ├── domain/             # Core Business Logic (Orders, Execution)
│   │   ├── ports/              # Interfaces defining the boundaries
│   │   ├── services/           # Application Services
│   │   └── adapters/           # Subsystem-specific adapters
│   │       ├── primary/        # Driving Adapters (HTTP handlers)
│   │       └── secondary/      # Driven Adapters
│   │           └── postgres/   # Trade-specific Repositories & Migrations
│   │               ├── migrations/
│   │               └── models/
│   │
│   └── gateways/               # === SHARED GATEWAYS (External Systems) ===
│       ├── ethereum/           # Ethereum Client / Data Access
│       ├── arbitrum/           # Arbitrum Client / Data Access
│       └── postgres/           # Shared DB Connection / Utilities
│
├── pkg/                        # Public Libraries
│   └── stl-sdk/                # Go SDK for external consumers to interact with STL
│
├── infra/                      # Infrastructure as Code
│   ├── terraform/              # Terraform configurations for cloud resources
│   │   ├── environments/       # Environment-specific config (dev, prod)
│   │   │   └── dev/
│   │   └── modules/            # Reusable Terraform modules
│   └── docker/                 # Dockerfiles for containerization
│
├── pipelines/                  # Data Pipelines
│   ├── temporal/               # Temporal.io Workflows & Activities
│   │   ├── workflows/          # Workflow Definitions
│   │   ├── activities/         # Activity Definitions
│   │   ├── worker/             # Worker Registration Logic
│   │   └── starter/            # Workflow Trigger/Starter
│   └── schemas/                # Data Schemas (Avro, etc.)
│
└── tests/                      # Test Suites
    ├── integration/            # Integration tests (require DB)
    │   ├── stl-trade/
    │   └── stl-verify/
    ├── e2e/                    # End-to-end tests (full system)
    ├── benchmarks/             # Performance benchmarks
    └── testutil/               # Shared mocks and fixtures
```

## Sub-Systems

### Verify (`internal/stl-verify`)
The core subsystem for risk analysis and verification.
- **Domain**: Contains pure business logic for risk calculations and verification models.
- **Adapters**:
    - **Primary**: CLI and HTTP adapters for driving the application.
    - **Secondary/Postgres**: Contains the subsystem-specific repositories and SQL migrations.

### Trade (`internal/stl-trade`)
The subsystem responsible for trade execution and order management.
- **Domain**: Contains logic for orders, execution strategies, and portfolio management.
- **Adapters**:
    - **Primary**: HTTP handlers for trade requests.
    - **Secondary/Postgres**: Contains trade-specific repositories and migrations.

### Shared Gateways (`internal/gateways`)
Contains reusable clients and adapters for connecting to external systems.
- **Ethereum/Arbitrum**: Clients for fetching on-chain data.
- **Postgres**: Shared database connection logic and utilities.

## Testing

Unit tests are kept alongside source code. Other tests are in the `tests/` directory:

```bash
# Unit tests only
go test ./internal/... ./pkg/...

# All tests (skip slow ones)
go test -short ./...

# Integration tests
go test ./tests/integration/...

# E2E tests
go test ./tests/e2e/...

# Benchmarks
go test -bench=. ./tests/benchmarks/...
```

## Infrastructure
- **Terraform**: Manages cloud resources (AWS/GCP).
- **Docker**: Containerization for deployment.
- **Temporal.io**: Workflows for ETL and data processing.
