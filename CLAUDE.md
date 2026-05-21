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
make dev-suspend         # Suspend local kind nodes (local dev only; do not use in CI/prod)
make dev-resume          # Resume suspended local kind nodes (local dev only; do not use in CI/prod)
make dev-down            # Delete local kind cluster
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

# Formatting & Linting (Go/Python/TypeScript)
make install-hooks      # Install lefthook git pre-commit hooks (auto-runs on dev-up)
make format             # Auto-format all code locally (Go, Python, TS)
make lint               # Run linters locally (delegates to language pipelines)

# Note: CI workflows run per-language in .github/workflows/:
# - go-ci.yml:      `make ci-checks && make test-race`
# - python-ci.yml:  `make lint` + `make test-unit` + `make test-integration` (in python/)
# - ts-ci.yml:      `npm run lint` + `npm run format:check` + `npm run build` (in ts/)

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

- **Keep an eye out for deduplicate possibilities**: Try to consolidate lots of duplicated code. Create shareable libraries instead of duplicating code everywhere.
- **Interfaces**: Use `-er` suffix (Reader, Publisher)
- **Constructors**: Use `New` prefix
- **Files**: snake_case
- **Errors**:
Wrap with context: `fmt.Errorf("doing X: %w", err)`.
Never ignore errors.
Lean towards returning errors instead of continuing, unless there is an extremely good reason to continue instead.
**Fail hard and early on unexpected errors.**
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

## Review phase

After completing a substantive code change (new feature, refactor, bug fix touching multiple files), and before declaring the work done, spawn the following reviewer subagents **in parallel** — a single message containing multiple `Agent` tool calls. Sequential review wastes wall-clock time and starves later reviewers of attention.

Always spawn:

1. **`pr-review-toolkit:code-reviewer`** — adherence to project guidelines, style, conventions.
2. **`pr-review-toolkit:silent-failure-hunter`** — error swallowing, ignored errors, inadequate fallback, NotFound-treated-as-success.
3. **General-purpose with architecture brief** — hexagonal layering, dependency direction, port/adapter boundary, single responsibility, separation of concerns, coupling.
4. **General-purpose with code-quality/patterns brief** — function size, naming, idiomatic Go, DRY, premature abstraction, test design, SOLID-ish concerns.

Spawn additionally when applicable:

5. **`pr-review-toolkit:pr-test-analyzer`** — when new tests are added or coverage is at risk.
6. **`pr-review-toolkit:type-design-analyzer`** — when new types or interfaces are introduced.
7. **`pr-review-toolkit:comment-analyzer`** — when substantive new docstrings or comments are added.

Each reviewer prompt must include:

- The plan file path (if a plan exists) and a precise list of files in scope.
- A specific audit checklist tailored to that reviewer's lens — don't ask reviewers to "review the diff"; tell them what to look for.
- The expected output format: **Blocking** / **Should-fix** / **Nice-to-have** / **Verified correct**, with file:line citations.

Apply blocking and should-fix items before declaring the work done. Nice-to-have items are surfaced to the user for an explicit decision.

## Environment

- Go 1.26+
- Docker for local development (PostgreSQL, Redis, Jaeger, LocalStack)
- AWS for production (EKS on Graviton arm64 — migrating from ECS Fargate. RDS Aurora (TimescaleDB via TigerData), ElastiCache Redis, SNS/SQS, S3)
- Alchemy API key required for Ethereum mainnet access

## Linting & Code Quality

### Architecture

Quality is enforced at three levels:

1. **Git Hooks (Lefthook)** — Runs on `git commit` and `git push`
   - Automatically fixes formatting issues (`stage_fixed: true`)
   - Prevents broken code from being pushed
   - Pre-commit: runs on staged files only (fast)
   - Pre-push: runs full-module checks (go vet, etc.)
   - Configured per-language: `stl-verify/lefthook.yml`, `stl-verify/python/lefthook.yml`, `stl-verify/ts/lefthook.yml`

2. **Local Development** — Convenience Makefile targets
   - `make install-hooks` — Install git hooks
   - `make format` — Auto-format code across all languages
   - `make lint` — Run linters locally
   - Delegates to language-specific tooling (Go, Python, TS)

3. **CI Workflows** — Post-push checks (`.github/workflows/`)
   - `go-ci.yml` — Go linting, vet, staticcheck, tests, vulncheck
   - `python-ci.yml` — Ruff lint/format, unit & integration tests
   - `ts-ci.yml` — oxlint, oxfmt, typecheck, build
   - Triggered on changed files (via `changed-files.yml`)
   - **These are the source of truth** — CI definitions are authoritative

### Linting by Language

**Go:**
- Pre-commit hooks: gofmt, goimports (staged files only)
- Pre-push hooks: go vet (full module)
- CI: `make ci-checks` (vet, staticcheck, golangci-lint, vulncheck, tidy)
- Tools: Install with `make tools`

**Python:**
- Hooks: ruff lint, ruff format
- CI: `make lint` (from `stl-verify/python/Makefile`)
- Tools: `uv sync --all-extras` (includes ruff, pytest, etc.)

**TypeScript:**
- Hooks: oxlint, oxfmt
- CI: `npm run lint`, `npm run format:check`, `npm run build`
- Tools: `npm ci` (includes oxlint, oxfmt, etc.)

### Running Manually

```bash
# Run git hooks manually without committing
lefthook run pre-commit   # Run all pre-commit hooks

# Format and lint locally before committing
make format
make lint

# Or let git hooks do it automatically on commit
git commit  # Hooks auto-fix, may stage changes
```

### Key Points

- **Don't bypass hooks**: They catch issues early and cheaply
- **Hooks are fast**: Only check staged files, not entire codebase
- **CI is strict**: Fails if any checks don't pass (source of truth)
- **Language pipelines are independent**: Changes to one language don't trigger linting for others
