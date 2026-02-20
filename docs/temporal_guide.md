# Temporal Implementation

This document describes the Temporal scheduled jobs implementation in STL Verify — what exists, how it works, and how to add new jobs.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Scheduled Jobs](#scheduled-jobs)
  - [Price Fetch](#price-fetch)
  - [Data Validation](#data-validation)
- [Composition Root (main.go)](#composition-root-maingo)
  - [Startup Flow](#startup-flow)
  - [Optional Job Registration](#optional-job-registration)
  - [Schedule Management](#schedule-management)
- [Local Development](#local-development)
- [Docker Build & Push](#docker-build--push)
- [Environment Variables](#environment-variables)
- [Adding a New Job](#adding-a-new-job)
- [Gotchas](#gotchas)

---

## Overview

STL Verify uses [Temporal](https://temporal.io) for recurring scheduled jobs. A single worker process polls one shared task queue (`sentinel-workers`) and executes two types of work:

| Job | Schedule | Purpose |
|-----|----------|---------|
| Price Fetch | Every 5m | Fetch off-chain token prices from CoinGecko |
| Data Validation | Every 1h | Cross-check stored block data against Etherscan |

Price Fetch is always enabled. Data Validation is optional — it activates only when `ETHERSCAN_API_KEY` is set.

---

## Architecture

Temporal adapters live in `internal/adapters/inbound/temporal/` following hexagonal architecture:

```text
cmd/temporal-worker/main.go              ← Composition root: wires deps, registers, runs
internal/adapters/inbound/temporal/
├── constants.go                         ← Shared task queue name + schedule IDs
├── price_fetch_activities.go             ← Price fetch activity (PriceFetcher interface)
├── price_fetch_workflow.go               ← Price fetch workflow
├── data_validation_activities.go        ← Data validation activity (DataValidator interface)
├── data_validation_workflow.go          ← Data validation workflow
└── *_test.go                            ← Unit tests for all of the above
```

Each activity file defines a **port interface** that a domain service satisfies:

```text
Temporal Schedule → Workflow (orchestration) → Activity (delegates to interface) → Domain Service
```

Activities never import domain services directly — they depend on an interface defined in the same file. The composition root (`main.go`) wires the concrete service implementation at startup.

---

## Scheduled Jobs

### Price Fetch

**Files:** `price_fetch_activities.go`, `price_fetch_workflow.go`

Fetches current token prices from CoinGecko and stores them in PostgreSQL.

| Property | Value |
|----------|-------|
| Schedule ID | `coingecko-price-fetch` |
| Default Interval | 5m (`PRICE_FETCH_INTERVAL`) |
| Start-To-Close Timeout | 2 minutes |
| Schedule-To-Close Timeout | 4 minutes |
| Retry Policy | 1s initial, 2x backoff, 30s max interval |
| Required Env | `COINGECKO_API_KEY` |
| Domain Service | `offchain_price_fetcher.Service` |
| Outbound Deps | CoinGecko API, PostgreSQL price repository |

**Port interface:**
```go
type PriceFetcher interface {
    FetchCurrentPrices(ctx context.Context, assetIDs []string) error
}
```

---

### Data Validation

**Files:** `data_validation_activities.go`, `data_validation_workflow.go`

Cross-checks stored block data against Etherscan's API to detect discrepancies. Returns a detailed report with pass/fail/error counts.

| Property | Value |
|----------|-------|
| Schedule ID | `data-validation` |
| Default Interval | 1h (`DATA_VALIDATION_INTERVAL`) |
| Start-To-Close Timeout | 10 minutes |
| Schedule-To-Close Timeout | 30 minutes |
| Retry Policy | 2s initial, 2x backoff, 1m max interval |
| Required Env | `ETHERSCAN_API_KEY` |
| Domain Service | `data_validator.Service` |
| Outbound Deps | Etherscan API, PostgreSQL block state repository |

**Port interface:**
```go
type DataValidator interface {
    Validate(ctx context.Context) (*data_validator.Report, error)
}
```

The activity converts the `Report` struct into a flat `ValidateDataOutput` with serializable fields (no `time.Duration`, just `DurationMs`).

---

## Composition Root (main.go)

`cmd/temporal-worker/main.go` is the entry point. It wires all dependencies and runs a single Temporal worker.

### Startup Flow

```text
1. Parse log level, create logger
2. Connect to Temporal Server (TEMPORAL_HOST_PORT, namespace "sentinel")
3. Connect to PostgreSQL (DATABASE_URL)
4. Parse chain ID (CHAIN_ID, default "1")
5. Create a single worker on task queue "sentinel-workers"
6. Register price fetch (required — fails if COINGECKO_API_KEY missing)
7. Attempt to register data validation (skip if ETHERSCAN_API_KEY missing)
8. Ensure schedules exist for all enabled jobs (idempotent create-if-not-exists)
9. Start worker polling loop (blocks until SIGINT/SIGTERM)
```

### Optional Job Registration

Each optional job follows the same pattern:

```go
activities, err := createXxxActivities(pool, logger, chainID)
if err != nil {
    logger.Warn("xxx disabled", "reason", err)  // Log + skip
} else {
    w.RegisterWorkflow(temporaladapter.XxxWorkflow)
    w.RegisterActivity(activities)
    logger.Info("xxx registered")
}
```

If the factory fails (usually because the env var is missing), the job is disabled — no activity registered, no schedule created. The worker continues with whatever jobs succeeded.

### Schedule Management

Schedules are created idempotently on startup via `ensureSchedule()`. It uses a generalized `scheduleConfig` struct:

```go
type scheduleConfig struct {
    ID              string        // e.g. "coingecko-price-fetch"
    IntervalEnv     string        // e.g. "PRICE_FETCH_INTERVAL"
    IntervalDefault string        // e.g. "5m"
    Workflow        any           // e.g. temporaladapter.PriceFetchWorkflow
    Args            []any         // Workflow input (nil for no-arg workflows)
    WorkflowID      string        // e.g. "scheduled-price-fetch"
}
```

If a schedule with the given ID already exists, it's skipped. This means **changing the interval of an existing schedule requires deleting it first** (via UI or CLI) so it gets recreated with the new value.

---

## Local Development

### 1. Start infrastructure

```bash
make dev-up
```

This starts PostgreSQL, Redis, LocalStack, Jaeger, **and** the Temporal stack:

| Container | Port | Purpose |
|-----------|------|---------|
| `stl-verify-temporal-db` | 5433 | PostgreSQL for Temporal internal state |
| `stl-verify-temporal` | 7233 | Temporal Server (gRPC) |
| `stl-verify-temporal-ui` | 8233 | Temporal Web UI |

The `temporalio/auto-setup` image automatically creates the `sentinel` namespace.

### 2. Start the worker

```bash
# Minimum (only price fetch):
COINGECKO_API_KEY=xxx make run-temporal-worker

# All jobs enabled:
COINGECKO_API_KEY=xxx \
ETHERSCAN_API_KEY=xxx \
make run-temporal-worker
```

### 3. Verify

Open **http://localhost:8233** → namespace `sentinel`. You should see:
- Active schedules in the Schedules tab
- Workflow executions as they run

---

## Docker Build & Push

The worker has a multi-stage Dockerfile (`Dockerfile.temporal-worker`):

```bash
# Build ARM64 Docker image
make docker-build-temporal-worker ENV=sentinelstaging

# Push to ECR
make docker-push-temporal-worker ENV=sentinelstaging

# Build + push in one step
make docker-release-temporal-worker ENV=sentinelstaging
```

The image injects `GitCommit`, `GitBranch`, and `BuildTime` via ldflags for observability.

---

## Environment Variables

### Worker Configuration

| Variable | Default | Required | Purpose |
|----------|---------|----------|---------|
| `TEMPORAL_HOST_PORT` | `localhost:7233` | No | Temporal Server gRPC address |
| `TEMPORAL_NAMESPACE` | `sentinel` | No | Temporal namespace |
| `DATABASE_URL` | `postgres://...localhost:5432/stl_verify` | No | App database (blocks, prices, etc.) |
| `CHAIN_ID` | `1` | No | Ethereum chain ID |
| `LOG_LEVEL` | `info` | No | Log verbosity |
| `COINGECKO_API_KEY` | — | **Yes** | CoinGecko API key (price fetch) |
| `COINGECKO_BASE_URL` | CoinGecko default | No | Override for Pro API |
| `ETHERSCAN_API_KEY` | — | No | Enables data validation |

### Schedule Intervals (override defaults)

| Variable | Default | Affects |
|----------|---------|---------|
| `PRICE_FETCH_INTERVAL` | `5m` | Price fetch schedule |
| `DATA_VALIDATION_INTERVAL` | `1h` | Data validation schedule |

### Temporal Server (docker-compose only)

| Variable | Value | Purpose |
|----------|-------|---------|
| `DB` | `postgres12` | Database driver |
| `POSTGRES_SEEDS` | `temporal-db` | Database host |
| `POSTGRES_USER` | `temporal` | Database user |
| `POSTGRES_PWD` | `temporal` | Database password |
| `DEFAULT_NAMESPACE` | `sentinel` | Namespace auto-created on boot |

---

## Adding a New Job

Follow this pattern to add a new scheduled job.

### 1. Add a schedule ID to `constants.go`

```go
const (
    PriceFetchScheduleID     = "coingecko-price-fetch"
    DataValidationScheduleID = "data-validation"
    NewJobScheduleID         = "new-job"  // ← add
)
```

### 2. Create the activity file

Create `internal/adapters/inbound/temporal/new_job_activities.go`:

```go
package temporal

// 1. Define a port interface (satisfied by a domain service)
type NewJobRunner interface {
    Run(ctx context.Context) error
}

// 2. Activity struct holds the interface
type NewJobActivities struct {
    runner NewJobRunner
}

// 3. Constructor with nil check
func NewNewJobActivities(runner NewJobRunner) (*NewJobActivities, error) { ... }

// 4. Activity method — delegates to the interface
func (a *NewJobActivities) RunNewJob(ctx context.Context) (*NewJobOutput, error) { ... }
```

### 3. Create the workflow file

Create `internal/adapters/inbound/temporal/new_job_workflow.go`:

```go
package temporal

func NewJobWorkflow(ctx workflow.Context) (*NewJobWorkflowOutput, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout:    10 * time.Minute,
        ScheduleToCloseTimeout: 30 * time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            InitialInterval:    time.Second,
            BackoffCoefficient: 2.0,
            MaximumInterval:    time.Minute,
        },
    })

    var activities *NewJobActivities
    var result NewJobOutput
    err := workflow.ExecuteActivity(ctx, activities.RunNewJob).Get(ctx, &result)
    // ...
}
```

Key rules:
- Workflows are **standalone functions** (not methods)
- Use `workflow.Context`, not `context.Context`
- `var activities *NewJobActivities` — nil pointer is a Temporal convention for method reference resolution
- Workflows must be **deterministic**: no `time.Now()`, no random, no network calls

### 4. Create tests

Create `*_activities_test.go` and `*_workflow_test.go` with:
- Mock implementation of the port interface
- Table-driven tests for the activity
- `testsuite.WorkflowTestSuite` for the workflow

### 5. Register in main.go

Add a factory function and register with the worker:

```go
// In main.go:
func createNewJobActivities(...) (*temporaladapter.NewJobActivities, error) { ... }

// In run():
newJobActivities, err := createNewJobActivities(...)
if err != nil {
    logger.Warn("new job disabled", "reason", err)
} else {
    w.RegisterWorkflow(temporaladapter.NewJobWorkflow)
    w.RegisterActivity(newJobActivities)
}

// Add schedule in ensureSchedules():
func ensureNewJobSchedule(ctx context.Context, c client.Client, logger *slog.Logger) error {
    return ensureSchedule(ctx, c, logger, scheduleConfig{
        ID:              temporaladapter.NewJobScheduleID,
        IntervalEnv:     "NEW_JOB_INTERVAL",
        IntervalDefault: "1h",
        Workflow:        temporaladapter.NewJobWorkflow,
        Args:            nil,
        WorkflowID:      "scheduled-new-job",
    })
}
```

---

## Gotchas

1. **Schedule intervals are only set on creation.** Changing `IntervalDefault` or the env var has no effect on an existing schedule. Delete the old schedule first (UI or `temporal schedule delete --schedule-id <id> --namespace sentinel`), then restart the worker.

2. **Renaming a schedule ID creates a duplicate.** The old schedule persists until manually deleted.

3. **Worker connects to two databases.** `TEMPORAL_HOST_PORT` → Temporal Server (workflow orchestration). `DATABASE_URL` → App database (blocks, prices, etc.). These are completely separate databases.

4. **No `.env` file for temporal-worker.** Unlike the watcher, `make dev-env` does not generate one. Pass env vars directly.

5. **Workflows must be deterministic.** No `time.Now()`, no `rand`, no network calls, no goroutines. All side effects go through activities.

6. **`temporalio/auto-setup` runs migrations on every start.** It creates `temporal` and `temporal_visibility` databases automatically. Pin the image tag in production to avoid unexpected schema changes.

7. **Prefer defining schedules in code.** The Temporal UI supports creating and managing schedules, but defining them in `main.go` ensures they are version-controlled, reproducible, and created automatically on startup.
