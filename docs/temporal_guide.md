---
title: Temporal Cronjobs - Developer Guide
audience: [developers, ai-agents]
repo: stl
applies_to: stl-verify
shared_package: stl-verify/internal/adapters/outbound/temporal
entrypoint: temporal.RunCronjob
job_dir: stl-verify/cmd/cronjobs
key_files:
  - stl-verify/internal/adapters/outbound/temporal/temporal.go   # RunCronjob, CronjobConfig, client dial, ensureSchedule
  - stl-verify/internal/adapters/outbound/temporal/workflow.go   # cronjobWorkflow, cronjobActivities, Runner, RunnerFunc, ScheduledAtFromContext
  - stl-verify/internal/adapters/outbound/temporal/metrics.go    # cronjob.runs.total, cronjob.run.duration_seconds
  - stl-verify/cmd/cronjobs/offchain-price-indexer/main.go       # canonical example to copy
related_docs:
  - infrastructure repo: docs/temporal-workflow-automation-guide.md   # platform + cross-repo onboarding
task_recipes: [add-a-new-cronjob]
---

# Temporal Cronjobs - Developer Guide

How to develop, run, and add Temporal scheduled jobs ("cronjobs") in STL Verify.

For the platform itself (where the central Temporal server lives, how to provision a
namespace, how *other* repos onboard) see the infrastructure repo's
`docs/temporal-workflow-automation-guide.md`. This guide is the application-side view.

## For agents: read these first

To add or modify a cronjob, load these files (paths are repo-relative from the stl root):

1. `stl-verify/cmd/cronjobs/offchain-price-indexer/main.go` - the canonical example. Copy its shape.
2. `stl-verify/internal/adapters/outbound/temporal/temporal.go` - `RunCronjob` and `CronjobConfig` (the contract you fill in).
3. `stl-verify/internal/adapters/outbound/temporal/workflow.go` - the `Runner` interface (the only thing you implement) and `ScheduledAtFromContext`.

Do NOT edit the shared package to add a job. Adding a job = one new `main.go` + k8s manifests + a `dev-env` block. The shared package is generic.

## How it works

Each cronjob is a small `main.go` under `stl-verify/cmd/cronjobs/<name>/` that calls one
shared entry point, `temporal.RunCronjob`. All the Temporal plumbing (client connection,
worker, workflow, activity, retries, schedule creation, metrics, graceful shutdown) lives
in `stl-verify/internal/adapters/outbound/temporal/`. A job supplies a config and a
`Setup` function; the only interface it implements is `Runner`.

```text
Temporal Schedule (per job)
  -> cronjobWorkflow (generic, shared)
    -> cronjobActivities.Execute (generic, shared; retries + metrics)
      -> Runner.Run(ctx)            # your domain service
```

The orchestration is an **outbound adapter**. The activity calls a `Runner`, satisfied by
a domain service that knows nothing about Temporal. Keep Temporal types out of the domain
and application layers; wire the concrete service to a `RunnerFunc` only in the `cmd/`
composition root.

**Naming convention:** task queue, schedule ID, and workflow ID are all derived from
`CronjobConfig.Name`. One task queue and one schedule per job.

### Shared package (`stl-verify/internal/adapters/outbound/temporal/`)

| File | Responsibility | Key symbols |
|------|----------------|-------------|
| `temporal.go` | worker lifecycle, schedule | `RunCronjob`, `CronjobConfig`, `BuildMeta`, `Dependencies`, `ensureSchedule` |
| `workflow.go` | generic workflow + activity | `cronjobWorkflow`, `cronjobActivities`, `Runner`, `RunnerFunc`, `ContextWithScheduledAt`, `ScheduledAtFromContext` |
| `metrics.go` | OTel metrics | `cronjob.runs.total{status}`, `cronjob.run.duration_seconds` |

You normally never touch these to add a job.

## Current cronjobs

| Job (`stl-verify/cmd/cronjobs/`) | Interval env | Default | Purpose |
|-----------------------|--------------|---------|---------|
| `offchain-price-indexer` | `PRICE_FETCH_INTERVAL` | 5m | Fetch off-chain token prices from CoinGecko |
| `watcher-data-validator` | `DATA_VALIDATION_INTERVAL` | 1h | Cross-check stored block data (per chain; `SERVICE_NAME` sets the queue) |
| `anchorage-indexer` | `ANCHORAGE_INDEX_INTERVAL` | 15m | Snapshot Anchorage collateral |
| `maple-graphql-indexer` | `MAPLE_SYNC_INTERVAL` | 10m | Sync Maple positions via GraphQL |

## Recipe: add a new cronjob

Replace `<your-job>` with a kebab-case name and `<YOUR_JOB>` with the upper-snake form.

### Step 1 - Create `stl-verify/cmd/cronjobs/<your-job>/main.go`

This is usually the only Go file you write.

```go
// Package main implements a Temporal cronjob worker for <your-job>.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() { buildinfo.PopulateFromVCS(&GitCommit, &BuildTime) }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:            "<your-job>",
		IntervalEnv:     "<YOUR_JOB>_INTERVAL",
		IntervalDefault: "10m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// setupRunner wires dependencies (from deps.Pool / deps.Logger) and returns the
// business logic as a Runner. Return an error to fail fast on bad config.
func setupRunner(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	service, err := newYourService(deps) // build clients, repositories, the domain service
	if err != nil {
		return nil, fmt.Errorf("creating service: %w", err)
	}
	return temporal.RunnerFunc(func(ctx context.Context) error {
		return service.Run(ctx)
	}), nil
}
```

### Step 2 - Add Kubernetes manifests

Create `k8s/base/<your-job>/deployment.yaml` and `serviceaccount.yaml`, modelled on
`k8s/base/offchain-price-indexer/`. A worker is a normal long-running Deployment that
polls a task queue, NOT a Kubernetes `CronJob`. Use `replicas: 1` (Temporal serializes a
schedule's executions). The ConfigMap must set `TEMPORAL_HOST_PORT`, `TEMPORAL_NAMESPACE`,
`DATABASE_URL`, and `<YOUR_JOB>_INTERVAL`. Add the deployment to the relevant overlays
(`k8s/overlays/{dev,staging,prod}/`).

### Step 3 - Add a local `.env` block

In `stl-verify/Makefile`, extend the `dev-env` target with a block that writes
`cmd/cronjobs/<your-job>/.env` (copy an existing block; include `TEMPORAL_HOST_PORT=127.0.0.1:7233`,
`TEMPORAL_NAMESPACE=vector`, `DATABASE_URL`, `CHAIN_ID`, `BUILD_GIT_HASH=dev`, `LOG_LEVEL=debug`).

### Step 4 - Build, run, verify

```bash
cd stl-verify
make build-cronjob-<your-job>            # compile binary to dist/<your-job>
make dev-env                             # regenerate .env files
make run-cronjob-<your-job>              # run locally against the kind Temporal
```

Cronjob images are discovered automatically from `cmd/cronjobs/*`:
`make docker-build-cronjob-<your-job>` and `make docker-release-cronjob-<your-job> ENV=...`.

**Verify:** open the local Temporal UI, select namespace `vector`, confirm a schedule
named `<your-job>` appears under Schedules and that a workflow execution runs.

### Step 5 - Alerts

No new rules are needed for liveness/errors: the shared `cronjob.runs.*` metrics are
already covered by `alerts/vector-cronjobs.yaml`. Add job-specific data-quality alerts and
matching runbook sections in `docs/runbooks/` only if the generic error path cannot catch a
silent hole (e.g. "ran successfully but wrote zero rows").

## Local development

```bash
cd stl-verify
make dev-up                                  # kind cluster incl. Temporal (server, DB, UI)
make dev-env                                 # generate cmd/cronjobs/*/.env
make run-cronjob-offchain-price-indexer      # run one cronjob, sourcing its .env
```

`make dev-up` applies `k8s/dev-infra/temporal*.yaml`. The `temporalio/auto-setup` server
auto-creates the `vector` namespace and exposes the Temporal UI via a nodePort; open it and
select namespace `vector` to watch schedules and executions.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `TEMPORAL_HOST_PORT` | `localhost:7233` | Temporal server gRPC address (in-cluster: `temporal-server.temporal:7233`) |
| `TEMPORAL_NAMESPACE` | `sentinel` | Temporal namespace (deployed envs use `vector`) |
| `DATABASE_URL` | local default | **App** database, separate from Temporal's own DB |
| `<JOB>_INTERVAL` | per job | Override the schedule interval |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Where metrics export; unset means metrics are no-ops (logged at startup) |

## Gotchas

1. **Schedule interval is set only on creation.** `ensureSchedule` skips an existing
   schedule, so changing the default or env var has no effect until you delete the schedule
   (`temporal schedule delete --schedule-id <name> --namespace <ns>`) and let the worker
   recreate it.
2. **Renaming `Name` orphans the old schedule** - it keeps firing until deleted manually.
3. **Workflows must be deterministic.** No `time.Now()`, `rand`, network calls, or
   goroutines in the workflow. All side effects go through the activity (your `Runner`).
4. **Make your `Runner` idempotent.** Activities retry (5x by default) and every retry sees
   the same `scheduledAt` (read it with `ScheduledAtFromContext`); key time-bucketed writes
   off it so retries do not double-write.
5. **Two databases.** `TEMPORAL_HOST_PORT` points at Temporal's orchestration DB.
   `DATABASE_URL` points at the app DB. They are completely separate.
</content>
