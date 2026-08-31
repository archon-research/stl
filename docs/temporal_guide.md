---
title: Temporal Jobs (scheduled + on-demand) - Developer Guide
audience: [developers, ai-agents]
repo: stl
applies_to: stl-verify
shared_package: stl-verify/internal/adapters/outbound/temporal
entrypoints: [temporal.RunCronjob, temporal.RunWorker]
job_dirs: [stl-verify/cmd/cronjobs, stl-verify/cmd/backfillers]
key_files:
  - stl-verify/internal/adapters/outbound/temporal/temporal.go   # RunCronjob, CronjobConfig, newBootstrap, ensureSchedule
  - stl-verify/internal/adapters/outbound/temporal/ondemand.go   # RunWorker, WorkerConfig, RegisterRunner, RunnerJob (hand-started jobs, no schedule)
  - stl-verify/internal/adapters/outbound/temporal/workflow.go   # cronjobWorkflow, cronjobActivities, Runner, RunnerFunc, ScheduledAtFromContext
  - stl-verify/internal/adapters/outbound/temporal/metrics.go    # cronjob.runs.total, cronjob.run.duration_seconds
  - stl-verify/cmd/cronjobs/offchain-price-indexer/main.go       # canonical scheduled cronjob to copy
  - stl-verify/cmd/backfillers/offchain-price-backfill/          # canonical on-demand job to copy
related_docs:
  - infrastructure repo: docs/temporal-workflow-automation-guide.md   # platform + cross-repo onboarding
task_recipes: [add-a-new-cronjob, add-an-on-demand-job]
---

# Temporal Jobs - Developer Guide

How to develop, run, and add Temporal jobs in STL Verify — both **scheduled**
("cronjobs") and **on-demand** (started by hand, with or without parameters).

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

This section covers **scheduled** jobs. For a job started by hand (a backfill, a
one-shot repair), skip to [On-demand jobs](#on-demand-jobs-no-schedule-started-by-hand).

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
| `workflow.go` | generic workflow + activity | `cronjobWorkflow`, `cronjobActivities`, `Runner`, `RunnerFunc`, `ActivityTimeouts`, `ContextWithScheduledAt`, `ScheduledAtFromContext` |
| `metrics.go` | OTel metrics | `cronjob.runs.total{status}`, `cronjob.run.duration_seconds` |

You normally never touch these to add a job.

## Current cronjobs

| Job (`stl-verify/cmd/cronjobs/`) | Interval env | Default | Purpose |
|-----------------------|--------------|---------|---------|
| `offchain-price-indexer` | `PRICE_FETCH_INTERVAL` | 5m | Fetch off-chain token prices from CoinGecko |
| `watcher-data-validator` | `DATA_VALIDATION_INTERVAL` | 1h | Cross-check stored block data (per chain; `SERVICE_NAME` sets the queue) |
| `anchorage-indexer` | `ANCHORAGE_INDEX_INTERVAL` | 15m | Snapshot Anchorage collateral |
| `maple-graphql-indexer` | `MAPLE_SYNC_INTERVAL` | 10m | Sync Maple positions via GraphQL |

`stl-verify/cmd/cronjobs/morpho-v2-bootstrap/` sits in that directory by
neighbourhood, not by lifecycle: it carries no schedule and is listed under
[on-demand jobs](#current-on-demand-jobs).

A job that must never run unattended — a one-shot repair, a destructive migration — is
**not** a cronjob with the schedule switched off. Every `RunCronjob` job has a schedule
that fires, so give it none at all: see
[on-demand jobs](#on-demand-jobs-no-schedule-started-by-hand).

A cronjob whose tick legitimately takes hours needs `CronjobConfig.ActivityTimeouts`; the
shared defaults (10m `StartToClose`, 30m `ScheduleToClose`, 5 attempts) would kill it
mid-run. A zero `ActivityTimeouts` keeps those defaults, so existing jobs are unaffected.

### Resuming a long run after a pod kill

A run measured in hours should not restart from scratch when a deploy rolls its pod.
`temporal.NewActivityProgress[T]()` records a runner's progress in the activity's heartbeat
details, which live on the Temporal server:

- pass the SAME instance to the runner and to the `Progress` field of whichever config
  carries it (`CronjobConfig` on a schedule, `RunnerJob` on an on-demand worker), because
  the liveness ticker beats through it — Temporal keeps only the last heartbeat's details,
  so a bare ping in between would erase the resume point;
- the runner reads `LoadProgress` at the start and `SaveProgress` after each completed unit
  of work, through a port it declares itself, so the service never imports the Temporal SDK;
- the details are readable only by a LATER ATTEMPT of the same activity, so
  `ActivityTimeouts.MaximumAttempts` must allow more than one — and a hand-started rerun is
  a new workflow execution, so it always starts from the beginning;
- resume must be alignment-safe: record a position only once the unit that reached it is
  fully done, and scope the record so a record computed for different inputs is refused.

`morpho-v2-bootstrap` is the worked example. A job with nothing to resume leaves
`Progress` nil and heartbeats exactly as before.

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
The image build is automatic; **promotion and pinning are not** — add a
`cronjob <your-job> <your-job>` line to `k8s/image-roster.txt` (ORB-362), which
drives both the prod promotion and the overlays' generated `images:` entries.

**Verify:** open the local Temporal UI, select namespace `vector`, confirm a schedule
named `<your-job>` appears under Schedules and that a workflow execution runs.

### Step 5 - Alerts

No new rules are needed for liveness/errors: the shared `cronjob.runs.*` metrics are
already covered by `alerts/vector-cronjobs.yaml`. Add job-specific data-quality alerts and
matching runbook sections in `docs/runbooks/` only if the generic error path cannot catch a
silent hole (e.g. "ran successfully but wrote zero rows").

## On-demand jobs (no schedule, started by hand)

Some work is not periodic. A backfill's range is an argument, decided by whoever
runs it; a one-shot repair has no argument at all but must still never run
unattended. `RunCronjob` expresses neither — `CronjobConfig` requires an interval
and it always calls `ensureSchedule`. Use `temporal.RunWorker` (`ondemand.go`)
instead. It shares the same bootstrap (logging, OTel, database, client) but
registers **your** workflow and creates **no schedule**, so the pod idles on its
task queue until a run is started.

| | `RunCronjob` | `RunWorker` |
|---|---|---|
| Schedule | created and reconciled | none |
| Workflow | shared `cronjobWorkflow` | yours, with typed parameters — or the shared one via `RegisterRunner`, with none |
| Interface you implement | `Runner` | `Register` (workflow + activities), or `Runner` again with `RegisterRunner` |
| Started by | Temporal schedule | a human, or `temporal workflow start` |

Reference implementations: `stl-verify/cmd/backfillers/offchain-price-backfill/`
for the parameterised shape (`main.go` is the composition root; `backfill.go`
holds the workflow, params and activity), and
`stl-verify/cmd/cronjobs/morpho-v2-bootstrap/main.go` for the parameterless one.

### Current on-demand jobs

Every one of them is started the same way — a task queue, a workflow type, and
whatever input the job declares. Nothing here has a schedule or a button.

| Job | Task queue | Workflow Type | Input |
|---|---|---|---|
| `cmd/backfillers/offchain-price-backfill` | `offchain-price-backfill` | `OffchainPriceBackfill` | `{"assets":["weth"],"from":"2020-01-01T00:00:00Z","to":"2026-08-05T00:00:00Z"}` |
| `cmd/backfillers/morpho-vault-backfill` | `morpho-vault-backfill` | `MorphoVaultBackfill` | `{"from":24765588,"to":24786366}` (or `{"to":24786366,"fromV2Deploy":true}` for the whole VaultV2 era) |
| `cmd/cronjobs/morpho-v2-bootstrap` | `morpho-v2-bootstrap` | `MorphoV2Bootstrap` | none (`{}` is accepted and ignored) |
| `cmd/backfillers/block-republisher` | `block-republisher` | `BlockRepublish` | `{"blocks":[25395651,25087888],"version":1}` (`version` defaults to 1, and must be at least 1) |

### Shape of an on-demand job

```go
func run(ctx context.Context) error {
	return temporal.RunWorker(ctx, meta, temporal.WorkerConfig{
		Name:         "<your-job>",   // task queue + OTel service name
		OpenDatabase: postgres.PoolOpener(...),
		Register:     register,
	})
}

func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	service, err := newService(ctx, deps)
	if err != nil {
		return err
	}
	// Register with an EXPLICIT name: this string is what an operator types into
	// the UI's "Workflow Type" box, so it must not drift with Go renames.
	r.RegisterWorkflowWithOptions(myWorkflow, workflow.RegisterOptions{Name: "MyWorkflow"})
	r.RegisterActivity(&myActivities{service: service})
	return nil
}
```

A job with **no parameters** registers a `Runner` instead of writing a workflow.
`RegisterRunner` puts it on the shared `cronjobWorkflow` with the bounds closed
over, so a run starts with no input payload and still gets the retry policy, the
timeouts and the progress-preserving heartbeat a scheduled cronjob gets:

```go
func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	progress := temporal.NewActivityProgress[SweepProgress]()
	runner, err := setupRunner(ctx, deps, progress)
	if err != nil {
		return err
	}
	return temporal.RegisterRunner(r, temporal.RunnerJob{
		WorkflowType: "MyOneShotRepair",
		Runner:       runner,
		Timeouts:     temporal.ActivityTimeouts{StartToClose: 6 * time.Hour, MaximumAttempts: 3},
		Progress:     progress,
	})
}
```

`Timeouts` is bound here rather than taken as input for the same reason the
workflow takes none: an operator supplies nothing. Changing it is a redeploy —
unlike a schedule, whose action bakes the bounds in until the schedule is
deleted.

### Starting a run from the Temporal UI

> **Just want to backfill prices?** [backfilling-offchain-prices.md](backfilling-offchain-prices.md)
> is the task-oriented version of this section: valid asset IDs, how to read the
> result, verification SQL and troubleshooting. The rest of this section is the
> generic mechanism.


Namespace is **`vector`** — the UI lands on `default`, which is empty for us.

`http://temporal-staging:8080/namespaces/vector/workflows` → **Start Workflow**

| Field | Value |
|---|---|
| Task Queue | the job's `Name` (e.g. `offchain-price-backfill`) |
| Workflow Type | the registered name (e.g. `OffchainPriceBackfill`) |
| Workflow ID | descriptive and unique, e.g. `backfill-weth-wbtc-2020-01-01` |
| Input | the params struct as JSON, or nothing for a job that declares none |

```json
{"assets":["weth","wrapped-bitcoin"],"from":"2020-01-01T00:00:00Z","to":"2026-08-05T00:00:00Z"}
```

The equivalent CLI call:

```bash
temporal workflow start --namespace vector \
  --task-queue offchain-price-backfill --type OffchainPriceBackfill \
  --workflow-id backfill-weth-wbtc-2020-01-01 \
  --input '{"assets":["weth","wrapped-bitcoin"],"from":"2020-01-01T00:00:00Z","to":"2026-08-05T00:00:00Z"}'
```

A `RegisterRunner` job takes no `--input` at all:

```bash
temporal workflow start --namespace vector \
  --task-queue morpho-v2-bootstrap --type MorphoV2Bootstrap \
  --workflow-id morpho-v2-bootstrap-2026-08-20
```

The **Workflow ID is the concurrency guard**: Temporal rejects a duplicate while a
run with that ID is in flight, so a double-click cannot launch the same run
twice. Re-running later means the same form with a new ID.

### Design rules for an on-demand job

1. **Fan out one activity per unit of work**, not one activity for the whole job.
   The backfill uses one per (asset, 30-day chunk) — about 162 for a six-year range
   — so a failure at chunk 140 retries that chunk instead of redoing 139 good ones.
   A `RegisterRunner` job is the deliberate exception: it is one activity, and its
   resume point lives in that activity's heartbeat details instead of in workflow
   history (see [Resuming a long run](#resuming-a-long-run-after-a-pod-kill)).
2. **Make the unit idempotent.** Activities retry, and an operator will re-run
   overlapping ranges. The backfill upserts `ON CONFLICT DO NOTHING`, which makes a
   retry free — but note the scope: `offchain_token_price`'s PK includes
   `processing_version`, and its trigger reuses a version only for the same
   `build_id`. A re-run from a *different* build appends a new version rather than
   doing nothing (ADR-0002 §3). Additive, never destructive — but do not read
   "idempotent" as "byte-identical across deploys".
3. **Validate parameters in the workflow and fail non-retryably**
   (`temporalsdk.NewNonRetryableApplicationError`). Bad input fails identically on
   every attempt; retrying it just buries the mistake behind five backoffs.
4. **Expose a `SetQueryHandler` for progress.** It is the only way to see how far a
   long run has got from the UI's Query tab without reading raw event history.
5. **Judge "did this produce anything" in the workflow, not the activity.** Only the
   workflow sees every unit, so only it can tell a genuinely empty result from one
   legitimately-empty slice. See `assertCoverage`.
6. **Set `OTEL_EXPORTER_OTLP_ENDPOINT` in the ConfigMap.** `RunWorker` instruments
   every registered activity through an interceptor, so the job emits the same
   `cronjob_runs_total` / `cronjob_run_duration_seconds` series a scheduled cronjob
   does — one record per activity execution — and inherits the alerts keyed on them
   with no per-job wiring. But that only reaches a collector if the endpoint is set:
   unset makes the providers silent no-ops, which is why `offchain-price-indexer`
   exported nothing for months while running perfectly.

   Note what the coverage does *not* include: `VectorCronjobAllRunsFailing` pages on
   "errors and no successes in an hour", which is an on-demand job's normal idle
   state, so such jobs are excluded from it. Liveness comes from
   `VectorCronjobWorkerDown` instead — add the Deployment name to its regexes.

### Deploying one

A long-running `Deployment` with `replicas: 1` (not a k8s `Job`): it has to poll the
task queue to receive a hand-started run. Model it on
`k8s/base/offchain-price-backfill/` — including its `strategy: {type: Recreate}`, so a
rollout never runs two pods against the same task queue.

Declare that strategy in the *same* commit that first adds the Deployment. Retrofitting it
onto a workload already running in staging or prod cannot be applied: the API server has
since defaulted a `rollingUpdate` block that ArgoCD does not own, server-side apply only
prunes what it owns, and every sync of the whole Application then fails — indefinitely,
with app health still green. That is what `reference-capital-backfill` hit in #640; undoing
it took two merges (restore an explicit `RollingUpdate`, then change it once that is
Synced). Declaring it up front is the only point at which it costs nothing; see
`k8s/AGENTS.md`, "Rollout strategy".

Backfillers are **not** auto-discovered like cronjobs, so the release needs wiring in
three places. Miss any of them and the tag is promoted without an image ever being
built, which surfaces as `ImagePullBackOff`:

1. Explicit `docker-build-<name>` / `docker-release-<name>` targets in
   `stl-verify/Makefile` (the generic `build-backfiller-%` and `run-backfiller-%`
   pattern rules already work; the docker ones do not).
2. A `_docker-release-<name>-internal` line in the `docker-release-all` target
   (`stl-verify/Makefile`), or nothing builds the image on release.
3. A `cronjob <name> <alias>` line in `k8s/image-roster.txt` — promotion and the
   overlay `images:` entries are both generated from it (ORB-362). Without it the
   Deployment renders an unpinned image name and sits in `ImagePullBackOff`.

## Local development

```bash
cd stl-verify
make dev-up                                  # kind cluster incl. Temporal (server, DB, UI)
make dev-env                                 # write the .env files dev-env-files names
make run-cronjob-offchain-price-indexer      # run one cronjob, sourcing its .env
make run-backfiller-offchain-price-backfill  # run the on-demand worker locally
make run-backfiller-morpho-vault-backfill    # ditto; reads the real staging raw bucket
make run-cronjob-solo NAME=morpho-v2-bootstrap  # ditto, with the cluster pod scaled to 0
```

`dev-env` covers only the jobs the `dev-env-files` target names, not every job under
`cmd/cronjobs/` and `cmd/backfillers/`. For one it does not cover, copy a covered job's
`.env` and edit the job-specific keys — the `run-*` targets say so when the file is missing.

An on-demand worker started this way registers nothing on a schedule and simply idles
on its task queue — it does no work until you start a run from the Temporal UI (or with
`temporal workflow start`), so an idle log is the expected steady state.

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
