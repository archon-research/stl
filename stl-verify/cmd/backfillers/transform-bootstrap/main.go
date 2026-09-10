// Package main implements an on-demand Temporal worker that backfills the
// transformation layer with pre-existing raw history.
//
// Steady-state refresh is queue-driven (an AFTER INSERT trigger on each raw
// table enqueues new rows, and the transform-worker drains the queues), but that
// only covers rows written after the trigger exists. This worker copies
// everything older — see internal/services/transform_bootstrap for the walk
// itself.
//
// # How to start a run
//
// It carries no schedule: the worker idles on its task queue, so deploying it
// never starts a run, and a deploy that re-stamps the image cannot re-trigger
// one. An operator starts a run from the Temporal UI ("Start Workflow", Workflow
// Type "TransformBootstrap") or:
//
//	temporal workflow start --namespace vector \
//	  --task-queue transform-bootstrap --type TransformBootstrap \
//	  --workflow-id transform-bootstrap-<date>
//
// The workflow ID is the concurrency guard: Temporal rejects a duplicate while a
// run with that ID is in flight, so a double-start cannot run two copies over the
// same windows.
//
// # Window
//
// The run takes no workflow input; its window comes from the environment
// (BOOTSTRAP_FROM / BOOTSTRAP_STEP / BOOTSTRAP_SOURCE), read at startup so a
// malformed value fails the pod rather than a run hours in. Changing the window
// is a ConfigMap change plus a rollout, not something typed at start time —
// which is what keeps a re-run reproducible.
//
// # Idempotency
//
// _bootstrap_<source> upserts ON CONFLICT DO UPDATE guarded by IS DISTINCT FROM,
// and the enqueue triggers are already live, so rows written while a run is in
// flight are picked up by the queue. Re-running the whole job is safe.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/worker"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/writerrun"
	"github.com/archon-research/stl/stl-verify/internal/services/transform_bootstrap"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := run(ctx)
	cancel()
	if err != nil {
		slog.Error("transform-bootstrap worker exited with error", "error", err)
		os.Exit(1)
	}
}

const (
	jobName = "transform-bootstrap"

	// workflowTypeName is what an operator types into the Temporal UI's "Workflow
	// Type" field, so it is registered explicitly rather than derived from the Go
	// function name — a rename must not invalidate the runbook or muscle memory.
	workflowTypeName = "TransformBootstrap"

	defaultBootstrapStep = 30 * 24 * time.Hour
)

// Build metadata, populated from VCS in init() (GitBranch is set at link time).
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() { buildinfo.PopulateFromVCS(&GitCommit, &BuildTime) }

func run(ctx context.Context) error {
	// Require DATABASE_URL rather than default to localhost: a one-off backfill
	// that silently ran against a local (empty) database would do nothing and
	// report success.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		return err
	}

	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         env.Get("SERVICE_NAME", jobName),
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(dbURL)),
		Register:     register,
	})
}

// register resolves the backfill window from the environment before registering,
// so a malformed BOOTSTRAP_* value fails the pod at startup instead of a run an
// operator has already started.
func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	if _, _, err := writerrun.Open(ctx, deps.Pool); err != nil {
		return err
	}

	params, err := paramsFromEnv()
	if err != nil {
		return err
	}

	runner := temporal.RunnerFunc(func(ctx context.Context) error {
		return transform_bootstrap.Run(ctx, deps.Pool, params, deps.Logger)
	})

	return temporal.RegisterRunner(r, temporal.RunnerJob{
		WorkflowType: workflowTypeName,
		Runner:       runner,
		Timeouts:     bootstrapActivityTimeouts,
	})
}

// bootstrapActivityTimeouts sizes one run against a full-history copy of every
// source: the shared 10m default would kill it mid-walk. They are bound here
// rather than left to whoever starts a run, so an operator supplies no input and
// cannot mistype a ceiling that only surfaces hours in.
//
// MaximumAttempts is 1, unlike morpho-v2-bootstrap's 3: this walk records no
// resumable progress, so a second attempt would restart at the first window
// rather than continue, turning one transient failure into a repeat of hours of
// work. The copy is idempotent, so re-running is safe — it is just expensive
// enough to be an operator's decision to re-start rather than automatic.
//
// Heartbeat is what makes the 24h ceiling tolerable: without it, a worker killed
// mid-run (a deploy rolls this Deployment like any other) would hold the activity
// open until StartToClose expired. With it, Temporal notices in minutes.
var bootstrapActivityTimeouts = temporal.ActivityTimeouts{
	StartToClose:    24 * time.Hour,
	ScheduleToClose: 24 * time.Hour,
	MaximumAttempts: 1,
	Heartbeat:       2 * time.Minute,
}

func paramsFromEnv() (transform_bootstrap.Params, error) {
	var p transform_bootstrap.Params

	// GetPositiveDuration rather than a bare parse: a non-positive step never
	// advances the per-window loop, which Run rejects anyway.
	step, err := env.GetPositiveDuration("BOOTSTRAP_STEP", defaultBootstrapStep)
	if err != nil {
		return p, err
	}
	p.Step = step

	// Unset BOOTSTRAP_FROM leaves From zero, the "derive per source" sentinel:
	// each source starts at its own earliest raw row (see transform_bootstrap.Params).
	if fromStr := env.Get("BOOTSTRAP_FROM", ""); fromStr != "" {
		from, err := transform_bootstrap.ParseTime(fromStr)
		if err != nil {
			return p, fmt.Errorf("parsing BOOTSTRAP_FROM: %w", err)
		}
		p.From = from
	}

	p.Source = env.Get("BOOTSTRAP_SOURCE", "")
	return p, nil
}
