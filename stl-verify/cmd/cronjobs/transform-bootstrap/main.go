// Package main implements the transform-bootstrap Temporal worker: a trigger-only
// (manual) cronjob that copies pre-existing raw history into the transformation
// layer. The worker sits idle — the schedule is created paused with no interval —
// until an operator clicks Trigger in the Temporal UI (or runs
// `temporal schedule trigger --schedule-id transform-bootstrap`). Deploys never
// start a backfill; retries are a click, not a re-merge (VEC-490).
//
// The backfill itself is the Temporal-free transform_bootstrap service; this
// binary only wires it into the shared cronjob worker. Params come from the
// environment (set in the ConfigMap), not flags:
//
//	BOOTSTRAP_FROM    RFC3339 or YYYY-MM-DD; unset = each source's earliest raw row
//	BOOTSTRAP_STEP    window size per _bootstrap call (Go duration, e.g. "720h")
//	BOOTSTRAP_SOURCE  restrict to a single source; unset = all
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/transform_bootstrap"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name: "transform-bootstrap",
		// Trigger-only: the schedule is created paused with no interval; a human
		// runs it from the Temporal UI. IntervalDefault is intentionally unset.
		Manual: true,
		// A full-history backfill runs for hours: give the activity a generous
		// start-to-close (and matching schedule-to-close, else it caps at the 30m
		// default), a single attempt (mirrors the old Job's backoffLimit: 0 —
		// idempotent but expensive, so a retry is a human decision), and a short
		// heartbeat so a crashed worker is detected in minutes, not at 24h.
		ActivityStartToCloseTimeout:    24 * time.Hour,
		ActivityScheduleToCloseTimeout: 24 * time.Hour,
		ActivityHeartbeatTimeout:       2 * time.Minute,
		ActivityMaxAttempts:            1,
		// DATABASE_URL is required (a one-off backfill that silently ran against a
		// local empty DB would do nothing and report success).
		OpenDatabase: func(ctx context.Context) (*pgxpool.Pool, error) {
			dsn, err := env.Require("DATABASE_URL")
			if err != nil {
				return nil, err
			}
			return postgres.PoolOpener(postgres.DefaultDBConfig(dsn))(ctx)
		},
		Setup: setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// setupRunner reads the backfill params from the environment (so a bad
// BOOTSTRAP_FROM fails fast at startup, not mid-run) and returns a Runner that
// invokes the transform_bootstrap service on each Trigger.
func setupRunner(_ context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	params, err := paramsFromEnv()
	if err != nil {
		return nil, err
	}
	return temporal.RunnerFunc(func(ctx context.Context) error {
		return transform_bootstrap.Run(ctx, deps.Pool, params, deps.Logger)
	}), nil
}

func paramsFromEnv() (transform_bootstrap.Params, error) {
	var p transform_bootstrap.Params

	stepStr := env.Get("BOOTSTRAP_STEP", "720h")
	step, err := time.ParseDuration(stepStr)
	if err != nil {
		return p, fmt.Errorf("parsing BOOTSTRAP_STEP %q: %w", stepStr, err)
	}
	p.Step = step

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
