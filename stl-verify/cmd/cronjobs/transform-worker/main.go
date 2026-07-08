// Package main implements a Temporal cronjob worker that materializes the
// transformation layer incrementally. On each scheduled run it invokes every
// transformed._run_<table>() function, each of which reads raw rows at or past
// its build_id watermark, applies the canonical rename/cast/fill, upserts into
// the transformed hypertable, and advances the watermark.
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
	"github.com/archon-research/stl/stl-verify/internal/services/transform_worker"
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
		Name:            env.Get("SERVICE_NAME", "transform-worker"),
		IntervalEnv:     "TRANSFORM_INTERVAL",
		IntervalDefault: "10m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(_ context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	telemetry, err := transform_worker.NewTelemetry()
	if err != nil {
		return nil, fmt.Errorf("creating transform telemetry: %w", err)
	}

	runner := postgres.NewTransformRunnerRepository(deps.Pool, deps.Logger)

	service, err := transform_worker.NewService(runner, deps.Logger, telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating transform worker service: %w", err)
	}

	return temporal.RunnerFunc(service.RunOnce), nil
}
