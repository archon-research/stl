// Package main implements a Temporal cronjob worker that crystallizes position_daily.
// On each scheduled run it calls crystallize_position_daily(), which recomputes every
// settled UTC day's winning position_state observation and writes the ones that are
// missing. The pick and the conflict handling live in the database procedure, so a
// tick is one statement and writing nothing is the steady state.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/position_daily_crystallizer"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// Require DATABASE_URL rather than default to localhost: a deployed worker that
	// silently connected to a local (empty) database would report healthy while
	// crystallizing nothing.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		cancel()
		slog.Error("position-daily-crystallizer startup failed: missing configuration", "error", err)
		os.Exit(1)
	}

	err = temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:              env.Get("SERVICE_NAME", "position-daily-crystallizer"),
		IntervalEnv:       "POSITION_DAILY_INTERVAL",
		IntervalDefault:   "24h",
		IntervalOffsetEnv: "POSITION_DAILY_SCHEDULE_OFFSET",
		OpenDatabase:      postgres.PoolOpener(postgres.DefaultDBConfig(dbURL)),
		Setup:             setupRunner,
	})
	cancel()
	if err != nil {
		slog.Error("position-daily-crystallizer cronjob exited with error", "error", err)
		os.Exit(1)
	}
}

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.Populate(&GitCommit, &GitBranch, &BuildTime)
}

// settleAfterDefault holds back a day that has only just closed. It buys quiet rather
// than correctness: a day crystallized early is repaired by the next tick appending its
// real winner, because the pass recomputes the whole day rather than reacting to an event.
const settleAfterDefault = time.Hour

func setupRunner(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	settleAfter, err := env.GetDuration("POSITION_DAILY_SETTLE_AFTER", settleAfterDefault)
	if err != nil {
		return nil, fmt.Errorf("reading POSITION_DAILY_SETTLE_AFTER: %w", err)
	}

	telemetry, err := position_daily_crystallizer.NewTelemetry()
	if err != nil {
		return nil, fmt.Errorf("creating position daily telemetry: %w", err)
	}

	service, err := position_daily_crystallizer.NewService(
		postgres.NewPositionDailyCrystallizerRepository(deps.Pool), settleAfter, deps.Logger, telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating position daily crystallizer service: %w", err)
	}

	return temporal.RunnerFunc(service.RunOnce), nil
}
