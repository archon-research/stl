// Package main implements a Temporal cronjob worker that exports per-table INSERT
// write cost from the application database's own statement statistics.
//
// Postgres already measures what every INSERT costs in pg_stat_statements, but only
// as cumulative counters — readable by hand, invisible to Grafana. Each tick reads
// them, diffs them against the previous reading, and publishes the per-table
// increment as OTel counters.
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
	"github.com/archon-research/stl/stl-verify/internal/services/db_statement_stats"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("db-statement-stats cronjob exited with error", "error", err)
		os.Exit(1)
	}
}

// Build metadata, populated from VCS in init() (GitBranch is set at link time).
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func run(ctx context.Context) error {
	// Require DATABASE_URL rather than default to localhost: a deployed worker that
	// silently connected to a local (empty) database would report healthy while
	// measuring a database nobody writes to.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		return fmt.Errorf("missing configuration: %w", err)
	}

	return temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:            "db-statement-stats",
		IntervalEnv:     "DB_STATEMENT_STATS_INTERVAL",
		IntervalDefault: "1m",
		OpenDatabase:    postgres.PoolOpener(statementStatsDBConfig(dbURL)),
		Setup:           setupRunner,
	})
}

// statementStatsDBConfig caps the tick's single statement, which the default cron
// pool config deliberately leaves uncapped for the backfillers and validators that
// share it.
//
// A tick reads one system view and normally takes milliseconds, so it is genuinely
// latency-bounded in the sense db.go describes. Uncapped, a wedged read would hang
// up to the activity's StartToCloseTimeout (10m) while the schedule's SKIP overlap
// policy discarded every tick behind it — around ten windows lost to one stuck
// statement. Ten seconds is far above a healthy read and far below the 1m tick, so a
// wedged read fails inside its own window.
func statementStatsDBConfig(url string) postgres.DBConfig {
	cfg := postgres.DefaultDBConfig(url)
	cfg.StatementTimeout = 10 * time.Second
	return cfg
}

func setupRunner(_ context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	telemetry, err := db_statement_stats.NewTelemetry()
	if err != nil {
		return nil, fmt.Errorf("creating db statement stats telemetry: %w", err)
	}

	reader := postgres.NewStatementStatsRepository(deps.Pool)

	service, err := db_statement_stats.NewService(db_statement_stats.ServiceConfig{
		Logger: deps.Logger,
	}, reader, telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating db statement stats service: %w", err)
	}

	return temporal.RunnerFunc(service.RunOnce), nil
}
