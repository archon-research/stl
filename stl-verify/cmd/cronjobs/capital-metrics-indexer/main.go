// Package main implements a Temporal cronjob worker that snapshots per-prime
// capital metrics from the upstream Star risk-capital monitor on a schedule.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/star"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/capital_metrics_fetcher"
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
		Name:            "capital-metrics-indexer",
		IntervalEnv:     "CAPITAL_METRICS_INTERVAL",
		IntervalDefault: "5m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(_ context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	upstreamURL := env.Get("STAR_RISK_CAPITAL_URL", star.DefaultBaseURL)

	provider, err := star.NewClient(star.ClientConfig{
		BaseURL: upstreamURL,
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating star provider: %w", err)
	}

	txm, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}

	repo := postgres.NewCapitalMetricsRepository(deps.Pool, txm, deps.Logger)

	service, err := capital_metrics_fetcher.NewService(capital_metrics_fetcher.ServiceConfig{
		BenchmarkSource: upstreamURL,
		Logger:          deps.Logger,
	}, provider, repo)
	if err != nil {
		return nil, fmt.Errorf("creating capital metrics service: %w", err)
	}

	return temporal.RunnerFunc(func(ctx context.Context) error {
		return service.FetchAndStore(ctx)
	}), nil
}
