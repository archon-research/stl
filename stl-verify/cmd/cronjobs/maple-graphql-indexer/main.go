// Package main implements a Temporal cronjob worker that snapshots Maple
// Finance borrower positions, pool stats, Sky strategies, and Syrup globals
// from the Maple GraphQL API on a schedule.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/maple"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/maple_graphql_indexer"
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
		Name:            "maple-graphql-indexer",
		IntervalEnv:     "MAPLE_SYNC_INTERVAL",
		IntervalDefault: "10m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return nil, err
	}

	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("registering build: %w", err)
	}

	client, err := maple.NewClient(maple.Config{
		Endpoint: os.Getenv("MAPLE_GRAPHQL_ENDPOINT"),
		Logger:   logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating maple client: %w", err)
	}

	repo, err := postgres.NewMapleGraphQLRepository(deps.Pool, logger, buildReg.BuildID(), 0)
	if err != nil {
		return nil, fmt.Errorf("creating maple repository: %w", err)
	}

	txManager, err := postgres.NewTxManager(deps.Pool, logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}

	telemetry, err := maple_graphql_indexer.NewTelemetry()
	if err != nil {
		return nil, fmt.Errorf("creating telemetry: %w", err)
	}

	service, err := maple_graphql_indexer.NewService(maple_graphql_indexer.ServiceConfig{
		ChainID: int64(chainID),
		Logger:  logger,
	}, client, repo, txManager, telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating maple graphql indexer service: %w", err)
	}

	return temporal.RunnerFunc(func(ctx context.Context) error {
		// The workflow-recorded schedule time is stable across activity
		// retries, so retried runs dedupe instead of multiplying snapshots.
		if scheduledAt, ok := temporal.ScheduledAtFromContext(ctx); ok {
			return service.SyncAt(ctx, scheduledAt)
		}
		// Expected only for in-flight workflows started by an old binary
		// during rollout; if it persists, retry idempotency is silently lost.
		logger.Warn("no schedule time in context; falling back to wall-clock synced_at (retries will not dedupe)")
		return service.Sync(ctx)
	}), nil
}
