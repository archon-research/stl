// Package main implements a Temporal cronjob worker that records the upstream
// CORE model's published results as reference data on a schedule.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coremodelfeed"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/writerrun"
	"github.com/archon-research/stl/stl-verify/internal/services/core_model_reference_indexer"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.Populate(&GitCommit, &GitBranch, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:        "core-model-reference-indexer",
		IntervalEnv: "CORE_MODEL_REFERENCE_SYNC_INTERVAL",
		// The feed advances once a day, so any sub-day cadence sees every
		// result; 30m keeps a single failed tick inside
		// VectorCronjobAllRunsFailing's 1h window, where an hourly tick would
		// page critical on one transient failure.
		IntervalDefault: "30m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	})
	cancel()
	if err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	buildReg, runID, err := writerrun.Open(ctx, deps.Pool)
	if err != nil {
		return nil, err
	}

	txm, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}

	coreClient, err := coremodelfeed.NewClient(coremodelfeed.ClientConfig{
		BaseURL: env.Get("CORE_MODEL_REFERENCE_URL", ""),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating core feed client: %w", err)
	}

	syncTelemetry, err := core_model_reference_indexer.NewTelemetry(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating telemetry: %w", err)
	}

	service, err := core_model_reference_indexer.NewService(
		core_model_reference_indexer.Deps{
			Provider:   coreClient,
			MarketRepo: postgres.NewCoreModelReferenceMarketResultRepository(deps.Logger, runID),
			VaultRepo:  postgres.NewCoreModelReferenceVaultResultRepository(deps.Logger, runID),
			TxManager:  txm,
		},
		int(buildReg.BuildID()),
		time.Now,
		syncTelemetry,
		deps.Logger,
	)
	if err != nil {
		return nil, fmt.Errorf("creating indexer service: %w", err)
	}

	return temporal.RunnerFunc(func(ctx context.Context) error {
		return service.Run(ctx)
	}), nil
}
