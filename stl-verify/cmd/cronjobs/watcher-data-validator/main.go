// Package main implements a Temporal cronjob worker that validates blockchain
// data stored by the watcher against Etherscan on a schedule.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/etherscan"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
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
		Name:            "watcher-data-validator",
		IntervalEnv:     "DATA_VALIDATION_INTERVAL",
		IntervalDefault: "1h",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(_ context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return nil, err
	}

	etherscanAPIKey, err := env.Require("ETHERSCAN_API_KEY")
	if err != nil {
		return nil, err
	}

	etherscanClient, err := etherscan.NewClient(etherscan.ClientConfig{
		APIKey:  etherscanAPIKey,
		ChainID: int64(chainID),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etherscan client: %w", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(deps.Pool, int64(chainID), deps.Logger)

	service, err := data_validator.NewService(
		data_validator.DefaultConfig(),
		blockStateRepo,
		etherscanClient,
	)
	if err != nil {
		return nil, fmt.Errorf("creating data validator service: %w", err)
	}

	// Wrap Validate as a Runner — returns error on validation failure.
	return temporal.RunnerFunc(func(ctx context.Context) error {
		report, err := service.Validate(ctx)
		if err != nil {
			return fmt.Errorf("running validation: %w", err)
		}
		report.Finalize()
		if !report.Success() {
			return fmt.Errorf("validation failed: %d failures, %d errors", report.Failed, report.Errors)
		}
		return nil
	}), nil
}
