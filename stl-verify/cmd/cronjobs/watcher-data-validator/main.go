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
	"github.com/archon-research/stl/stl-verify/internal/pkg/temporalutil"
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

	if err := temporalutil.RunCronjob(ctx, temporalutil.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporalutil.CronjobConfig{
		Name:            "watcher-data-validator",
		IntervalEnv:     "DATA_VALIDATION_INTERVAL",
		IntervalDefault: "1h",
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(_ context.Context, deps temporalutil.Dependencies) (any, error) {
	etherscanAPIKey := os.Getenv("ETHERSCAN_API_KEY")
	if etherscanAPIKey == "" {
		return nil, fmt.Errorf("ETHERSCAN_API_KEY environment variable is required")
	}

	etherscanClient, err := etherscan.NewClient(etherscan.ClientConfig{
		APIKey:  etherscanAPIKey,
		ChainID: int64(deps.ChainID),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etherscan client: %w", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(deps.Pool, int64(deps.ChainID), deps.Logger)

	service, err := data_validator.NewService(
		data_validator.DefaultConfig(),
		blockStateRepo,
		etherscanClient,
	)
	if err != nil {
		return nil, fmt.Errorf("creating data validator service: %w", err)
	}

	// Wrap Validate as a Runner — returns error on validation failure.
	return temporalutil.RunnerFunc(func(ctx context.Context) error {
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
