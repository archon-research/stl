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

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/blockverifier"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
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
		// SERVICE_NAME is injected per deployment from the pod's app label so each
		// per-chain validator registers its own Temporal schedule and task queue.
		// The mainnet deployment's app label resolves to "watcher-data-validator",
		// matching this default, so its existing schedule ID is preserved. The
		// default only applies to local/non-k8s runs that set no SERVICE_NAME.
		Name:            env.Get("SERVICE_NAME", "watcher-data-validator"),
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

	// DATA_VALIDATION_ENABLED lets us deploy a validator per chain uniformly while
	// keeping it off for chains our Etherscan plan does not cover yet. A disabled
	// chain returns a no-op success runner here, before requiring the API key or
	// touching the canonical source, so it makes zero Etherscan calls and never
	// reports a false failure. Re-enabling is a configmap flip, not a code change.
	enabled, err := env.GetBool("DATA_VALIDATION_ENABLED", true)
	if err != nil {
		return nil, err
	}
	if !enabled {
		deps.Logger.Info("data validation disabled for chain", "chain_id", chainID)
		return temporal.RunnerFunc(func(context.Context) error {
			// Logged on every scheduled run (not just at worker startup) so a
			// chain left disabled is visibly skipping rather than looking
			// identical to a successful validation in run history.
			deps.Logger.Info("data validation skipped: disabled for chain", "chain_id", chainID)
			return nil
		}), nil
	}

	etherscanAPIKey, err := env.Require("ETHERSCAN_API_KEY")
	if err != nil {
		return nil, err
	}

	verifier, err := blockverifier.New(int64(chainID), blockverifier.Options{
		EtherscanAPIKey: etherscanAPIKey,
		Logger:          deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating block verifier: %w", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(deps.Pool, int64(chainID), deps.Logger)

	service, err := data_validator.NewService(
		data_validator.DefaultConfig(),
		blockStateRepo,
		verifier,
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
