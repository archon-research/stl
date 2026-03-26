// Package main implements a Temporal cronjob worker that indexes Anchorage
// collateral package snapshots and operations on a schedule.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/temporalutil"
	tracker "github.com/archon-research/stl/stl-verify/internal/services/anchorage_tracker"
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
		Name:            "anchorage-indexer",
		IntervalEnv:     "ANCHORAGE_INDEX_INTERVAL",
		IntervalDefault: "15m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(ctx context.Context, deps temporalutil.Dependencies) (temporalutil.Runner, error) {
	apiURL, err := env.Require("ANCHORAGE_API_URL")
	if err != nil {
		return nil, err
	}

	apiKey, err := env.Require("ANCHORAGE_API_KEY")
	if err != nil {
		return nil, err
	}

	// IMPORTANT: An API key only has access to one prime, but there is no error if the prime doesnt match the api key,
	// we just get back the data for the api key prime, but write the wrong prime id in the database.
	primeName, err := env.Require("ANCHORAGE_PRIME")
	if err != nil {
		return nil, err
	}

	txm, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}

	repo := postgres.NewAnchorageRepository(deps.Pool, txm, deps.Logger)
	primeRepo := postgres.NewPrimeRepository(deps.Pool)

	primeID, err := primeRepo.GetPrimeIDByName(ctx, primeName)
	if err != nil {
		return nil, fmt.Errorf("resolving prime %q: %w", primeName, err)
	}
	deps.Logger.Info("resolved prime", "name", primeName, "id", primeID)

	client := tracker.NewClient(apiURL, apiKey)
	return tracker.NewService(client, repo, repo, primeID, deps.Logger), nil
}
