// Package main implements a Temporal cronjob worker that fetches token prices
// from CoinGecko on a schedule.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/temporalutil"
	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
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
		Name:            "offchain-price-indexer",
		IntervalEnv:     "PRICE_FETCH_INTERVAL",
		IntervalDefault: "5m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	}); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(_ context.Context, deps temporalutil.Dependencies) (temporalutil.Runner, error) {
	apiKey, err := env.Require("COINGECKO_API_KEY")
	if err != nil {
		return nil, err
	}

	provider, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:  apiKey,
		BaseURL: os.Getenv("COINGECKO_BASE_URL"),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating coingecko provider: %w", err)
	}

	priceRepo, err := postgres.NewPriceRepository(deps.Pool, deps.Logger, 0)
	if err != nil {
		return nil, fmt.Errorf("creating price repository: %w", err)
	}

	service, err := offchain_price_fetcher.NewService(offchain_price_fetcher.ServiceConfig{
		ChainID:     deps.ChainID,
		Concurrency: 5,
		Logger:      deps.Logger,
	}, provider, priceRepo)
	if err != nil {
		return nil, fmt.Errorf("creating price fetcher service: %w", err)
	}

	// Wrap FetchCurrentPrices as a Runner — empty AssetIDs loads all from DB.
	return temporalutil.RunnerFunc(func(ctx context.Context) error {
		return service.FetchCurrentPrices(ctx, []string{})
	}), nil
}
