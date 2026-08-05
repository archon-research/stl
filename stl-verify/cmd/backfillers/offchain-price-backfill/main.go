// Package main implements an on-demand Temporal worker that backfills historical
// off-chain prices from CoinGecko into offchain_token_price.
//
// It carries no schedule. The worker idles on its task queue until someone starts
// a run and supplies the range, either from the Temporal UI ("Start Workflow",
// Workflow Type "OffchainPriceBackfill") or via `temporal workflow start`. That is
// the whole reason it is not a cronjob: a backfill's window is an argument, and
// cronjobWorkflow accepts none.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
)

const (
	jobName = "offchain-price-backfill"

	// workflowTypeName is what an operator types into the Temporal UI's "Workflow
	// Type" field, so it is registered explicitly rather than derived from the Go
	// function name — a rename must not invalidate the runbook or muscle memory.
	workflowTypeName = "OffchainPriceBackfill"

	// progressQueryName is queryable mid-run from the UI's Query tab, which is the
	// only way to see how far a long backfill has got without reading raw history.
	progressQueryName = "progress"

	defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() { buildinfo.PopulateFromVCS(&GitCommit, &BuildTime) }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         jobName,
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", defaultDatabaseURL))),
		Register:     register,
	})
}

func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	service, err := newPriceFetcher(ctx, deps)
	if err != nil {
		return fmt.Errorf("creating price fetcher service: %w", err)
	}

	r.RegisterWorkflowWithOptions(backfillWorkflow, workflow.RegisterOptions{Name: workflowTypeName})
	r.RegisterActivity(&backfillActivities{service: service})
	return nil
}

func newPriceFetcher(ctx context.Context, deps temporal.Dependencies) (*offchain_price_fetcher.Service, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return nil, err
	}

	apiKey, err := env.Require("COINGECKO_API_KEY")
	if err != nil {
		return nil, err
	}

	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("registering build: %w", err)
	}

	provider, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:  apiKey,
		BaseURL: os.Getenv("COINGECKO_BASE_URL"),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating coingecko provider: %w", err)
	}

	priceRepo, err := postgres.NewPriceRepository(deps.Pool, deps.Logger, buildReg.BuildID(), 0)
	if err != nil {
		return nil, fmt.Errorf("creating price repository: %w", err)
	}

	return offchain_price_fetcher.NewService(offchain_price_fetcher.ServiceConfig{
		ChainID: chainID,
		Logger:  deps.Logger,
	}, provider, priceRepo)
}
