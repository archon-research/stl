// Package main implements a Temporal worker that polls for and executes
// scheduled price-fetching workflows via CoinGecko.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	temporaladapter "github.com/archon-research/stl/stl-verify/internal/adapters/inbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/temporalutil"
	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info.
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

	if err := run(ctx); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	logger.Info("starting offchain-price-indexer worker",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	chainID, err := getChainID()
	if err != nil {
		return err
	}

	if err := validateRequiredEnv(); err != nil {
		return err
	}

	pool, err := openDatabase(ctx)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	temporalClient, err := temporalutil.CreateClient()
	if err != nil {
		return fmt.Errorf("creating temporal client: %w", err)
	}
	defer temporalClient.Close()

	if err := temporalutil.WaitForServer(ctx, temporalClient, logger); err != nil {
		return fmt.Errorf("waiting for Temporal: %w", err)
	}

	activities, err := createActivities(pool, logger, chainID)
	if err != nil {
		return fmt.Errorf("creating activities: %w", err)
	}

	w := worker.New(temporalClient, temporaladapter.TaskQueue, worker.Options{})
	w.RegisterWorkflow(temporaladapter.PriceFetchWorkflow)
	w.RegisterActivity(activities)

	if err := ensureSchedule(ctx, temporalClient, logger); err != nil {
		return fmt.Errorf("ensuring schedule: %w", err)
	}

	logger.Info("starting worker", "taskQueue", temporaladapter.TaskQueue)

	if err := w.Run(temporalutil.InterruptFromContext(ctx)); err != nil {
		return fmt.Errorf("running worker: %w", err)
	}

	logger.Info("worker stopped")
	return nil
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

func ensureSchedule(ctx context.Context, c client.Client, logger *slog.Logger) error {
	return temporalutil.EnsureSchedule(ctx, c, logger, temporalutil.ScheduleConfig{
		ID:              temporaladapter.PriceFetchScheduleID,
		IntervalEnv:     "PRICE_FETCH_INTERVAL",
		IntervalDefault: "5m",
		Workflow:        temporaladapter.PriceFetchWorkflow,
		// Empty AssetIDs signals the price fetcher service to load all known
		// assets from the database at execution time rather than being
		// constrained to a static list baked into the schedule.
		Args:       []any{temporaladapter.PriceFetchWorkflowInput{AssetIDs: []string{}}},
		WorkflowID: "scheduled-price-fetch",
		TaskQueue:  temporaladapter.TaskQueue,
	})
}

// ---------------------------------------------------------------------------
// Activity factory
// ---------------------------------------------------------------------------

func createActivities(pool *pgxpool.Pool, logger *slog.Logger, chainID int) (*temporaladapter.PriceFetchActivities, error) {
	apiKey := os.Getenv("COINGECKO_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("COINGECKO_API_KEY environment variable is required")
	}

	provider, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:  apiKey,
		BaseURL: os.Getenv("COINGECKO_BASE_URL"),
		Logger:  logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating coingecko provider: %w", err)
	}

	priceRepo, err := postgres.NewPriceRepository(pool, logger, 0)
	if err != nil {
		return nil, fmt.Errorf("creating price repository: %w", err)
	}

	service, err := offchain_price_fetcher.NewService(offchain_price_fetcher.ServiceConfig{
		ChainID:     chainID,
		Concurrency: 5,
		Logger:      logger,
	}, provider, priceRepo)
	if err != nil {
		return nil, fmt.Errorf("creating price fetcher service: %w", err)
	}

	return temporaladapter.NewPriceFetchActivities(service)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func openDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	postgresURL := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	return postgres.OpenPool(ctx, postgres.DefaultDBConfig(postgresURL))
}

func validateRequiredEnv() error {
	if os.Getenv("COINGECKO_API_KEY") == "" {
		return fmt.Errorf("COINGECKO_API_KEY environment variable is required")
	}
	return nil
}

func getChainID() (int, error) {
	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.Atoi(chainIDStr)
	if err != nil {
		return 0, fmt.Errorf("CHAIN_ID must be a valid integer: %w", err)
	}
	return chainID, nil
}
