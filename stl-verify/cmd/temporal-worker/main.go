// Package main implements a Temporal worker that polls for and executes
// scheduled workflows: price fetching and data validation.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	temporaladapter "github.com/archon-research/stl/stl-verify/internal/adapters/inbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/etherscan"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if GitCommit == "" {
					GitCommit = setting.Value
				}
			case "vcs.time":
				if BuildTime == "" {
					BuildTime = setting.Value
				}
			}
		}
	}
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

	logger.Info("starting temporal worker",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	temporalClient, err := createTemporalClient()
	if err != nil {
		return fmt.Errorf("creating temporal client: %w", err)
	}
	defer temporalClient.Close()

	pool, err := openDatabase(ctx)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	chainID, err := getChainID()
	if err != nil {
		return err
	}

	w := worker.New(temporalClient, temporaladapter.TaskQueue, worker.Options{})

	// --- Price Fetcher (required) ---
	priceFetchActivities, err := createPriceFetchActivities(pool, logger, chainID)
	if err != nil {
		return fmt.Errorf("creating price fetch activities: %w", err)
	}
	w.RegisterWorkflow(temporaladapter.PriceFetchWorkflow)
	w.RegisterActivity(priceFetchActivities)

	// --- Data Validation (optional — needs ETHERSCAN_API_KEY) ---
	dataValidationActivities, err := createDataValidationActivities(pool, logger, chainID)
	if err != nil {
		logger.Warn("data validation disabled", "reason", err)
	} else {
		w.RegisterWorkflow(temporaladapter.DataValidationWorkflow)
		w.RegisterActivity(dataValidationActivities)
		logger.Info("data validation registered")
	}

	if err := ensureSchedules(ctx, temporalClient, logger, dataValidationActivities != nil); err != nil {
		return fmt.Errorf("ensuring schedules: %w", err)
	}

	logger.Info("starting worker", "taskQueue", temporaladapter.TaskQueue)

	if err := w.Run(interruptFromContext(ctx)); err != nil {
		return fmt.Errorf("running worker: %w", err)
	}

	logger.Info("worker stopped")
	return nil
}

// ---------------------------------------------------------------------------
// Temporal client
// ---------------------------------------------------------------------------

func createTemporalClient() (client.Client, error) {
	hostPort := env.Get("TEMPORAL_HOST_PORT", "localhost:7233")
	namespace := env.Get("TEMPORAL_NAMESPACE", "sentinel")

	return client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
	})
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

func openDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	postgresURL := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	return postgres.OpenPool(ctx, postgres.DefaultDBConfig(postgresURL))
}

// ---------------------------------------------------------------------------
// Schedule management
// ---------------------------------------------------------------------------

// scheduleConfig holds the parameters for creating a Temporal schedule.
type scheduleConfig struct {
	ID              string
	IntervalEnv     string
	IntervalDefault string
	Workflow        any
	Args            []any
	WorkflowID      string
}

func ensureSchedules(ctx context.Context, c client.Client, logger *slog.Logger, dataValidation bool) error {
	if err := ensurePriceFetchSchedule(ctx, c, logger); err != nil {
		return fmt.Errorf("price fetch schedule: %w", err)
	}

	if dataValidation {
		if err := ensureDataValidationSchedule(ctx, c, logger); err != nil {
			return fmt.Errorf("data validation schedule: %w", err)
		}
	}

	return nil
}

func ensurePriceFetchSchedule(ctx context.Context, c client.Client, logger *slog.Logger) error {
	return ensureSchedule(ctx, c, logger, scheduleConfig{
		ID:              temporaladapter.PriceFetchScheduleID,
		IntervalEnv:     "PRICE_FETCH_INTERVAL",
		IntervalDefault: "5m",
		Workflow:        temporaladapter.PriceFetchWorkflow,
		Args:            []any{temporaladapter.PriceFetchWorkflowInput{AssetIDs: []string{}}},
		WorkflowID:      "scheduled-price-fetch",
	})
}

func ensureDataValidationSchedule(ctx context.Context, c client.Client, logger *slog.Logger) error {
	return ensureSchedule(ctx, c, logger, scheduleConfig{
		ID:              temporaladapter.DataValidationScheduleID,
		IntervalEnv:     "DATA_VALIDATION_INTERVAL",
		IntervalDefault: "1h",
		Workflow:        temporaladapter.DataValidationWorkflow,
		Args:            nil,
		WorkflowID:      "scheduled-data-validation",
	})
}

func ensureSchedule(ctx context.Context, c client.Client, logger *slog.Logger, cfg scheduleConfig) error {
	interval := env.Get(cfg.IntervalEnv, cfg.IntervalDefault)

	intervalDuration, err := time.ParseDuration(interval)
	if err != nil {
		return fmt.Errorf("parsing %s %q: %w", cfg.IntervalEnv, interval, err)
	}

	handle := c.ScheduleClient().GetHandle(ctx, cfg.ID)
	if _, err := handle.Describe(ctx); err == nil {
		logger.Info("schedule already exists", "scheduleID", cfg.ID)
		return nil
	}

	_, err = c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: cfg.ID,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{
				{Every: intervalDuration},
			},
		},
		Action: &client.ScheduleWorkflowAction{
			Workflow:  cfg.Workflow,
			Args:      cfg.Args,
			ID:        cfg.WorkflowID,
			TaskQueue: temporaladapter.TaskQueue,
		},
	})
	if err != nil {
		return fmt.Errorf("creating schedule %q: %w", cfg.ID, err)
	}

	logger.Info("schedule created", "scheduleID", cfg.ID, "interval", intervalDuration)
	return nil
}

// ---------------------------------------------------------------------------
// Activity factories
// ---------------------------------------------------------------------------

func createPriceFetchActivities(pool *pgxpool.Pool, logger *slog.Logger, chainID int) (*temporaladapter.PriceFetchActivities, error) {
	provider, err := createCoinGeckoProvider(logger)
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

func createDataValidationActivities(pool *pgxpool.Pool, logger *slog.Logger, chainID int) (*temporaladapter.DataValidationActivities, error) {
	etherscanAPIKey := os.Getenv("ETHERSCAN_API_KEY")
	if etherscanAPIKey == "" {
		return nil, fmt.Errorf("ETHERSCAN_API_KEY not set")
	}

	etherscanClient, err := etherscan.NewClient(etherscan.ClientConfig{
		APIKey:  etherscanAPIKey,
		ChainID: int64(chainID),
		Logger:  logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etherscan client: %w", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(pool, int64(chainID), logger)

	service, err := data_validator.NewService(
		data_validator.DefaultConfig(),
		blockStateRepo,
		etherscanClient,
	)
	if err != nil {
		return nil, fmt.Errorf("creating data validator service: %w", err)
	}

	return temporaladapter.NewDataValidationActivities(service)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func createCoinGeckoProvider(logger *slog.Logger) (*coingecko.Client, error) {
	apiKey := os.Getenv("COINGECKO_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("COINGECKO_API_KEY environment variable is required")
	}

	return coingecko.NewClient(coingecko.ClientConfig{
		APIKey:  apiKey,
		BaseURL: os.Getenv("COINGECKO_BASE_URL"),
		Logger:  logger,
	})
}

func getChainID() (int, error) {
	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.Atoi(chainIDStr)
	if err != nil {
		return 0, fmt.Errorf("CHAIN_ID must be a valid integer: %w", err)
	}
	return chainID, nil
}

// interruptFromContext returns a channel that closes when the context is cancelled.
// This bridges context.Context cancellation to Temporal's worker.InterruptCh pattern.
func interruptFromContext(ctx context.Context) <-chan any {
	ch := make(chan any)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}
