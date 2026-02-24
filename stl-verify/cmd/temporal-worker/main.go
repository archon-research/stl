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
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	temporaladapter "github.com/archon-research/stl/stl-verify/internal/adapters/inbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/etherscan"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
	"github.com/archon-research/stl/stl-verify/internal/services/offchain_price_fetcher"
)

// Build metadata injected via -X ldflags at build time.
// readBuildMetadata() falls back to vcs.* from debug.ReadBuildInfo() for local runs.
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

// buildMetadata holds resolved build info for logging.
type buildMetadata struct {
	commit    string
	branch    string
	buildTime string
}

// readBuildMetadata returns build metadata, preferring ldflag values and
// falling back to vcs.* settings embedded by the Go toolchain.
func readBuildMetadata() buildMetadata {
	m := buildMetadata{
		commit:    GitCommit,
		branch:    GitBranch,
		buildTime: BuildTime,
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if m.commit == "" {
					m.commit = setting.Value
				}
			case "vcs.time":
				if m.buildTime == "" {
					m.buildTime = setting.Value
				}
			}
		}
	}
	return m
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

	meta := readBuildMetadata()
	logger.Info("starting temporal worker",
		"commit", meta.commit,
		"branch", meta.branch,
		"buildTime", meta.buildTime,
	)

	// Validate required configuration before opening expensive connections.
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

	temporalClient, err := createTemporalClient()
	if err != nil {
		return fmt.Errorf("creating temporal client: %w", err)
	}
	defer temporalClient.Close()

	if err := waitForTemporal(ctx, temporalClient, logger); err != nil {
		return fmt.Errorf("waiting for Temporal: %w", err)
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

// waitForTemporal polls the Temporal server until it is reachable or ctx is cancelled.
// This handles the startup race where the worker launches before Temporal has finished
// initializing (common in docker-compose dev environments).
func waitForTemporal(ctx context.Context, c client.Client, logger *slog.Logger) error {
	for {
		_, err := c.WorkflowService().GetSystemInfo(ctx, &workflowservicepb.GetSystemInfoRequest{})
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if st, ok := grpcstatus.FromError(err); ok {
			switch st.Code() {
			case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
				// Transient — keep retrying.
			default:
				return fmt.Errorf("unexpected Temporal error: %w", err)
			}
		}
		logger.Info("waiting for Temporal to become ready", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
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
		// Empty AssetIDs signals the price fetcher service to load all known
		// assets from the database at execution time rather than being
		// constrained to a static list baked into the schedule.
		Args:       []any{temporaladapter.PriceFetchWorkflowInput{AssetIDs: []string{}}},
		WorkflowID: "scheduled-price-fetch",
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

	// Attempt to create the schedule directly. Temporal returns AlreadyExists if it is
	// already present (normal after any restart). Skipping a Describe-first check avoids
	// a race window and also sidesteps the case where Describe returns an internal error
	// due to a stuck workflow task left over from a previous run.
	//
	// Note: changes to the interval env var will NOT take effect until the existing
	// schedule is deleted from Temporal and the worker is restarted.
	// Use the Temporal UI or CLI to delete a schedule.
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
		// The Temporal SDK wraps the gRPC AlreadyExists error inconsistently across
		// SDK versions, so we check both the gRPC status code and the error message.
		if grpcstatus.Code(err) == codes.AlreadyExists || strings.Contains(err.Error(), "already registered") {
			// Schedule already exists (normal on restart or concurrent worker startup).
			logger.Info("schedule already exists", "scheduleID", cfg.ID)
			return nil
		}
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

// validateRequiredEnv checks that all required environment variables are set
// before opening expensive connections (DB, Temporal).
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
