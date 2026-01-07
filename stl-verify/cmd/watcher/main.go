// Package main provides a test application for the Watcher service.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
)

func main() {
	// Parse command-line flags
	disableBlobs := flag.Bool("disable-blobs", false, "Disable fetching blob sidecars")
	pprofAddr := flag.String("pprof", "", "Enable pprof profiling server (e.g., ':6060')")
	flag.Parse()

	// Load .env file if present
	loadEnvFile(".env")

	// Set up structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Start pprof server if enabled
	if *pprofAddr != "" {
		// Enable block and mutex profiling
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)

		go func() {
			logger.Info("starting pprof server", "addr", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				logger.Error("pprof server failed", "error", err)
			}
		}()
	}

	// Initialize OpenTelemetry tracer
	jaegerEndpoint := getEnv("JAEGER_ENDPOINT", "localhost:4317")
	shutdownTracer, err := telemetry.InitTracer(context.Background(), telemetry.TracerConfig{
		ServiceName:    "stl-watcher",
		ServiceVersion: "0.1.0",
		Environment:    getEnv("ENVIRONMENT", "development"),
		JaegerEndpoint: jaegerEndpoint,
	})
	if err != nil {
		logger.Warn("failed to init tracer, continuing without tracing", "error", err)
	} else {
		defer func() {
			if err := shutdownTracer(context.Background()); err != nil {
				logger.Warn("failed to shutdown tracer", "error", err)
			}
		}()
		logger.Info("tracer initialized", "endpoint", jaegerEndpoint)
	}

	// Get configuration from environment
	alchemyAPIKey := getEnv("ALCHEMY_API_KEY", "")
	if alchemyAPIKey == "" {
		logger.Error("ALCHEMY_API_KEY environment variable is required")
		os.Exit(1)
	}
	postgresURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	enableBackfill := getEnv("ENABLE_BACKFILL", "false") == "true"

	// Set up PostgreSQL connection for block state tracking
	db, err := sql.Open("pgx", postgresURL)
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		logger.Error("failed to ping PostgreSQL", "error", err)
		os.Exit(1)
	}

	blockStateRepo := postgres.NewBlockStateRepository(db)

	// Run migration
	if err := blockStateRepo.Migrate(context.Background()); err != nil {
		logger.Error("failed to migrate block_states table", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connected, block state tracking enabled")

	// Create Alchemy subscriber (WebSocket only)
	subscriberConfig := alchemy.SubscriberConfig{
		WebSocketURL:      fmt.Sprintf("wss://eth-mainnet.g.alchemy.com/v2/%s", alchemyAPIKey),
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        30 * time.Second,
		PingInterval:      30 * time.Second,
		PongTimeout:       10 * time.Second,
		ReadTimeout:       60 * time.Second,
		ChannelBufferSize: 100,
		HealthTimeout:     30 * time.Second,
		Logger:            logger,
	}
	subscriber, err := alchemy.NewSubscriber(subscriberConfig)
	if err != nil {
		logger.Error("failed to create subscriber", "error", err)
		os.Exit(1)
	}

	// Create OpenTelemetry instrumentation for Alchemy client
	alchemyTelemetry, err := alchemy.NewTelemetry()
	if err != nil {
		logger.Warn("failed to create alchemy telemetry, continuing without instrumentation", "error", err)
	}

	// Create Alchemy HTTP client
	httpURL := fmt.Sprintf("https://eth-mainnet.g.alchemy.com/v2/%s", alchemyAPIKey)
	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:      httpURL,
		DisableBlobs: *disableBlobs,
		Logger:       logger,
		Telemetry:    alchemyTelemetry,
	})
	if err != nil {
		logger.Error("failed to create client", "error", err)
		os.Exit(1)
	}

	// Create in-memory cache and event sink for testing
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Create LiveService (handles WebSocket subscription and reorg detection)
	config := live_data.LiveConfig{
		ChainID:              1, // Ethereum mainnet
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 128,
		DisableBlobs:         *disableBlobs,
		Logger:               logger,
	}

	liveService, err := live_data.NewLiveService(
		config,
		subscriber,
		client,
		blockStateRepo,
		cache,
		eventSink,
	)
	if err != nil {
		logger.Error("failed to create live service", "error", err)
		os.Exit(1)
	}

	// Create BackfillService (handles gap filling from DB state)
	var backfillService *backfill_gaps.BackfillService
	if enableBackfill {
		backfillConfig := backfill_gaps.BackfillConfig{
			ChainID:      1,
			BatchSize:    10,
			PollInterval: 30 * time.Second,
			Logger:       logger,
		}

		backfillService, err = backfill_gaps.NewBackfillService(
			backfillConfig,
			client,
			blockStateRepo,
			cache,
			eventSink,
		)
		if err != nil {
			logger.Error("failed to create backfill service", "error", err)
			os.Exit(1)
		}
	}

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start both services
	logger.Info("starting live service...")
	if err := liveService.Start(ctx); err != nil {
		logger.Error("failed to start live service", "error", err)
		os.Exit(1)
	}

	if enableBackfill && backfillService != nil {
		logger.Info("starting backfill service...")
		if err := backfillService.Start(ctx); err != nil {
			logger.Error("failed to start backfill service", "error", err)
			os.Exit(1)
		}
	}

	logger.Info("services started, waiting for blocks...", "backfill", enableBackfill)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("received signal, shutting down...", "signal", sig)

	// Cancel context first to signal all goroutines to stop
	cancel()

	// Create shutdown timeout context
	// Fargate default stopTimeout is 30s; we use 25s to ensure clean logging before force-kill
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	// Stop services with timeout
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		if enableBackfill && backfillService != nil {
			if err := backfillService.Stop(); err != nil {
				logger.Error("error stopping backfill service", "error", err)
			}
		}
		if err := liveService.Stop(); err != nil {
			logger.Error("error stopping live service", "error", err)
		}
	}()

	select {
	case <-shutdownDone:
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		logger.Error("shutdown timed out, forcing exit")
		os.Exit(1)
	}
}

// getEnv returns the value of an environment variable or a default value.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// loadEnvFile loads environment variables from a file.
// Each line should be in KEY=VALUE format. Lines starting with # are ignored.
// Does not override existing environment variables.
func loadEnvFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		return // Silently ignore if file doesn't exist
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split on first = only
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Don't override existing env vars
		if os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}
}
