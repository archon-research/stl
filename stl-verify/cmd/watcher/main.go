// Package main provides a test application for the Watcher service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
)

func main() {
	// Parse command-line flags
	disableBlobs := flag.Bool("disable-blobs", false, "Disable fetching blob sidecars")
	pprofAddr := flag.String("pprof", "", "Enable pprof profiling server (e.g., ':6060')")
	flag.Parse()

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
	alchemyHTTPURL := getEnv("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	alchemyWSURL := getEnv("ALCHEMY_WS_URL", "wss://eth-mainnet.g.alchemy.com/v2")
	if alchemyAPIKey == "" {
		logger.Error("ALCHEMY_API_KEY environment variable is required")
		os.Exit(1)
	}
	postgresURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")

	// Set up PostgreSQL connection pool for block state tracking
	db, err := postgres.OpenDB(context.Background(), postgres.DefaultDBConfig(postgresURL))
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	blockStateRepo := postgres.NewBlockStateRepository(db, logger)

	// Run migration
	if err := blockStateRepo.Migrate(context.Background()); err != nil {
		logger.Error("failed to migrate block_states table", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connected, block state tracking enabled")

	// Create Alchemy subscriber (WebSocket only)
	subscriberConfig := alchemy.SubscriberConfig{
		WebSocketURL:      fmt.Sprintf("%s/%s", alchemyWSURL, alchemyAPIKey),
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
	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:      fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey),
		DisableBlobs: *disableBlobs,
		Logger:       logger,
		Telemetry:    alchemyTelemetry,
	})
	if err != nil {
		logger.Error("failed to create client", "error", err)
		os.Exit(1)
	}

	// Create Redis cache
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      redisAddr,
		Password:  getEnv("REDIS_PASSWORD", ""),
		DB:        0,
		TTL:       24 * time.Hour,
		KeyPrefix: "stl",
	}, logger)
	if err != nil {
		logger.Error("failed to create Redis cache", "error", err)
		os.Exit(1)
	}
	if err := cache.Ping(context.Background()); err != nil {
		logger.Error("Redis not reachable", "error", err)
		os.Exit(1)
	}
	logger.Info("Redis cache connected", "addr", redisAddr)
	defer func() {
		if err := cache.Close(); err != nil {
			logger.Warn("failed to close Redis connection", "error", err)
		}
	}()

	// Create SNS event sink
	snsEndpoint := getEnv("AWS_SNS_ENDPOINT", "http://localhost:4566")
	awsRegion := getEnv("AWS_REGION", "us-east-1")

	// Configure SNS topics for each event type
	snsTopics := snsadapter.TopicARNs{
		Blocks:   requireEnv("AWS_SNS_TOPIC_BLOCKS"),
		Receipts: requireEnv("AWS_SNS_TOPIC_RECEIPTS"),
		Traces:   requireEnv("AWS_SNS_TOPIC_TRACES"),
		Blobs:    requireEnv("AWS_SNS_TOPIC_BLOBS"),
	}

	// Configure AWS SDK for LocalStack or production
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(awsRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			getEnv("AWS_ACCESS_KEY_ID", "test"),
			getEnv("AWS_SECRET_ACCESS_KEY", "test"),
			"",
		)),
	)
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	// Create SNS client with custom endpoint for LocalStack
	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		if snsEndpoint != "" {
			o.BaseEndpoint = aws.String(snsEndpoint)
		}
	})

	eventSink, err := snsadapter.NewEventSink(snsClient, snsadapter.Config{
		Topics: snsTopics,
		Logger: logger,
	})
	if err != nil {
		logger.Error("failed to create SNS event sink", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := eventSink.Close(); err != nil {
			logger.Error("failed to close SNS event sink", "error", err)
		}
	}()
	logger.Info("SNS event sink created",
		"endpoint", snsEndpoint,
		"blocks_topic", snsTopics.Blocks,
		"receipts_topic", snsTopics.Receipts,
		"traces_topic", snsTopics.Traces,
		"blobs_topic", snsTopics.Blobs,
	)

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
	enableBackfill := getEnv("ENABLE_BACKFILL", "false") == "true"
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

// requireEnv returns the value of an environment variable or exits if not set.
func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return value
}
