// Package main provides a test application for the Watcher service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"strconv"
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
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	// Use Go's built-in build info (Go 1.18+) if ldflags weren't provided
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
	// Parse command-line flags
	enableTraces := flag.Bool("enable-traces", true, "Enable fetching execution traces (trace_block)")
	enableBlobs := flag.Bool("enable-blobs", false, "Enable fetching blob sidecars")
	parallelRPC := flag.Bool("parallel-rpc", true, "Use parallel goroutines for RPC calls instead of batching (faster but uses more credits)")
	pprofAddr := flag.String("pprof", "", "Enable pprof profiling server (e.g., ':6060')")
	traceFile := flag.String("trace", "", "Write execution trace to file")
	showVersion := flag.Bool("version", false, "Show version information and exit")
	flag.Parse()

	if *traceFile != "" {
		f, err := os.Create(*traceFile)
		if err != nil {
			log.Fatalf("failed to create trace file: %v", err)
		}
		defer f.Close()
		if err := trace.Start(f); err != nil {
			log.Fatalf("failed to start trace: %v", err)
		}
		defer trace.Stop()
	}

	// Show version if requested
	if *showVersion {
		fmt.Printf("stl-watcher\n")
		fmt.Printf("  Commit:     %s\n", GitCommit)
		fmt.Printf("  Branch:     %s\n", GitBranch)
		fmt.Printf("  Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	// Set up context with cancellation (created early for consistent use throughout init)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Log version info on startup
	logger.Info("starting stl-watcher",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	// Start pprof server if enabled
	if *pprofAddr != "" {
		// Enable block and mutex profiling
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		runtime.SetCPUProfileRate(1)

		go func() {
			logger.Info("starting pprof server", "addr", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				logger.Error("pprof server failed", "error", err)
			}
		}()
	}

	// Initialize OpenTelemetry tracer
	// JAEGER_ENDPOINT is the OTLP endpoint - works with ADOT/X-Ray in AWS or Jaeger locally
	traceEndpoint := env.Get("JAEGER_ENDPOINT", "localhost:4317")
	shutdownTracer, err := telemetry.InitTracer(ctx, telemetry.TracerConfig{
		ServiceName:    "stl-watcher",
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Environment:    env.Get("ENVIRONMENT", "development"),
		JaegerEndpoint: traceEndpoint,
	})
	if err != nil {
		logger.Warn("failed to init tracer, continuing without tracing", "error", err)
	} else {
		defer func() {
			if err := shutdownTracer(context.Background()); err != nil {
				logger.Warn("failed to shutdown tracer", "error", err)
			}
		}()
		logger.Info("tracer initialized", "endpoint", traceEndpoint)
	}

	// Initialize OpenTelemetry metrics
	otelEndpoint := env.Get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdownMetrics, err := telemetry.InitMetrics(ctx, telemetry.MetricConfig{
		ServiceName:    "stl-watcher",
		ServiceVersion: "0.1.0",
		Environment:    env.Get("ENVIRONMENT", "development"),
		OTLPEndpoint:   otelEndpoint,
	})
	if err != nil {
		logger.Warn("failed to init metrics, continuing without metrics export", "error", err)
	} else {
		defer func() {
			if err := shutdownMetrics(context.Background()); err != nil {
				logger.Warn("failed to shutdown metrics", "error", err)
			}
		}()
		if otelEndpoint != "" {
			logger.Info("metrics initialized", "endpoint", otelEndpoint)
		}
	}

	// Get configuration from environment
	alchemyAPIKey := env.Get("ALCHEMY_API_KEY", "")
	alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	alchemyWSURL := env.Get("ALCHEMY_WS_URL", "wss://eth-mainnet.g.alchemy.com/v2")
	if alchemyAPIKey == "" {
		logger.Error("ALCHEMY_API_KEY environment variable is required")
		os.Exit(1)
	}
	chainID, err := strconv.ParseInt(requireEnv("CHAIN_ID"), 10, 64)
	if err != nil {
		logger.Error("CHAIN_ID must be a valid integer", "error", err)
		os.Exit(1)
	}

	postgresURL := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")

	// Set up PostgreSQL connection pool for block state tracking
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(postgresURL))
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	blockStateRepo := postgres.NewBlockStateRepository(pool, chainID, logger)

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
		EnableTraces: *enableTraces,
		EnableBlobs:  *enableBlobs,
		ParallelRPC:  *parallelRPC,
		Logger:       logger,
		Telemetry:    alchemyTelemetry,
	})
	if err != nil {
		logger.Error("failed to create client", "error", err)
		os.Exit(1)
	}

	logger.Info("alchemy client configured",
		"enableTraces", *enableTraces,
		"enableBlobs", *enableBlobs,
		"parallelRPC", *parallelRPC,
		"chainID", chainID,
	)

	// Create Redis cache
	redisAddr := env.Get("REDIS_ADDR", "localhost:6379")
	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      redisAddr,
		Password:  env.Get("REDIS_PASSWORD", ""),
		DB:        0,
		TTL:       2 * 24 * time.Hour, // 2 days - allows retry of failed backups
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
	snsEndpoint := env.Get("AWS_SNS_ENDPOINT", "http://localhost:4566")
	awsRegion := env.Get("AWS_REGION", "us-east-1")

	// Single SNS FIFO topic for all event types
	snsTopicARN := requireEnv("AWS_SNS_TOPIC_ARN")

	// Configure AWS SDK for LocalStack or production
	// In production (ECS/Fargate), use the default credential chain which picks up IAM role credentials.
	// For local development with LocalStack, use static credentials from environment variables.
	awsConfigOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(awsRegion),
	}

	// Only use static credentials if explicitly set (for LocalStack)
	// In ECS/Fargate, these won't be set and the SDK will use the IAM role
	if accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); accessKeyID != "" {
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		awsConfigOptions = append(awsConfigOptions,
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				accessKeyID,
				secretKey,
				"",
			)),
		)
		logger.Debug("using static AWS credentials from environment")
	} else {
		logger.Debug("using default AWS credential chain (IAM role)")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsConfigOptions...)
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
		TopicARN: snsTopicARN,
		Logger:   logger,
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
		"topic", snsTopicARN,
	)

	// Create LiveService (handles WebSocket subscription and reorg detection)
	config := live_data.LiveConfig{
		ChainID:            chainID,
		FinalityBlockCount: 64,
		EnableTraces:       *enableTraces,
		EnableBlobs:        *enableBlobs,
		Logger:             logger,
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
	enableBackfill := env.Get("ENABLE_BACKFILL", "false") == "true"
	if enableBackfill {
		backfillConfig := backfill_gaps.BackfillConfig{
			ChainID:      chainID,
			BatchSize:    10,
			PollInterval: 30 * time.Second,
			EnableTraces: *enableTraces,
			EnableBlobs:  *enableBlobs,
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

// requireEnv returns the value of an environment variable or exits if not set.
func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return value
}
