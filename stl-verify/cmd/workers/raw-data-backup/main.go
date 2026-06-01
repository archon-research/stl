// Package main provides the raw data backup service.
// This service consumes block events from SQS, fetches data from Redis cache,
// and stores it to S3 for long-term backup.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"

	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	rawdatabackup "github.com/archon-research/stl/stl-verify/internal/services/raw_data_backup"
)

// Build-time variables
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	// Parse command-line flags
	showVersion := flag.Bool("version", false, "Show version information and exit")
	workers := flag.Int("workers", 2, "Number of concurrent workers")
	flag.Parse()

	if *showVersion {
		fmt.Printf("stl-raw-data-backup\n")
		fmt.Printf("  Commit:     %s\n", GitCommit)
		fmt.Printf("  Branch:     %s\n", GitBranch)
		fmt.Printf("  Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	// Set up structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	logger.Info("starting stl-raw-data-backup",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	// Handle shutdown signals
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, logger, *workers); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("backup service failed", "error", err)
		os.Exit(1)
	}

	logger.Info("raw data backup service stopped")
}

// workerConfig holds the resolved configuration for the backup worker.
type workerConfig struct {
	queueURL            string
	dlqQueueURL         string
	bucket              string
	redisAddr           string
	chainID             int64
	deployEnv           string
	awsRegion           string
	workers             int
	cacheMissMaxRetries int
}

// parseConfig resolves the worker configuration from the environment. Required
// variables that are missing produce an error so misconfiguration is loud.
func parseConfig(workers int) (workerConfig, error) {
	cfg := workerConfig{workers: workers}

	cfg.queueURL = os.Getenv("SQS_QUEUE_URL")
	if cfg.queueURL == "" {
		return workerConfig{}, fmt.Errorf("SQS_QUEUE_URL environment variable is required")
	}

	// The DLQ URL is optional: by default it is derived from the main queue URL
	// via the infra naming convention, with an explicit override. Deriving it
	// keeps the rollout to just the image plus the sqs:SendMessage IAM grant,
	// with no new secret or env wiring.
	cfg.dlqQueueURL = os.Getenv("DLQ_QUEUE_URL")
	if cfg.dlqQueueURL == "" {
		derived, err := deriveDLQURL(cfg.queueURL)
		if err != nil {
			return workerConfig{}, err
		}
		cfg.dlqQueueURL = derived
	}
	// The DLQ is FIFO and the publisher always sends a MessageGroupId, which a
	// standard queue rejects at runtime. Reject a non-FIFO override early so a
	// misconfigured DLQ_QUEUE_URL fails fast rather than silently re-blocking the
	// main queue (permanent failures would be preserved instead of dead-lettered).
	if !strings.HasSuffix(cfg.dlqQueueURL, ".fifo") {
		return workerConfig{}, fmt.Errorf("DLQ_QUEUE_URL must be a FIFO queue URL ending in .fifo, got %q", cfg.dlqQueueURL)
	}

	cfg.bucket = os.Getenv("S3_BUCKET")
	if cfg.bucket == "" {
		return workerConfig{}, fmt.Errorf("S3_BUCKET environment variable is required")
	}

	cfg.redisAddr = os.Getenv("REDIS_ADDR")
	if cfg.redisAddr == "" {
		return workerConfig{}, fmt.Errorf("REDIS_ADDR environment variable is required")
	}

	chainIDStr := os.Getenv("CHAIN_ID")
	if chainIDStr == "" {
		return workerConfig{}, fmt.Errorf("CHAIN_ID environment variable is required")
	}
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return workerConfig{}, fmt.Errorf("invalid CHAIN_ID %q: %w", chainIDStr, err)
	}
	cfg.chainID = chainID

	cfg.deployEnv = os.Getenv("DEPLOY_ENV")
	if cfg.deployEnv == "" {
		return workerConfig{}, fmt.Errorf("DEPLOY_ENV environment variable is required")
	}

	if err := chainutil.ValidateS3BucketForChain(cfg.chainID, cfg.bucket, cfg.deployEnv); err != nil {
		return workerConfig{}, fmt.Errorf("S3 bucket validation failed: %w", err)
	}

	cfg.awsRegion = env.Get("AWS_REGION", "eu-west-1")

	// Optional drain-speed lever: how many extra cache reads to attempt before
	// treating a miss as permanent. Defaults to the service default when unset.
	cacheMissMaxRetries, err := env.GetInt("CACHE_MISS_MAX_RETRIES", rawdatabackup.ConfigDefaults().CacheMissMaxRetries)
	if err != nil {
		return workerConfig{}, err
	}
	if cacheMissMaxRetries < 0 {
		return workerConfig{}, fmt.Errorf("CACHE_MISS_MAX_RETRIES must be >= 0, got %d", cacheMissMaxRetries)
	}
	cfg.cacheMissMaxRetries = cacheMissMaxRetries

	if workers <= 0 {
		cfg.workers = 2
	}
	if workersEnv := os.Getenv("WORKERS"); workersEnv != "" {
		if w, err := strconv.Atoi(workersEnv); err == nil && w > 0 {
			cfg.workers = w
		}
	}

	return cfg, nil
}

// deriveDLQURL derives the dead-letter queue URL from the main backup queue URL
// using the infra naming convention (<name>.fifo becomes <name>-dlq.fifo). The
// backup queues are FIFO, so the URL always ends in ".fifo".
func deriveDLQURL(queueURL string) (string, error) {
	const suffix = ".fifo"
	if !strings.HasSuffix(queueURL, suffix) {
		return "", fmt.Errorf("cannot derive DLQ URL from %q: expected a .fifo queue URL (set DLQ_QUEUE_URL to override)", queueURL)
	}
	return strings.TrimSuffix(queueURL, suffix) + "-dlq" + suffix, nil
}

// run wires up the worker's dependencies and runs the backup service until the
// context is cancelled. It returns an error instead of calling os.Exit so it
// can be exercised in tests.
func run(ctx context.Context, logger *slog.Logger, workers int) error {
	cfg, err := parseConfig(workers)
	if err != nil {
		return err
	}

	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.awsRegion))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create Redis cache
	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      cfg.redisAddr,
		KeyPrefix: "stl",
	}, logger)
	if err != nil {
		return fmt.Errorf("failed to create Redis cache: %w", err)
	}
	defer cache.Close()

	// Ping Redis to verify connection
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	logger.Info("Redis cache connected", "addr", cfg.redisAddr)

	// Create SQS consumer
	consumer, err := sqs.NewConsumer(awsCfg, sqs.Config{
		QueueURL:     cfg.queueURL,
		BaseEndpoint: env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("failed to create SQS consumer: %w", err)
	}
	defer consumer.Close()
	logger.Info("SQS consumer created", "queueURL", cfg.queueURL)

	// Create the dead-letter publisher for permanent failures
	deadLetter, err := sqs.NewDeadLetterPublisher(awsCfg, sqs.Config{
		QueueURL:     cfg.dlqQueueURL,
		BaseEndpoint: env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("failed to create dead-letter publisher: %w", err)
	}
	logger.Info("SQS dead-letter publisher created", "dlqQueueURL", cfg.dlqQueueURL)

	// Create S3 writer
	writer := s3.NewWriter(awsCfg, logger)
	logger.Info("S3 writer created", "bucket", cfg.bucket)

	// Initialize OpenTelemetry tracing and metrics
	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "raw-data-backup",
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize telemetry: %w", err)
	}
	defer shutdownOTEL(context.Background())

	metricsRec, err := telemetry.NewMetrics("stl-verify/raw_data_backup")
	if err != nil {
		logger.Error("failed to update metrics recorder", "error", err)
		// Proceed without metrics
	}

	// Create and run the backup service
	service, err := rawdatabackup.NewService(rawdatabackup.Config{
		ChainID:             cfg.chainID,
		Bucket:              cfg.bucket,
		Workers:             cfg.workers,
		BatchSize:           10, // Max messages per SQS receive call
		CacheMissMaxRetries: cfg.cacheMissMaxRetries,
		Logger:              logger,
		Metrics:             metricsRec,
	}, consumer, cache, writer, deadLetter)
	if err != nil {
		return fmt.Errorf("failed to create backup service: %w", err)
	}

	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("backup service failed: %w", err)
	}

	return nil
}
