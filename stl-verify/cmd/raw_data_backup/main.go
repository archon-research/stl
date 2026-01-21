// Package main provides the raw data backup service.
// This service consumes block events from SQS, fetches data from Redis cache,
// and stores it to S3 for long-term backup.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"

	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	rawdatabackup "github.com/archon-research/stl/stl-verify/internal/services/raw_data_backup"
)

// Build-time variables
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
	Main()
}

func Main() {
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

	// Get configuration from environment
	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		logger.Error("SQS_QUEUE_URL environment variable is required")
		os.Exit(1)
	}

	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		logger.Error("S3_BUCKET environment variable is required")
		os.Exit(1)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		logger.Error("REDIS_ADDR environment variable is required")
		os.Exit(1)
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	chainIDStr := os.Getenv("CHAIN_ID")
	if chainIDStr == "" {
		logger.Error("CHAIN_ID environment variable is required")
		os.Exit(1)
	}
	chainID := int64(0) // Default to Ethereum mainnet
	if chainIDStr != "" {
		var err error
		chainID, err = strconv.ParseInt(chainIDStr, 10, 64)
		if err != nil {
			logger.Error("invalid CHAIN_ID", "value", chainIDStr, "error", err)
			os.Exit(1)
		}
	}

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "eu-west-1"
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion))
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	// Create Redis cache
	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      redisAddr,
		Password:  redisPassword,
		KeyPrefix: "stl",
	}, logger)
	if err != nil {
		logger.Error("failed to create Redis cache", "error", err)
		os.Exit(1)
	}
	defer cache.Close()

	// Ping Redis to verify connection
	if err := cache.Ping(ctx); err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	logger.Info("Redis cache connected", "addr", redisAddr)

	// Create SQS consumer
	consumer, err := sqs.NewConsumer(awsCfg, sqs.Config{
		QueueURL: queueURL,
	}, logger)
	if err != nil {
		logger.Error("failed to create SQS consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()
	logger.Info("SQS consumer created", "queueURL", queueURL)

	// Create S3 writer
	writer := s3.NewWriter(awsCfg, logger)
	logger.Info("S3 writer created", "bucket", bucket)

	// Create and run the backup service
	service, err := rawdatabackup.NewService(rawdatabackup.Config{
		ChainID: chainID,
		Bucket:  bucket,
		Workers: *workers,
		Logger:  logger,
	}, consumer, cache, writer)
	if err != nil {
		logger.Error("failed to create backup service", "error", err)
		os.Exit(1)
	}

	if err := service.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("backup service failed", "error", err)
		os.Exit(1)
	}

	logger.Info("raw data backup service stopped")
}
