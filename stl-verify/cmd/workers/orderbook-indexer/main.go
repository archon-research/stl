// Package main provides the orderbook-indexer worker entry point.
//
// One source-agnostic worker process consumes raw CEX frames from SQS,
// dispatches to per-exchange parsers, updates the Redis "latest" cache
// write-through, and batches snapshots for periodic DB flushes.
//
// Scale horizontally by running more pods — SQS handles work distribution.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/orderbook_indexer"
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
	showVersion := flag.Bool("version", false, "Show version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("stl-orderbook-indexer\n")
		fmt.Printf("  Commit:     %s\n", GitCommit)
		fmt.Printf("  Branch:     %s\n", GitBranch)
		fmt.Printf("  Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	logger.Info("starting stl-orderbook-indexer",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "stl-orderbook-indexer",
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}
	defer shutdownOTEL(context.Background())

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(
		env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"),
	))
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	repo := postgres.NewOrderbookRepository(pool, logger)
	logger.Info("PostgreSQL connected")

	redisAddr := env.Get("REDIS_ADDR", "localhost:6379")
	cache := rediscache.NewOrderbookCache(rediscache.Config{
		Addr:     redisAddr,
		Password: env.Get("REDIS_PASSWORD", ""),
		DB:       0,
		TTL:      5 * time.Minute,
	}, logger)
	defer func() {
		if err := cache.Close(); err != nil {
			logger.Warn("redis close error", "error", err)
		}
	}()
	logger.Info("Redis cache connected", "addr", redisAddr)

	consumer, err := buildSQSConsumer(ctx, logger)
	if err != nil {
		logger.Error("failed to build SQS consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	flushInterval, err := time.ParseDuration(env.Get("DB_FLUSH_INTERVAL", "10s"))
	if err != nil {
		logger.Error("invalid DB_FLUSH_INTERVAL", "error", err)
		os.Exit(1)
	}

	service, err := orderbook_indexer.NewService(
		orderbook_indexer.Config{
			FlushInterval: flushInterval,
			Logger:        logger,
		},
		consumer,
		cex.AllParsers(),
		repo,
		cache,
	)
	if err != nil {
		logger.Error("failed to create service", "error", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	logger.Info("indexer running", "flushInterval", flushInterval)
	if err := service.Run(ctx); err != nil {
		logger.Error("indexer exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("indexer stopped")
}

func buildSQSConsumer(ctx context.Context, logger *slog.Logger) (*sqs.Consumer, error) {
	queueURL := requireEnv("AWS_SQS_CEX_FEED_QUEUE_URL")
	awsRegion := env.Get("AWS_REGION", "us-east-1")
	sqsEndpoint := env.Get("AWS_SQS_ENDPOINT", "")

	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(awsRegion)}
	if accessKey := os.Getenv("AWS_ACCESS_KEY_ID"); accessKey != "" {
		secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secret, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	return sqs.NewConsumer(awsCfg, sqs.Config{
		QueueURL:     queueURL,
		BaseEndpoint: sqsEndpoint,
	}, logger)
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return value
}
