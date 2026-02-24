// Package main implements a SparkLend position tracker that monitors lending protocol
// activity on Ethereum. It processes transaction receipts from Redis (triggered by SQS
// messages), extracts position-changing events with collateral data, and stores the
// results in PostgreSQL for downstream analysis.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_position_tracker"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info
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

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

type cliConfig struct {
	queueURL    string
	redisAddr   string
	dbURL       string
	alchemyURL  string
	maxMessages int
	waitTime    int
	chainID     int64
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("sparklend-position-tracker", flag.ContinueOnError)
	queueURL := fs.String("queue", "", "SQS Queue URL")
	redisAddr := fs.String("redis", "", "Redis address")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	maxMessages := fs.Int("max", 10, "Max messages per poll")
	waitTime := fs.Int("wait", 20, "Wait time in seconds (long polling)")
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		queueURL:    *queueURL,
		redisAddr:   *redisAddr,
		dbURL:       *dbURL,
		maxMessages: *maxMessages,
		waitTime:    *waitTime,
	}

	if cfg.queueURL == "" {
		cfg.queueURL = env.Get("AWS_SQS_QUEUE_URL", "")
	}
	if cfg.queueURL == "" {
		return cliConfig{}, fmt.Errorf("queue URL not provided (use -queue flag or AWS_SQS_QUEUE_URL env var)")
	}

	if cfg.dbURL == "" {
		cfg.dbURL = env.Get("DATABASE_URL", "")
	}
	if cfg.dbURL == "" {
		return cliConfig{}, fmt.Errorf("database URL not provided (use -db flag or DATABASE_URL env var)")
	}

	alchemyAPIKey := os.Getenv("ALCHEMY_API_KEY")
	if alchemyAPIKey == "" {
		return cliConfig{}, fmt.Errorf("ALCHEMY_API_KEY environment variable is required")
	}
	alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	cfg.alchemyURL = fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)

	if cfg.redisAddr == "" {
		cfg.redisAddr = env.Get("REDIS_ADDR", "")
	}
	if cfg.redisAddr == "" {
		return cliConfig{}, fmt.Errorf("redis address not provided (use -redis flag or REDIS_ADDR env var)")
	}

	chainIDStr := env.Get("CHAIN_ID", "1")
	cfg.chainID = 1
	_, _ = fmt.Sscanf(chainIDStr, "%d", &cfg.chainID)

	return cfg, nil
}

func run(ctx context.Context, args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	logger.Info("starting sparklend position tracker",
		"queue", cfg.queueURL,
		"redis", cfg.redisAddr,
		"chainID", cfg.chainID,
		"commit", GitCommit)

	// AWS config
	awsRegion := env.Get("AWS_REGION", "us-east-1")
	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(awsRegion),
	}

	if accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); accessKeyID != "" {
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		awsOpts = append(awsOpts, awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				Source:          "StaticCredentials",
			}, nil
		})))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	// SQS
	sqsConsumer, err := sqsAdapter.NewConsumer(awsCfg, sqsAdapter.Config{
		QueueURL:          cfg.queueURL,
		WaitTimeSeconds:   int32(cfg.waitTime),
		VisibilityTimeout: 300,
		BaseEndpoint:      env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("creating SQS consumer: %w", err)
	}
	defer sqsConsumer.Close()

	// Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.redisAddr,
		Password: env.Get("REDIS_PASSWORD", ""),
		DB:       0,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connecting to Redis: %w", err)
	}
	defer redisClient.Close()
	logger.Info("Redis connected", "addr", cfg.redisAddr)

	// Ethereum
	ethClient, err := ethclient.Dial(cfg.alchemyURL)
	if err != nil {
		return fmt.Errorf("connecting to Ethereum node: %w", err)
	}
	logger.Info("Ethereum node connected")

	// PostgreSQL
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	// Repositories
	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("creating transaction manager: %w", err)
	}

	userRepo, err := postgres.NewUserRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating user repository: %w", err)
	}

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating protocol repository: %w", err)
	}

	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating token repository: %w", err)
	}

	positionRepo, err := postgres.NewPositionRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating position repository: %w", err)
	}

	eventRepo := postgres.NewEventRepository(logger)

	// Service
	service, err := sparklend_position_tracker.NewService(
		shared.SQSConsumerConfig{
			MaxMessages: cfg.maxMessages,
			Logger:      logger,
		},
		sqsConsumer,
		redisClient,
		ethClient,
		txManager,
		userRepo,
		protocolRepo,
		tokenRepo,
		positionRepo,
		eventRepo,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	logger.Info("sparklend position tracker started, waiting for messages...")

	return lifecycle.Run(ctx, logger, service)
}
