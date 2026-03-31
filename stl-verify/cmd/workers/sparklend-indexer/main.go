// Package main implements a SparkLend position tracker that monitors lending protocol
// activity on Ethereum. It processes transaction receipts from cache (triggered by SQS
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
	"strconv"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cache"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/aavelike_position_tracker"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
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
	s3Bucket    string
	deployEnv   string
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
		return cliConfig{}, fmt.Errorf("parsing CLI flags: %w", err)
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
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return cliConfig{}, fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}
	cfg.chainID = chainID

	cfg.s3Bucket = env.Get("S3_BUCKET", "")
	if cfg.s3Bucket == "" {
		return cliConfig{}, fmt.Errorf("S3_BUCKET environment variable is required")
	}

	cfg.deployEnv = env.Get("DEPLOY_ENV", "")
	if cfg.deployEnv == "" {
		return cliConfig{}, fmt.Errorf("DEPLOY_ENV environment variable is required")
	}

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
		if secretKey == "" {
			return fmt.Errorf("AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is missing")
		}
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

	// Redis (block cache)
	cacheCfg := redisAdapter.ConfigDefaults()
	cacheCfg.Addr = cfg.redisAddr
	cacheCfg.Password = env.Get("REDIS_PASSWORD", "")
	blockCache, err := redisAdapter.NewBlockCache(cacheCfg, logger)
	if err != nil {
		return fmt.Errorf("creating block cache: %w", err)
	}
	if err := blockCache.Ping(ctx); err != nil {
		return fmt.Errorf("connecting to Redis: %w", err)
	}
	defer blockCache.Close()
	logger.Info("Redis connected", "addr", cfg.redisAddr)
	s3Reader := s3adapter.NewReader(awsCfg, logger)
	cacheReader, err := cache.NewReaderWithFallback(blockCache, s3Reader, cfg.chainID, cfg.deployEnv, cfg.s3Bucket, logger)
	if err != nil {
		return fmt.Errorf("creating cache reader: %w", err)
	}
	// Ethereum
	ethClient, err := ethclient.Dial(cfg.alchemyURL)
	if err != nil {
		return fmt.Errorf("connecting to Ethereum node: %w", err)
	}
	defer ethClient.Close()
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

	receiptTokenRepo, err := postgres.NewReceiptTokenRepository(pool, logger)
	if err != nil {
		return fmt.Errorf("creating receipt token repository: %w", err)
	}

	// Service
	service, err := aavelike_position_tracker.NewService(
		shared.SQSConsumerConfig{
			MaxMessages: cfg.maxMessages,
			Logger:      logger,
			ChainID:     cfg.chainID,
		},
		sqsConsumer,
		cacheReader,
		ethClient,
		txManager,
		userRepo,
		protocolRepo,
		tokenRepo,
		positionRepo,
		eventRepo,
		receiptTokenRepo,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	logger.Info("sparklend position tracker started, waiting for messages...")

	return lifecycle.Run(ctx, logger, service)
}
