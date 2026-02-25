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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
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
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

type cliConfig struct {
	queueURL          string
	redisAddr         string
	dbURL             string
	alchemyURL        string
	maxMessages       int
	waitTime          int
	visibilityTimeout int
	chainID           int64
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("morpho-indexer", flag.ContinueOnError)
	queueURL := fs.String("queue", "", "SQS Queue URL")
	redisAddr := fs.String("redis", "", "Redis address")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	maxMessages := fs.Int("max", 10, "Max messages per poll")
	waitTime := fs.Int("wait", 20, "Wait time in seconds (long polling)")
	visibilityTimeout := fs.Int("visibility-timeout", 300, "SQS visibility timeout in seconds")
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		queueURL:          *queueURL,
		redisAddr:         *redisAddr,
		dbURL:             *dbURL,
		maxMessages:       *maxMessages,
		waitTime:          *waitTime,
		visibilityTimeout: *visibilityTimeout,
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

	if waitTimeStr := env.Get("SQS_WAIT_TIME", ""); waitTimeStr != "" {
		v, err := strconv.Atoi(waitTimeStr)
		if err != nil {
			return cliConfig{}, fmt.Errorf("parsing SQS_WAIT_TIME %q: %w", waitTimeStr, err)
		}
		cfg.waitTime = v
	}
	if visTimeStr := env.Get("SQS_VISIBILITY_TIMEOUT", ""); visTimeStr != "" {
		v, err := strconv.Atoi(visTimeStr)
		if err != nil {
			return cliConfig{}, fmt.Errorf("parsing SQS_VISIBILITY_TIMEOUT %q: %w", visTimeStr, err)
		}
		cfg.visibilityTimeout = v
	}

	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return cliConfig{}, fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}
	cfg.chainID = chainID

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

	logger.Info("starting morpho indexer",
		"queue", cfg.queueURL,
		"redis", cfg.redisAddr,
		"chainID", cfg.chainID,
		"commit", GitCommit)

	// Initialize OpenTelemetry tracing and metrics
	shutdownOTEL := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "morpho-indexer",
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	defer shutdownOTEL(context.Background())

	// Service telemetry
	morphoTelemetry, err := morpho_indexer.NewTelemetry()
	if err != nil {
		logger.Warn("failed to create morpho telemetry", "error", err)
	}

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
		VisibilityTimeout: int32(cfg.visibilityTimeout),
		BaseEndpoint:      env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("creating SQS consumer: %w", err)
	}
	defer sqsConsumer.Close()

	// Redis
	cache, err := redisAdapter.NewBlockCache(redisAdapter.Config{
		Addr:      cfg.redisAddr,
		Password:  env.Get("REDIS_PASSWORD", ""),
		DB:        0,
		TTL:       2 * 24 * time.Hour,
		KeyPrefix: "stl",
	}, logger)
	if err != nil {
		return fmt.Errorf("creating Redis cache: %w", err)
	}
	defer cache.Close()
	logger.Info("Redis connected", "addr", cfg.redisAddr)

	// Ethereum
	ethClient, err := ethclient.DialContext(ctx, cfg.alchemyURL)
	if err != nil {
		return fmt.Errorf("connecting to Ethereum node: %w", err)
	}
	defer ethClient.Close()
	logger.Info("Ethereum node connected")

	// Multicall3
	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("creating multicall client: %w", err)
	}

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

	morphoRepo, err := postgres.NewMorphoRepository(pool, logger)
	if err != nil {
		return fmt.Errorf("creating morpho repository: %w", err)
	}

	eventRepo := postgres.NewEventRepository(logger)

	// Service
	svcConfig := morpho_indexer.Config{
		SQSConsumerConfig: shared.SQSConsumerConfig{
			MaxMessages: cfg.maxMessages,
			Logger:      logger,
		},
		ChainID:   cfg.chainID,
		Telemetry: morphoTelemetry,
	}

	service, err := morpho_indexer.NewService(
		svcConfig,
		sqsConsumer,
		cache,
		mc,
		txManager,
		userRepo,
		protocolRepo,
		tokenRepo,
		morphoRepo,
		eventRepo,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	logger.Info("morpho indexer started, waiting for messages...")

	return lifecycle.Run(ctx, logger, service)
}
