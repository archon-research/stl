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
	"runtime/debug"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_position_tracker"
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
	queueURL := flag.String("queue", "", "SQS Queue URL")
	redisAddr := flag.String("redis", "", "Redis address")
	dbURL := flag.String("db", "", "PostgreSQL connection URL")
	maxMessages := flag.Int("max", 10, "Max messages per poll")
	waitTime := flag.Int("wait", 20, "Wait time in seconds (long polling)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	if *queueURL == "" {
		*queueURL = env.Get("AWS_SQS_QUEUE_URL", "")
	}
	if *queueURL == "" {
		logger.Error("queue URL not provided (use -queue flag or AWS_SQS_QUEUE_URL env var)")
		os.Exit(1)
	}

	if *dbURL == "" {
		*dbURL = env.Get("DATABASE_URL", "")
	}
	if *dbURL == "" {
		logger.Error("database URL not provided (use -db flag or DATABASE_URL env var)")
		os.Exit(1)
	}

	rpcAPIKey := requireEnv("ETH_RPC_API_KEY", logger)
	rpcHTTPURL := env.Get("ETH_RPC_HTTP_URL", "")
	if rpcHTTPURL == "" {
		logger.Error("ETH_RPC_HTTP_URL environment variable is required")
		os.Exit(1)
	}
	rpcURL := fmt.Sprintf("%s/%s", rpcHTTPURL, rpcAPIKey)

	if *redisAddr == "" {
		*redisAddr = env.Get("REDIS_ADDR", "")
	}
	if *redisAddr == "" {
		logger.Error("Redis address not provided (use -redis flag or REDIS_ADDR env var)")
		os.Exit(1)
	}

	chainIDStr := env.Get("CHAIN_ID", "1")
	var chainID int64 = 1
	_, _ = fmt.Sscanf(chainIDStr, "%d", &chainID)

	logger.Info("starting sparklend position tracker",
		"queue", *queueURL,
		"redis", *redisAddr,
		"chainID", chainID)

	ctx := context.Background()

	// Configure AWS SDK for LocalStack or production
	// In production (ECS/Fargate), use the default credential chain which picks up IAM role credentials.
	// For local development with LocalStack, use static credentials from environment variables.
	awsRegion := env.Get("AWS_REGION", "us-east-1")
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(awsRegion),
	}

	// Only use static credentials if explicitly set (for LocalStack)
	// In ECS/Fargate, these won't be set and the SDK will use the IAM role
	if accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); accessKeyID != "" {
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		opts = append(opts, config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				Source:          "StaticCredentials",
			}, nil
		})))
		logger.Debug("using static AWS credentials from environment")
	} else {
		logger.Debug("using default AWS credential chain (IAM role)")
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	sqsConsumer, err := sqsAdapter.NewConsumer(cfg, sqsAdapter.Config{
		QueueURL:          *queueURL,
		WaitTimeSeconds:   int32(*waitTime),
		VisibilityTimeout: 300, // 5 minutes — processing involves multiple RPC calls per message
		BaseEndpoint:      env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		logger.Error("failed to create SQS consumer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := sqsConsumer.Close(); err != nil {
			logger.Warn("failed to close SQS consumer", "error", err)
		}
	}()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: env.Get("REDIS_PASSWORD", ""),
		DB:       0,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Warn("failed to close Redis connection", "error", err)
		}
	}()
	logger.Info("Redis connected", "addr", *redisAddr)

	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		logger.Error("failed to connect to Ethereum node", "error", err)
		os.Exit(1)
	}
	logger.Info("Ethereum node connected")

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(*dbURL))
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		logger.Error("failed to create transaction manager", "error", err)
		os.Exit(1)
	}

	userRepo, err := postgres.NewUserRepository(pool, logger, 0)
	if err != nil {
		logger.Error("failed to create user repository", "error", err)
		os.Exit(1)
	}

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, 0)
	if err != nil {
		logger.Error("failed to create protocol repository", "error", err)
		os.Exit(1)
	}

	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 0)
	if err != nil {
		logger.Error("failed to create token repository", "error", err)
		os.Exit(1)
	}

	positionRepo, err := postgres.NewPositionRepository(pool, logger, 0)
	if err != nil {
		logger.Error("failed to create position repository", "error", err)
		os.Exit(1)
	}

	eventRepo := postgres.NewEventRepository(logger)

	processorConfig := sparklend_position_tracker.Config{
		MaxMessages: *maxMessages,
		Logger:      logger,
	}

	processor, err := sparklend_position_tracker.NewService(
		processorConfig,
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
		logger.Error("failed to create processor", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("starting processor...")
	if err := processor.Start(ctx); err != nil {
		logger.Error("failed to start processor", "error", err)
		os.Exit(1)
	}

	logger.Info("processor started, waiting for messages...")

	sig := <-sigChan
	logger.Info("received signal, shutting down...", "signal", sig)

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		if err := processor.Stop(); err != nil {
			logger.Error("error stopping processor", "error", err)
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

func requireEnv(key string, logger *slog.Logger) string {
	value := os.Getenv(key)
	if value == "" {
		logger.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return value
}
