// Package main implements a borrow event processor that monitors lending protocol
// activity on Ethereum. It processes transaction receipts from Redis (triggered by SQS
// messages), extracts borrow events with collateral data, and stores the results in
// PostgreSQL for downstream analysis.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/services/borrow_processor"
)

const (
	Multicall3Address = "0xcA11bde05977b3631167028862bE2a173976CA11"
)

func main() {
	queueURL := flag.String("queue", "", "SQS Queue URL")
	redisAddr := flag.String("redis", "", "Redis address")
	dbURL := flag.String("db", "", "PostgreSQL connection URL")
	maxMessages := flag.Int("max", 10, "Max messages per poll")
	waitTime := flag.Int("wait", 20, "Wait time in seconds (long polling)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	if *queueURL == "" {
		*queueURL = getEnv("AWS_SQS_QUEUE_URL", "")
	}
	if *queueURL == "" {
		logger.Error("queue URL not provided (use -queue flag or AWS_SQS_QUEUE_RECEIPTS env var)")
		os.Exit(1)
	}

	if *dbURL == "" {
		*dbURL = getEnv("DATABASE_URL", "")
	}
	if *dbURL == "" {
		logger.Error("database URL not provided (use -db flag or DATABASE_URL env var)")
		os.Exit(1)
	}

	alchemyAPIKey := requireEnv("ALCHEMY_API_KEY", logger)
	alchemyHTTPURL := getEnv("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	fullAlchemyURL := fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)

	if *redisAddr == "" {
		*redisAddr = requireEnv("REDIS_ADDR", logger)
	}
	if *redisAddr == "" {
		logger.Error("Redis address not provided (use -redis flag or REDIS_ADDR env var)")
		os.Exit(1)
	}

	chainIDStr := getEnv("CHAIN_ID", "1")
	var chainID int64 = 1
	_, _ = fmt.Sscanf(chainIDStr, "%d", &chainID)

	logger.Info("starting borrow event processor",
		"queue", *queueURL,
		"redis", *redisAddr,
		"chainID", chainID)

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(getEnv("AWS_REGION", "us-east-1")),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     getEnv("AWS_ACCESS_KEY_ID", "test"),
				SecretAccessKey: getEnv("AWS_SECRET_ACCESS_KEY", "test"),
				Source:          "Static",
			}, nil
		})),
	)
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint := getEnv("AWS_SQS_ENDPOINT", ""); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	redisClient := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: getEnv("REDIS_PASSWORD", ""),
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

	ethClient, err := ethclient.Dial(fullAlchemyURL)
	if err != nil {
		logger.Error("failed to connect to Ethereum node", "error", err)
		os.Exit(1)
	}
	logger.Info("Ethereum node connected")

	db, err := sql.Open("pgx", *dbURL)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Warn("failed to close database connection", "error", err)
		}
	}()

	if err := db.Ping(); err != nil {
		logger.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connected")

	lendingRepo := postgres.NewLendingRepository(db, chainID, logger)

	processorConfig := borrow_processor.Config{
		QueueURL:        *queueURL,
		MaxMessages:     int32(*maxMessages),
		WaitTimeSeconds: int32(*waitTime),
		Logger:          logger,
	}

	processor, err := borrow_processor.NewService(
		processorConfig,
		sqsClient,
		redisClient,
		ethClient,
		common.HexToAddress(Multicall3Address),
		common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"),
		common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"),
		lendingRepo,
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

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func requireEnv(key string, logger *slog.Logger) string {
	value := os.Getenv(key)
	if value == "" {
		logger.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return value
}
