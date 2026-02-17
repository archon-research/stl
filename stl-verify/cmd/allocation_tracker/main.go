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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/allocation_tracker"
)

func main() {

	queueURL := flag.String("queue", "", "SQS Queue URL")
	redisAddr := flag.String("redis", "", "Redis address")
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

	if *redisAddr == "" {
		*redisAddr = env.Get("REDIS_ADDR", "")
	}
	if *redisAddr == "" {
		logger.Error("Redis address not provided (use -redis flag or REDIS_ADDR env var)")
		os.Exit(1)
	}

	alchemyAPIKey := requireEnv("ALCHEMY_API_KEY", logger)
	alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	fullAlchemyURL := fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)

	logger.Info("starting allocation tracker",
		"queue", *queueURL,
		"redis", *redisAddr)

	ctx := context.Background()

	// AWS / SQS
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(env.Get("AWS_REGION", "us-east-1")),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     env.Get("AWS_ACCESS_KEY_ID", "test"),
				SecretAccessKey: env.Get("AWS_SECRET_ACCESS_KEY", "test"),
				Source:          "Static",
			}, nil
		})),
	)
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint := env.Get("AWS_SQS_ENDPOINT", ""); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	// Redis
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
			logger.Warn("failed to close Redis", "error", err)
		}
	}()
	logger.Info("Redis connected", "addr", *redisAddr)

	// Ethereum client (for token metadata via multicall)
	ethClient, err := ethclient.Dial(fullAlchemyURL)
	if err != nil {
		logger.Error("failed to connect to Ethereum node", "error", err)
		os.Exit(1)
	}
	logger.Info("Ethereum node connected")

	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		logger.Error("failed to create multicall client", "error", err)
		os.Exit(1)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		logger.Error("failed to load ERC20 ABI", "error", err)
		os.Exit(1)
	}

	tokenCache := allocation_tracker.NewTokenCache(mc, erc20ABI, logger)

	// Handler — just log for now, swap in a DB handler later
	handler := allocation_tracker.NewLogHandler(logger)

	// Service
	svcConfig := allocation_tracker.Config{
		QueueURL:        *queueURL,
		MaxMessages:     int32(*maxMessages),
		WaitTimeSeconds: int32(*waitTime),
		Logger:          logger,
	}

	service, err := allocation_tracker.NewService(
		svcConfig,
		sqsClient,
		redisClient,
		tokenCache,
		handler,
		allocation_tracker.DefaultProxies(),
	)
	if err != nil {
		logger.Error("failed to create service", "error", err)
		os.Exit(1)
	}

	// Start
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if err := service.Start(ctx); err != nil {
		logger.Error("failed to start service", "error", err)
		os.Exit(1)
	}

	logger.Info("allocation tracker running, waiting for messages...")

	sig := <-sigChan
	logger.Info("received signal, shutting down...", "signal", sig)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		if err := service.Stop(); err != nil {
			logger.Error("error stopping service", "error", err)
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
