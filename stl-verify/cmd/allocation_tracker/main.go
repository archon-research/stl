package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/redis/go-redis/v9"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	at "github.com/archon-research/stl/stl-verify/internal/services/allocation_tracker"
)

// ethClientWrapper adapts *ethclient.Client to the at.EthClient interface.
type ethClientWrapper struct {
	client *ethclient.Client
}

func (w *ethClientWrapper) BlockNumber(ctx context.Context) (uint64, error) {
	n, err := w.client.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("eth block number: %w", err)
	}
	return n, nil
}

func (w *ethClientWrapper) FinalizedBlockNumber(ctx context.Context) (uint64, error) {
	header, err := w.client.HeaderByNumber(
		ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)),
	)
	if err != nil {
		return 0, fmt.Errorf("eth finalized block header: %w", err)
	}
	return header.Number.Uint64(), nil
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	queueURL := flag.String("queue", "", "SQS Queue URL")
	redisAddr := flag.String("redis", "", "Redis address")
	maxMessages := flag.Int("max", 10, "Max messages per poll")
	waitTime := flag.Int("wait", 20, "Wait time seconds")
	sweepMinutes := flag.Int("sweep", 5, "Sweep interval minutes")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))

	if *queueURL == "" {
		*queueURL = env.Get("AWS_SQS_QUEUE_URL", "")
	}
	if *redisAddr == "" {
		*redisAddr = env.Get("REDIS_ADDR", "")
	}

	if *queueURL == "" {
		return fmt.Errorf("queue URL required (-queue or AWS_SQS_QUEUE_URL)")
	}
	if *redisAddr == "" {
		return fmt.Errorf("redis address required (-redis or REDIS_ADDR)")
	}

	alchemyKey := env.Get("ALCHEMY_API_KEY", "")
	if alchemyKey == "" {
		return fmt.Errorf("ALCHEMY_API_KEY is required")
	}
	rpcURL := fmt.Sprintf(
		"%s/%s",
		env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2"),
		alchemyKey,
	)

	ctx := context.Background()

	// AWS SQS
	var awsOpts []func(*config.LoadOptions) error
	awsOpts = append(awsOpts,
		config.WithRegion(env.Get("AWS_REGION", "us-east-1")),
	)

	accessKey := env.Get("AWS_ACCESS_KEY_ID", "")
	secretKey := env.Get("AWS_SECRET_ACCESS_KEY", "")
	if accessKey != "" && secretKey != "" {
		awsOpts = append(awsOpts,
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
			),
		)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}

	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if ep := env.Get("AWS_SQS_ENDPOINT", ""); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
	})

	// Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: env.Get("REDIS_PASSWORD", ""),
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	defer redisClient.Close()
	logger.Info("redis connected", "addr", *redisAddr)

	// Ethereum
	rawClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		return fmt.Errorf("eth dial: %w", err)
	}
	defer rawClient.Close()
	ethClient := &ethClientWrapper{client: rawClient}

	mc, err := multicall.NewClient(rawClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return fmt.Errorf("erc20 abi: %w", err)
	}

	// Build source registry
	registry := at.NewSourceRegistry(logger)

	// 1. Skip sources (existing worker handles these)
	for _, s := range at.DefaultSkipSources(logger) {
		registry.Register(s)
	}

	// 2. BalanceOf source
	registry.Register(at.NewBalanceOfSource(mc, erc20ABI, logger))

	// 3. ERC4626 source
	erc4626, err := at.NewERC4626Source(mc, logger)
	if err != nil {
		return fmt.Errorf("erc4626 source: %w", err)
	}
	registry.Register(erc4626)

	// 4. Curve source
	curve, err := at.NewCurveSource(mc, logger)
	if err != nil {
		return fmt.Errorf("curve source: %w", err)
	}
	registry.Register(curve)

	// 5. Stub sources
	for _, s := range at.DefaultStubSources(logger) {
		registry.Register(s)
	}

	// Service
	entries := at.DefaultTokenEntries()

	// Handler setup
	var handler at.AllocationHandler

	dbURL := env.Get("DATABASE_URL", "")
	if dbURL != "" {
		dbPool, err := pgxpool.New(ctx, dbURL)
		if err != nil {
			return fmt.Errorf("db connect: %w", err)
		}
		defer dbPool.Close()
		if err := dbPool.Ping(ctx); err != nil {
			return fmt.Errorf("db ping: %w", err)
		}
		logger.Info("postgres connected")

		tokenRepo, err := postgres.NewTokenRepository(dbPool, logger, 1)
		if err != nil {
			return fmt.Errorf("token repo: %w", err)
		}
		allocRepo := postgres.NewAllocationRepository(dbPool, tokenRepo, logger)
		pgHandler := at.NewPostgresHandler(allocRepo, mc, erc20ABI, logger)

		handler = at.NewMultiHandler(at.NewLogHandler(logger), pgHandler)
	} else {
		logger.Warn("DATABASE_URL not set — running log-only mode")
		handler = at.NewLogHandler(logger)
	}

	svc, err := at.NewService(
		at.Config{
			QueueURL:        *queueURL,
			MaxMessages:     int32(*maxMessages),
			WaitTimeSeconds: int32(*waitTime),
			SweepInterval:   time.Duration(*sweepMinutes) * time.Minute,
			Logger:          logger,
		},
		sqsClient,
		redisClient,
		ethClient,
		registry,
		entries,
		handler,
		at.DefaultProxies(),
	)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	// Start
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	logger.Info("running",
		"entries", len(entries),
		"sweep", fmt.Sprintf("%dm", *sweepMinutes))
	sig := <-sigChan
	logger.Info("shutting down", "signal", sig)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = svc.Stop()
	}()

	select {
	case <-done:
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timeout")
	}

	return nil
}
