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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	at "github.com/archon-research/stl/stl-verify/internal/services/allocation_tracker"
)

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
	sweepBlocks := flag.Int("sweep-blocks", 75, "Sweep every N blocks")
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

	chainIDStr, err := env.Require("CHAIN_ID")
	if err != nil {
		return err
	}
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid CHAIN_ID %q: %w", chainIDStr, err)
	}

	alchemyKey, err := env.Require("ALCHEMY_API_KEY")
	if err != nil {
		return err
	}
	rpcURL := fmt.Sprintf(
		"%s/%s",
		env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2"),
		alchemyKey,
	)

	ctx := context.Background()

	// AWS
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

	// SQS consumer
	sqsConsumer, err := sqsAdapter.NewConsumer(awsCfg, sqsAdapter.Config{
		QueueURL:          *queueURL,
		WaitTimeSeconds:   int32(*waitTime),
		VisibilityTimeout: 30,
		BaseEndpoint:      env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("sqs consumer: %w", err)
	}
	defer sqsConsumer.Close()

	// Redis (block cache)
	blockCache, err := redisAdapter.NewBlockCache(redisAdapter.Config{
		Addr:     *redisAddr,
		Password: env.Get("REDIS_PASSWORD", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("creating block cache: %w", err)
	}
	if err := blockCache.Ping(ctx); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	defer blockCache.Close()
	logger.Info("redis connected", "addr", *redisAddr)

	// Ethereum
	rawClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		return fmt.Errorf("eth dial: %w", err)
	}
	defer rawClient.Close()

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

	for _, s := range at.DefaultSkipSources(logger) {
		registry.Register(s)
	}

	registry.Register(at.NewBalanceOfSource(mc, erc20ABI, logger))

	erc4626, err := at.NewERC4626Source(mc, logger)
	if err != nil {
		return fmt.Errorf("erc4626 source: %w", err)
	}
	registry.Register(erc4626)

	curveABI, err := abis.GetCurvePoolABI()
	if err != nil {
		return fmt.Errorf("curve abi: %w", err)
	}
	registry.Register(at.NewCurveSource(mc, curveABI, logger))

	for _, s := range at.DefaultStubSources(logger) {
		registry.Register(s)
	}

	// Token entries filtered by chain
	entries := at.EntriesForChainID(at.DefaultTokenEntries(), chainID)
	if len(entries) == 0 {
		return fmt.Errorf("no token entries for chain ID %d", chainID)
	}

	proxies := at.ProxiesForChainID(at.DefaultProxies(), chainID)

	// Database
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		return err
	}

	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	defer dbPool.Close()
	if err := dbPool.Ping(ctx); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}
	logger.Info("postgres connected")

	txm, err := postgres.NewTxManager(dbPool, logger)
	if err != nil {
		return fmt.Errorf("tx manager: %w", err)
	}

	tokenRepo, err := postgres.NewTokenRepository(dbPool, logger, 1)
	if err != nil {
		return fmt.Errorf("token repo: %w", err)
	}
	allocRepo := postgres.NewAllocationRepository(dbPool, txm, tokenRepo, logger)
	pgHandler := at.NewPrimePositionHandler(allocRepo, mc, erc20ABI, logger)

	handler := at.NewMultiHandler(at.NewLogHandler(logger), pgHandler)

	svc, err := at.NewService(
		at.Config{
			MaxMessages:       *maxMessages,
			SweepEveryNBlocks: *sweepBlocks,
			ChainID:           chainID,
			Logger:            logger,
		},
		sqsConsumer,
		blockCache,
		registry,
		entries,
		handler,
		proxies,
	)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	// Start
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	logger.Info("running",
		"chainID", chainID,
		"entries", len(entries),
		"sweepEveryNBlocks", *sweepBlocks)
	sig := <-sigChan
	logger.Info("shutting down", "signal", sig)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	var stopErr error
	go func() {
		defer close(done)
		stopErr = svc.Stop()
	}()

	select {
	case <-done:
		if stopErr != nil {
			return fmt.Errorf("stop: %w", stopErr)
		}
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timeout")
	}

	return nil
}
