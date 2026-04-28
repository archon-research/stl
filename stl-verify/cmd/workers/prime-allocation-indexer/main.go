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
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cache"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	at "github.com/archon-research/stl/stl-verify/internal/services/allocation_tracker"
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
	s3Bucket          string
	deployEnv         string
	maxMessages       int
	waitTime          int
	visibilityTimeout int
	sweepBlocks       int
	chainID           int64
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("allocation-tracker", flag.ContinueOnError)
	queueURL := fs.String("queue", "", "SQS Queue URL")
	redisAddr := fs.String("redis", "", "Redis address")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	maxMessages := fs.Int("max", 10, "Max messages per poll")
	waitTime := fs.Int("wait", 20, "Wait time in seconds (long polling)")
	visibilityTimeout := fs.Int("visibility-timeout", 300, "SQS visibility timeout in seconds")
	sweepBlocks := fs.Int("sweep-blocks", 75, "Sweep every N blocks")
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
		sweepBlocks:       *sweepBlocks,
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

	// AWS config
	awsRegion := env.Get("AWS_REGION", "eu-west-1")
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

	// Redis (block cache)
	cacheCfg := redisAdapter.ConfigDefaults()
	cacheCfg.Addr = cfg.redisAddr
	cacheCfg.Password = env.Get("REDIS_PASSWORD", "")
	blockCache, err := redisAdapter.NewBlockCache(cacheCfg, logger)
	if err != nil {
		return fmt.Errorf("creating block cache: %w", err)
	}
	if err := blockCache.Ping(ctx); err != nil {
		return fmt.Errorf("connecting to Redis at %s: %w", cfg.redisAddr, err)
	}
	defer blockCache.Close()
	logger.Info("Redis connected", "addr", cfg.redisAddr)

	// S3 + cache reader with fallback
	s3Opts := []func(*awss3.Options){}
	if s3Endpoint := env.Get("AWS_S3_ENDPOINT", ""); s3Endpoint != "" {
		s3Opts = append(s3Opts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(s3Endpoint)
			o.UsePathStyle = true
		})
	}
	s3Reader := s3adapter.NewReaderWithOptions(awsCfg, logger, s3Opts...)
	cacheReader, err := cache.NewReaderWithFallback(blockCache, s3Reader, cfg.chainID, cfg.deployEnv, cfg.s3Bucket, logger)
	if err != nil {
		return fmt.Errorf("creating cache reader: %w", err)
	}

	// Ethereum
	rawClient, err := ethclient.DialContext(ctx, cfg.alchemyURL)
	if err != nil {
		return fmt.Errorf("eth dial: %w", err)
	}
	defer rawClient.Close()
	logger.Info("Ethereum node connected")

	mc, err := multicall.NewClient(rawClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return fmt.Errorf("erc20 abi: %w", err)
	}
	atokenReadABI, err := abis.GetATokenReadABI()
	if err != nil {
		return fmt.Errorf("atoken read abi: %w", err)
	}

	// Build source registry
	registry := at.NewSourceRegistry(logger)

	for _, s := range at.DefaultSkipSources(logger) {
		registry.Register(s)
	}

	registry.Register(at.NewBalanceOfSource(mc, erc20ABI, atokenReadABI, logger))

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

	uniV3, err := at.NewUniV3Source(mc, logger)
	if err != nil {
		return fmt.Errorf("univ3 source: %w", err)
	}
	registry.Register(uniV3)

	for _, s := range at.DefaultStubSources(logger) {
		registry.Register(s)
	}

	// Token entries filtered by chain
	entries := at.EntriesForChainID(at.DefaultTokenEntries(), cfg.chainID)
	if len(entries) == 0 {
		return fmt.Errorf("no token entries for chain ID %d", cfg.chainID)
	}

	proxies := at.ProxiesForChainID(at.DefaultProxies(), cfg.chainID)

	// Database
	dbPool, err := pgxpool.New(ctx, cfg.dbURL)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer dbPool.Close()
	if err := dbPool.Ping(ctx); err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	logger.Info("PostgreSQL connected")

	buildReg, err := buildregistry.New(ctx, dbPool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	logger.Info("starting allocation tracker",
		"queue", cfg.queueURL,
		"redis", cfg.redisAddr,
		"chainID", cfg.chainID,
		"commit", buildReg.GitHash())

	txm, err := postgres.NewTxManager(dbPool, logger)
	if err != nil {
		return fmt.Errorf("tx manager: %w", err)
	}

	// Load primes from DB to build star → prime_id lookup
	primeRepo := postgres.NewPrimeDebtRepository(dbPool, txm, logger, buildReg.BuildID())
	primes, err := primeRepo.GetPrimes(ctx)
	if err != nil {
		return fmt.Errorf("load primes: %w", err)
	}
	if len(primes) == 0 {
		return fmt.Errorf("no primes found in database")
	}
	primeLookup := make(map[string]int64, len(primes))
	for _, p := range primes {
		primeLookup[p.Name] = p.ID
	}
	logger.Info("primes loaded", "count", len(primes))

	tokenRepo, err := postgres.NewTokenRepository(dbPool, logger, 1)
	if err != nil {
		return fmt.Errorf("token repo: %w", err)
	}
	allocRepo := postgres.NewAllocationRepository(dbPool, txm, tokenRepo, logger, buildReg.BuildID())
	supplyRepo := postgres.NewTokenTotalSupplyRepository(dbPool, txm, tokenRepo, logger, buildReg.BuildID())
	pgHandler := at.NewPrimePositionHandler(allocRepo, supplyRepo, txm, mc, erc20ABI, primeLookup, logger)

	handler := at.NewMultiHandler(at.NewLogHandler(logger), pgHandler)

	svc, err := at.NewService(
		at.Config{
			MaxMessages:       cfg.maxMessages,
			SweepEveryNBlocks: cfg.sweepBlocks,
			ChainID:           cfg.chainID,
			Logger:            logger,
		},
		sqsConsumer,
		cacheReader,
		registry,
		entries,
		handler,
		proxies,
	)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	// Start
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if err := svc.Start(runCtx); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	logger.Info("running",
		"chainID", cfg.chainID,
		"entries", len(entries),
		"sweepEveryNBlocks", cfg.sweepBlocks)

	select {
	case sig := <-sigChan:
		logger.Info("shutting down", "signal", sig)
	case <-ctx.Done():
		logger.Info("shutting down", "reason", "context cancelled")
	}
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
