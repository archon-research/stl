package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cache"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/services/transfer_user_discovery"
)

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
	chainID     int64
}

type runtimeRepos struct {
	ethClient        *ethclient.Client
	txManager        outbound.TxManager
	userRepo         outbound.UserRepository
	protocolRepo     outbound.ProtocolRepository
	tokenRepo        outbound.TokenRepository
	positionRepo     outbound.PositionRepository
	receiptTokenRepo outbound.ReceiptTokenRepository
	closeFn          func() error
}

type runtimeResources struct {
	consumer    outbound.SQSConsumer
	cacheReader outbound.BlockCacheReader
	repos       runtimeRepos
	closeFn     func()
}

type runtimeDependencies struct {
	logger           *slog.Logger
	newConsumer      func(context.Context, cliConfig, *slog.Logger) (outbound.SQSConsumer, error)
	newCacheReader   func(context.Context, cliConfig, *slog.Logger) (outbound.BlockCacheReader, error)
	openRepositories func(context.Context, cliConfig, *slog.Logger) (runtimeRepos, error)
	newSnapshotter   func(context.Context, cliConfig, runtimeRepos, *slog.Logger) (*shared.PositionSnapshotter, error)
	newService       func(shared.SQSConsumerConfig, outbound.SQSConsumer, outbound.BlockCacheReader, outbound.TxManager, outbound.UserRepository, *shared.PositionSnapshotter, []outbound.TrackedReceiptToken, *slog.Logger) (lifecycle.Service, error)
	lifecycleRun     func(context.Context, *slog.Logger, lifecycle.Service) error
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("sparklend-transfer-user-discovery", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, fmt.Errorf("parsing CLI flags: %w", err)
	}

	cfg := cliConfig{
		queueURL:   requiredEnv("QUEUE_URL"),
		dbURL:      requiredEnv("DATABASE_URL"),
		redisAddr:  requiredEnv("REDIS_ADDR"),
		alchemyURL: requiredEnv("ALCHEMY_WS_URL"),
		s3Bucket:   requiredEnv("S3_BUCKET"),
		deployEnv:  requiredEnv("DEPLOY_ENV"),
	}

	if err := validateRequiredConfig(cfg); err != nil {
		return cliConfig{}, err
	}

	chainID, err := parseInt64Env("CHAIN_ID", 1)
	if err != nil {
		return cliConfig{}, err
	}
	maxMessages, err := parseIntEnv("MAX_MESSAGES", 10)
	if err != nil {
		return cliConfig{}, err
	}

	cfg.chainID = chainID
	cfg.maxMessages = maxMessages

	return cfg, nil
}

func requiredEnv(key string) string {
	return os.Getenv(key)
}

func validateRequiredConfig(cfg cliConfig) error {
	required := []struct {
		name  string
		value string
	}{
		{name: "QUEUE_URL", value: cfg.queueURL},
		{name: "DATABASE_URL", value: cfg.dbURL},
		{name: "REDIS_ADDR", value: cfg.redisAddr},
		{name: "ALCHEMY_WS_URL", value: cfg.alchemyURL},
		{name: "S3_BUCKET", value: cfg.s3Bucket},
		{name: "DEPLOY_ENV", value: cfg.deployEnv},
	}

	for _, field := range required {
		if field.value == "" {
			return fmt.Errorf("%s environment variable is required", field.name)
		}
	}

	return nil
}

func run(ctx context.Context, args []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	return runWithConfig(ctx, cfg, defaultRuntimeDependencies(logger))
}

func defaultRuntimeDependencies(logger *slog.Logger) runtimeDependencies {
	return runtimeDependencies{logger: logger}
}

func runWithConfig(ctx context.Context, cfg cliConfig, deps runtimeDependencies) error {
	deps = deps.withDefaults()
	logger := deps.logger
	if logger == nil {
		logger = slog.Default()
	}

	resources, err := openRuntimeResources(ctx, cfg, deps, logger)
	if err != nil {
		return err
	}
	defer resources.closeFn()

	trackedTokens, err := resources.repos.receiptTokenRepo.ListTrackedReceiptTokens(ctx, cfg.chainID)
	if err != nil {
		return fmt.Errorf("loading tracked receipt tokens: %w", err)
	}

	snapshotter, err := deps.newSnapshotter(ctx, cfg, resources.repos, logger)
	if err != nil {
		return fmt.Errorf("creating position snapshotter: %w", err)
	}

	service, err := deps.newService(shared.SQSConsumerConfig{
		MaxMessages: cfg.maxMessages,
		ChainID:     cfg.chainID,
		Logger:      logger,
	}, resources.consumer, resources.cacheReader, resources.repos.txManager, resources.repos.userRepo, snapshotter, trackedTokens, logger)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	return deps.lifecycleRun(ctx, logger, service)
}

func openRuntimeResources(ctx context.Context, cfg cliConfig, deps runtimeDependencies, logger *slog.Logger) (runtimeResources, error) {
	resources := runtimeResources{}
	resources.closeFn = func() {
		if resources.repos.closeFn != nil {
			closeAndLog(logger, "repositories", resources.repos.closeFn)
		}
		closeAndLog(logger, "cache reader", closeCacheReader(resources.cacheReader))
		closeAndLog(logger, "SQS consumer", closeConsumer(resources.consumer))
	}

	consumer, err := deps.newConsumer(ctx, cfg, logger)
	if err != nil {
		return runtimeResources{}, fmt.Errorf("creating SQS consumer: %w", err)
	}
	resources.consumer = consumer

	cacheReader, err := deps.newCacheReader(ctx, cfg, logger)
	if err != nil {
		resources.closeFn()
		return runtimeResources{}, fmt.Errorf("creating cache reader: %w", err)
	}
	resources.cacheReader = cacheReader

	repos, err := deps.openRepositories(ctx, cfg, logger)
	if err != nil {
		resources.closeFn()
		return runtimeResources{}, fmt.Errorf("opening repositories: %w", err)
	}
	resources.repos = repos

	return resources, nil
}

func (d runtimeDependencies) withDefaults() runtimeDependencies {
	if d.logger == nil {
		d.logger = slog.Default()
	}
	if d.newConsumer == nil {
		d.newConsumer = defaultNewConsumer
	}
	if d.newCacheReader == nil {
		d.newCacheReader = defaultNewCacheReader
	}
	if d.openRepositories == nil {
		d.openRepositories = defaultOpenRepositories
	}
	if d.newSnapshotter == nil {
		d.newSnapshotter = defaultNewSnapshotter
	}
	if d.newService == nil {
		d.newService = defaultNewService
	}
	if d.lifecycleRun == nil {
		d.lifecycleRun = lifecycle.Run
	}
	return d
}

func defaultNewConsumer(ctx context.Context, cfg cliConfig, logger *slog.Logger) (outbound.SQSConsumer, error) {
	awsCfg, err := loadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}

	sqsCfg := sqsAdapter.ConfigDefaults()
	sqsCfg.QueueURL = cfg.queueURL
	sqsCfg.VisibilityTimeout = 300
	sqsCfg.BaseEndpoint = env.Get("AWS_SQS_ENDPOINT", "")

	return sqsAdapter.NewConsumer(awsCfg, sqsCfg, logger)
}

func defaultNewCacheReader(ctx context.Context, cfg cliConfig, logger *slog.Logger) (outbound.BlockCacheReader, error) {
	awsCfg, err := loadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}

	redisCfg := redisAdapter.ConfigDefaults()
	redisCfg.Addr = cfg.redisAddr
	redisCfg.Password = os.Getenv("REDIS_PASSWORD")

	blockCache, err := redisAdapter.NewBlockCache(redisCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("creating Redis block cache: %w", err)
	}
	if err := blockCache.Ping(ctx); err != nil {
		_ = blockCache.Close()
		return nil, fmt.Errorf("connecting to Redis: %w", err)
	}

	s3Reader := s3adapter.NewReader(awsCfg, logger)
	cacheReader, err := cache.NewReaderWithFallback(blockCache, s3Reader, cfg.chainID, cfg.deployEnv, cfg.s3Bucket, logger)
	if err != nil {
		_ = blockCache.Close()
		return nil, fmt.Errorf("creating cache reader with fallback: %w", err)
	}

	return cacheReader, nil
}

func defaultOpenRepositories(ctx context.Context, cfg cliConfig, logger *slog.Logger) (runtimeRepos, error) {
	ethClient, err := ethclient.DialContext(ctx, cfg.alchemyURL)
	if err != nil {
		return runtimeRepos{}, fmt.Errorf("connecting to Ethereum node: %w", err)
	}

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		ethClient.Close()
		return runtimeRepos{}, fmt.Errorf("opening database: %w", err)
	}

	closeFn := func() error {
		ethClient.Close()
		pool.Close()
		return nil
	}

	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		_ = closeFn()
		return runtimeRepos{}, fmt.Errorf("creating transaction manager: %w", err)
	}

	userRepo, err := postgres.NewUserRepository(pool, logger, 0)
	if err != nil {
		_ = closeFn()
		return runtimeRepos{}, fmt.Errorf("creating user repository: %w", err)
	}

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, 0)
	if err != nil {
		_ = closeFn()
		return runtimeRepos{}, fmt.Errorf("creating protocol repository: %w", err)
	}

	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 0)
	if err != nil {
		_ = closeFn()
		return runtimeRepos{}, fmt.Errorf("creating token repository: %w", err)
	}

	positionRepo, err := postgres.NewPositionRepository(pool, logger, 0)
	if err != nil {
		_ = closeFn()
		return runtimeRepos{}, fmt.Errorf("creating position repository: %w", err)
	}

	receiptTokenRepo, err := postgres.NewReceiptTokenRepository(pool, logger, 0)
	if err != nil {
		_ = closeFn()
		return runtimeRepos{}, fmt.Errorf("creating receipt token repository: %w", err)
	}

	return runtimeRepos{
		ethClient:        ethClient,
		txManager:        txManager,
		userRepo:         userRepo,
		protocolRepo:     protocolRepo,
		tokenRepo:        tokenRepo,
		positionRepo:     positionRepo,
		receiptTokenRepo: receiptTokenRepo,
		closeFn:          closeFn,
	}, nil
}

func defaultNewSnapshotter(_ context.Context, _ cliConfig, repos runtimeRepos, logger *slog.Logger) (*shared.PositionSnapshotter, error) {
	if repos.ethClient == nil {
		return nil, fmt.Errorf("ethClient is required")
	}

	return shared.NewPositionSnapshotter(
		repos.ethClient,
		repos.txManager,
		repos.userRepo,
		repos.protocolRepo,
		repos.tokenRepo,
		repos.positionRepo,
		logger,
	)
}

func defaultNewService(
	cfg shared.SQSConsumerConfig,
	consumer outbound.SQSConsumer,
	cacheReader outbound.BlockCacheReader,
	txManager outbound.TxManager,
	userRepo outbound.UserRepository,
	snapshotter *shared.PositionSnapshotter,
	trackedTokens []outbound.TrackedReceiptToken,
	logger *slog.Logger,
) (lifecycle.Service, error) {
	return transfer_user_discovery.NewService(
		cfg,
		consumer,
		cacheReader,
		txManager,
		userRepo,
		snapshotter,
		trackedTokens,
		logger,
	)
}

func loadAWSConfig(ctx context.Context) (aws.Config, error) {
	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(env.Get("AWS_REGION", "us-east-1")),
	}

	if accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); accessKeyID != "" {
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretKey == "" {
			return aws.Config{}, fmt.Errorf("AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is missing")
		}

		awsOpts = append(awsOpts, awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				Source:          "StaticCredentials",
			}, nil
		})))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("loading AWS config: %w", err)
	}

	return awsCfg, nil
}

func parseInt64Env(key string, defaultValue int64) (int64, error) {
	raw := env.Get(key, strconv.FormatInt(defaultValue, 10))
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s %q: %w", key, raw, err)
	}
	return value, nil
}

func parseIntEnv(key string, defaultValue int) (int, error) {
	raw := env.Get(key, strconv.Itoa(defaultValue))
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parsing %s %q: %w", key, raw, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}
	return value, nil
}

func closeAndLog(logger *slog.Logger, name string, closeFn func() error) {
	if closeFn == nil {
		return
	}
	if err := closeFn(); err != nil {
		logger.Warn("failed to close resource", "resource", name, "error", err)
	}
}

func closeConsumer(consumer outbound.SQSConsumer) func() error {
	if consumer == nil {
		return nil
	}
	return consumer.Close
}

func closeCacheReader(cacheReader outbound.BlockCacheReader) func() error {
	if cacheReader == nil {
		return nil
	}
	return cacheReader.Close
}
