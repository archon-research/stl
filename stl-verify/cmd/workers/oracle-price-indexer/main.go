// Package main implements an SQS consumer that fetches oracle prices for
// each new Ethereum block and stores price changes in PostgreSQL.
// All oracles are loaded from the DB — no hardcoded oracle configuration.
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
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cache"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	redisadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/oracle_price_worker"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

var (
	GitCommit string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

type cliConfig struct {
	queueURL           string
	dbURL              string
	alchemyURL         string
	alchemyHTTPBaseURL string
	redisAddr          string
	s3Bucket           string
	deployEnv          string
	chainID            int64
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("oracle-price-worker", flag.ContinueOnError)
	queueURL := fs.String("queue", "", "SQS Queue URL")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	redisAddr := fs.String("redis", "", "Redis address")
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		queueURL:  *queueURL,
		dbURL:     *dbURL,
		redisAddr: *redisAddr,
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
	cfg.alchemyHTTPBaseURL = env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	cfg.alchemyURL = fmt.Sprintf("%s/%s", cfg.alchemyHTTPBaseURL, alchemyAPIKey)

	if cfg.redisAddr == "" {
		cfg.redisAddr = env.Get("REDIS_ADDR", "")
	}
	if cfg.redisAddr == "" {
		return cliConfig{}, fmt.Errorf("redis address not provided (use -redis flag or REDIS_ADDR env var)")
	}

	cfg.s3Bucket = env.Get("S3_BUCKET", "")
	if cfg.s3Bucket == "" {
		return cliConfig{}, fmt.Errorf("S3_BUCKET environment variable is required")
	}

	cfg.deployEnv = env.Get("DEPLOY_ENV", "")
	if cfg.deployEnv == "" {
		return cliConfig{}, fmt.Errorf("DEPLOY_ENV environment variable is required")
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

	logger.Info("starting oracle price worker", "queue", cfg.queueURL, "chainID", cfg.chainID)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(env.Get("AWS_REGION", "eu-west-1")),
	)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	consumer, err := sqsadapter.NewConsumer(awsCfg, sqsadapter.Config{
		QueueURL:     cfg.queueURL,
		BaseEndpoint: env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("creating SQS consumer: %w", err)
	}

	cacheCfg := redisadapter.ConfigDefaults()
	cacheCfg.Addr = cfg.redisAddr
	cacheCfg.Password = env.Get("REDIS_PASSWORD", "")
	blockCache, err := redisadapter.NewBlockCache(cacheCfg, logger)
	if err != nil {
		return fmt.Errorf("creating block cache: %w", err)
	}
	defer blockCache.Close()
	if err := blockCache.Ping(ctx); err != nil {
		return fmt.Errorf("connecting to Redis at %s: %w", cfg.redisAddr, err)
	}
	logger.Info("Redis connected", "addr", cfg.redisAddr)

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

	ethClient, err := rpchttp.DialEthereum(ctx, cfg.alchemyURL)
	if err != nil {
		return fmt.Errorf("connecting to Ethereum node: %w", err)
	}
	logger.Info("Ethereum node connected")

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	// Initialize OpenTelemetry tracing and metrics
	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "oracle-price-worker",
		ServiceVersion: buildReg.GitHash(),
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("initializing telemetry: %w", err)
	}
	defer shutdownOTEL(context.Background())

	// Service telemetry
	oracleTelemetry, err := oracle_price_worker.NewTelemetry()
	if err != nil {
		return fmt.Errorf("creating oracle telemetry: %w", err)
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, buildReg.BuildID(), 0)
	if err != nil {
		return fmt.Errorf("creating repository: %w", err)
	}

	service, err := oracle_price_worker.NewService(
		shared.SQSConsumerConfig{
			Logger:  logger,
			ChainID: cfg.chainID,
		},
		consumer,
		cacheReader,
		repo,
		func(oracleType entity.OracleType) (outbound.Multicaller, error) {
			if oracleType == entity.OracleTypeChronicle {
				return multicall.NewDirectCaller(ethClient.Client())
			}
			return multicall.NewClient(ethClient, blockchain.Multicall3)
		},
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}
	service.WithTelemetry(oracleTelemetry)

	logger.Info("oracle price worker started, waiting for messages...")

	return lifecycle.Run(ctx, logger, service)
}
