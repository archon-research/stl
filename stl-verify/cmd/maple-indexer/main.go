// Package main implements an SQS consumer that fetches Maple Finance borrower
// debt and collateral snapshots on each new Ethereum block, persisting results
// to PostgreSQL.
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

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/maple"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/services/maple_indexer"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

type serviceDependencies struct {
	consumer          *sqsadapter.Consumer
	mapleClient       *maple.Client
	protocolRepo      *postgres.ProtocolRepository
	userRepo          *postgres.UserRepository
	maplePositionRepo *postgres.MaplePositionRepository
	txManager         *postgres.TxManager
}

type cliConfig struct {
	queueURL        string
	dbURL           string
	mapleEndpoint   string
	protocolAddress string
	chainID         int64
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("maple-indexer", flag.ContinueOnError)
	queueURL := fs.String("queue", "", "SQS Queue URL")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		queueURL: *queueURL,
		dbURL:    *dbURL,
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

	cfg.mapleEndpoint = env.Get("MAPLE_GRAPHQL_ENDPOINT", "https://api.maple.finance/v2/graphql")
	cfg.protocolAddress = "0x804a6F5F667170F545Bf14e5DDB48C70B788390C"

	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return cliConfig{}, fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}
	cfg.chainID = chainID

	return cfg, nil
}

func setupDependencies(ctx context.Context, cfg cliConfig, logger *slog.Logger) (serviceDependencies, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(env.Get("AWS_REGION", "eu-west-1")),
	)
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("loading AWS config: %w", err)
	}

	consumer, err := sqsadapter.NewConsumer(awsCfg, sqsadapter.Config{
		QueueURL:     cfg.queueURL,
		BaseEndpoint: env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("creating SQS consumer: %w", err)
	}

	mapleClient, err := maple.NewClient(maple.Config{
		Endpoint: cfg.mapleEndpoint,
		Logger:   logger,
	})
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("creating Maple client: %w", err)
	}

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, 0)
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("creating protocol repository: %w", err)
	}

	userRepo, err := postgres.NewUserRepository(pool, logger, 0)
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("creating user repository: %w", err)
	}

	maplePositionRepo, err := postgres.NewMaplePositionRepository(pool, logger, 0)
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("creating maple position repository: %w", err)
	}

	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return serviceDependencies{}, fmt.Errorf("creating tx manager: %w", err)
	}

	return serviceDependencies{
		consumer:          consumer,
		mapleClient:       mapleClient,
		protocolRepo:      protocolRepo,
		userRepo:          userRepo,
		maplePositionRepo: maplePositionRepo,
		txManager:         txManager,
	}, nil
}

// run executes the main application logic
func run(ctx context.Context, args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	logger.Info("starting maple indexer",
		"queue", cfg.queueURL,
		"protocolAddress", cfg.protocolAddress,
		"chainID", cfg.chainID)

	deps, err := setupDependencies(ctx, cfg, logger)
	if err != nil {
		return err
	}

	service, err := maple_indexer.NewService(
		maple_indexer.Config{
			ChainID:         cfg.chainID,
			ProtocolAddress: common.HexToAddress(cfg.protocolAddress),
			Logger:          logger,
		},
		deps.consumer,
		deps.mapleClient,
		deps.txManager,
		deps.userRepo,
		deps.maplePositionRepo,
		deps.protocolRepo,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	logger.Info("maple indexer started, waiting for messages...")

	return lifecycle.Run(ctx, logger, service)
}
