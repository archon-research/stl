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
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/maple"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
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

	// AWS + SQS consumer.
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

	// Maple GraphQL client.
	mapleClient, err := maple.NewClient(maple.Config{
		Endpoint: cfg.mapleEndpoint,
		Logger:   logger,
	})
	if err != nil {
		return fmt.Errorf("creating Maple client: %w", err)
	}

	// PostgreSQL.
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating protocol repository: %w", err)
	}

	positionRepo, err := postgres.NewPositionRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating position repository: %w", err)
	}

	userRepo, err := postgres.NewUserRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating user repository: %w", err)
	}

	protocolAssetRepo, err := postgres.NewProtocolAssetRepository(pool)
	if err != nil {
		return fmt.Errorf("creating protocol asset repository: %w", err)
	}

	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("creating tx manager: %w", err)
	}

	// Service.
	service, err := maple_indexer.NewService(
		maple_indexer.Config{
			ChainID:         cfg.chainID,
			ProtocolAddress: common.HexToAddress(cfg.protocolAddress),
			Logger:          logger,
		},
		consumer,
		mapleClient,
		txManager,
		userRepo,
		protocolAssetRepo,
		positionRepo,
		protocolRepo,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	logger.Info("starting service...")
	if err := service.Start(ctx); err != nil {
		return fmt.Errorf("starting service: %w", err)
	}

	logger.Info("service started, waiting for messages...")

	// Block until context is cancelled (signal or test cancellation).
	<-ctx.Done()
	logger.Info("shutting down...")

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
		return fmt.Errorf("shutdown timed out")
	}

	return nil
}
