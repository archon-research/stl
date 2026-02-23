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
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/oracle_price_worker"
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
	queueURL       string
	dbURL          string
	rpcURL         string
	rpcHTTPBaseURL string
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("oracle-price-worker", flag.ContinueOnError)
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

	rpcAPIKey := os.Getenv("ETH_RPC_API_KEY")
	if rpcAPIKey == "" {
		return cliConfig{}, fmt.Errorf("ETH_RPC_API_KEY environment variable is required")
	}
	cfg.rpcHTTPBaseURL = env.Get("ETH_RPC_HTTP_URL", "")
	if cfg.rpcHTTPBaseURL == "" {
		return cliConfig{}, fmt.Errorf("ETH_RPC_HTTP_URL environment variable is required")
	}
	cfg.rpcURL = fmt.Sprintf("%s/%s", cfg.rpcHTTPBaseURL, rpcAPIKey)

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

	logger.Info("starting oracle price worker", "queue", cfg.queueURL)

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

	ethClient, err := ethclient.Dial(cfg.rpcURL)
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

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating repository: %w", err)
	}

	service, err := oracle_price_worker.NewService(
		oracle_price_worker.Config{
			Logger: logger,
		},
		consumer,
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
