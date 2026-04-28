// Package main provides a CLI tool for backfilling historical SparkLend position
// data by reading transaction receipts from S3 and processing them into PostgreSQL.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/services/aavelike_position_tracker"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_backfill"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
	slog.Info("completed successfully")
}

type cliConfig struct {
	rpcURL      string
	fromBlock   int64
	toBlock     int64
	concurrency int
	bucket      string
	dbURL       string
	chainID     int64
	awsRegion   string
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("sparklend-backfill", flag.ContinueOnError)
	rpcURL := fs.String("rpc-url", "", "Ethereum HTTP RPC endpoint (e.g., http://erigon:8545)")
	fromBlock := fs.Int64("from", -1, "Start block number (required)")
	toBlock := fs.Int64("to", -1, "End block number (required)")
	concurrency := fs.Int("concurrency", 10, "Number of concurrent workers")
	bucket := fs.String("bucket", "", "S3 bucket containing transaction receipts")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	chainID := fs.Int64("chain-id", 1, "Ethereum chain ID")
	awsRegion := fs.String("aws-region", "us-east-1", "AWS region")

	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		rpcURL:      *rpcURL,
		fromBlock:   *fromBlock,
		toBlock:     *toBlock,
		concurrency: *concurrency,
		bucket:      *bucket,
		dbURL:       *dbURL,
		chainID:     *chainID,
		awsRegion:   *awsRegion,
	}

	if cfg.rpcURL == "" {
		return cliConfig{}, fmt.Errorf("--rpc-url is required")
	}
	if cfg.fromBlock < 0 {
		return cliConfig{}, fmt.Errorf("--from is required")
	}
	if cfg.toBlock < 0 {
		return cliConfig{}, fmt.Errorf("--to is required")
	}
	if cfg.toBlock < cfg.fromBlock {
		return cliConfig{}, fmt.Errorf("--to must be >= --from")
	}
	if cfg.bucket == "" {
		return cliConfig{}, fmt.Errorf("--bucket is required")
	}

	if cfg.dbURL == "" {
		cfg.dbURL = env.Get("DATABASE_URL", "")
	}
	if cfg.dbURL == "" {
		return cliConfig{}, fmt.Errorf("database URL not provided (use --db flag or DATABASE_URL env var)")
	}

	return cfg, nil
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	go func() {
		select {
		case sig := <-sigChan:
			logger.Info("received signal, shutting down...", "signal", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	// Create Ethereum client with connection pooling scaled to concurrency.
	// Retry 429/5xx/network errors via rpchttp so transient RPC failures
	// don't mark blocks bad.
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          cfg.concurrency * 2,
		MaxIdleConnsPerHost:   cfg.concurrency,
		MaxConnsPerHost:       cfg.concurrency,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	httpClient := rpchttp.NewClient(rpchttp.Config{
		MaxRetries:  5,
		BaseBackoff: 250 * time.Millisecond,
		MaxBackoff:  10 * time.Second,
		Transport:   transport,
	})
	httpClient.Timeout = 120 * time.Second

	rpcClient, err := rpc.DialOptions(ctx, cfg.rpcURL, rpc.WithHTTPClient(httpClient))
	if err != nil {
		return fmt.Errorf("connecting to RPC: %w", err)
	}
	defer rpcClient.Close()
	ethClient := ethclient.NewClient(rpcClient)
	logger.Info("Ethereum RPC connected", "url", cfg.rpcURL)

	// Load AWS config; support LocalStack via AWS_ENDPOINT_URL
	awsOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.awsRegion),
	}
	if endpointURL := os.Getenv("AWS_ENDPOINT_URL"); endpointURL != "" {
		awsOpts = append(awsOpts, config.WithBaseEndpoint(endpointURL))
		logger.Info("using custom AWS endpoint", "url", endpointURL)
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	// When a custom endpoint is configured (e.g. LocalStack), path-style addressing
	// is required because virtual-hosted-style URLs won't resolve correctly.
	var s3Reader *s3adapter.Reader
	if os.Getenv("AWS_ENDPOINT_URL") != "" {
		s3Reader = s3adapter.NewReaderWithOptions(awsCfg, logger, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	} else {
		s3Reader = s3adapter.NewReader(awsCfg, logger)
	}
	logger.Info("S3 reader created")

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

	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("creating tx manager: %w", err)
	}

	userRepo, err := postgres.NewUserRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating user repository: %w", err)
	}

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, buildReg.BuildID(), 0)
	if err != nil {
		return fmt.Errorf("creating protocol repository: %w", err)
	}

	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating token repository: %w", err)
	}

	positionRepo, err := postgres.NewPositionRepository(pool, logger, buildReg.BuildID(), 0)
	if err != nil {
		return fmt.Errorf("creating position repository: %w", err)
	}

	eventRepo := postgres.NewEventRepository(logger, buildReg.BuildID())

	receiptTokenRepo, err := postgres.NewReceiptTokenRepository(pool, logger)
	if err != nil {
		return fmt.Errorf("creating receipt token repository: %w", err)
	}

	// Build position tracker (nil consumer and nil redisClient for backfill mode)
	trackerSvc, err := aavelike_position_tracker.NewService(
		shared.SQSConsumerConfig{
			Logger:  logger,
			ChainID: cfg.chainID,
		},
		nil,
		nil,
		ethClient,
		txManager,
		userRepo,
		protocolRepo,
		tokenRepo,
		positionRepo,
		eventRepo,
		receiptTokenRepo,
	)
	if err != nil {
		return fmt.Errorf("creating position tracker service: %w", err)
	}

	// Build backfill service
	backfillSvc, err := sparklend_backfill.NewService(
		sparklend_backfill.Config{
			Concurrency: cfg.concurrency,
			Logger:      logger,
		},
		s3Reader,
		trackerSvc,
		cfg.bucket,
		cfg.chainID,
	)
	if err != nil {
		return fmt.Errorf("creating backfill service: %w", err)
	}

	logger.Info("starting sparklend backfill",
		"fromBlock", cfg.fromBlock,
		"toBlock", cfg.toBlock,
		"concurrency", cfg.concurrency,
		"bucket", cfg.bucket,
		"chainID", cfg.chainID,
	)

	return backfillSvc.Run(ctx, cfg.fromBlock, cfg.toBlock)
}
