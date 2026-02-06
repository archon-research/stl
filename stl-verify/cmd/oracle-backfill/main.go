// Package main provides a CLI tool for bulk-fetching historical oracle prices
// from an Erigon node and storing price changes in PostgreSQL.
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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/oracle_backfill"
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
	batchSize   int
	dbURL       string
	verbose     bool
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("oracle-backfill", flag.ContinueOnError)
	rpcURL := fs.String("rpc-url", "", "Erigon HTTP RPC endpoint (e.g., http://erigon:8545)")
	fromBlock := fs.Int64("from", 0, "Start block number (required)")
	toBlock := fs.Int64("to", 0, "End block number (required)")
	concurrency := fs.Int("concurrency", 100, "Number of concurrent workers")
	batchSize := fs.Int("batch-size", 1000, "Batch size for DB inserts")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		rpcURL:      *rpcURL,
		fromBlock:   *fromBlock,
		toBlock:     *toBlock,
		concurrency: *concurrency,
		batchSize:   *batchSize,
		dbURL:       *dbURL,
		verbose:     *verbose,
	}

	if cfg.rpcURL == "" {
		return cliConfig{}, fmt.Errorf("--rpc-url is required")
	}
	if cfg.fromBlock == 0 {
		return cliConfig{}, fmt.Errorf("--from is required")
	}
	if cfg.toBlock == 0 {
		return cliConfig{}, fmt.Errorf("--to is required")
	}
	if cfg.toBlock < cfg.fromBlock {
		return cliConfig{}, fmt.Errorf("--to must be >= --from")
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

	logLevel := slog.LevelInfo
	if cfg.verbose {
		logLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("received signal, shutting down...", "signal", sig)
		cancel()
	}()

	// Create Ethereum client with connection pooling
	httpClient := &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
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
		},
	}

	rpcClient, err := rpc.DialOptions(ctx, cfg.rpcURL, rpc.WithHTTPClient(httpClient))
	if err != nil {
		return fmt.Errorf("connecting to RPC: %w", err)
	}
	ethClient := ethclient.NewClient(rpcClient)
	logger.Info("Ethereum RPC connected", "url", cfg.rpcURL)

	newMulticaller := func() (outbound.Multicaller, error) {
		return multicall.NewClient(ethClient, blockchain.Multicall3)
	}

	// Connect to PostgreSQL
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, cfg.batchSize)
	if err != nil {
		return fmt.Errorf("creating repository: %w", err)
	}

	// Load oracle and token addresses
	oracle, err := repo.GetOracle(ctx, "sparklend")
	if err != nil {
		return fmt.Errorf("getting oracle source: %w", err)
	}

	tokenAddrBytes, err := repo.GetTokenAddresses(ctx, oracle.ID)
	if err != nil {
		return fmt.Errorf("getting token addresses: %w", err)
	}

	tokenAddresses := make(map[int64]common.Address, len(tokenAddrBytes))
	for tokenID, addr := range tokenAddrBytes {
		tokenAddresses[tokenID] = common.BytesToAddress(addr)
	}
	logger.Info("loaded token addresses", "count", len(tokenAddresses))

	service, err := oracle_backfill.NewService(
		oracle_backfill.Config{
			Concurrency:  cfg.concurrency,
			BatchSize:    cfg.batchSize,
			OracleSource: "sparklend",
			Logger:       logger,
		},
		ethClient,
		newMulticaller,
		repo,
		tokenAddresses,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	return service.Run(ctx, cfg.fromBlock, cfg.toBlock)
}
