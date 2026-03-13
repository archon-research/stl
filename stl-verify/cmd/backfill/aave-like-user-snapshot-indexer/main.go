// Package main provides a CLI tool for indexing current on-chain positions
// (collaterals + debts) for a list of Aave-like protocol users read from a CSV file.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/errgroup"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/aavelike_position_tracker"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
	slog.Info("completed successfully")
}

type cliConfig struct {
	rpcURL          string
	csvPath         string
	protocolSlug    string
	dbURL           string
	concurrency     int
	chainID         int64
	protocolAddress common.Address
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("aave-like-user-snapshot-indexer", flag.ContinueOnError)
	rpcURL := fs.String("rpc-url", "", "Ethereum HTTP RPC endpoint")
	csvPath := fs.String("csv", "", "Path to CSV file with user addresses")
	protocol := fs.String("protocol", "", "Protocol slug (e.g. spark_ethereum, aave_v3_ethereum)")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	concurrency := fs.Int("concurrency", 10, "Number of concurrent RPC workers")

	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		rpcURL:       *rpcURL,
		csvPath:      *csvPath,
		protocolSlug: *protocol,
		dbURL:        *dbURL,
		concurrency:  *concurrency,
	}

	if cfg.rpcURL == "" {
		return cliConfig{}, fmt.Errorf("--rpc-url is required")
	}
	if cfg.csvPath == "" {
		return cliConfig{}, fmt.Errorf("--csv is required")
	}
	if cfg.protocolSlug == "" {
		return cliConfig{}, fmt.Errorf("--protocol is required")
	}
	if cfg.concurrency <= 0 {
		return cliConfig{}, fmt.Errorf("--concurrency must be greater than 0")
	}

	key, _, ok := blockchain.GetProtocolBySlug(cfg.protocolSlug)
	if !ok {
		return cliConfig{}, fmt.Errorf("unknown protocol slug: %s", cfg.protocolSlug)
	}
	cfg.chainID = key.ChainID
	cfg.protocolAddress = key.PoolAddress

	if cfg.dbURL == "" {
		cfg.dbURL = env.Get("DATABASE_URL", "")
	}
	if cfg.dbURL == "" {
		return cliConfig{}, fmt.Errorf("database URL not provided (use --db flag or DATABASE_URL env var)")
	}

	return cfg, nil
}

func parseCSVUsers(r io.Reader, protocolSlug string) ([]common.Address, error) {
	reader := csv.NewReader(r)

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("reading CSV header: %w", err)
	}

	addrCol, protoCol := -1, -1
	for i, h := range header {
		switch strings.TrimSpace(h) {
		case "user_address":
			addrCol = i
		case "protocols":
			protoCol = i
		}
	}
	if addrCol < 0 || protoCol < 0 {
		return nil, fmt.Errorf("CSV must have 'user_address' and 'protocols' columns")
	}

	var users []common.Address
	seen := make(map[common.Address]bool)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading CSV row: %w", err)
		}

		if len(record) <= max(addrCol, protoCol) {
			return nil, fmt.Errorf("row %d: expected at least %d columns, got %d", len(users)+1, max(addrCol, protoCol)+1, len(record))
		}

		protocols := record[protoCol]
		// Strip brackets: "[aave_v3_ethereum spark_ethereum]" -> "aave_v3_ethereum spark_ethereum"
		protocols = strings.TrimPrefix(protocols, "[")
		protocols = strings.TrimSuffix(protocols, "]")

		if !containsSlug(protocols, protocolSlug) {
			continue
		}

		rawAddr := strings.TrimSpace(record[addrCol])
		if !common.IsHexAddress(rawAddr) {
			return nil, fmt.Errorf("row %d: invalid address %q", len(users)+1, rawAddr)
		}
		addr := common.HexToAddress(rawAddr)
		if !seen[addr] {
			seen[addr] = true
			users = append(users, addr)
		}
	}

	return users, nil
}

func containsSlug(protocols, slug string) bool {
	for _, p := range strings.Fields(protocols) {
		if p == slug {
			return true
		}
	}
	return false
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

	// Parse CSV
	csvFile, err := os.Open(cfg.csvPath)
	if err != nil {
		return fmt.Errorf("opening CSV: %w", err)
	}
	defer csvFile.Close()

	users, err := parseCSVUsers(csvFile, cfg.protocolSlug)
	if err != nil {
		return fmt.Errorf("parsing CSV: %w", err)
	}
	logger.Info("parsed CSV", "users", len(users), "protocol", cfg.protocolSlug)

	if len(users) == 0 {
		logger.Info("no users found for protocol, exiting")
		return nil
	}

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
	defer rpcClient.Close()
	ethClient := ethclient.NewClient(rpcClient)
	logger.Info("Ethereum RPC connected", "url", cfg.rpcURL)

	// Connect to PostgreSQL
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("creating tx manager: %w", err)
	}

	userRepo, err := postgres.NewUserRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating user repository: %w", err)
	}

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating protocol repository: %w", err)
	}

	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating token repository: %w", err)
	}

	positionRepo, err := postgres.NewPositionRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating position repository: %w", err)
	}

	eventRepo := postgres.NewEventRepository(logger)

	trackerSvc, err := aavelike_position_tracker.NewService(
		shared.SQSConsumerConfig{
			Logger:  logger,
			ChainID: cfg.chainID,
		},
		nil, // no SQS consumer
		nil, // no cache reader
		ethClient,
		txManager,
		userRepo,
		protocolRepo,
		tokenRepo,
		positionRepo,
		eventRepo,
	)
	if err != nil {
		return fmt.Errorf("creating position tracker service: %w", err)
	}

	// Get latest block number
	blockNumber, err := ethClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("fetching latest block: %w", err)
	}
	logger.Info("using block number", "block", blockNumber)

	// Process users concurrently
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.concurrency)

	var processed atomic.Int64
	var errCount atomic.Int64

	for _, user := range users {
		user := user
		g.Go(func() error {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}

			if err := trackerSvc.IndexUserPosition(gCtx, user, cfg.protocolAddress, cfg.chainID, int64(blockNumber), 0); err != nil {
				errCount.Add(1)
				logger.Warn("failed to index user position",
					"user", user.Hex(),
					"error", err)
				return nil // continue on individual user failures
			}

			n := processed.Add(1)
			if n%1000 == 0 {
				logger.Info("progress", "processed", n, "total", len(users), "errors", errCount.Load())
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("processing users: %w", err)
	}

	logger.Info("indexing complete",
		"processed", processed.Load(),
		"errors", errCount.Load(),
		"total", len(users),
		"block", blockNumber)

	return nil
}
