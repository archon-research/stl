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
	"github.com/archon-research/stl/stl-verify/internal/pkg/aavelike"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
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
	batchSize       int
	blockNumber     int64
	chainID         int64
	protocolAddress common.Address
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("aave-like-user-snapshot-indexer", flag.ContinueOnError)
	rpcURL := fs.String("rpc-url", "", "Ethereum HTTP RPC endpoint")
	csvPath := fs.String("csv", "", "Path to CSV file with user addresses")
	protocol := fs.String("protocol", "", "Protocol slug (e.g. spark_ethereum, aave_v3_ethereum)")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	concurrency := fs.Int("concurrency", 10, "Number of concurrent batch workers")
	batchSize := fs.Int("batch-size", 100, "Number of users per RPC batch")
	block := fs.Int64("block", 0, "Block number to snapshot at (0 = latest)")

	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		rpcURL:       *rpcURL,
		csvPath:      *csvPath,
		protocolSlug: *protocol,
		dbURL:        *dbURL,
		concurrency:  *concurrency,
		batchSize:    *batchSize,
		blockNumber:  *block,
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
	if cfg.batchSize <= 0 {
		return cliConfig{}, fmt.Errorf("--batch-size must be greater than 0")
	}
	if cfg.blockNumber < 0 {
		return cliConfig{}, fmt.Errorf("--block must be non-negative")
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
	if addrCol < 0 {
		return nil, fmt.Errorf("CSV must have a 'user_address' column")
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

		if len(record) <= addrCol {
			return nil, fmt.Errorf("row %d: expected at least %d columns, got %d", len(users)+1, addrCol+1, len(record))
		}

		// If CSV has a protocols column, filter by it; otherwise include all rows.
		if protoCol >= 0 && len(record) > protoCol {
			protocols := record[protoCol]
			// Strip brackets: "[aave_v3_ethereum spark_ethereum]" -> "aave_v3_ethereum spark_ethereum"
			protocols = strings.TrimPrefix(protocols, "[")
			protocols = strings.TrimSuffix(protocols, "]")

			if !containsSlug(protocols, protocolSlug) {
				continue
			}
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
	logger.Info("Ethereum RPC connected")

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

	// Create PositionReader directly for batch RPC reads
	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("creating multicall client: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return fmt.Errorf("loading ERC20 ABI: %w", err)
	}

	reader := aavelike.NewPositionReader(ethClient, mc, erc20ABI, logger)

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

	// Determine block number
	var blockNumber uint64
	if cfg.blockNumber > 0 {
		blockNumber = uint64(cfg.blockNumber)
		logger.Info("using specified block number", "block", blockNumber)
	} else {
		blockNumber, err = ethClient.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("fetching latest block: %w", err)
		}
		logger.Info("using latest block number", "block", blockNumber)
	}

	// Split users into batches and process concurrently
	batches := splitIntoBatches(users, cfg.batchSize)
	logger.Info("processing users in batches",
		"totalUsers", len(users),
		"batchSize", cfg.batchSize,
		"batches", len(batches))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.concurrency)

	var processed atomic.Int64
	var errCount atomic.Int64

	for batchIdx, batch := range batches {
		g.Go(func() error {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}

			batchStart := time.Now()
			logger.Info("batch starting", "batch", batchIdx+1, "of", len(batches), "users", len(batch))

			rpcStart := time.Now()
			results, err := reader.GetBatchUserPositionData(gCtx, batch, cfg.protocolAddress, cfg.chainID, int64(blockNumber))
			if err != nil {
				return fmt.Errorf("batch %d RPC failed after %s: %w", batchIdx+1, time.Since(rpcStart), err)
			}
			logger.Info("batch RPC done", "batch", batchIdx+1, "rpcDuration", time.Since(rpcStart).Round(time.Millisecond))

			dbStart := time.Now()
			var positions []aavelike_position_tracker.UserPositionData
			for _, user := range batch {
				result, ok := results[user]
				if !ok {
					return fmt.Errorf("missing result for user %s", user.Hex())
				}
				if result.Err != nil {
					errCount.Add(1)
					logger.Warn("failed to get user position data",
						"user", user.Hex(),
						"error", result.Err)
					processed.Add(1)
					continue
				}
				if len(result.Collaterals) == 0 && len(result.Debts) == 0 {
					processed.Add(1)
					continue
				}
				positions = append(positions, aavelike_position_tracker.UserPositionData{
					User:        user,
					Collaterals: result.Collaterals,
					Debts:       result.Debts,
				})
			}

			if len(positions) > 0 {
				if err := trackerSvc.PersistUserPositionBatch(
					gCtx, positions, cfg.protocolAddress, cfg.chainID,
					int64(blockNumber), 0,
				); err != nil {
					return fmt.Errorf("batch %d DB persist failed: %w", batchIdx+1, err)
				}
			}
			processed.Add(int64(len(positions)))

			n := processed.Load()
			logger.Info("batch done",
				"batch", batchIdx+1,
				"persisted", len(positions),
				"rpc", time.Since(rpcStart).Round(time.Millisecond),
				"db", time.Since(dbStart).Round(time.Millisecond),
				"total", time.Since(batchStart).Round(time.Millisecond),
				"progress", fmt.Sprintf("%d/%d", n, len(users)),
				"errors", errCount.Load())
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

func splitIntoBatches(users []common.Address, batchSize int) [][]common.Address {
	var batches [][]common.Address
	for i := 0; i < len(users); i += batchSize {
		end := i + batchSize
		if end > len(users) {
			end = len(users)
		}
		batches = append(batches, users[i:end])
	}
	return batches
}
