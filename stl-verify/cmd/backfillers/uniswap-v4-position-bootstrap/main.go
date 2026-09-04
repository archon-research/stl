// Package main is the one-shot CLI that snapshots every historical Uniswap V4
// LP position the live indexer's event-driven coverage can never reach.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving/archivingwire"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
)

func main() {
	if err := runWithSignals(os.Args[1:]); err != nil {
		slog.Error("uniswap-v4 position bootstrap failed", "error", err)
		os.Exit(1)
	}
}

// runWithSignals owns the signal context so main holds no defer for os.Exit to skip.
func runWithSignals(args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return run(ctx, args)
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

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer pool.Close()

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	logScan, err := alchemy.NewClient(alchemy.ClientConfig{HTTPURL: cfg.rpcURL, Logger: logger})
	if err != nil {
		return fmt.Errorf("creating log scan client: %w", err)
	}

	multicaller, closeRPC, err := newMulticaller(ctx, logger, cfg, buildReg.BuildID())
	if err != nil {
		return err
	}
	defer closeRPC()

	svc, err := newBootstrapService(ctx, logger, pool, buildReg.BuildID(), logScan, multicaller, cfg)
	if err != nil {
		return err
	}

	summary, err := svc.Run(ctx)
	if err != nil {
		return resumableError(summary, err)
	}
	logger.Info("uniswap-v4 position bootstrap finished",
		"chainId", cfg.bootstrap.ChainID,
		"pinnedBlock", summary.PinnedBlock, "pinnedHash", summary.PinnedHash,
		"fromBlock", summary.FromBlock, "pools", summary.Pools,
		"keys", summary.Keys, "keysByPool", summary.KeysByPool,
		"positionsRead", summary.PositionsRead, "positionsWritten", summary.PositionsWritten,
		"batches", summary.Batches, "scanWindows", summary.ScanWindows,
		"scanNarrowings", summary.ScanNarrowings, "scanLogs", summary.ScanLogs)
	return nil
}

// A bare rerun re-derives its own head-64 pin, stitching one snapshot across
// two heights, so a failed run must name the pin to resume it with.
func resumableError(summary uniswapv4bootstrap.Summary, err error) error {
	if summary.PinnedBlock == 0 {
		return err
	}
	return fmt.Errorf("bootstrap pinned at block %d failed; resume this snapshot with -pin %d: %w",
		summary.PinnedBlock, summary.PinnedBlock, err)
}

func newMulticaller(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	buildID buildregistry.BuildID,
) (outbound.Multicaller, func(), error) {
	ethClient, err := rpchttp.DialEthereum(ctx, cfg.rpcURL)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to the RPC endpoint: %w", err)
	}

	chainID, err := ethClient.ChainID(ctx)
	if err != nil {
		ethClient.Close()
		return nil, nil, fmt.Errorf("reading the RPC chain id: %w", err)
	}
	if chainID.Int64() != cfg.bootstrap.ChainID {
		ethClient.Close()
		return nil, nil, fmt.Errorf("RPC chain id mismatch: the endpoint reports %d, the config says %d", chainID.Int64(), cfg.bootstrap.ChainID)
	}

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		ethClient.Close()
		return nil, nil, fmt.Errorf("creating the multicall client: %w", err)
	}

	archiveWrap, _, archiveDrain, err := archivingwire.Bootstrap(ctx, logger, cfg.bootstrap.ChainID, int64(buildID), "uniswap-v4-position-bootstrap")
	if err != nil {
		ethClient.Close()
		return nil, nil, err
	}
	return archiveWrap(multicaller), func() {
		archiveDrain()
		ethClient.Close()
	}, nil
}

func newBootstrapService(
	ctx context.Context,
	logger *slog.Logger,
	db *pgxpool.Pool,
	buildID buildregistry.BuildID,
	logScan outbound.LogScanClient,
	multicaller outbound.Multicaller,
	cfg config,
) (*uniswapv4bootstrap.Service, error) {
	repo := postgres.NewUniswapV4Repository(db, buildID)
	poolRows, err := repo.LoadPools(ctx, cfg.bootstrap.ChainID)
	if err != nil {
		return nil, fmt.Errorf("loading uniswap v4 pools: %w", err)
	}
	if len(poolRows) == 0 {
		return nil, fmt.Errorf("no uniswap v4 pools registered for chain %d", cfg.bootstrap.ChainID)
	}

	txMgr, err := postgres.NewTxManager(db, logger)
	if err != nil {
		return nil, fmt.Errorf("creating the tx manager: %w", err)
	}

	return uniswapv4bootstrap.New(uniswapv4bootstrap.Deps{
		Pools:       uniswapv4indexer.RegisteredPoolsFromRows(poolRows),
		LogScan:     logScan,
		Multicaller: multicaller,
		Repo:        repo,
		TxManager:   txMgr,
		Logger:      logger,
		Config:      cfg.bootstrap,
	})
}
