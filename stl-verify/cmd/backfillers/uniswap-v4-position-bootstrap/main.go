// One-shot backfiller that gives every historical Uniswap V4 LP position a row
// in uniswap_v4_position.
//
// The live indexer learns a position only from a ModifyLiquidity log, so a
// position minted before it went live and never touched since never appears.
// This run replays the PoolManager's whole ModifyLiquidity history for the
// registered snapshot-supported pools, then reads each discovered key's state
// at one finality-safe block through the indexer's own StateView getter and
// append-on-change write path.
//
// Run it after the indexer's first deploy on a chain, and again after any
// suspected gap (a long outage, a DLQ'd stretch, a newly registered pool). A
// run over already-covered history writes nothing, so re-running is free.
//
// It keeps no progress state, so an interrupted run is resumed by re-running it
// with -pin set to the pin the failed run logged. A bare rerun re-derives its
// own head-64 pin and would stitch one snapshot across two heights, which is
// exactly what pinning a single block exists to prevent.
//
// Usage:
//
//	go run ./cmd/backfillers/uniswap-v4-position-bootstrap \
//	  -db "$DATABASE_URL" -rpc-url "$RPC_URL" -chain-id 1
//
// Every flag also reads from the environment (DATABASE_URL, ALCHEMY_API_KEY /
// ALCHEMY_HTTP_URL, CHAIN_ID, FROM_BLOCK, PIN_BLOCK), so `make
// run-backfiller-uniswap-v4-position-bootstrap` works off a generated .env.
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

// resumableError names the pin the failed run was using, so the operator
// resumes that same snapshot instead of re-deriving a fresh head-64 pin and
// stitching one snapshot across two heights. A failure before the pin was
// resolved has nothing to add.
func resumableError(summary uniswapv4bootstrap.Summary, err error) error {
	if summary.PinnedBlock == 0 {
		return err
	}
	return fmt.Errorf("bootstrap pinned at block %d failed; resume this snapshot with -pin %d: %w",
		summary.PinnedBlock, summary.PinnedBlock, err)
}

// newMulticaller dials the RPC endpoint and returns the hash-pinned state
// reader, optionally wrapped for raw SC call archiving (VEC-81, off unless
// ARCHIVE_SC_CALLS=true). The returned func closes both the archive drain and
// the RPC connection.
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

	archiveWrap, archiveDrain, err := archivingwire.Bootstrap(ctx, logger, cfg.bootstrap.ChainID, int64(buildID), "uniswap-v4-position-bootstrap")
	if err != nil {
		ethClient.Close()
		return nil, nil, err
	}
	return archiveWrap(multicaller), func() {
		archiveDrain()
		ethClient.Close()
	}, nil
}

// newBootstrapService loads the pool registry and wires the service. An empty
// registry is an error rather than an empty run: it means the chain's V4 seed
// migration has not been applied, not that there is nothing to backfill.
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
