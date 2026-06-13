package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	ct "github.com/archon-research/stl/stl-verify/internal/services/curve_tracker"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	logger.Info("starting curve tracker", "commit", GitCommit)

	// Validate required config upfront.
	dbURL := env.Get("DATABASE_URL", "")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	alchemyAPIKey := os.Getenv("ALCHEMY_API_KEY")
	if alchemyAPIKey == "" {
		return fmt.Errorf("ALCHEMY_API_KEY is required")
	}
	alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	alchemyURL := fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)

	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}

	if _, ok := entity.ChainIDToName[chainID]; !ok {
		return fmt.Errorf("unsupported CHAIN_ID: %d", chainID)
	}

	// Database
	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	logger.Info("PostgreSQL connected")

	txm, err := postgres.NewTxManager(dbPool, logger)
	if err != nil {
		return fmt.Errorf("tx manager: %w", err)
	}

	// Ethereum
	ethClient, err := ethclient.DialContext(ctx, alchemyURL)
	if err != nil {
		return fmt.Errorf("eth dial: %w", err)
	}
	defer ethClient.Close()
	logger.Info("Ethereum node connected")

	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}

	// ABIs
	poolABI, err := abis.GetCurveStableswapNGABI()
	if err != nil {
		return fmt.Errorf("curve pool abi: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return fmt.Errorf("erc20 abi: %w", err)
	}

	// Repository
	curveRepo := postgres.NewCurveRepository(dbPool, txm, logger)

	// Service — pure on-chain, no API dependency
	pools := ct.PoolsForChainID(ct.DefaultPools(), chainID)
	if len(pools) == 0 {
		return fmt.Errorf("no Curve pools configured for chain ID %d", chainID)
	}

	svc := ct.NewService(
		ethClient,
		mc,
		poolABI,
		erc20ABI,
		curveRepo,
		pools,
		logger,
	)

	// Run once
	logger.Info("running", "pools", len(pools), "chainID", chainID)

	if err := svc.Run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	logger.Info("done")
	return nil
}
