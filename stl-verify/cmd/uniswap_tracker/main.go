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
	ut "github.com/archon-research/stl/stl-verify/internal/services/uniswap_tracker"
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

	logger.Info("starting uniswap tracker", "commit", GitCommit)

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

	// Optional TWAP window override (default: 30 minutes).
	var twapWindow uint32
	if v := env.Get("TWAP_WINDOW", ""); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return fmt.Errorf("parsing TWAP_WINDOW %q: %w", v, err)
		}
		twapWindow = uint32(parsed)
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

	nodeChainID, err := ethClient.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("fetching rpc chain ID: %w", err)
	}
	if nodeChainID.Int64() != chainID {
		return fmt.Errorf("rpc chain ID %d does not match configured CHAIN_ID %d", nodeChainID.Int64(), chainID)
	}

	logger.Info("Ethereum node connected")

	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}

	// ABIs
	poolABI, err := abis.GetUniswapV3PoolABI()
	if err != nil {
		return fmt.Errorf("uniswap v3 pool abi: %w", err)
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return fmt.Errorf("erc20 abi: %w", err)
	}

	// Repositories
	uniRepo := postgres.NewUniswapRepository(dbPool, txm, logger)

	tokenRepo, err := postgres.NewTokenRepository(dbPool, logger, 0)
	if err != nil {
		return fmt.Errorf("token repository: %w", err)
	}

	// Service
	pools := ut.PoolsForChainID(ut.TrackedPools(), chainID)
	if len(pools) == 0 {
		return fmt.Errorf("no Uniswap V3 pools configured for chain ID %d", chainID)
	}

	svc := ut.NewService(
		ethClient,
		mc,
		poolABI,
		erc20ABI,
		uniRepo,
		tokenRepo,
		pools,
		twapWindow,
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
