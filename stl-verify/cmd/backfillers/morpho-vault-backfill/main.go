// Backfill discovers MetaMorpho vaults by scanning historical Ethereum receipt
// files stored in S3 for Morpho Blue events. Candidate addresses (caller/onBehalf)
// are collected, then probed on-chain via multicall (MORPHO() must return the
// Morpho Blue singleton). Confirmed vaults are stored in the morpho_vault table.
// A final phase replays the persisted VaultV2 vaults' structured events.
//
// A run keeps no progress state: every run redoes the full requested range, and
// an interrupted run is resumed by re-running the same command.
//
// Usage:
//
//	go run ./cmd/backfillers/morpho-vault-backfill \
//	  -from 18883124 -to 24600000 \
//	  -bucket stl-sentinelstaging-ethereum-raw-89d540d0 \
//	  -db "$DATABASE_URL" \
//	  -rpc-url "$RPC_URL" \
//	  -goroutines 64
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving/archivingwire"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
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

	logger.Info("starting morpho vault backfill",
		"from", cfg.from,
		"to", cfg.to,
		"bucket", cfg.bucket,
		"chainID", cfg.chainID,
		"goroutines", cfg.goroutines)

	// AWS + S3
	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{
		StaticCredentialsFromEnv: true,
	})
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}
	s3HTTPClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          cfg.goroutines + 64,
			MaxIdleConnsPerHost:   cfg.goroutines + 64,
			MaxConnsPerHost:       cfg.goroutines + 64,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	s3Reader := s3adapter.NewReaderWithHTTPClient(awsCfg, s3HTTPClient, logger)

	// PostgreSQL
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	// Ethereum RPC. Retry 429/5xx/network errors via rpchttp so transient
	// RPC failures don't mark blocks bad. RPC-side concurrency is
	// deliberately decoupled from cfg.goroutines (which sizes the S3
	// reader pool above) — 10 is the historical RPC budget here.
	rpcClient, err := rpc.DialOptions(ctx, cfg.rpcURL, rpc.WithHTTPClient(rpchttp.NewBackfillerClient(10)))
	if err != nil {
		return fmt.Errorf("connecting to RPC: %w", err)
	}
	defer rpcClient.Close()
	ethClient := ethclient.NewClient(rpcClient)
	logger.Info("Ethereum RPC connected")

	rpcChainID, err := ethClient.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("fetching RPC chain ID: %w", err)
	}
	if rpcChainID.Int64() != cfg.chainID {
		return fmt.Errorf("RPC chain ID mismatch: RPC reports %d, config says %d", rpcChainID.Int64(), cfg.chainID)
	}

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("creating multicall client: %w", err)
	}

	// Optional raw SC call archiving (VEC-81). Off unless ARCHIVE_SC_CALLS=true.
	archiveWrap, archiveDrain, err := archivingwire.Bootstrap(ctx, logger, cfg.chainID, int64(buildReg.BuildID()), "morpho-vault")
	if err != nil {
		return err
	}
	defer archiveDrain()
	multicaller = archiveWrap(multicaller)

	// Shared vault prober (handles MetaMorpho ABI internally)
	sharedProber, err := morpho_indexer.NewVaultProber()
	if err != nil {
		return fmt.Errorf("creating vault prober: %w", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return fmt.Errorf("loading ERC20 ABI: %w", err)
	}

	// Event extractor (thread-safe, read-only after init)
	extractor, err := morpho_indexer.NewEventExtractor()
	if err != nil {
		return fmt.Errorf("creating event extractor: %w", err)
	}

	prober := &vaultProber{
		multicaller:  multicaller,
		sharedProber: sharedProber,
		erc20ABI:     erc20ABI,
		logger:       logger,
	}

	// Phases 1–3: scan S3 receipts for candidates, probe them on-chain, persist
	// confirmed vaults.
	if err := discoverAndPersistVaults(ctx, logger, s3Reader, extractor, prober, pool, buildReg.BuildID(), cfg); err != nil {
		return err
	}

	// Phase 4: replay VaultV2 structured (adapter / cap / fee) events for the
	// persisted V2 vaults, driving each log through the same handler path the
	// live worker uses. Runs off the vaults in the database (this run's plus
	// earlier runs'), so it covers a range that only carries governance events
	// for a pre-existing V2 vault — which produces no discovery candidate.
	if err := replayV2StructuredEvents(ctx, logger, s3Reader, ethClient, multicaller, pool, buildReg.BuildID(), cfg); err != nil {
		return fmt.Errorf("replaying VaultV2 structured events: %w", err)
	}

	logger.Info("backfill complete")
	return nil
}
