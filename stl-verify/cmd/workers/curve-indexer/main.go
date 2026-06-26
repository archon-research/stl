package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/cmd/workers/internal/dexbootstrap"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/services/curveindexer"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
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

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := dexbootstrap.ParseConfig("curve-indexer", args)
	if err != nil {
		return err
	}
	deps, err := dexbootstrap.Bootstrap(ctx, cfg, dexbootstrap.BootstrapOptions{
		ServiceName:  "curve-indexer",
		MetricPrefix: "curve",
		BuildTime:    BuildTime,
	})
	if err != nil {
		return err
	}
	defer deps.Close()
	if err := deps.CommonDeps().Validate(); err != nil {
		return fmt.Errorf("validating deps: %w", err)
	}

	repo, err := postgres.NewCurveRepository(deps.PostgresPool, deps.Logger, deps.BuildRegistry.BuildID())
	if err != nil {
		return fmt.Errorf("creating curve repository: %w", err)
	}
	poolRows, err := repo.LoadPools(ctx, cfg.ChainID)
	if err != nil {
		return fmt.Errorf("loading curve pools: %w", err)
	}
	pools := curveindexer.IndexPoolsByAddress(poolRows)

	stableABI, err := abis.CurveStableswapABI()
	if err != nil {
		return fmt.Errorf("loading stableswap abi: %w", err)
	}
	cryptoABI, err := abis.CurveCryptoswapABI()
	if err != nil {
		return fmt.Errorf("loading cryptoswap abi: %w", err)
	}

	protocolID, err := dexconsumer.ResolveProtocolID(ctx, deps.TxManager, deps.ProtocolRepo, dexconsumer.ProtocolDescriptor{
		Address:      common.HexToAddress("0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf"),
		Name:         "Curve",
		ProtocolType: "dex",
		DeployBlock:  19421686,
	}, cfg.ChainID)
	if err != nil {
		return fmt.Errorf("resolving curve protocol_id: %w", err)
	}
	eventWriter := dexconsumer.NewProtocolEventWriter(protocolID, deps.EventRepo)

	coord, err := curveindexer.NewCoordinator(curveindexer.CoordinatorDeps{
		Pools:           pools,
		Handlers:        curveindexer.NewHandlerRegistry(curveindexer.NewStableswapHandler(stableABI), curveindexer.NewCryptoswapHandler(cryptoABI)),
		Multicaller:     deps.Multicaller,
		Repo:            repo,
		EventWriter:     eventWriter,
		TxManager:       deps.TxManager,
		HeartbeatBlocks: cfg.HeartbeatBlocks,
		ChainID:         cfg.ChainID,
		Logger:          deps.Logger,
		Telemetry:       deps.DexTelemetry,
	})
	if err != nil {
		return fmt.Errorf("creating coordinator: %w", err)
	}

	bp := dexconsumer.NewBlockProcessorWithFinalizer(deps.CacheReader, deps.DexTelemetry, coord.ReceiptHandler(), coord.Finalizer())
	deps.Logger.Info("curve-indexer started", "pools", len(poolRows), "heartbeatBlocks", cfg.HeartbeatBlocks)
	sqsutil.RunLoop(ctx, sqsutil.Config{
		Consumer:     deps.SQSConsumer,
		MaxMessages:  cfg.MaxMessages,
		PollInterval: 1 * time.Second,
		Logger:       deps.Logger,
		ChainID:      cfg.ChainID,
	}, bp.ProcessBlockEvent)
	return nil
}
