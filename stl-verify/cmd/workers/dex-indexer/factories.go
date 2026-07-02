package main

import (
	"context"
	"fmt"

	"github.com/archon-research/stl/stl-verify/cmd/workers/internal/dexbootstrap"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/curveindexer"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv3indexer"
)

// Factory builds the per-DEX dexconsumer.BlockHandler from the shared
// bootstrap deps + config. Concrete factories own the repo<->service wiring
// for exactly one DEX, so this package (cmd, not a service package) is the
// only place that imports both the postgres adapters and the service
// packages; services never import adapters.
type Factory interface {
	// Kind matches cfg.Dex (e.g. "curve", "uniswap-v3") and is the registry key.
	Kind() string
	// ServiceName feeds dexbootstrap.BootstrapOptions.ServiceName (OTEL/logger identity).
	ServiceName() string
	// MetricPrefix feeds dexbootstrap.BootstrapOptions.MetricPrefix (dextelemetry metric names).
	MetricPrefix() string
	BuildHandler(ctx context.Context, deps *dexbootstrap.Deps, cfg dexbootstrap.Config) (dexconsumer.BlockHandler, error)
}

// curveFactory wires the Curve indexer. Migrated from the standalone
// curve-indexer/main.go: same repository, ABIs, protocol-ID derivation, and
// SweepBlocks-driven service construction.
type curveFactory struct{}

func (curveFactory) Kind() string         { return "curve" }
func (curveFactory) ServiceName() string  { return "curve-indexer" }
func (curveFactory) MetricPrefix() string { return "curve" }

func (curveFactory) BuildHandler(ctx context.Context, deps *dexbootstrap.Deps, cfg dexbootstrap.Config) (dexconsumer.BlockHandler, error) {
	repo, err := postgres.NewCurveRepository(deps.PostgresPool, deps.Logger, deps.BuildRegistry.BuildID())
	if err != nil {
		return nil, fmt.Errorf("creating curve repository: %w", err)
	}
	poolRows, err := repo.LoadPools(ctx, cfg.ChainID)
	if err != nil {
		return nil, fmt.Errorf("loading curve pools: %w", err)
	}
	if len(poolRows) == 0 {
		return nil, fmt.Errorf("no curve pools registered for chain %d", cfg.ChainID)
	}
	// Per-pool A_precise availability comes from curated DB metadata
	// (curve_pool.has_a_precise, carried through LoadPools), so the snapshot issues
	// that call only where it exists — no startup capability probe.
	pools := curveindexer.IndexPoolsByAddress(poolRows)

	stableABI, err := abis.CurveStableswapABI()
	if err != nil {
		return nil, fmt.Errorf("loading stableswap abi: %w", err)
	}
	cryptoABI, err := abis.CurveCryptoswapABI()
	if err != nil {
		return nil, fmt.Errorf("loading cryptoswap abi: %w", err)
	}

	protocolID := poolRows[0].ProtocolID
	eventWriter := dexconsumer.NewProtocolEventWriter(protocolID, deps.EventRepo)

	coord, err := curveindexer.NewCurveService(curveindexer.CurveServiceDeps{
		Pools:       pools,
		Handlers:    curveindexer.NewHandlerRegistry(curveindexer.NewStableswapHandler(stableABI), curveindexer.NewCryptoswapHandler(cryptoABI)),
		Multicaller: deps.Multicaller,
		Repo:        repo,
		EventWriter: eventWriter,
		TxManager:   deps.TxManager,
		SweepBlocks: cfg.SweepBlocks,
		ChainID:     cfg.ChainID,
		Logger:      deps.Logger,
		Telemetry:   deps.DexTelemetry,
	})
	if err != nil {
		return nil, fmt.Errorf("creating curve coordinator: %w", err)
	}
	deps.Logger.Info("curve-indexer started", "pools", len(poolRows), "sweepBlocks", cfg.SweepBlocks)
	return coord.BlockHandler(), nil
}

// uniswapV3Factory wires the Uniswap V3 indexer.
type uniswapV3Factory struct{}

func (uniswapV3Factory) Kind() string         { return "uniswap-v3" }
func (uniswapV3Factory) ServiceName() string  { return "uniswap-v3-indexer" }
func (uniswapV3Factory) MetricPrefix() string { return "uniswap_v3" }

func (uniswapV3Factory) BuildHandler(ctx context.Context, deps *dexbootstrap.Deps, cfg dexbootstrap.Config) (dexconsumer.BlockHandler, error) {
	repo := postgres.NewUniswapV3Repository(deps.PostgresPool, deps.BuildRegistry.BuildID())
	poolRows, err := repo.LoadPools(ctx, cfg.ChainID)
	if err != nil {
		return nil, fmt.Errorf("loading uniswap v3 pools: %w", err)
	}
	if len(poolRows) == 0 {
		return nil, fmt.Errorf("no uniswap v3 pools registered for chain %d", cfg.ChainID)
	}
	pools := uniswapv3indexer.RegisteredPoolsFromRows(poolRows)

	protocolID := poolRows[0].ProtocolID
	eventWriter := dexconsumer.NewProtocolEventWriter(protocolID, deps.EventRepo)

	service, err := uniswapv3indexer.NewUniswapV3Service(uniswapv3indexer.UniswapV3ServiceDeps{
		Pools:       pools,
		Multicaller: deps.Multicaller,
		Repo:        repo,
		EventWriter: eventWriter,
		TxManager:   deps.TxManager,
		ChainID:     cfg.ChainID,
		Logger:      deps.Logger,
		Telemetry:   deps.DexTelemetry,
	})
	if err != nil {
		return nil, fmt.Errorf("creating uniswap v3 service: %w", err)
	}
	deps.Logger.Info("uniswap-v3-indexer started", "pools", len(poolRows))
	return service.BlockHandler(), nil
}
