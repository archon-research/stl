//go:build integration

package uniswapv3indexer

import (
	"context"
	"log/slog"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// reorgIntegrationFixture wires the real Postgres UniswapV3Repository, event
// repository, and tx manager against a fresh schema, with only the multicaller
// (the archive RPC — the data source we cannot control) faked. The seeded pool
// carries a real uniswap_v3_pool.id so the fact-table FKs are satisfied.
type reorgIntegrationFixture struct {
	svc  *UniswapV3Service
	mc   *recordingMulticaller
	db   *pgxpool.Pool
	pool RegisteredPool
}

// seedReorgPool inserts the UniswapV3 protocol row, the pool's token pair, and
// the pool row itself, returning the pool's real id. Uses uniswapTestPool's
// address/fee/tick-spacing so the reused swap/mint log fixtures resolve.
func seedReorgPool(t *testing.T, ctx context.Context, db *pgxpool.Pool, p RegisteredPool) int64 {
	t.Helper()

	var protocolID int64
	if err := db.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		 VALUES (1, '\x1F98431c8aD98523631AE4a59f267346ea31F984'::bytea, 'UniswapV3', 'dex', 12369621)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&protocolID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}

	var token0ID, token1ID int64
	if err := db.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, $1, $2, $3)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
		p.Token0.Bytes(), "WETH", p.Token0Decimals,
	).Scan(&token0ID); err != nil {
		t.Fatalf("seed token0: %v", err)
	}
	if err := db.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, $1, $2, $3)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
		p.Token1.Bytes(), "USDC", p.Token1Decimals,
	).Scan(&token1ID); err != nil {
		t.Fatalf("seed token1: %v", err)
	}

	var poolID int64
	if err := db.QueryRow(ctx,
		`INSERT INTO uniswap_v3_pool
		    (chain_id, protocol_id, pool_address, token0_id, token1_id,
		     fee, tick_spacing, max_liquidity_per_tick, deploy_block)
		 VALUES (1, $1, $2, $3, $4, $5, $6, 11505743598341114571880798222544994, $7)
		 ON CONFLICT (chain_id, pool_address) DO UPDATE SET deploy_block = EXCLUDED.deploy_block
		 RETURNING id`,
		protocolID, p.Address.Bytes(), token0ID, token1ID, p.Fee, p.TickSpacing, p.DeployBlock,
	).Scan(&poolID); err != nil {
		t.Fatalf("seed uniswap_v3_pool: %v", err)
	}
	return poolID
}

func setupReorgIntegration(t *testing.T) *reorgIntegrationFixture {
	t.Helper()
	ctx := context.Background()

	db, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	pool := uniswapTestPool()
	poolID := seedReorgPool(t, ctx, db, pool)
	pool.ID = poolID

	var protocolID int64
	if err := db.QueryRow(ctx,
		`SELECT protocol_id FROM uniswap_v3_pool WHERE id = $1`, poolID,
	).Scan(&protocolID); err != nil {
		t.Fatalf("reading protocol_id: %v", err)
	}

	repo := postgres.NewUniswapV3Repository(db, 1)
	txMgr, err := postgres.NewTxManager(db, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	eventRepo := postgres.NewEventRepository(nil, 1)
	writer := dexconsumer.NewProtocolEventWriter(protocolID, eventRepo)

	mc := &recordingMulticaller{
		stateResults: stateResultsFixture(t),
		tickResults:  map[int32]outbound.Result{},
	}

	svc, err := NewUniswapV3Service(UniswapV3ServiceDeps{
		Pools:       []RegisteredPool{pool},
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Service: %v", err)
	}

	return &reorgIntegrationFixture{svc: svc, mc: mc, db: db, pool: pool}
}

// tickResult packs a ticks() multicall return with the given values so a test
// can distinguish an initialized tick from a now-uninitialized (zeroed) one.
func tickResult(t *testing.T, liquidityGross, liquidityNet int64, initialized bool) outbound.Result {
	t.Helper()
	a, err := tickViewABI()
	if err != nil {
		t.Fatalf("tickViewABI: %v", err)
	}
	packed, err := a.Methods["ticks"].Outputs.Pack(
		big.NewInt(liquidityGross),
		big.NewInt(liquidityNet),
		big.NewInt(0), // feeGrowthOutside0X128
		big.NewInt(0), // feeGrowthOutside1X128
		big.NewInt(0), // tickCumulativeOutside
		big.NewInt(0), // secondsPerLiquidityOutsideX128
		uint32(0),     // secondsOutside
		initialized,
	)
	if err != nil {
		t.Fatalf("packing ticks() return: %v", err)
	}
	return outbound.Result{Success: true, ReturnData: packed}
}

// latestTick reads the canonical-latest uniswap_v3_tick row for (pool, tick)
// using the on-chain snapshot ordering.
func latestTick(t *testing.T, ctx context.Context, db *pgxpool.Pool, poolID int64, tick int) (blockNumber int64, blockVersion int, liquidityGross string, initialized bool) {
	t.Helper()
	if err := db.QueryRow(ctx,
		`SELECT block_number, block_version, liquidity_gross::text, initialized
		 FROM uniswap_v3_tick
		 WHERE pool_id = $1 AND tick = $2
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC
		 LIMIT 1`,
		poolID, tick,
	).Scan(&blockNumber, &blockVersion, &liquidityGross, &initialized); err != nil {
		t.Fatalf("reading latest tick (pool=%d tick=%d): %v", poolID, tick, err)
	}
	return blockNumber, blockVersion, liquidityGross, initialized
}

// TestIntegration_ReorgReconcilesStaleTicks proves the VEC-487 fix: a tick
// initialized on an orphaned fork (N, v0) is reconciled when block N is
// redelivered at v1 whose receipts do NOT touch the pool. The pool is
// re-snapshotted (DueSet reorg rule), and its prior-version tick rows are
// re-read at v1: a now-uninitialized tick gets a superseding zeroed (N, v1)
// row, and a still-initialized-but-changed tick gets its v1 value. Without the
// fix the stale (N, v0) row remains canonical-latest.
func TestIntegration_ReorgReconcilesStaleTicks(t *testing.T) {
	ctx := context.Background()
	f := setupReorgIntegration(t)

	const (
		blockN     = int64(200)
		tickGoneT  = -120 // initialized on v0, uninitialized on v1
		tickKeptT2 = 180  // initialized on both, value changes on v1
	)

	// v0 delivery: a mint initializes both ticks at (N, v0).
	f.mc.tickResults[int32(tickGoneT)] = tickResult(t, 1000, 500, true)
	f.mc.tickResults[int32(tickKeptT2)] = tickResult(t, 1000, 500, true)

	v0 := blockEvent(blockN) // Version 0
	v0.BlockHash = common.HexToHash("0xaa").Hex()
	mintReceipt := shared.TransactionReceipt{Logs: []shared.Log{mintLog(t, f.pool, "0x0", int64(tickGoneT), int64(tickKeptT2))}}
	bh := f.svc.BlockHandler()
	if err := bh(ctx, v0, []shared.TransactionReceipt{mintReceipt}); err != nil {
		t.Fatalf("BlockHandler (v0): %v", err)
	}

	// Precondition: both ticks are initialized at (N, v0).
	if bn, ver, gross, init := latestTick(t, ctx, f.db, f.pool.ID, tickGoneT); bn != blockN || ver != 0 || gross != "1000" || !init {
		t.Fatalf("after v0, tick %d latest = (bn=%d ver=%d gross=%s init=%t), want (200,0,1000,true)", tickGoneT, bn, ver, gross, init)
	}

	// v1 redelivery of the SAME block at a higher version, whose receipts do NOT
	// touch the pool (mirrors the #2 reorg re-snapshot scenario). On the new
	// fork tickGoneT is uninitialized (zeroed) and tickKeptT2's value changed.
	f.mc.tickResults[int32(tickGoneT)] = tickResult(t, 0, 0, false)
	f.mc.tickResults[int32(tickKeptT2)] = tickResult(t, 2000, 750, true)

	v1 := blockEvent(blockN)
	v1.Version = 1
	v1.BlockHash = common.HexToHash("0xbb").Hex()
	if err := bh(ctx, v1, nil); err != nil {
		t.Fatalf("BlockHandler (v1 reorg redelivery): %v", err)
	}

	// The stale (N, v0) row for tickGoneT must be superseded by a zeroed,
	// uninitialized (N, v1) row.
	bn, ver, gross, init := latestTick(t, ctx, f.db, f.pool.ID, tickGoneT)
	if bn != blockN || ver != 1 {
		t.Errorf("tick %d latest = (bn=%d ver=%d), want (200,1) — stale orphaned-fork row survived", tickGoneT, bn, ver)
	}
	if gross != "0" || init {
		t.Errorf("tick %d latest = (gross=%s init=%t), want (0,false) — now-uninitialized tick not reconciled", tickGoneT, gross, init)
	}

	// A still-initialized-but-changed tick gets its v1 value.
	bn2, ver2, gross2, init2 := latestTick(t, ctx, f.db, f.pool.ID, tickKeptT2)
	if bn2 != blockN || ver2 != 1 || gross2 != "2000" || !init2 {
		t.Errorf("tick %d latest = (bn=%d ver=%d gross=%s init=%t), want (200,1,2000,true)", tickKeptT2, bn2, ver2, gross2, init2)
	}
}
