//go:build integration

package uniswapv4indexer

import (
	"context"
	"math/big"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// integrationBlock sits above every seeded pool's deploy block, so the
// deploy-gate in DueSet accepts a touch here.
const integrationBlock = int64(25_600_000)

// v4IntegrationFixture wires the real Postgres UniswapV4Repository, event
// repository, and tx manager against a freshly migrated schema, faking only the
// multicaller — the archive RPC is the one data source we cannot control. The
// registry comes from LoadPools over the migration's own seed, so a seed that
// disagreed with its PoolIds would fail the service constructor here.
type v4IntegrationFixture struct {
	svc  *UniswapV4Service
	deps UniswapV4ServiceDeps
	mc   *recordingMulticaller
	db   *pgxpool.Pool
	pool RegisteredPool
}

// restarted builds a second service over the same database with an empty
// snapshot tracker, standing in for a worker process that was replaced between
// the two deliveries of a block.
func (f *v4IntegrationFixture) restarted(t *testing.T) *UniswapV4Service {
	t.Helper()
	svc, err := NewUniswapV4Service(f.deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service (restart): %v", err)
	}
	return svc
}

func setupV4Integration(t *testing.T) *v4IntegrationFixture {
	t.Helper()
	ctx := context.Background()

	db, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	repo := postgres.NewUniswapV4Repository(db, 1)
	rows, err := repo.LoadPools(ctx, testChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("LoadPools returned no pools; the migration seed is missing")
	}
	pools := RegisteredPoolsFromRows(rows)

	wanted := common.HexToHash(wbtcWstethPoolID)
	idx := slices.IndexFunc(pools, func(p RegisteredPool) bool { return p.PoolIDHash == wanted })
	if idx < 0 {
		t.Fatalf("seeded registry has no pool %s", wanted)
	}

	txMgr, err := postgres.NewTxManager(db, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	mc := &recordingMulticaller{
		stateResults: buildStateResults(t, defaultStateFixture()),
		tickResults:  map[int32]outbound.Result{},
	}

	deps := UniswapV4ServiceDeps{
		Pools:       pools,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: dexconsumer.NewProtocolEventWriter(rows[0].ProtocolID, postgres.NewEventRepository(nil, 1)),
		TxManager:   txMgr,
		ChainID:     testChainID,
		Logger:      testLogger(),
	}
	svc, err := NewUniswapV4Service(deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service: %v", err)
	}

	return &v4IntegrationFixture{svc: svc, deps: deps, mc: mc, db: db, pool: pools[idx]}
}

func countRows(t *testing.T, ctx context.Context, db *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("counting rows (%s): %v", query, err)
	}
	return n
}

// TestIntegration_PersistsEveryTableForATouchedBlock drives one block carrying
// a swap, a liquidity change, and a donate through the real repository, and
// checks each of the six tables this block writes to received its row.
func TestIntegration_PersistsEveryTableForATouchedBlock(t *testing.T) {
	ctx := context.Background()
	f := setupV4Integration(t)
	f.mc.tickResults[-100] = goodTickResult(t)
	f.mc.tickResults[200] = goodTickResult(t)

	receipt := shared.TransactionReceipt{Logs: []shared.Log{
		swapLog(t, f.pool, "0x0"),
		modifyLog(t, f.pool, "0x1", -100, 200, 5000),
		donateLog(t, f.pool, "0x2"),
	}}
	event := blockEvent(integrationBlock)
	event.BlockHash = common.HexToHash("0xaa").Hex()

	if err := f.svc.BlockHandler()(ctx, event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	tests := []struct {
		table string
		query string
		want  int
	}{
		{table: "uniswap_v4_pool_state", query: `SELECT count(*) FROM uniswap_v4_pool_state WHERE pool_id = $1`, want: 1},
		{table: "uniswap_v4_swap", query: `SELECT count(*) FROM uniswap_v4_swap WHERE pool_id = $1`, want: 1},
		{table: "uniswap_v4_liquidity_event", query: `SELECT count(*) FROM uniswap_v4_liquidity_event WHERE pool_id = $1`, want: 1},
		{table: "uniswap_v4_pool_event", query: `SELECT count(*) FROM uniswap_v4_pool_event WHERE pool_id = $1`, want: 1},
		{table: "uniswap_v4_tick", query: `SELECT count(*) FROM uniswap_v4_tick WHERE pool_id = $1`, want: 2},
	}
	for _, tt := range tests {
		if got := countRows(t, ctx, f.db, tt.query, f.pool.ID); got != tt.want {
			t.Errorf("%s rows = %d, want %d", tt.table, got, tt.want)
		}
	}

	if got := countRows(t, ctx, f.db, `SELECT count(*) FROM protocol_event WHERE block_number = $1`, integrationBlock); got != 3 {
		t.Errorf("protocol_event rows = %d, want 3 (one per decoded PoolManager log)", got)
	}
}

// tickResultWith packs a getTickInfo return with the given liquidity, so the
// reorg test can distinguish an initialized tick from a cleared (all-zero) one.
func tickResultWith(t *testing.T, liquidityGross, liquidityNet int64) outbound.Result {
	t.Helper()
	return outbound.Result{Success: true, ReturnData: packTickInfoReturn(t, big.NewInt(liquidityGross), big.NewInt(liquidityNet), big.NewInt(0), big.NewInt(0))}
}

// latestTick reads the canonical-latest uniswap_v4_tick row for (pool, tick).
func latestTick(t *testing.T, ctx context.Context, db *pgxpool.Pool, poolID int64, tick int) (blockNumber int64, blockVersion int, liquidityGross string) {
	t.Helper()
	if err := db.QueryRow(ctx,
		`SELECT block_number, block_version, liquidity_gross::text
		 FROM uniswap_v4_tick
		 WHERE pool_id = $1 AND tick = $2
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC
		 LIMIT 1`,
		poolID, tick,
	).Scan(&blockNumber, &blockVersion, &liquidityGross); err != nil {
		t.Fatalf("reading latest tick (pool=%d tick=%d): %v", poolID, tick, err)
	}
	return blockNumber, blockVersion, liquidityGross
}

// TestIntegration_ReorgReconcilesStaleTicks proves the reconciliation against
// the real writer: a tick initialized on an orphaned fork (N, v0) is superseded
// when block N is redelivered at v1 whose receipts do NOT touch the pool.
// Without the prior-version re-read the stale (N, v0) row would stay
// canonical-latest forever.
func TestIntegration_ReorgReconcilesStaleTicks(t *testing.T) {
	ctx := context.Background()
	f := setupV4Integration(t)

	const (
		clearedTick = -100 // initialized on v0, cleared on v1
		changedTick = 200  // initialized on both, value changes on v1
	)

	f.mc.tickResults[clearedTick] = goodTickResult(t)
	f.mc.tickResults[changedTick] = goodTickResult(t)

	bh := f.svc.BlockHandler()
	v0 := blockEvent(integrationBlock)
	v0.BlockHash = common.HexToHash("0xaa").Hex()
	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, f.pool, "0x0", clearedTick, changedTick, 5000)}}
	if err := bh(ctx, v0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (v0): %v", err)
	}
	if bn, ver, gross := latestTick(t, ctx, f.db, f.pool.ID, clearedTick); bn != integrationBlock || ver != 0 || gross != "1000" {
		t.Fatalf("after v0, tick %d latest = (bn=%d ver=%d gross=%s), want (%d, 0, 1000)", clearedTick, bn, ver, gross, integrationBlock)
	}

	f.mc.tickResults[clearedTick] = tickResultWith(t, 0, 0)
	f.mc.tickResults[changedTick] = tickResultWith(t, 2000, 750)

	v1 := blockEvent(integrationBlock)
	v1.Version = 1
	v1.BlockHash = common.HexToHash("0xbb").Hex()
	if err := bh(ctx, v1, nil); err != nil {
		t.Fatalf("BlockHandler (v1 reorg redelivery): %v", err)
	}

	if bn, ver, gross := latestTick(t, ctx, f.db, f.pool.ID, clearedTick); bn != integrationBlock || ver != 1 || gross != "0" {
		t.Errorf("tick %d latest = (bn=%d ver=%d gross=%s), want (%d, 1, 0) — the orphaned-fork row survived", clearedTick, bn, ver, gross, integrationBlock)
	}
	if bn, ver, gross := latestTick(t, ctx, f.db, f.pool.ID, changedTick); bn != integrationBlock || ver != 1 || gross != "2000" {
		t.Errorf("tick %d latest = (bn=%d ver=%d gross=%s), want (%d, 1, 2000)", changedTick, bn, ver, gross, integrationBlock)
	}
}

// TestIntegration_ReorgAfterRestartReconcilesStaleTicks is the same
// reconciliation across a process boundary: the redelivering service has never
// seen block N, so the pools to re-snapshot can only come from the rows already
// persisted at that height.
func TestIntegration_ReorgAfterRestartReconcilesStaleTicks(t *testing.T) {
	ctx := context.Background()
	f := setupV4Integration(t)

	const (
		clearedTick = -100
		changedTick = 200
	)
	f.mc.tickResults[clearedTick] = goodTickResult(t)
	f.mc.tickResults[changedTick] = goodTickResult(t)

	v0 := blockEvent(integrationBlock)
	v0.BlockHash = common.HexToHash("0xaa").Hex()
	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, f.pool, "0x0", clearedTick, changedTick, 5000)}}
	if err := f.svc.BlockHandler()(ctx, v0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (v0): %v", err)
	}

	f.mc.tickResults[clearedTick] = tickResultWith(t, 0, 0)
	f.mc.tickResults[changedTick] = tickResultWith(t, 2000, 750)

	v1 := blockEvent(integrationBlock)
	v1.Version = 1
	v1.BlockHash = common.HexToHash("0xbb").Hex()
	if err := f.restarted(t).BlockHandler()(ctx, v1, nil); err != nil {
		t.Fatalf("BlockHandler (v1 reorg redelivery after a restart): %v", err)
	}

	if bn, ver, gross := latestTick(t, ctx, f.db, f.pool.ID, clearedTick); bn != integrationBlock || ver != 1 || gross != "0" {
		t.Errorf("tick %d latest = (bn=%d ver=%d gross=%s), want (%d, 1, 0) — the orphaned-fork row survived the restart", clearedTick, bn, ver, gross, integrationBlock)
	}
	if n := countRows(t, ctx, f.db,
		`SELECT count(*) FROM uniswap_v4_pool_state WHERE pool_id=$1 AND block_number=$2 AND block_version=1`,
		f.pool.ID, integrationBlock); n != 1 {
		t.Errorf("v1 pool_state rows = %d, want 1 (the restarted service must re-snapshot the pool)", n)
	}
}
