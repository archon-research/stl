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

// restarted builds a second service over the same database, standing in for a
// worker process that was replaced between two deliveries. Its snapshot tracker
// starts empty; its baseline set does not — the constructor re-seeds that from
// the persisted rows, which is exactly the difference these tests exercise.
func (f *v4IntegrationFixture) restarted(t *testing.T) *UniswapV4Service {
	t.Helper()
	svc, err := NewUniswapV4Service(context.Background(), f.deps)
	if err != nil {
		t.Fatalf("NewUniswapV4Service (restart): %v", err)
	}
	return svc
}

// seededDeployBlocks is the Initialize height of every pool the V4 migration
// seeds, transcribed from the verified Initialize-log scan. deploy_block is
// load-bearing (DueSet's gate hard-errors when a touched pool reports a height
// above the block), and this is the only place the migration's OWN seed is read
// back: the postgres package re-seeds the registry itself because sibling tests
// TRUNCATE it away.
var seededDeployBlocks = map[common.Hash]int64{
	common.HexToHash("0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76"): 21743144,
	common.HexToHash("0xbc21dd4a44766fadfd6447f4b222a6185dcc2e6a3b15eb79e0cc637e30e7e97f"): 25199556,
	common.HexToHash("0x056c3c5d8aceeb400b674c27db54e4a90d2f468d786582571ee9394b4c5e3a11"): 25199299,
	common.HexToHash("0x9e0032112d580d8f45a0e356c48148846a3306a991da398dde4f92071e853d09"): 24857024,
	common.HexToHash("0xaea49399167b73015a01e9ca9754c2b438e8aaf42d911468443540eea235735e"): 25494004,
	common.HexToHash("0x58299b9ad89104f189f5efcdf4910615cb9e3296afb0c5a1d1d3befdd1bf7ae4"): 23188451,
	common.HexToHash("0x1d6ebf506eacf0e98a8c4566687380ddf1601192acd9bce29feeaf0c0245ea6f"): 24363425,
	common.HexToHash("0xe7c7bbac1cb017812f5129246ba1ace4aeaadb96fed67cc43d94ac2c6c5048d8"): 23796248,
	common.HexToHash("0xbb78d828ded564d7dfcf041eb1316200e4ec5380dc601c7b4872c0a2727a580e"): 24284325,
	common.HexToHash("0x84a2753546221b6aedf1b96098235f8eb5494b1ddd7d57583d99b2d174cd2103"): 22962297,
	common.HexToHash("0xef3a1d51982c20ee2f125e6d6d1f9d3a10c1e94391b828040943005a1ea27e14"): 22552041,
	common.HexToHash("0x904e8ad11c6f8abb44ea77c507355900e7f9d2907ab0a29353cb1ef0f06b0852"): 23326185,
	common.HexToHash("0xa068c5ab2de0c5fed15f8c187d911915437ed55e6a47d2e42710f9174e6db9a2"): 22240740,
	common.HexToHash("0x4d9cc597ec7d8848af463fca5f4c750279f0d02d2745844c1e9f52a7930cc4d7"): 25492487,
	common.HexToHash("0xe63e32b2ae40601662f760d6bf5d771057324fbd97784fe1d3717069f7b75d45"): 24229945,
	common.HexToHash("0x3b1b1f2e775a6db1664f8e7d59ad568605ea2406312c11aef03146c0cf89d5b9"): 24230047,
	common.HexToHash("0xb54ece65cc2ddd3eaec0ad18657470fb043097220273d87368a062c7d4e59180"): 23153381,
	common.HexToHash("0xa2a5a544a8cbd9c24557b8393fd909360779cf0f48a0b88895a7d9d83ce9d437"): 22268982,
	common.HexToHash("0x2d04d518afae8b57a702a6f679edf49f39593d818f9342cc57b457ea738a7460"): 25036987,
	common.HexToHash("0x51ccd46db78d6988ab156c9b0d023e14b2e848240bc719718e63c4cc5c258bcf"): 22989795,
	common.HexToHash("0x2f5dff74b96e2df0fa8a5695318d59839c3ce5d058b19024fbfe276100b676ff"): 24363921,
}

// assertSeededDeployBlocks checks the loaded registry against seededDeployBlocks
// by PoolId, so a mistyped seed is caught regardless of the order LoadPools
// returns rows in.
func assertSeededDeployBlocks(t *testing.T, pools []RegisteredPool) {
	t.Helper()
	if len(pools) != len(seededDeployBlocks) {
		t.Fatalf("LoadPools returned %d pools, want %d seeded", len(pools), len(seededDeployBlocks))
	}
	for _, pool := range pools {
		want, seeded := seededDeployBlocks[pool.PoolIDHash]
		if !seeded {
			t.Errorf("pool %s is registered but absent from the expected seed", pool.PoolIDHash)
			continue
		}
		if pool.DeployBlock != want {
			t.Errorf("pool %s deploy_block = %d, want %d", pool.PoolIDHash, pool.DeployBlock, want)
		}
	}
}

func setupV4Integration(t *testing.T) *v4IntegrationFixture {
	t.Helper()
	ctx := context.Background()

	db, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
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
	assertSeededDeployBlocks(t, pools)

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
	svc, err := NewUniswapV4Service(context.Background(), deps)
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
	assertPinnedTo(t, f.mc, common.HexToHash(v0.BlockHash))
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
	assertPinnedTo(t, f.mc, common.HexToHash(v1.BlockHash))

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
	assertPinnedTo(t, f.mc, common.HexToHash(v0.BlockHash))

	f.mc.tickResults[clearedTick] = tickResultWith(t, 0, 0)
	f.mc.tickResults[changedTick] = tickResultWith(t, 2000, 750)

	v1 := blockEvent(integrationBlock)
	v1.Version = 1
	v1.BlockHash = common.HexToHash("0xbb").Hex()
	if err := f.restarted(t).BlockHandler()(ctx, v1, nil); err != nil {
		t.Fatalf("BlockHandler (v1 reorg redelivery after a restart): %v", err)
	}
	assertPinnedTo(t, f.mc, common.HexToHash(v1.BlockHash))

	if bn, ver, gross := latestTick(t, ctx, f.db, f.pool.ID, clearedTick); bn != integrationBlock || ver != 1 || gross != "0" {
		t.Errorf("tick %d latest = (bn=%d ver=%d gross=%s), want (%d, 1, 0) — the orphaned-fork row survived the restart", clearedTick, bn, ver, gross, integrationBlock)
	}
	if n := countRows(t, ctx, f.db,
		`SELECT count(*) FROM uniswap_v4_pool_state WHERE pool_id=$1 AND block_number=$2 AND block_version=1`,
		f.pool.ID, integrationBlock); n != 1 {
		t.Errorf("v1 pool_state rows = %d, want 1 (the restarted service must re-snapshot the pool)", n)
	}
}

// TestIntegration_RestartDoesNotRebaselineAnIndexedPool proves the boot seed
// against the real repository: baselineSeen lives in memory, so before it was
// seeded from persisted rows every rollout re-enumerated every pool's tick
// bitmap — seconds of archive reads per pool, inside the block duration the 3s
// p99 alert watches.
func TestIntegration_RestartDoesNotRebaselineAnIndexedPool(t *testing.T) {
	ctx := context.Background()
	f := setupV4Integration(t)
	f.mc.tickResults[-100] = goodTickResult(t)
	f.mc.tickResults[200] = goodTickResult(t)

	first := blockEvent(integrationBlock)
	first.BlockHash = common.HexToHash("0xaa").Hex()
	receipt := shared.TransactionReceipt{Logs: []shared.Log{modifyLog(t, f.pool, "0x0", -100, 200, 5000)}}
	if err := f.svc.BlockHandler()(ctx, first, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (first touch): %v", err)
	}
	if f.mc.baselineCalls == 0 {
		t.Fatal("the first touch issued no getTickBitmap scan; the fixture is not exercising the baseline")
	}
	f.mc.baselineCalls = 0

	next := blockEvent(integrationBlock + 1)
	next.BlockHash = common.HexToHash("0xbb").Hex()
	if err := f.restarted(t).BlockHandler()(ctx, next, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler (after restart): %v", err)
	}
	if f.mc.baselineCalls != 0 {
		t.Errorf("getTickBitmap batches after restart = %d, want 0: the baseline is already persisted", f.mc.baselineCalls)
	}
}
