//go:build integration

package uniswapv4bootstrap

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
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// integrationPin sits above every seeded pool's deploy block.
const integrationPin = int64(25_600_000)

// v4BootstrapFixture wires the real Postgres UniswapV4Repository and tx manager
// against a freshly migrated schema, faking only the two data sources we cannot
// control: the log-scan RPC and the archive multicall. The registry comes from
// LoadPools over the migration's own seed.
type v4BootstrapFixture struct {
	deps   Deps
	client *fakeLogScanClient
	mc     *testutil.MockMulticaller
	db     *pgxpool.Pool
	pools  []uniswapv4indexer.RegisteredPool
}

func setupBootstrapIntegration(t *testing.T, liquidity int64) *v4BootstrapFixture {
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
	pools := uniswapv4indexer.RegisteredPoolsFromRows(rows)

	txMgr, err := postgres.NewTxManager(db, slog.New(slog.NewTextHandler(os.Stdout, nil)))
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}

	client := newFakeLogScanClient(integrationPin+64, map[int64]*outbound.BlockHeader{
		integrationPin: header(integrationPin, pinHash),
	})
	mc := positionReturningMulticaller(t, liquidity)

	return &v4BootstrapFixture{
		deps: Deps{
			Pools:       pools,
			LogScan:     client,
			Multicaller: mc,
			Repo:        repo,
			TxManager:   txMgr,
			Logger:      testLogger(),
			Config: Config{
				ChainID:  testChainID,
				PinBlock: integrationPin,
				// One window over the whole seeded history keeps the fixture's log
				// set in a single GetLogs answer.
				InitialWindow: 10_000_000,
				MaxWindow:     10_000_000,
				PositionBatch: 2,
			},
		},
		client: client,
		mc:     mc,
		db:     db,
		pools:  pools,
	}
}

// run builds a fresh Service over the fixture's deps and runs it, standing in
// for a second invocation of the binary against the same database.
func (f *v4BootstrapFixture) run(t *testing.T) Summary {
	t.Helper()
	svc, err := New(f.deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	summary, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return summary
}

// poolByHash returns the loaded registry entry for an on-chain PoolId.
func (f *v4BootstrapFixture) poolByHash(t *testing.T, poolIDHash string) uniswapv4indexer.RegisteredPool {
	t.Helper()
	want := common.HexToHash(poolIDHash)
	for _, pool := range f.pools {
		if pool.PoolIDHash == want {
			return pool
		}
	}
	t.Fatalf("pool %s is not in the seeded registry", poolIDHash)
	return uniswapv4indexer.RegisteredPool{}
}

func countRows(t *testing.T, db *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("counting rows (%s): %v", query, err)
	}
	return n
}

// twoPoolLogs answers every window with three ModifyLiquidity logs across two
// seeded pools, one of them a repeated touch of the same position.
func (f *v4BootstrapFixture) twoPoolLogs(t *testing.T) func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
	t.Helper()
	return func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_900_000, 1),
			modifyLiquidityFilteredLog(t, poolBIDHash, ownerB, -60, 60, saltB, 22_990_000, 2),
		}, nil
	}
}

func TestIntegration_DiscoveredKeysBecomePersistedPositionRows(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)
	f.client.GetLogsFn = f.twoPoolLogs(t)

	summary := f.run(t)

	if summary.Keys != 2 {
		t.Errorf("summary.Keys = %d, want 2", summary.Keys)
	}
	if got := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position`); got != 2 {
		t.Fatalf("uniswap_v4_position rows = %d, want 2", got)
	}

	poolA := f.poolByHash(t, poolAIDHash)
	var (
		owner, salt          []byte
		tickLower, tickUpper int32
		blockNumber          int64
		blockVersion         int
		liquidity            string
	)
	err := f.db.QueryRow(context.Background(),
		`SELECT owner, tick_lower, tick_upper, salt, block_number, block_version, liquidity::text
		 FROM uniswap_v4_position WHERE pool_id = $1`, poolA.ID).
		Scan(&owner, &tickLower, &tickUpper, &salt, &blockNumber, &blockVersion, &liquidity)
	if err != nil {
		t.Fatalf("reading back pool %d's position: %v", poolA.ID, err)
	}

	if common.BytesToAddress(owner) != common.HexToAddress(ownerA) {
		t.Errorf("owner = %s, want %s", common.BytesToAddress(owner), ownerA)
	}
	if tickLower != -100 || tickUpper != 200 {
		t.Errorf("tick range = [%d, %d], want [-100, 200]", tickLower, tickUpper)
	}
	if common.BytesToHash(salt) != common.HexToHash(saltA) {
		t.Errorf("salt = %s, want %s", common.BytesToHash(salt), saltA)
	}
	if blockNumber != integrationPin || blockVersion != 0 {
		t.Errorf("block identity = (%d, %d), want (%d, 0)", blockNumber, blockVersion, integrationPin)
	}
	if liquidity != "5000" {
		t.Errorf("liquidity = %s, want 5000", liquidity)
	}
}

func TestIntegration_RerunWritesNoNewRows(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)
	f.client.GetLogsFn = f.twoPoolLogs(t)

	f.run(t)
	after := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position`)

	f.run(t)

	if got := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position`); got != after {
		t.Errorf("rows after the rerun = %d, want %d: an unchanged position must not append", got, after)
	}
	if got := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position WHERE processing_version > 0`); got != 0 {
		t.Errorf("processing_version > 0 rows = %d, want 0: a rerun is not a reprocess", got)
	}
}

func TestIntegration_ChangedStateOnARerunAppendsANewVersion(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)
	f.client.GetLogsFn = f.twoPoolLogs(t)
	f.run(t)

	// A later pin re-reads the same keys and finds a different liquidity, which
	// is exactly the append-on-change case.
	const laterPin = integrationPin + 1000
	f.client.HeaderByNumber[laterPin] = header(laterPin, pinHash)
	f.deps.Config.PinBlock = laterPin
	f.deps.Multicaller = positionReturningMulticaller(t, 9999)

	f.run(t)

	if got := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position`); got != 4 {
		t.Errorf("rows = %d, want 4 (two positions at two heights)", got)
	}
	if got := countRows(t, f.db,
		`SELECT COUNT(*) FROM uniswap_v4_position WHERE block_number = $1 AND liquidity = 9999`, laterPin); got != 2 {
		t.Errorf("rows at the later pin = %d, want 2", got)
	}
}

func TestIntegration_ZeroLiquidityPositionIsPersistedAsAClosedRow(t *testing.T) {
	f := setupBootstrapIntegration(t, 0)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
		}, nil
	}

	f.run(t)

	if got := countRows(t, f.db,
		`SELECT COUNT(*) FROM uniswap_v4_position
		 WHERE liquidity = 0 AND fee_growth_inside0_last_x128 = 0 AND fee_growth_inside1_last_x128 = 0`); got != 1 {
		t.Errorf("all-zero rows = %d, want 1: a burned position reads back zeroed and that erasure is the row", got)
	}
}

func TestIntegration_NoDiscoveredKeysLeavesTheTableEmpty(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)

	summary := f.run(t)

	if summary.Keys != 0 {
		t.Errorf("summary.Keys = %d, want 0", summary.Keys)
	}
	if got := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position`); got != 0 {
		t.Errorf("uniswap_v4_position rows = %d, want 0", got)
	}
}

func TestIntegration_WritesOnlyToTheUniswapV4PositionTable(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)
	f.client.GetLogsFn = f.twoPoolLogs(t)

	f.run(t)

	for _, table := range []string{"uniswap_v4_pool_state", "uniswap_v4_tick", "uniswap_v4_swap", "uniswap_v4_liquidity_event", "uniswap_v4_pool_event"} {
		if got := countRows(t, f.db, `SELECT COUNT(*) FROM `+table); got != 0 {
			t.Errorf("%s rows = %d, want 0: the bootstrap owns positions only", table, got)
		}
	}
}

func TestIntegration_BatchingSplitsOnePoolsKeysAcrossTransactions(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltB, 21_800_001, 1),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerB, -100, 200, saltA, 21_800_002, 2),
		}, nil
	}

	summary := f.run(t)

	if summary.Batches != 2 {
		t.Errorf("batches = %d, want 2 (PositionBatch is 2)", summary.Batches)
	}
	if got := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position`); got != 3 {
		t.Errorf("uniswap_v4_position rows = %d, want 3", got)
	}
}

// TestIntegration_LivePositionRowIsUnaffectedByAnEarlierBootstrapRow pins the
// interaction with the live indexer: the bootstrap writes at a lower height, so
// the newest-row-wins read still returns the live value.
func TestIntegration_BootstrapRowDoesNotSupersedeANewerLiveRow(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)
	poolA := f.poolByHash(t, poolAIDHash)

	// Stand in for the live indexer having already written this position higher up.
	const liveBlock = integrationPin + 5000
	_, err := f.db.Exec(context.Background(),
		`INSERT INTO uniswap_v4_position
		   (pool_id, owner, tick_lower, tick_upper, salt, block_number, block_version, block_timestamp,
		    liquidity, fee_growth_inside0_last_x128, fee_growth_inside1_last_x128, build_id)
		 VALUES ($1,$2,-100,200,$3,$4,0,now(),777,0,0,1)`,
		poolA.ID, common.HexToAddress(ownerA).Bytes(), common.HexToHash(saltA).Bytes(), liveBlock)
	if err != nil {
		t.Fatalf("seeding the live row: %v", err)
	}

	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0)}, nil
	}
	f.run(t)

	var latest string
	err = f.db.QueryRow(context.Background(),
		`SELECT liquidity::text FROM uniswap_v4_position
		 WHERE pool_id = $1
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1`, poolA.ID).Scan(&latest)
	if err != nil {
		t.Fatalf("reading the latest row: %v", err)
	}
	if latest != "777" {
		t.Errorf("latest liquidity = %s, want the live row's 777", latest)
	}
	if got := countRows(t, f.db, `SELECT COUNT(*) FROM uniswap_v4_position WHERE pool_id = $1`, poolA.ID); got != 2 {
		t.Errorf("rows = %d, want 2 (the live row plus the backfilled one)", got)
	}
}

// TestIntegration_BigIntCheckpointsSurviveTheRoundTrip guards the NUMERIC
// conversion on values above int64.
func TestIntegration_LargeFeeGrowthCheckpointsRoundTrip(t *testing.T) {
	f := setupBootstrapIntegration(t, 5000)
	huge, ok := new(big.Int).SetString("340282366920938463463374607431768211455", 10) // 2^128 - 1
	if !ok {
		t.Fatal("parsing the uint128 max")
	}
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: true, ReturnData: packPositionInfoReturn(t, huge, huge, huge)}
		}
		return results, nil
	}
	f.deps.Multicaller = mc
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0)}, nil
	}

	f.run(t)

	var liquidity, fee0 string
	if err := f.db.QueryRow(context.Background(),
		`SELECT liquidity::text, fee_growth_inside0_last_x128::text FROM uniswap_v4_position`).Scan(&liquidity, &fee0); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if liquidity != huge.String() || fee0 != huge.String() {
		t.Errorf("stored (%s, %s), want (%s, %s)", liquidity, fee0, huge, huge)
	}
}
