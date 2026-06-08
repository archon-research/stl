//go:build integration

package curve_dex

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// TestIntegration_SwapWritesAllRows drives one V1 stETH-classic TokenExchange
// log through the full Curve DEX service against a real TimescaleDB schema,
// then asserts the expected protocol_event / curve_pool_swap / curve_pool_state
// / curve_pool_exchange_rate rows landed. The curve_pool registry row is seeded
// by migration 20260521_110000.
func TestIntegration_SwapWritesAllRows(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "integration-test-swap-writes-all-rows")
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry.New: %v", err)
	}

	txm, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	protoRepo, err := postgres.NewProtocolRepository(pool, nil, buildReg.BuildID(), 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	eventRepo := postgres.NewEventRepository(nil, buildReg.BuildID())
	curveRepo, err := postgres.NewCurvePoolRepository(pool, nil, buildReg.BuildID())
	if err != nil {
		t.Fatalf("NewCurvePoolRepository: %v", err)
	}

	mc := testutil.NewMockMulticaller()
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1
	svc, err := NewService(cfg, consumer, cache, mc, txm, curveRepo, tokenRepo, protoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// loadRegistry calls MetaRegistry.get_gauge for each tracked pool without
	// a curve_gauge row. Mock the bootstrap to return the zero address so the
	// startup completes without trying to upsert a gauge.
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: common.LeftPadBytes(common.Address{}.Bytes(), 32)}}, nil
	}

	// Start loads the (seeded) stETH-classic pool into the in-memory registry.
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	// Pre-flight: confirm the registry picked up the seeded pool.
	stETHClassic := common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")
	regPool := svc.registry.poolByAddress(stETHClassic)
	if regPool == nil {
		t.Fatal("seeded stETH-classic pool not present in registry; check migration seed")
	}

	// Canned 2-coin V1 pool state for readPoolState (8 sub-calls).
	uintT, _ := abi.NewType("uint256", "", nil)
	uintArgs := abi.Arguments{{Type: uintT}}
	mk := func(v int64) outbound.Result {
		data, _ := uintArgs.Pack(big.NewInt(v))
		return outbound.Result{Success: true, ReturnData: data}
	}
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			mk(1_000_000), mk(2_000_000), mk(1_000_000_000), mk(100), mk(4_000_000),
			mk(3_000_000), mk(999_999), mk(1_000_001),
		}, nil
	}

	// Build a real V1 TokenExchange log.
	v1, err := abis.GetCurveStableswapV1EventsABI()
	if err != nil {
		t.Fatalf("v1 events ABI: %v", err)
	}
	ev := v1.Events["TokenExchange"]
	buyer := common.HexToAddress("0x9999999999999999999999999999999999999999")
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(0), big.NewInt(50_000), big.NewInt(1), big.NewInt(49_900))
	if err != nil {
		t.Fatalf("pack TokenExchange: %v", err)
	}
	const txHashHex = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	log := shared.Log{
		Address: stETHClassic.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(buyer.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: txHashHex,
		LogIndex:        "0x7",
	}
	receipt := shared.TransactionReceipt{TransactionHash: txHashHex, Logs: []shared.Log{log}}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})

	const blockNumber int64 = 19_500_000
	const blockVersion = 0
	cache.SetReceipts(1, blockNumber, blockVersion, body)

	if err := svc.processBlockEvent(ctx, outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        blockVersion,
		BlockTimestamp: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	// Assertions: one row per fact table.
	type counts struct {
		protoEvents      int
		swaps            int
		states           int
		exchangeRateRows int
	}
	var c counts
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM protocol_event WHERE block_number = $1`, blockNumber).Scan(&c.protoEvents); err != nil {
		t.Fatalf("count protocol_event: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM curve_pool_swap WHERE block_number = $1`, blockNumber).Scan(&c.swaps); err != nil {
		t.Fatalf("count curve_pool_swap: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM curve_pool_state WHERE block_number = $1`, blockNumber).Scan(&c.states); err != nil {
		t.Fatalf("count curve_pool_state: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM curve_pool_exchange_rate WHERE block_number = $1`, blockNumber).Scan(&c.exchangeRateRows); err != nil {
		t.Fatalf("count curve_pool_exchange_rate: %v", err)
	}

	if c.protoEvents != 1 {
		t.Errorf("protocol_event rows = %d, want 1", c.protoEvents)
	}
	if c.swaps != 1 {
		t.Errorf("curve_pool_swap rows = %d, want 1", c.swaps)
	}
	if c.states != 1 {
		t.Errorf("curve_pool_state rows = %d, want 1", c.states)
	}
	// stETH-classic has 2 coins ⇒ N(N-1)=2 directional pairs.
	if c.exchangeRateRows != 2 {
		t.Errorf("curve_pool_exchange_rate rows = %d, want 2 (N=2 ⇒ 2 directional pairs)", c.exchangeRateRows)
	}
}

// TestIntegration_NGSwap_UninitialisedOracleWritesNullElements drives an NG
// TokenExchange through the full service against a real TimescaleDB where
// EVERY oracle sub-call (indexed and no-arg) reverts — the multicall stores
// nil elements and the repository must serialise them as SQL NULL elements
// instead of erroring. Pre-fix this failed with "converting price_oracle:
// element 0 must not be nil", poisoning the SQS message (observed live at
// block 25229189 in the 2026-06-02 E2E run).
func TestIntegration_NGSwap_UninitialisedOracleWritesNullElements(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "integration-test-ng-null-oracle")
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry.New: %v", err)
	}
	txm, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	protoRepo, err := postgres.NewProtocolRepository(pool, nil, buildReg.BuildID(), 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	eventRepo := postgres.NewEventRepository(nil, buildReg.BuildID())
	curveRepo, err := postgres.NewCurvePoolRepository(pool, nil, buildReg.BuildID())
	if err != nil {
		t.Fatalf("NewCurvePoolRepository: %v", err)
	}

	mc := testutil.NewMockMulticaller()
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1
	svc, err := NewService(cfg, consumer, cache, mc, txm, curveRepo, tokenRepo, protoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: common.LeftPadBytes(common.Address{}.Bytes(), 32)}}, nil
	}
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	stETHNG := common.HexToAddress("0x21E27a5E5513D6e65C4f830167390997aA84843a")
	if svc.registry.poolByAddress(stETHNG) == nil {
		t.Fatal("seeded stETH-ng pool not present in registry; check migration seed")
	}

	// 12-call layout for 2-coin NG (balances×2, vp, A, fee, indexed oracle×2,
	// no-arg oracle×2, totalSupply, get_dy×2). All four oracle calls revert.
	uintT, _ := abi.NewType("uint256", "", nil)
	uintArgs := abi.Arguments{{Type: uintT}}
	mk := func(v int64) outbound.Result {
		data, _ := uintArgs.Pack(big.NewInt(v))
		return outbound.Result{Success: true, ReturnData: data}
	}
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			mk(1_000_000), mk(2_000_000), mk(1_000_000_000), mk(100), mk(4_000_000),
			{Success: false}, {Success: false}, // last_price(0) / price_oracle(0)
			{Success: false}, {Success: false}, // last_price() / price_oracle()
			mk(3_000_000), mk(999_999), mk(1_000_001),
		}, nil
	}

	ngEvents, err := abis.GetCurveStableswapNGEventsABI()
	if err != nil {
		t.Fatalf("ng events ABI: %v", err)
	}
	ev := ngEvents.Events["TokenExchange"]
	buyer := common.HexToAddress("0x9999999999999999999999999999999999999999")
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(0), big.NewInt(50_000), big.NewInt(1), big.NewInt(49_900))
	if err != nil {
		t.Fatalf("pack TokenExchange: %v", err)
	}
	const txHashHex = "0xbbbbefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	log := shared.Log{
		Address:         stETHNG.Hex(),
		Topics:          []string{ev.ID.Hex(), common.BytesToHash(buyer.Bytes()).Hex()},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: txHashHex,
		LogIndex:        "0x8",
	}
	receipt := shared.TransactionReceipt{TransactionHash: txHashHex, Logs: []shared.Log{log}}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})

	const blockNumber int64 = 19_600_000
	cache.SetReceipts(1, blockNumber, 0, body)

	if err := svc.processBlockEvent(ctx, outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockTimestamp: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent must not fail when oracle slots are uninitialised: %v", err)
	}

	// The state row must exist with NULL oracle elements.
	var states int
	var oracleElemIsNull, lastPriceElemIsNull bool
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM curve_pool_state WHERE block_number = $1`, blockNumber).Scan(&states); err != nil {
		t.Fatalf("count curve_pool_state: %v", err)
	}
	if states != 1 {
		t.Fatalf("curve_pool_state rows = %d, want 1", states)
	}
	if err := pool.QueryRow(ctx,
		`SELECT price_oracle[1] IS NULL, last_price[1] IS NULL FROM curve_pool_state WHERE block_number = $1`,
		blockNumber).Scan(&oracleElemIsNull, &lastPriceElemIsNull); err != nil {
		t.Fatalf("read oracle elements: %v", err)
	}
	if !oracleElemIsNull || !lastPriceElemIsNull {
		t.Errorf("oracle elements: price_oracle[1] null=%v, last_price[1] null=%v — want both NULL", oracleElemIsNull, lastPriceElemIsNull)
	}
}

// TestIntegration_ProcessingVersionRetryAndReprocessing exercises both halves
// of the ADR-0002 retry/reprocessing contract end-to-end against a real
// TimescaleDB schema:
//
//  1. Same natural key + same build_id → idempotent retry. The trigger reuses
//     the existing processing_version, the INSERT collides on the PK, and the
//     adapter's `ON CONFLICT DO NOTHING` swallows it. Net effect: still one
//     row at v=0. This is the SQS-redelivery / pod-restart case.
//
//  2. Same natural key + DIFFERENT build_id → reprocessing. The trigger sees
//     no row at the new build, assigns MAX(processing_version)+1, and the
//     INSERT lands as a fresh row at v=1. This is the "code shipped a bug
//     fix, rerun the block range" case (ADR-0002, "Retry vs. Reprocessing").
//
// Pre-fix (no ON CONFLICT on the INSERT) this raised 23505 unique_violation
// on the same-build retry, livelocking the SQS handler. The test would fail
// at the second SaveCurvePoolState call. With ON CONFLICT in place both
// halves of the contract hold.
func TestIntegration_ProcessingVersionRetryAndReprocessing(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	// Build registry for the first "deployment" (build_id A).
	t.Setenv("BUILD_GIT_HASH", "integration-test-n4-retry-build-A")
	buildA, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry.New(A): %v", err)
	}
	txm, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	repoA, err := postgres.NewCurvePoolRepository(pool, nil, buildA.BuildID())
	if err != nil {
		t.Fatalf("NewCurvePoolRepository(A): %v", err)
	}

	// Look up the seeded stETH-classic pool id.
	var poolID int64
	stETHClassic := common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")
	if err := pool.QueryRow(ctx,
		`SELECT id FROM curve_pool WHERE chain_id = 1 AND address = $1`,
		stETHClassic.Bytes(),
	).Scan(&poolID); err != nil {
		t.Fatalf("looking up stETH-classic curve_pool.id: %v", err)
	}

	blockNumber := int64(20_000_000)
	blockVersion := int32(0)
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	stateFor := func(repo outbound.CurvePoolRepository) (*entity.CurvePoolState, outbound.CurvePoolRepository) {
		return &entity.CurvePoolState{
			CurvePoolID:  poolID,
			BlockNumber:  blockNumber,
			BlockVersion: blockVersion,
			Timestamp:    ts,
			Source:       entity.CurvePoolStateSourceEvent,
			Balances:     []*big.Int{big.NewInt(1_000_000), big.NewInt(2_000_000)},
			VirtualPrice: big.NewInt(1_000_000_000),
			AFactor:      big.NewInt(100),
			Fee:          big.NewInt(4_000_000),
			TotalSupply:  big.NewInt(123),
		}, repo
	}

	write := func(repo outbound.CurvePoolRepository) error {
		return txm.WithTransaction(ctx, func(tx pgx.Tx) error {
			s, _ := stateFor(repo)
			return repo.SaveCurvePoolState(ctx, tx, s)
		})
	}

	// First write under build A — should land at processing_version = 0.
	if err := write(repoA); err != nil {
		t.Fatalf("first SaveCurvePoolState (build A): %v", err)
	}

	// Same-build retry (build A again). Before the ON CONFLICT fix this would
	// raise 23505 unique_violation. After the fix it is a silent no-op: the
	// trigger reuses processing_version=0, the PK collides, ON CONFLICT
	// swallows. Row count must stay at 1.
	if err := write(repoA); err != nil {
		t.Fatalf("same-build retry (build A) must be a silent no-op, got: %v", err)
	}

	// Switch to build B (simulating a redeployed worker with a different git
	// hash). Same natural key, different build_id → reprocessing path.
	t.Setenv("BUILD_GIT_HASH", "integration-test-n4-retry-build-B")
	buildB, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry.New(B): %v", err)
	}
	if buildA.BuildID() == buildB.BuildID() {
		t.Fatalf("buildA and buildB must differ; got both = %d", buildA.BuildID())
	}
	repoB, err := postgres.NewCurvePoolRepository(pool, nil, buildB.BuildID())
	if err != nil {
		t.Fatalf("NewCurvePoolRepository(B): %v", err)
	}

	// Reprocessing under build B → new row at processing_version = 1.
	if err := write(repoB); err != nil {
		t.Fatalf("reprocessing under build B: %v", err)
	}

	// Same-build retry of build B → silent no-op (still 2 rows total).
	if err := write(repoB); err != nil {
		t.Fatalf("same-build retry (build B) must be a silent no-op, got: %v", err)
	}

	// Assert the final state: exactly 2 rows for the natural key,
	// (processing_version, build_id) = {(0, A), (1, B)}.
	type row struct {
		PV      int32
		BuildID int64
	}
	rows, err := pool.Query(ctx, `
		SELECT processing_version, build_id
		FROM curve_pool_state
		WHERE curve_pool_id = $1 AND block_number = $2 AND block_version = $3 AND timestamp = $4
		ORDER BY processing_version
	`, poolID, blockNumber, blockVersion, ts)
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}
	defer rows.Close()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.PV, &r.BuildID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("row count = %d, want 2 (retries must be silent no-ops, only the build-B reprocessing adds a row)", len(got))
	}
	if got[0].PV != 0 || int64(got[0].BuildID) != int64(buildA.BuildID()) {
		t.Errorf("row[0] = (pv=%d, build=%d), want (0, %d)", got[0].PV, got[0].BuildID, buildA.BuildID())
	}
	if got[1].PV != 1 || int64(got[1].BuildID) != int64(buildB.BuildID()) {
		t.Errorf("row[1] = (pv=%d, build=%d), want (1, %d)", got[1].PV, got[1].BuildID, buildB.BuildID())
	}

	// MAX(processing_version) per natural key — the canonical "latest" read
	// path — must return the build-B row (v = 1).
	var maxV int32
	if err := pool.QueryRow(ctx, `
		SELECT MAX(processing_version)
		FROM curve_pool_state
		WHERE curve_pool_id = $1 AND block_number = $2 AND block_version = $3 AND timestamp = $4
	`, poolID, blockNumber, blockVersion, ts).Scan(&maxV); err != nil {
		t.Fatalf("max(processing_version): %v", err)
	}
	if maxV != 1 {
		t.Errorf("MAX(processing_version) = %d, want 1 (the build-B reprocessing)", maxV)
	}
}

// TestIntegration_CurvePoolCurrentTrigger verifies the curve_pool_current
// last-point projection maintained by trigger_refresh_curve_pool_current. The
// companion row must always equal the head the canonical "latest" query
// (ORDER BY block_number DESC, block_version DESC, processing_version DESC)
// would return, and must be immune to out-of-order arrival / replays.
//
// Raw INSERTs are used (rather than the service) so build_id can be set
// explicitly, which is what drives the BEFORE INSERT trigger to allocate a new
// processing_version — letting us exercise that leg of the version guard
// deterministically.
func TestIntegration_CurvePoolCurrentTrigger(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "integration-test-curve-pool-current")
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	var poolID int64
	stETHClassic := common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")
	if err := pool.QueryRow(ctx,
		`SELECT id FROM curve_pool WHERE chain_id = 1 AND address = $1`,
		stETHClassic.Bytes(),
	).Scan(&poolID); err != nil {
		t.Fatalf("looking up stETH-classic curve_pool.id: %v", err)
	}

	// insertState writes one curve_pool_state row. processing_version is left to
	// the BEFORE INSERT trigger; build_id is explicit so we can force a fresh
	// processing_version for an otherwise-identical natural key.
	insertState := func(bn int64, bv, buildID int32, ts time.Time, virtualPrice int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO curve_pool_state
				(curve_pool_id, block_number, block_version, timestamp, source,
				 balances, virtual_price, build_id)
			VALUES ($1, $2, $3, $4, 'event', '{1,2}', $5, $6)
			ON CONFLICT (curve_pool_id, block_number, block_version, processing_version, timestamp)
				DO NOTHING`,
			poolID, bn, bv, ts, virtualPrice, buildID); err != nil {
			t.Fatalf("insert state (bn=%d bv=%d build=%d): %v", bn, bv, buildID, err)
		}
	}

	// assertCurrent reads curve_pool_current and checks it matches the expected head.
	assertCurrent := func(stage string, wantBN int64, wantBV, wantPV int32, wantVP int64) {
		t.Helper()
		var bn, vp int64
		var bv, pv int32
		if err := pool.QueryRow(ctx, `
			SELECT block_number, block_version, processing_version, virtual_price::bigint
			FROM curve_pool_current WHERE curve_pool_id = $1`, poolID,
		).Scan(&bn, &bv, &pv, &vp); err != nil {
			t.Fatalf("%s: read curve_pool_current: %v", stage, err)
		}
		if bn != wantBN || bv != wantBV || pv != wantPV || vp != wantVP {
			t.Errorf("%s: current = (bn=%d bv=%d pv=%d vp=%d), want (bn=%d bv=%d pv=%d vp=%d)",
				stage, bn, bv, pv, vp, wantBN, wantBV, wantPV, wantVP)
		}
	}

	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	tEarlier := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	// 1. First insert seeds the companion row.
	insertState(100, 0, 1, t0, 1000)
	assertCurrent("first insert", 100, 0, 0, 1000)

	// 2. An older block arriving out of order must NOT regress the head.
	insertState(99, 0, 1, tEarlier, 999)
	assertCurrent("older block (out of order)", 100, 0, 0, 1000)

	// 3. A newer block advances the head.
	insertState(101, 0, 1, t1, 1010)
	assertCurrent("newer block", 101, 0, 0, 1010)

	// 4. A reorg of the same block (higher block_version) wins.
	insertState(101, 1, 1, t1, 1011)
	assertCurrent("reorg block_version bump", 101, 1, 0, 1011)

	// 5. Reprocessing the same (block, block_version) under a new build_id
	//    allocates processing_version = 1, which wins on the third tuple leg.
	insertState(101, 1, 2, t1, 1012)
	assertCurrent("processing_version bump", 101, 1, 1, 1012)

	// Round-trip a representative column from each "section" of the row to catch
	// a wrong-column mapping in the trigger's upsert (which assertCurrent's
	// virtual_price-only check would miss): the array column (balances), the
	// text column (source), build_id, and that an unset nullable column
	// (price_oracle) propagates as NULL rather than empty array.
	var source, balancesText string
	var buildID int32
	var priceOracleNull bool
	if err := pool.QueryRow(ctx, `
		SELECT source, build_id, price_oracle IS NULL, balances::text
		FROM curve_pool_current WHERE curve_pool_id = $1`, poolID,
	).Scan(&source, &buildID, &priceOracleNull, &balancesText); err != nil {
		t.Fatalf("read companion columns: %v", err)
	}
	if source != "event" {
		t.Errorf("source = %q, want event", source)
	}
	if buildID != 2 {
		t.Errorf("build_id = %d, want 2 (last winning insert)", buildID)
	}
	if balancesText != "{1,2}" {
		t.Errorf("balances = %s, want {1,2}", balancesText)
	}
	if !priceOracleNull {
		t.Error("price_oracle should round-trip as NULL (never set), got non-NULL")
	}
}

// TestIntegration_PoolCurrentMirrorsState guards the hand-maintained coupling
// between each *_pool_state hypertable and its *_pool_current companion. The
// companion table + its refresh trigger restate the full column list, with no
// compile-time link, so a future migration that adds a column to a state table
// without also adding it to the companion would silently drop that column from
// the projected head. This test makes that failure loud: every data column in
// the state table (except created_at, which the companion replaces with
// updated_at) must exist in the companion with the same type.
func TestIntegration_PoolCurrentMirrorsState(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "integration-test-pool-current-schema")
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	pairs := []struct{ state, current string }{
		{"curve_pool_state", "curve_pool_current"},
		{"uniswap_v3_pool_state", "uniswap_v3_pool_current"},
		{"balancer_pool_state", "balancer_pool_current"},
	}
	for _, p := range pairs {
		rows, err := pool.Query(ctx, `
			SELECT s.column_name, s.data_type
			FROM information_schema.columns s
			WHERE s.table_schema = current_schema()
			  AND s.table_name = $1
			  AND s.column_name <> 'created_at'
			  AND NOT EXISTS (
			      SELECT 1 FROM information_schema.columns c
			      WHERE c.table_schema = s.table_schema
			        AND c.table_name = $2
			        AND c.column_name = s.column_name
			        AND c.data_type = s.data_type)
			ORDER BY s.column_name`, p.state, p.current)
		if err != nil {
			t.Fatalf("%s drift query: %v", p.state, err)
		}
		var missing []string
		for rows.Next() {
			var name, typ string
			if err := rows.Scan(&name, &typ); err != nil {
				rows.Close()
				t.Fatalf("scan: %v", err)
			}
			missing = append(missing, name+" ("+typ+")")
		}
		rows.Close()
		if len(missing) > 0 {
			t.Errorf("%s has columns missing or type-mismatched in %s: %v\n"+
				"add them to the companion table and its refresh trigger in a new migration",
				p.state, p.current, missing)
		}
	}
}
