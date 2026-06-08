//go:build integration

package uniswap_v3_dex

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

// TestIntegration_SwapWritesAllRows persists a real V3 Swap event end-to-end
// against TimescaleDB and asserts that the protocol_event audit row, the
// uniswap_v3_pool_swap projection, and the uniswap_v3_pool_state snapshot all
// land — with non-null observe([0]) cumulatives on the state row.
func TestIntegration_SwapWritesAllRows(t *testing.T) {
	ctx := context.Background()
	pgPool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	t.Setenv("BUILD_GIT_HASH", "integration-test-swap-writes-all-rows")

	buildReg, err := buildregistry.New(ctx, pgPool)
	if err != nil {
		t.Fatalf("buildregistry.New: %v", err)
	}

	txm, err := postgres.NewTxManager(pgPool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	protoRepo, err := postgres.NewProtocolRepository(pgPool, nil, buildReg.BuildID(), 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pgPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	eventRepo := postgres.NewEventRepository(nil, buildReg.BuildID())
	uniswapRepo, err := postgres.NewUniswapV3PoolRepository(pgPool, nil, buildReg.BuildID())
	if err != nil {
		t.Fatalf("NewUniswapV3PoolRepository: %v", err)
	}

	// Seed: register the test pool in the database so the service registry
	// picks it up at Start.
	pool := makeIntegrationPool(ctx, t, pgPool, txm, tokenRepo)

	mc := testutil.NewMockMulticaller()
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults(), NFPMAddress: DefaultNFPMAddress}
	cfg.ChainID = 1
	svc, err := NewService(cfg, consumer, cache, mc, txm, uniswapRepo, tokenRepo, protoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = svc.Stop() }()
	if svc.registry.poolByAddress(pool.Address) == nil {
		t.Fatalf("expected test pool %s to be registered after Start", pool.Address.Hex())
	}
	// Prime token addresses so we don't need a static read; pool's token0/token1
	// are unknown to the test harness without a chain read.
	svc.registry.setTokenAddresses(pool.ID, integrationToken0, integrationToken1)

	// Canned multicall for the 5-call pool state plus possible static read fallback.
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 5 {
			return nil, nil
		}
		u160, _ := abi.NewType("uint160", "", nil)
		i24, _ := abi.NewType("int24", "", nil)
		u16, _ := abi.NewType("uint16", "", nil)
		u8, _ := abi.NewType("uint8", "", nil)
		bt, _ := abi.NewType("bool", "", nil)
		i56arr, _ := abi.NewType("int56[]", "", nil)
		u160arr, _ := abi.NewType("uint160[]", "", nil)
		u256, _ := abi.NewType("uint256", "", nil)

		slotArgs := abi.Arguments{{Type: u160}, {Type: i24}, {Type: u16}, {Type: u16}, {Type: u16}, {Type: u8}, {Type: bt}}
		slotData, err := slotArgs.Pack(big.NewInt(1e18), big.NewInt(100), uint16(1), uint16(10), uint16(100), uint8(0), true)
		if err != nil {
			return nil, err
		}
		obsArgs := abi.Arguments{{Type: i56arr}, {Type: u160arr}}
		obsData, err := obsArgs.Pack([]*big.Int{big.NewInt(99999)}, []*big.Int{big.NewInt(88888)})
		if err != nil {
			return nil, err
		}
		uintArgs := abi.Arguments{{Type: u256}}
		mk := func(v int64) outbound.Result {
			data, _ := uintArgs.Pack(big.NewInt(v))
			return outbound.Result{Success: true, ReturnData: data}
		}
		return []outbound.Result{
			{Success: true, ReturnData: slotData},
			mk(5000),
			{Success: true, ReturnData: obsData},
			mk(100000),
			mk(200000),
		}, nil
	}

	swapLog := buildSwapLog(t, pool.Address)
	receipts := []shared.TransactionReceipt{{TransactionHash: testTxHash, Logs: []shared.Log{swapLog}}}
	body, _ := json.Marshal(receipts)
	cache.SetReceipts(1, 100, 0, body)

	if err := svc.processBlockEvent(ctx, outbound.BlockEvent{
		ChainID: 1, BlockNumber: 100, Version: 0,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	// Assert: protocol_event, uniswap_v3_pool_swap, uniswap_v3_pool_state rows present.
	var protoCount int
	if err := pgPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_event WHERE event_name = 'Swap'`).Scan(&protoCount); err != nil {
		t.Fatalf("count protocol_event: %v", err)
	}
	if protoCount != 1 {
		t.Errorf("protocol_event Swap rows = %d, want 1", protoCount)
	}

	var swapCount int
	if err := pgPool.QueryRow(ctx, `SELECT COUNT(*) FROM uniswap_v3_pool_swap`).Scan(&swapCount); err != nil {
		t.Fatalf("count uniswap_v3_pool_swap: %v", err)
	}
	if swapCount != 1 {
		t.Errorf("uniswap_v3_pool_swap rows = %d, want 1", swapCount)
	}

	var stateCount int
	var tickCumulative, secsPerLiq *string
	if err := pgPool.QueryRow(ctx, `
		SELECT COUNT(*),
			   MAX(tick_cumulative::TEXT),
			   MAX(secs_per_liquidity_cumulative_x128::TEXT)
		FROM uniswap_v3_pool_state`).Scan(&stateCount, &tickCumulative, &secsPerLiq); err != nil {
		t.Fatalf("count uniswap_v3_pool_state: %v", err)
	}
	if stateCount != 1 {
		t.Errorf("uniswap_v3_pool_state rows = %d, want 1", stateCount)
	}
	if tickCumulative == nil || *tickCumulative != "99999" {
		t.Errorf("tick_cumulative = %v, want 99999", tickCumulative)
	}
	if secsPerLiq == nil || *secsPerLiq != "88888" {
		t.Errorf("secs_per_liquidity_cumulative_x128 = %v, want 88888", secsPerLiq)
	}
}

// -----------------------------------------------------------------------------
// fixtures
// -----------------------------------------------------------------------------

var (
	integrationPoolAddr = common.HexToAddress("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640")
	integrationToken0   = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	integrationToken1   = common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
)

func makeIntegrationPool(ctx context.Context, t *testing.T, pgPool any, txm outbound.TxManager, tokenRepo outbound.TokenRepository) *entity.UniswapV3Pool {
	t.Helper()
	pool := &entity.UniswapV3Pool{
		ChainID:     1,
		Address:     integrationPoolAddr,
		FeeTier:     500,
		TickSpacing: 10,
		Label:       "USDC/WETH 0.05%",
		Enabled:     true,
	}
	// Token registry is needed before we can FK-link the pool row.
	err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		t0, err := tokenRepo.GetOrCreateToken(ctx, tx, 1, integrationToken0, "USDC", 6, 12369621)
		if err != nil {
			return err
		}
		t1, err := tokenRepo.GetOrCreateToken(ctx, tx, 1, integrationToken1, "WETH", 18, 12369621)
		if err != nil {
			return err
		}
		pool.Token0ID = t0
		pool.Token1ID = t1
		// Direct INSERT via the same tx — no port method exposes the pool create
		// (registry rows are written by separate tooling); use a raw insert.
		row := tx.QueryRow(ctx, `
			INSERT INTO uniswap_v3_pool (chain_id, address, token0_id, token1_id, fee_tier, tick_spacing, label, enabled)
			VALUES ($1, $2, $3, $4, $5, $6, $7, true)
			RETURNING id`,
			pool.ChainID, pool.Address.Bytes(), pool.Token0ID, pool.Token1ID,
			pool.FeeTier, pool.TickSpacing, pool.Label)
		return row.Scan(&pool.ID)
	})
	if err != nil {
		t.Fatalf("seed pool: %v", err)
	}
	return pool
}

func buildSwapLog(t *testing.T, poolAddr common.Address) shared.Log {
	t.Helper()
	// Reuse the same approach as service_test.go but standalone for integration.
	poolEv, err := loadV3PoolEventsABI()
	if err != nil {
		t.Fatalf("loadV3PoolEventsABI: %v", err)
	}
	ev := poolEv.Events["Swap"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(-100), big.NewInt(101), big.NewInt(1e18), big.NewInt(5000), big.NewInt(123))
	if err != nil {
		t.Fatalf("pack Swap: %v", err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(common.HexToAddress("0x9999999999999999999999999999999999999999").Bytes()).Hex(),
			common.BytesToHash(common.HexToAddress("0x8888888888888888888888888888888888888888").Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x1",
	}
}

func loadV3PoolEventsABI() (*abi.ABI, error) {
	return abis.GetUniswapV3PoolEventsABI()
}

// TestIntegration_UniswapV3PoolCurrentTrigger verifies the
// uniswap_v3_pool_current projection maintained by
// trigger_refresh_uniswap_v3_pool_current. The trigger restates this table's
// upsert column-by-column by hand, so the test reads the companion back and
// checks the sqrt_price/tick/liquidity columns most prone to a transposition,
// alongside the version-guard ordering (older arrival is a no-op, newer wins).
func TestIntegration_UniswapV3PoolCurrentTrigger(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "integration-test-uniswap-pool-current")
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	var poolID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM uniswap_v3_pool ORDER BY id LIMIT 1`).Scan(&poolID); err != nil {
		t.Fatalf("look up seeded uniswap_v3_pool.id: %v", err)
	}

	insertState := func(bn int64, ts time.Time, sqrtPrice, liquidity int64, tick int32) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO uniswap_v3_pool_state
				(uniswap_v3_pool_id, block_number, block_version, timestamp, source,
				 sqrt_price_x96, tick, liquidity, build_id)
			VALUES ($1, $2, 0, $3, 'event', $4, $5, $6, 1)
			ON CONFLICT (uniswap_v3_pool_id, block_number, block_version, processing_version, timestamp)
				DO NOTHING`,
			poolID, bn, ts, sqrtPrice, tick, liquidity); err != nil {
			t.Fatalf("insert state (bn=%d): %v", bn, err)
		}
	}
	assertCurrent := func(stage string, wantBN, wantSqrt, wantLiq int64, wantTick int32) {
		t.Helper()
		var bn, sqrt, liq int64
		var tick int32
		if err := pool.QueryRow(ctx, `
			SELECT block_number, sqrt_price_x96::bigint, tick, liquidity::bigint
			FROM uniswap_v3_pool_current WHERE uniswap_v3_pool_id = $1`, poolID,
		).Scan(&bn, &sqrt, &tick, &liq); err != nil {
			t.Fatalf("%s: read uniswap_v3_pool_current: %v", stage, err)
		}
		if bn != wantBN || sqrt != wantSqrt || tick != wantTick || liq != wantLiq {
			t.Errorf("%s: current = (bn=%d sqrt=%d tick=%d liq=%d), want (bn=%d sqrt=%d tick=%d liq=%d)",
				stage, bn, sqrt, tick, liq, wantBN, wantSqrt, wantTick, wantLiq)
		}
	}

	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	tEarlier := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	insertState(100, t0, 1000, 2000, 5)
	assertCurrent("first insert", 100, 1000, 2000, 5)

	insertState(99, tEarlier, 999, 1999, 4) // older, out of order
	assertCurrent("older block (no-op)", 100, 1000, 2000, 5)

	insertState(101, t1, 1010, 2020, 6) // newer
	assertCurrent("newer block", 101, 1010, 2020, 6)
}
