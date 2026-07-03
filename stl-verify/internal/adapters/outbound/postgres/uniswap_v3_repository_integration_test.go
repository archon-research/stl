//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// testUniswapV3BuildID is the fixed buildID new UniswapV3Repository test
// instances are constructed with, so tests can assert build_id is threaded
// through instead of defaulting to 0.
const testUniswapV3BuildID = buildregistry.BuildID(1)

// newUniswapV3Repo builds a UniswapV3Repository backed by uniswapV3TestPool
// with buildID testUniswapV3BuildID.
func newUniswapV3Repo(t *testing.T) *UniswapV3Repository {
	t.Helper()
	return NewUniswapV3Repository(uniswapV3TestPool, testUniswapV3BuildID)
}

// withUniswapV3Tx runs fn inside a transaction against uniswapV3TestPool,
// committing on success. Rollback is deferred so a t.Fatal mid-fn still
// releases the connection.
func withUniswapV3Tx(t *testing.T, ctx context.Context, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := uniswapV3TestPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// seedUniswapV3TokenPair upserts two token rows for chain 1 and returns their
// ids. Idempotent so parallel/repeated test runs are safe against sibling
// tests that TRUNCATE token/protocol.
func seedUniswapV3TokenPair(t *testing.T, ctx context.Context, addr0, addr1 common.Address, decimals0, decimals1 int, symbol0, symbol1 string) (int64, int64) {
	t.Helper()

	var token0ID, token1ID int64
	if err := uniswapV3TestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, $1, $2, $3)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
		addr0.Bytes(), symbol0, decimals0,
	).Scan(&token0ID); err != nil {
		t.Fatalf("seed token0: %v", err)
	}
	if err := uniswapV3TestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, $1, $2, $3)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
		addr1.Bytes(), symbol1, decimals1,
	).Scan(&token1ID); err != nil {
		t.Fatalf("seed token1: %v", err)
	}
	return token0ID, token1ID
}

// seedUniswapV3Protocol upserts the UniswapV3 protocol row for chain 1 and
// returns its id.
func seedUniswapV3Protocol(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	var protocolID int64
	if err := uniswapV3TestPool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		 VALUES (1, '\x1F98431c8aD98523631AE4a59f267346ea31F984'::bytea, 'UniswapV3', 'dex', 12369621)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&protocolID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}
	return protocolID
}

// seedUniswapV3Pool inserts a pool row with the given address/tokens/fee/tick
// spacing/deploy block and returns its id. deployBlock == nil inserts a NULL.
func seedUniswapV3Pool(
	t *testing.T, ctx context.Context,
	poolAddr common.Address, token0ID, token1ID int64,
	fee, tickSpacing int, deployBlock *int64,
) int64 {
	t.Helper()
	protocolID := seedUniswapV3Protocol(t, ctx)

	var poolID int64
	if err := uniswapV3TestPool.QueryRow(ctx,
		`INSERT INTO uniswap_v3_pool
		    (chain_id, protocol_id, pool_address, token0_id, token1_id,
		     fee, tick_spacing, max_liquidity_per_tick, deploy_block)
		 VALUES (1, $1, $2, $3, $4, $5, $6, 11505743598341114571880798222544994, $7)
		 ON CONFLICT (chain_id, pool_address) DO UPDATE SET
		     token0_id = EXCLUDED.token0_id,
		     token1_id = EXCLUDED.token1_id,
		     fee = EXCLUDED.fee,
		     tick_spacing = EXCLUDED.tick_spacing,
		     deploy_block = EXCLUDED.deploy_block
		 RETURNING id`,
		protocolID, poolAddr.Bytes(), token0ID, token1ID, fee, tickSpacing, deployBlock,
	).Scan(&poolID); err != nil {
		t.Fatalf("seed uniswap_v3_pool: %v", err)
	}
	return poolID
}

// truncateUniswapV3FactTables clears all fact rows so each test starts clean.
func truncateUniswapV3FactTables(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, table := range []string{
		"uniswap_v3_pool_state",
		"uniswap_v3_swap",
		"uniswap_v3_liquidity_event",
		"uniswap_v3_tick",
		"uniswap_v3_pool_event",
	} {
		if _, err := uniswapV3TestPool.Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}

func TestUniswapV3Repository_LoadPools(t *testing.T) {
	ctx := context.Background()

	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	wsteth := common.HexToAddress("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0")
	poolAddr := common.HexToAddress("0x1010101010101010101010101010101010101a")

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx, wsteth, weth, 18, 18, "wstETH", "WETH")
	deployBlock := int64(15384250)
	poolID := seedUniswapV3Pool(t, ctx, poolAddr, token0ID, token1ID, 100, 1, &deployBlock)

	repo := newUniswapV3Repo(t)
	pools, err := repo.LoadPools(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var found *outbound.UniswapV3PoolRow
	for i := range pools {
		if pools[i].ID == poolID {
			found = &pools[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("seeded pool id=%d not found in LoadPools result", poolID)
	}
	if found.Address != poolAddr {
		t.Errorf("Address = %s, want %s", found.Address, poolAddr)
	}
	if found.Token0 != wsteth {
		t.Errorf("Token0 = %s, want %s", found.Token0, wsteth)
	}
	if found.Token1 != weth {
		t.Errorf("Token1 = %s, want %s", found.Token1, weth)
	}
	if found.Token0Decimals != 18 {
		t.Errorf("Token0Decimals = %d, want 18", found.Token0Decimals)
	}
	if found.Token1Decimals != 18 {
		t.Errorf("Token1Decimals = %d, want 18", found.Token1Decimals)
	}
	if found.Fee != 100 {
		t.Errorf("Fee = %d, want 100", found.Fee)
	}
	if found.TickSpacing != 1 {
		t.Errorf("TickSpacing = %d, want 1", found.TickSpacing)
	}
	if found.DeployBlock != deployBlock {
		t.Errorf("DeployBlock = %d, want %d", found.DeployBlock, deployBlock)
	}
	if found.ProtocolID == 0 {
		t.Errorf("ProtocolID = 0, want non-zero")
	}
}

// TestUniswapV3Repository_LoadPools_WstethAsToken1 verifies that LoadPools
// preserves pool orientation (does not "normalize" wstETH to token0) for
// pools where the live on-chain ordering makes wstETH token1.
func TestUniswapV3Repository_LoadPools_WstethAsToken1(t *testing.T) {
	ctx := context.Background()

	wsteth := common.HexToAddress("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0")
	mstETH := common.HexToAddress("0x49446A0874197839D15395B908328a74ccc96Bc0")
	poolAddr := common.HexToAddress("0x1010101010101010101010101010101010101b")

	// mstETH is token0, wstETH is token1 -- mirrors pool #14 in the real seed
	// (VEC-261 design spec), which the migration deliberately does not reorder.
	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx, mstETH, wsteth, 18, 18, "mstETH", "wstETH")
	deployBlock := int64(20011699)
	poolID := seedUniswapV3Pool(t, ctx, poolAddr, token0ID, token1ID, 500, 10, &deployBlock)

	repo := newUniswapV3Repo(t)
	pools, err := repo.LoadPools(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var found *outbound.UniswapV3PoolRow
	for i := range pools {
		if pools[i].ID == poolID {
			found = &pools[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("seeded pool id=%d not found in LoadPools result", poolID)
	}
	if found.Token0 != mstETH {
		t.Errorf("Token0 = %s, want %s (mstETH must stay token0)", found.Token0, mstETH)
	}
	if found.Token1 != wsteth {
		t.Errorf("Token1 = %s, want %s (wstETH must stay token1, not be normalized to token0)", found.Token1, wsteth)
	}
}

func TestUniswapV3Repository_LoadPools_NullDeployBlock(t *testing.T) {
	ctx := context.Background()

	token0Addr := common.HexToAddress("0x2020202020202020202020202020202020202a")
	token1Addr := common.HexToAddress("0x2020202020202020202020202020202020202b")
	poolAddr := common.HexToAddress("0x1010101010101010101010101010101010101c")

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx, token0Addr, token1Addr, 18, 6, "TOKX", "TOKY")
	poolID := seedUniswapV3Pool(t, ctx, poolAddr, token0ID, token1ID, 3000, 60, nil)

	repo := newUniswapV3Repo(t)
	if _, err := repo.LoadPools(ctx, 1); err == nil {
		t.Fatalf("LoadPools with NULL deploy_block on pool id=%d: want error, got nil", poolID)
	}
}

func TestUniswapV3Repository_SaveBlock_State_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x3030303030303030303030303030303030303a"),
		common.HexToAddress("0x3030303030303030303030303030303030303b"),
		18, 6, "STOKA", "STOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x4040404040404040404040404040404040404a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	twapTick := 5
	twapWindow := 1800
	st := &entity.UniswapV3PoolState{
		PoolID:                     poolID,
		BlockNumber:                18000000,
		BlockVersion:               0,
		BlockTimestamp:             time.Unix(1700000000, 0).UTC(),
		SqrtPriceX96:               bigFromString(t, "79228162514264337593543950336"),
		Tick:                       10,
		ObservationIndex:           1,
		ObservationCardinality:     2,
		ObservationCardinalityNext: 3,
		FeeProtocol:                4,
		Unlocked:                   true,
		Liquidity:                  big.NewInt(1_000_000_000_000_000_000),
		FeeGrowthGlobal0X128:       big.NewInt(11),
		FeeGrowthGlobal1X128:       big.NewInt(22),
		ProtocolFeesToken0:         big.NewInt(33),
		ProtocolFeesToken1:         big.NewInt(44),
		Balance0:                   big.NewInt(55),
		Balance1:                   big.NewInt(66),
		TwapTick:                   &twapTick,
		TwapWindowSecs:             &twapWindow,
	}
	if err := st.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	repo := newUniswapV3Repo(t)
	var stateRows int64
	withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
		var err error
		stateRows, err = repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{States: []*entity.UniswapV3PoolState{st}})
		if err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})
	if stateRows != 1 {
		t.Errorf("stateRows = %d, want 1", stateRows)
	}

	var (
		gotSqrtPrice, gotLiquidity, gotFeeGrowth0, gotFeeGrowth1 string
		gotProtoFee0, gotProtoFee1, gotBalance0, gotBalance1     string
		gotTick, gotObsIndex, gotObsCard, gotObsCardNext         int
		gotFeeProtocol                                           int
		gotUnlocked                                              bool
		gotTwapTick, gotTwapWindow                               *int
		gotBuildID                                               int
	)
	if err := uniswapV3TestPool.QueryRow(ctx,
		`SELECT sqrt_price_x96::text, tick, observation_index, observation_cardinality,
		        observation_cardinality_next, fee_protocol, unlocked, liquidity::text,
		        fee_growth_global0_x128::text, fee_growth_global1_x128::text,
		        protocol_fees_token0::text, protocol_fees_token1::text,
		        balance0::text, balance1::text, twap_tick, twap_window_secs, build_id
		 FROM uniswap_v3_pool_state WHERE pool_id=$1 AND block_number=18000000`,
		poolID,
	).Scan(&gotSqrtPrice, &gotTick, &gotObsIndex, &gotObsCard, &gotObsCardNext, &gotFeeProtocol, &gotUnlocked,
		&gotLiquidity, &gotFeeGrowth0, &gotFeeGrowth1, &gotProtoFee0, &gotProtoFee1, &gotBalance0, &gotBalance1,
		&gotTwapTick, &gotTwapWindow, &gotBuildID,
	); err != nil {
		t.Fatalf("read back state: %v", err)
	}
	if gotBuildID != int(testUniswapV3BuildID) {
		t.Errorf("build_id = %d, want %d (threaded from repository constructor, not defaulted)", gotBuildID, testUniswapV3BuildID)
	}

	if gotSqrtPrice != "79228162514264337593543950336" {
		t.Errorf("sqrt_price_x96 = %q, want 79228162514264337593543950336", gotSqrtPrice)
	}
	if gotTick != 10 {
		t.Errorf("tick = %d, want 10", gotTick)
	}
	if gotObsIndex != 1 || gotObsCard != 2 || gotObsCardNext != 3 {
		t.Errorf("observation fields = (%d,%d,%d), want (1,2,3)", gotObsIndex, gotObsCard, gotObsCardNext)
	}
	if gotFeeProtocol != 4 {
		t.Errorf("fee_protocol = %d, want 4", gotFeeProtocol)
	}
	if !gotUnlocked {
		t.Error("unlocked = false, want true")
	}
	if gotLiquidity != "1000000000000000000" {
		t.Errorf("liquidity = %q, want 1000000000000000000", gotLiquidity)
	}
	if gotFeeGrowth0 != "11" || gotFeeGrowth1 != "22" {
		t.Errorf("fee_growth = (%q,%q), want (11,22)", gotFeeGrowth0, gotFeeGrowth1)
	}
	if gotProtoFee0 != "33" || gotProtoFee1 != "44" {
		t.Errorf("protocol_fees = (%q,%q), want (33,44)", gotProtoFee0, gotProtoFee1)
	}
	if gotBalance0 != "55" || gotBalance1 != "66" {
		t.Errorf("balances = (%q,%q), want (55,66)", gotBalance0, gotBalance1)
	}
	if gotTwapTick == nil || *gotTwapTick != 5 {
		t.Errorf("twap_tick = %v, want 5", gotTwapTick)
	}
	if gotTwapWindow == nil || *gotTwapWindow != 1800 {
		t.Errorf("twap_window_secs = %v, want 1800", gotTwapWindow)
	}
}

// TestUniswapV3Repository_SaveBlock_State_Idempotent verifies redelivering the
// same state row within a new transaction yields 0 additional rows and the
// table still has exactly one row (ON CONFLICT DO NOTHING via pv trigger reuse).
func TestUniswapV3Repository_SaveBlock_State_Idempotent(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x3131313131313131313131313131313131313a"),
		common.HexToAddress("0x3131313131313131313131313131313131313b"),
		18, 18, "ITOKA", "ITOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x4141414141414141414141414141414141414a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	st := &entity.UniswapV3PoolState{
		PoolID:               poolID,
		BlockNumber:          18000010,
		BlockVersion:         0,
		BlockTimestamp:       time.Unix(1700000100, 0).UTC(),
		SqrtPriceX96:         big.NewInt(1),
		Tick:                 1,
		Liquidity:            big.NewInt(1),
		FeeGrowthGlobal0X128: big.NewInt(1),
		FeeGrowthGlobal1X128: big.NewInt(1),
		ProtocolFeesToken0:   big.NewInt(1),
		ProtocolFeesToken1:   big.NewInt(1),
		Balance0:             big.NewInt(1),
		Balance1:             big.NewInt(1),
	}
	if err := st.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	repo := newUniswapV3Repo(t)
	save := func() int64 {
		var n int64
		withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
			var err error
			n, err = repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{States: []*entity.UniswapV3PoolState{st}})
			if err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
		return n
	}

	if n := save(); n != 1 {
		t.Errorf("first save stateRows = %d, want 1", n)
	}
	if n := save(); n != 0 {
		t.Errorf("redelivery stateRows = %d, want 0", n)
	}

	var count int
	if err := uniswapV3TestPool.QueryRow(ctx,
		`SELECT count(*) FROM uniswap_v3_pool_state WHERE pool_id=$1 AND block_number=18000010`,
		poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1 (idempotent)", count)
	}
}

func TestUniswapV3Repository_SaveBlock_Swap_RoundTrip_SignedAmounts(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x5050505050505050505050505050505050505a"),
		common.HexToAddress("0x5050505050505050505050505050505050505b"),
		18, 18, "SWTOKA", "SWTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x6060606060606060606060606060606060606a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	negAmount0, ok := new(big.Int).SetString("-1000000000000000000", 10)
	if !ok {
		t.Fatal("parsing negAmount0")
	}
	posAmount1, ok := new(big.Int).SetString("990000000000000000", 10)
	if !ok {
		t.Fatal("parsing posAmount1")
	}

	swap := &entity.UniswapV3Swap{
		PoolID:         poolID,
		BlockNumber:    18000020,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1700000200, 0).UTC(),
		TxHash:         common.HexToHash("0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"),
		LogIndex:       0,
		Sender:         common.HexToAddress("0x7070707070707070707070707070707070707a"),
		Recipient:      common.HexToAddress("0x7070707070707070707070707070707070707b"),
		Amount0:        negAmount0,
		Amount1:        posAmount1,
		SqrtPriceX96:   bigFromString(t, "79228162514264337593543950336"),
		Liquidity:      big.NewInt(123456789),
		Tick:           -5,
	}
	if err := swap.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	repo := newUniswapV3Repo(t)
	withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{Swaps: []*entity.UniswapV3Swap{swap}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	var gotAmount0, gotAmount1, gotSqrtPrice, gotLiquidity string
	var gotTick int
	var gotSender, gotRecipient []byte
	if err := uniswapV3TestPool.QueryRow(ctx,
		`SELECT amount0::text, amount1::text, sqrt_price_x96::text, liquidity::text, tick, sender, recipient
		 FROM uniswap_v3_swap WHERE pool_id=$1 AND block_number=18000020`,
		poolID,
	).Scan(&gotAmount0, &gotAmount1, &gotSqrtPrice, &gotLiquidity, &gotTick, &gotSender, &gotRecipient); err != nil {
		t.Fatalf("read back swap: %v", err)
	}

	if gotAmount0 != "-1000000000000000000" {
		t.Errorf("amount0 = %q, want -1000000000000000000 (sign must survive NUMERIC round-trip)", gotAmount0)
	}
	if gotAmount1 != "990000000000000000" {
		t.Errorf("amount1 = %q, want 990000000000000000", gotAmount1)
	}
	if gotSqrtPrice != "79228162514264337593543950336" {
		t.Errorf("sqrt_price_x96 = %q, want 79228162514264337593543950336", gotSqrtPrice)
	}
	if gotLiquidity != "123456789" {
		t.Errorf("liquidity = %q, want 123456789", gotLiquidity)
	}
	if gotTick != -5 {
		t.Errorf("tick = %d, want -5", gotTick)
	}
	if common.BytesToAddress(gotSender) != swap.Sender {
		t.Errorf("sender = %s, want %s", common.BytesToAddress(gotSender), swap.Sender)
	}
	if common.BytesToAddress(gotRecipient) != swap.Recipient {
		t.Errorf("recipient = %s, want %s", common.BytesToAddress(gotRecipient), swap.Recipient)
	}
}

// TestUniswapV3Repository_SaveBlock_LiquidityEvent_Kinds round-trips one
// event of each LiquidityEventKind (mint, burn, collect) and asserts the
// kind-specific field shape (sender-only on mint, recipient-only on collect,
// no amount on collect) survives.
func TestUniswapV3Repository_SaveBlock_LiquidityEvent_Kinds(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x8080808080808080808080808080808080808a"),
		common.HexToAddress("0x8080808080808080808080808080808080808b"),
		18, 18, "LTOKA", "LTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x9090909090909090909090909090909090909a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	owner := common.HexToAddress("0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0")
	sender := common.HexToAddress("0xb0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0")
	recipient := common.HexToAddress("0xc0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0")

	events := []*entity.UniswapV3LiquidityEvent{
		{
			PoolID: poolID, BlockNumber: 18000030, BlockVersion: 0,
			BlockTimestamp: time.Unix(1700000300, 0).UTC(),
			TxHash:         common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111a"),
			LogIndex:       0, EventName: entity.LiquidityEventMint, Owner: owner, Sender: &sender,
			TickLower: -100, TickUpper: 100, Amount: big.NewInt(500), Amount0: big.NewInt(10), Amount1: big.NewInt(20),
		},
		{
			PoolID: poolID, BlockNumber: 18000031, BlockVersion: 0,
			BlockTimestamp: time.Unix(1700000301, 0).UTC(),
			TxHash:         common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222b"),
			LogIndex:       0, EventName: entity.LiquidityEventBurn, Owner: owner,
			TickLower: -100, TickUpper: 100, Amount: big.NewInt(500), Amount0: big.NewInt(10), Amount1: big.NewInt(20),
		},
		{
			PoolID: poolID, BlockNumber: 18000032, BlockVersion: 0,
			BlockTimestamp: time.Unix(1700000302, 0).UTC(),
			TxHash:         common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333c"),
			LogIndex:       0, EventName: entity.LiquidityEventCollect, Owner: owner, Recipient: &recipient,
			TickLower: -100, TickUpper: 100, Amount0: big.NewInt(10), Amount1: big.NewInt(20),
		},
	}
	for _, e := range events {
		if err := e.Validate(); err != nil {
			t.Fatalf("Validate %s: %v", e.EventName, err)
		}
	}

	repo := newUniswapV3Repo(t)
	withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{LiquidityEvents: events}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	for _, want := range events {
		var gotEventName string
		var gotSender, gotRecipient *[]byte
		var gotAmount *string
		if err := uniswapV3TestPool.QueryRow(ctx,
			`SELECT event_name, sender, recipient, amount::text
			 FROM uniswap_v3_liquidity_event WHERE pool_id=$1 AND block_number=$2`,
			poolID, want.BlockNumber,
		).Scan(&gotEventName, &gotSender, &gotRecipient, &gotAmount); err != nil {
			t.Fatalf("read back %s: %v", want.EventName, err)
		}
		if gotEventName != string(want.EventName) {
			t.Errorf("event_name = %q, want %q", gotEventName, want.EventName)
		}

		switch want.EventName {
		case entity.LiquidityEventMint:
			if gotSender == nil || common.BytesToAddress(*gotSender) != sender {
				t.Errorf("mint sender = %v, want %s", gotSender, sender)
			}
			if gotRecipient != nil {
				t.Errorf("mint recipient = %v, want nil", gotRecipient)
			}
			if gotAmount == nil || *gotAmount != "500" {
				t.Errorf("mint amount = %v, want 500", gotAmount)
			}
		case entity.LiquidityEventBurn:
			if gotSender != nil {
				t.Errorf("burn sender = %v, want nil", gotSender)
			}
			if gotRecipient != nil {
				t.Errorf("burn recipient = %v, want nil", gotRecipient)
			}
			if gotAmount == nil || *gotAmount != "500" {
				t.Errorf("burn amount = %v, want 500", gotAmount)
			}
		case entity.LiquidityEventCollect:
			if gotSender != nil {
				t.Errorf("collect sender = %v, want nil", gotSender)
			}
			if gotRecipient == nil || common.BytesToAddress(*gotRecipient) != recipient {
				t.Errorf("collect recipient = %v, want %s", gotRecipient, recipient)
			}
			if gotAmount != nil {
				t.Errorf("collect amount = %v, want nil", gotAmount)
			}
		}
	}
}

func TestUniswapV3Repository_SaveBlock_PoolEvent_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0xd0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0"),
		common.HexToAddress("0xd1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1"),
		18, 18, "PTOKA", "PTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0xe0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	params, err := json.Marshal(map[string]any{"sqrtPriceX96": "79228162514264337593543950336", "tick": 0})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	ev := &entity.UniswapV3PoolEvent{
		PoolID: poolID, BlockNumber: 18000040, BlockVersion: 0,
		BlockTimestamp: time.Unix(1700000400, 0).UTC(),
		TxHash:         common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444d"),
		LogIndex:       0, EventName: entity.PoolEventInitialize, Params: params,
	}
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	repo := newUniswapV3Repo(t)
	withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{PoolEvents: []*entity.UniswapV3PoolEvent{ev}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	var gotEventName string
	var gotParams []byte
	if err := uniswapV3TestPool.QueryRow(ctx,
		`SELECT event_name, params FROM uniswap_v3_pool_event WHERE pool_id=$1 AND block_number=18000040`,
		poolID,
	).Scan(&gotEventName, &gotParams); err != nil {
		t.Fatalf("read back pool event: %v", err)
	}
	if gotEventName != string(entity.PoolEventInitialize) {
		t.Errorf("event_name = %q, want %q", gotEventName, entity.PoolEventInitialize)
	}
	var gotParamsMap map[string]any
	if err := json.Unmarshal(gotParams, &gotParamsMap); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if gotParamsMap["tick"] != float64(0) {
		t.Errorf("params.tick = %v, want 0", gotParamsMap["tick"])
	}
	if gotParamsMap["sqrtPriceX96"] != "79228162514264337593543950336" {
		t.Errorf("params.sqrtPriceX96 = %v, want 79228162514264337593543950336", gotParamsMap["sqrtPriceX96"])
	}
}

func newUniswapV3TestTick(poolID int64, tick int, blockNumber int64, blockVersion int, liquidityNet *big.Int) *entity.UniswapV3Tick {
	return &entity.UniswapV3Tick{
		PoolID:                poolID,
		Tick:                  tick,
		BlockNumber:           blockNumber,
		BlockVersion:          blockVersion,
		BlockTimestamp:        time.Unix(1700000500+blockNumber, 0).UTC(),
		LiquidityGross:        big.NewInt(1000),
		LiquidityNet:          liquidityNet,
		FeeGrowthOutside0X128: big.NewInt(1),
		FeeGrowthOutside1X128: big.NewInt(2),
		Initialized:           true,
	}
}

// TestUniswapV3Repository_SaveBlock_Tick_AppendOnChange exercises the four
// append-on-change scenarios the tick table must satisfy: identical values
// insert once, a changed field inserts a new row, replaying an already-seen
// row is a no-op, and a different block_version (reorg) always inserts.
func TestUniswapV3Repository_SaveBlock_Tick_AppendOnChange(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0"),
		common.HexToAddress("0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1"),
		18, 18, "TTOKA", "TTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x1212121212121212121212121212121212121a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	repo := newUniswapV3Repo(t)
	saveTick := func(tick *entity.UniswapV3Tick) {
		withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{Ticks: []*entity.UniswapV3Tick{tick}}); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
	}
	countTicks := func(tick int) int {
		var count int
		if err := uniswapV3TestPool.QueryRow(ctx,
			`SELECT count(*) FROM uniswap_v3_tick WHERE pool_id=$1 AND tick=$2`,
			poolID, tick,
		).Scan(&count); err != nil {
			t.Fatalf("count ticks: %v", err)
		}
		return count
	}

	const testTick = 42

	t.Run("same_values_same_block_inserts_once", func(t *testing.T) {
		tk := newUniswapV3TestTick(poolID, testTick, 1000, 0, big.NewInt(100))
		saveTick(tk)
		if got := countTicks(testTick); got != 1 {
			t.Fatalf("row count = %d, want 1", got)
		}
	})

	t.Run("replay_identical_row_is_idempotent", func(t *testing.T) {
		tk := newUniswapV3TestTick(poolID, testTick, 1000, 0, big.NewInt(100))
		saveTick(tk)
		if got := countTicks(testTick); got != 1 {
			t.Fatalf("row count after replay = %d, want 1 (no new row for identical values)", got)
		}
	})

	t.Run("changed_field_inserts_new_row", func(t *testing.T) {
		tk := newUniswapV3TestTick(poolID, testTick, 1001, 0, big.NewInt(200))
		saveTick(tk)
		if got := countTicks(testTick); got != 2 {
			t.Fatalf("row count after change = %d, want 2", got)
		}

		var latestLiquidityNet string
		if err := uniswapV3TestPool.QueryRow(ctx,
			`SELECT liquidity_net::text FROM uniswap_v3_tick
			 WHERE pool_id=$1 AND tick=$2
			 ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1`,
			poolID, testTick,
		).Scan(&latestLiquidityNet); err != nil {
			t.Fatalf("querying latest tick: %v", err)
		}
		if latestLiquidityNet != "200" {
			t.Errorf("latest liquidity_net = %q, want 200", latestLiquidityNet)
		}
	})

	t.Run("different_block_version_reorg_inserts_new_row", func(t *testing.T) {
		// Same values as the current latest, but a bumped block_version (reorg):
		// must insert unconditionally, not be treated as "unchanged".
		tk := newUniswapV3TestTick(poolID, testTick, 1001, 1, big.NewInt(200))
		saveTick(tk)
		if got := countTicks(testTick); got != 3 {
			t.Fatalf("row count after reorg = %d, want 3", got)
		}
	})
}

// TestUniswapV3Repository_SaveBlock_Ticks_BatchAppendOnChange verifies the
// batched writeTicks path preserves per-tick append-on-change across a block
// that writes MANY ticks at once: a prior block seeds three ticks, then a
// later block re-writes all three in one SaveBlock where one tick is unchanged,
// one is changed, and a fourth is new. Exactly the changed + new ticks must get
// a second/first row; the unchanged tick must NOT (its single row is preserved
// despite being batched alongside inserting siblings).
func TestUniswapV3Repository_SaveBlock_Ticks_BatchAppendOnChange(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x1717171717171717171717171717171717171a"),
		common.HexToAddress("0x1717171717171717171717171717171717171b"),
		18, 18, "BTOKA", "BTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x1818181818181818181818181818181818181a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	const (
		tickUnchangedPos = 10 // seeded, re-written identical -> stays 1 row
		tickChangedPos   = 20 // seeded, re-written with new value -> 2 rows
		tickNewPos       = 30 // not seeded, first appears in batch -> 1 row
	)

	repo := newUniswapV3Repo(t)
	saveBatch := func(ticks []*entity.UniswapV3Tick) {
		withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{Ticks: ticks}); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
	}
	countTicks := func(tick int) int {
		var count int
		if err := uniswapV3TestPool.QueryRow(ctx,
			`SELECT count(*) FROM uniswap_v3_tick WHERE pool_id=$1 AND tick=$2`,
			poolID, tick,
		).Scan(&count); err != nil {
			t.Fatalf("count ticks: %v", err)
		}
		return count
	}

	// Prior block: seed the two ticks that will be re-observed later.
	saveBatch([]*entity.UniswapV3Tick{
		newUniswapV3TestTick(poolID, tickUnchangedPos, 3000, 0, big.NewInt(100)),
		newUniswapV3TestTick(poolID, tickChangedPos, 3000, 0, big.NewInt(200)),
	})

	// Later block: all three re-written in ONE batch. The unchanged tick keeps
	// its exact prior values; the changed tick bumps liquidity_net; the new tick
	// has never been seen. All at a strictly later block_number, same version.
	saveBatch([]*entity.UniswapV3Tick{
		newUniswapV3TestTick(poolID, tickUnchangedPos, 3001, 0, big.NewInt(100)),
		newUniswapV3TestTick(poolID, tickChangedPos, 3001, 0, big.NewInt(999)),
		newUniswapV3TestTick(poolID, tickNewPos, 3001, 0, big.NewInt(300)),
	})

	if got := countTicks(tickUnchangedPos); got != 1 {
		t.Errorf("unchanged tick row count = %d, want 1 (no new row despite batched-in siblings inserting)", got)
	}
	if got := countTicks(tickChangedPos); got != 2 {
		t.Errorf("changed tick row count = %d, want 2 (changed field must append a new row)", got)
	}
	if got := countTicks(tickNewPos); got != 1 {
		t.Errorf("new tick row count = %d, want 1 (first observation must insert)", got)
	}

	// The changed tick's latest row must reflect the new value, not the seed.
	var latestChangedNet string
	if err := uniswapV3TestPool.QueryRow(ctx,
		`SELECT liquidity_net::text FROM uniswap_v3_tick
		 WHERE pool_id=$1 AND tick=$2
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1`,
		poolID, tickChangedPos,
	).Scan(&latestChangedNet); err != nil {
		t.Fatalf("querying latest changed tick: %v", err)
	}
	if latestChangedNet != "999" {
		t.Errorf("latest changed liquidity_net = %q, want 999", latestChangedNet)
	}

	// The unchanged tick's single row must still carry its original block_number
	// (3000), proving the batch skipped it rather than re-inserting at 3001.
	var unchangedBlock int64
	if err := uniswapV3TestPool.QueryRow(ctx,
		`SELECT block_number FROM uniswap_v3_tick WHERE pool_id=$1 AND tick=$2`,
		poolID, tickUnchangedPos,
	).Scan(&unchangedBlock); err != nil {
		t.Fatalf("querying unchanged tick block: %v", err)
	}
	if unchangedBlock != 3000 {
		t.Errorf("unchanged tick block_number = %d, want 3000 (row must be the untouched seed, not a 3001 re-insert)", unchangedBlock)
	}
}

// TestUniswapV3Repository_SaveBlock_Ticks_BatchReorgAllInsert verifies that
// when a whole batch of ticks is re-observed at a bumped block_version (a
// reorg) with values IDENTICAL to the prior version, every tick still inserts
// a new row — the batched path must honor the "different block_version always
// inserts" rule for all keys, not collapse them as unchanged.
func TestUniswapV3Repository_SaveBlock_Ticks_BatchReorgAllInsert(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x1919191919191919191919191919191919191a"),
		common.HexToAddress("0x1919191919191919191919191919191919191b"),
		18, 18, "RTOKA", "RTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	ticksAtVersion := func(blockNumber int64, version int) []*entity.UniswapV3Tick {
		return []*entity.UniswapV3Tick{
			newUniswapV3TestTick(poolID, 40, blockNumber, version, big.NewInt(100)),
			newUniswapV3TestTick(poolID, 50, blockNumber, version, big.NewInt(200)),
		}
	}

	repo := newUniswapV3Repo(t)
	saveBatch := func(ticks []*entity.UniswapV3Tick) {
		withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{Ticks: ticks}); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
	}

	saveBatch(ticksAtVersion(4000, 0))
	saveBatch(ticksAtVersion(4000, 1)) // same values, bumped version -> must insert

	for _, tick := range []int{40, 50} {
		var count int
		if err := uniswapV3TestPool.QueryRow(ctx,
			`SELECT count(*) FROM uniswap_v3_tick WHERE pool_id=$1 AND tick=$2`,
			poolID, tick,
		).Scan(&count); err != nil {
			t.Fatalf("count tick %d: %v", tick, err)
		}
		if count != 2 {
			t.Errorf("tick %d row count = %d, want 2 (reorg re-observation must insert even with identical values)", tick, count)
		}
	}
}

// TestUniswapV3Repository_SaveBlock_Tick_SignedLiquidityNet verifies that a
// negative liquidity_net (ticks below the current price, where crossing
// left-to-right removes liquidity) round-trips with its sign intact.
func TestUniswapV3Repository_SaveBlock_Tick_SignedLiquidityNet(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x1313131313131313131313131313131313131a"),
		common.HexToAddress("0x1313131313131313131313131313131313131b"),
		18, 18, "NTOKA", "NTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x1414141414141414141414141414141414141a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	negNet, ok := new(big.Int).SetString("-123456789012345678", 10)
	if !ok {
		t.Fatal("parsing negNet")
	}
	tk := newUniswapV3TestTick(poolID, -777, 2000, 0, negNet)

	repo := newUniswapV3Repo(t)
	withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{Ticks: []*entity.UniswapV3Tick{tk}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	var gotNet string
	if err := uniswapV3TestPool.QueryRow(ctx,
		`SELECT liquidity_net::text FROM uniswap_v3_tick WHERE pool_id=$1 AND tick=-777`,
		poolID,
	).Scan(&gotNet); err != nil {
		t.Fatalf("read back liquidity_net: %v", err)
	}
	if gotNet != "-123456789012345678" {
		t.Errorf("liquidity_net = %q, want -123456789012345678 (sign must survive NUMERIC round-trip)", gotNet)
	}
}

// TestUniswapV3Repository_SaveBlock_State_BlockVersionStamping verifies that
// block_version is threaded through end to end: two states at the same
// block_number but different block_version (a reorg re-observation) are
// both persisted and independently readable.
func TestUniswapV3Repository_SaveBlock_State_BlockVersionStamping(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV3FactTables(t, ctx)

	token0ID, token1ID := seedUniswapV3TokenPair(t, ctx,
		common.HexToAddress("0x1515151515151515151515151515151515151a"),
		common.HexToAddress("0x1515151515151515151515151515151515151b"),
		18, 18, "VTOKA", "VTOKB")
	poolID := seedUniswapV3Pool(t, ctx, common.HexToAddress("0x1616161616161616161616161616161616161a"), token0ID, token1ID, 3000, 60, ptrInt64(100))

	mkState := func(blockVersion int, tick int) *entity.UniswapV3PoolState {
		return &entity.UniswapV3PoolState{
			PoolID:               poolID,
			BlockNumber:          18000050,
			BlockVersion:         blockVersion,
			BlockTimestamp:       time.Unix(1700000500, 0).UTC(),
			SqrtPriceX96:         big.NewInt(1),
			Tick:                 tick,
			Liquidity:            big.NewInt(1),
			FeeGrowthGlobal0X128: big.NewInt(1),
			FeeGrowthGlobal1X128: big.NewInt(1),
			ProtocolFeesToken0:   big.NewInt(1),
			ProtocolFeesToken1:   big.NewInt(1),
			Balance0:             big.NewInt(1),
			Balance1:             big.NewInt(1),
		}
	}

	repo := newUniswapV3Repo(t)
	for _, st := range []*entity.UniswapV3PoolState{mkState(0, 1), mkState(1, 2)} {
		if err := st.Validate(); err != nil {
			t.Fatalf("Validate blockVersion=%d: %v", st.BlockVersion, err)
		}
		withUniswapV3Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{States: []*entity.UniswapV3PoolState{st}}); err != nil {
				t.Fatalf("SaveBlock blockVersion=%d: %v", st.BlockVersion, err)
			}
		})
	}

	rows, err := uniswapV3TestPool.Query(ctx,
		`SELECT block_version, tick FROM uniswap_v3_pool_state
		 WHERE pool_id=$1 AND block_number=18000050 ORDER BY block_version`,
		poolID,
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	gotVersions := map[int]int{}
	for rows.Next() {
		var version, tick int
		if err := rows.Scan(&version, &tick); err != nil {
			t.Fatalf("scan: %v", err)
		}
		gotVersions[version] = tick
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if gotVersions[0] != 1 {
		t.Errorf("block_version=0 tick = %d, want 1", gotVersions[0])
	}
	if gotVersions[1] != 2 {
		t.Errorf("block_version=1 tick = %d, want 2", gotVersions[1])
	}
}

func ptrInt64(v int64) *int64 { return &v }

// bigFromString parses a decimal string too large for an int64 literal (e.g.
// a Q64.96 sqrt price near 2^96).
func bigFromString(t *testing.T, s string) *big.Int {
	t.Helper()
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("parsing big.Int literal %q", s)
	}
	return v
}
