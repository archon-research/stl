//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// seedCurvePool inserts a minimal 2-coin plain_pre_ng pool for tests and
// returns its id. All inserts are idempotent so parallel/repeated test runs
// are safe.
// seedCurvePoolWithTokens is like seedCurvePool but also returns the token IDs
// for coins at indices 0 and 1.
func seedCurvePool(t *testing.T, ctx context.Context) int64 {
	poolID, _, _ := seedCurvePoolWithTokens(t, ctx)
	return poolID
}

func seedCurvePoolWithTokens(t *testing.T, ctx context.Context) (int64, int64, int64) {
	t.Helper()

	// Ensure a chain row exists.
	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (999, 'testchain')
		 ON CONFLICT (chain_id) DO NOTHING`,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	// Ensure a protocol row exists.
	var protoID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (999, '\xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC'::bytea, 'Curve', 'dex', 0, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&protoID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}

	// Ensure two token rows exist.
	var tokenID0, tokenID1 int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (999, '\xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA00'::bytea, 'TOKA', 18)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
	).Scan(&tokenID0); err != nil {
		t.Fatalf("seed token0: %v", err)
	}
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (999, '\xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01'::bytea, 'TOKB', 6)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
	).Scan(&tokenID1); err != nil {
		t.Fatalf("seed token1: %v", err)
	}

	// Ensure the pool row exists.
	var poolID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, deploy_block)
		 VALUES (999, $1, '\xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB'::bytea, 'plain_pre_ng', 2, 100)
		 ON CONFLICT (chain_id, pool_address) DO UPDATE SET pool_kind = EXCLUDED.pool_kind
		 RETURNING id`,
		protoID,
	).Scan(&poolID); err != nil {
		t.Fatalf("seed curve_pool: %v", err)
	}

	// Ensure coin rows exist.
	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id)
		 VALUES ($1, 0, $2), ($1, 1, $3)
		 ON CONFLICT (curve_pool_id, coin_index) DO NOTHING`,
		poolID, tokenID0, tokenID1,
	); err != nil {
		t.Fatalf("seed curve_pool_coin: %v", err)
	}

	return poolID, tokenID0, tokenID1
}

// newCurveRepo builds a CurveRepository backed by curveTestPool with buildID 1.
func newCurveRepo(t *testing.T) *CurveRepository {
	t.Helper()
	repo, err := NewCurveRepository(curveTestPool, nil, buildregistry.BuildID(1))
	if err != nil {
		t.Fatalf("NewCurveRepository: %v", err)
	}
	return repo
}

// truncateCurveFactTables clears all fact rows so each test starts clean.
func truncateCurveFactTables(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, table := range []string{
		"curve_stableswap_state",
		"curve_cryptoswap_state",
		"curve_swap",
		"curve_liquidity_event",
		"curve_stableswap_config",
		"curve_cryptoswap_config",
		"curve_parameter_event",
		"curve_lp_token_event",
	} {
		if _, err := curveTestPool.Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}

// TestCurveRepository_SaveStableswapState_Idempotent verifies that saving the
// same stableswap state twice (same build -> same processing_version) results
// in exactly one row.
func TestCurveRepository_SaveStableswapState_Idempotent(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	st, err := entity.NewCurveStableswapState(entity.CurveStableswapStateParams{
		CurvePoolID:  poolID,
		BlockNumber:  200,
		BlockVersion: 0,
		Timestamp:    time.Unix(1700000000, 0).UTC(),
		Balances:     []*big.Int{big.NewInt(10), big.NewInt(11)},
		VirtualPrice: big.NewInt(1),
		TotalSupply:  big.NewInt(21),
		A:            big.NewInt(900),
		Fee:          big.NewInt(1000000),
		SpotDy:       []*big.Int{big.NewInt(1), big.NewInt(1)},
	})
	if err != nil {
		t.Fatalf("NewCurveStableswapState: %v", err)
	}

	save := func() int64 {
		tx, err := curveTestPool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer tx.Rollback(ctx)
		n, err := repo.SaveBlock(ctx, tx, outbound.BlockWrites{StableStates: []*entity.CurveStableswapState{st}})
		if err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
		return n
	}

	if n := save(); n != 1 {
		t.Errorf("first save rows affected = %d, want 1", n)
	}
	// redelivery; same build -> trigger reuses pv -> ON CONFLICT DO NOTHING
	if n := save(); n != 0 {
		t.Errorf("redelivery rows affected = %d, want 0 (ON CONFLICT DO NOTHING)", n)
	}

	var count int
	if err := curveTestPool.QueryRow(ctx,
		`SELECT count(*) FROM curve_stableswap_state WHERE curve_pool_id=$1 AND block_number=200`,
		poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1 (idempotent)", count)
	}
}

// TestCurveRepository_SaveCryptoswapState_RoundTrip verifies that a
// cryptoswap state round-trips through the database.
func TestCurveRepository_SaveCryptoswapState_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	st, err := entity.NewCurveCryptoswapState(entity.CurveCryptoswapStateParams{
		CurvePoolID:  poolID,
		BlockNumber:  300,
		BlockVersion: 0,
		Timestamp:    time.Unix(1700001000, 0).UTC(),
		Balances:     []*big.Int{big.NewInt(1000000), big.NewInt(1000000000000000000)},
		VirtualPrice: big.NewInt(1000000000000000000),
		TotalSupply:  big.NewInt(2000000000000000000),
		A:            big.NewInt(2700000),
		Gamma:        big.NewInt(145000000000000000),
		Fee:          big.NewInt(4000000),
		D:            big.NewInt(5000000000000000000),
		XcpProfit:    big.NewInt(1000100000000000000),
		PriceScale:   []*big.Int{big.NewInt(1234567890)},
		PriceOracle:  []*big.Int{big.NewInt(1234560000)},
		LastPrices:   []*big.Int{big.NewInt(1234500000)},
		SpotDy:       []*big.Int{big.NewInt(990000000000000000)},
	})
	if err != nil {
		t.Fatalf("NewCurveCryptoswapState: %v", err)
	}

	tx, err := curveTestPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	n, err := repo.SaveBlock(ctx, tx, outbound.BlockWrites{CryptoStates: []*entity.CurveCryptoswapState{st}})
	if err != nil {
		t.Fatalf("SaveBlock: %v", err)
	}
	if n != 1 {
		t.Errorf("rows affected = %d, want 1", n)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var count int
	if err := curveTestPool.QueryRow(ctx,
		`SELECT count(*) FROM curve_cryptoswap_state WHERE curve_pool_id=$1 AND block_number=300`,
		poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1", count)
	}

	// Read back specific fields to verify the round-trip, not just row presence.
	var virtualPrice, gamma, priceScale0 string
	if err := curveTestPool.QueryRow(ctx,
		`SELECT virtual_price::text, gamma::text, price_scale[1]::text
		 FROM curve_cryptoswap_state
		 WHERE curve_pool_id=$1 AND block_number=300`,
		poolID,
	).Scan(&virtualPrice, &gamma, &priceScale0); err != nil {
		t.Fatalf("read-back query: %v", err)
	}
	if virtualPrice != "1000000000000000000" {
		t.Errorf("virtual_price = %q, want 1000000000000000000", virtualPrice)
	}
	if gamma != "145000000000000000" {
		t.Errorf("gamma = %q, want 145000000000000000", gamma)
	}
	if priceScale0 != "1234567890" {
		t.Errorf("price_scale[0] = %q, want 1234567890", priceScale0)
	}
}

// TestCurveRepository_SaveSwap_Idempotent verifies that saving the same swap
// event twice results in exactly one row.
func TestCurveRepository_SaveSwap_Idempotent(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	in := outbound.SwapInput{
		CurvePoolID:    poolID,
		BlockNumber:    400,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1700002000, 0).UTC(),
		LogIndex:       0,
		TxHash:         common.HexToHash("0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"),
		Buyer:          common.HexToAddress("0x1111111111111111111111111111111111111111"),
		SoldID:         0,
		BoughtID:       1,
		TokensSold:     big.NewInt(1000000000000000000),
		TokensBought:   big.NewInt(990000000000000000),
		Fee:            nil,
	}

	save := func() {
		tx, err := curveTestPool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer tx.Rollback(ctx)
		if _, err := repo.SaveBlock(ctx, tx, outbound.BlockWrites{Swaps: []outbound.SwapInput{in}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	save()
	save() // redelivery

	var count int
	if err := curveTestPool.QueryRow(ctx,
		`SELECT count(*) FROM curve_swap WHERE curve_pool_id=$1 AND block_number=400`,
		poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1 (idempotent)", count)
	}
}

// TestCurveRepository_SaveLiquidityEvent_Idempotent verifies that saving the
// same liquidity event twice results in exactly one row.
func TestCurveRepository_SaveLiquidityEvent_Idempotent(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	in := outbound.LiquidityInput{
		CurvePoolID:    poolID,
		BlockNumber:    500,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1700003000, 0).UTC(),
		LogIndex:       1,
		TxHash:         common.HexToHash("0xbbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddee"),
		Provider:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Kind:           "add",
		TokenAmounts:   []*big.Int{big.NewInt(1000000000000000000), big.NewInt(990000000000000000)},
		CoinIndex:      nil,
		Fees:           nil,
		Invariant:      big.NewInt(2000000000000000000),
		TokenSupply:    big.NewInt(1999000000000000000),
	}

	save := func() {
		tx, err := curveTestPool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer tx.Rollback(ctx)
		if _, err := repo.SaveBlock(ctx, tx, outbound.BlockWrites{Liquidity: []outbound.LiquidityInput{in}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	save()
	save() // redelivery

	var count int
	if err := curveTestPool.QueryRow(ctx,
		`SELECT count(*) FROM curve_liquidity_event WHERE curve_pool_id=$1 AND block_number=500`,
		poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1 (idempotent)", count)
	}
}

// TestCurveRepository_LoadPools verifies that LoadPools returns the seeded
// pool with correct fields including a non-zero ProtocolID.
func TestCurveRepository_LoadPools(t *testing.T) {
	ctx := context.Background()
	repo := newCurveRepo(t)
	poolID, _, _ := seedCurvePoolWithTokens(t, ctx)

	pools, err := repo.LoadPools(ctx, 999)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var found *outbound.CurvePoolRow
	for i := range pools {
		if pools[i].ID == poolID {
			found = &pools[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("seeded pool id=%d not found in LoadPools result", poolID)
	}
	if found.ProtocolID == 0 {
		t.Errorf("ProtocolID = 0, want non-zero (must be sourced from curve_pool.protocol_id)")
	}
	if found.Kind != "plain_pre_ng" {
		t.Errorf("Kind = %q, want plain_pre_ng", found.Kind)
	}
	if found.NCoins != 2 {
		t.Errorf("NCoins = %d, want 2", found.NCoins)
	}
	if len(found.CoinDecimals) != 2 {
		t.Fatalf("len(CoinDecimals) = %d, want 2", len(found.CoinDecimals))
	}
	if len(found.CoinDecimals) == 2 {
		if found.CoinDecimals[0] != 18 {
			t.Errorf("CoinDecimals[0] = %d, want 18", found.CoinDecimals[0])
		}
		if found.CoinDecimals[1] != 6 {
			t.Errorf("CoinDecimals[1] = %d, want 6", found.CoinDecimals[1])
		}
	}
}

// TestCurveRepository_LoadPools_HasAPrecise verifies that curve_pool.has_a_precise
// round-trips through LoadPools: FALSE by default and TRUE once curated, since it
// gates the A_precise snapshot read (replacing the old startup capability probe).
func TestCurveRepository_LoadPools_HasAPrecise(t *testing.T) {
	ctx := context.Background()
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	loadPool := func() outbound.CurvePoolRow {
		t.Helper()
		pools, err := repo.LoadPools(ctx, 999)
		if err != nil {
			t.Fatalf("LoadPools: %v", err)
		}
		for _, p := range pools {
			if p.ID == poolID {
				return p
			}
		}
		t.Fatalf("seeded pool id=%d not found", poolID)
		return outbound.CurvePoolRow{}
	}

	if loadPool().HasAPrecise {
		t.Error("HasAPrecise = true, want false (default for a freshly seeded pool)")
	}

	if _, err := curveTestPool.Exec(ctx,
		`UPDATE curve_pool SET has_a_precise = TRUE WHERE id = $1`, poolID,
	); err != nil {
		t.Fatalf("set has_a_precise: %v", err)
	}

	if !loadPool().HasAPrecise {
		t.Error("HasAPrecise = false, want true after curating has_a_precise=TRUE")
	}
}

// TestCurveRepository_LoadPools_NullDeployBlock verifies that a pool whose
// deploy_block is NULL (registered before its deploy height was backfilled) is
// still returned by LoadPools, with DeployBlock mapped to 0 rather than a scan
// error.
func TestCurveRepository_LoadPools_NullDeployBlock(t *testing.T) {
	ctx := context.Background()
	repo := newCurveRepo(t)
	poolID := seedCurvePoolWithNullDeployBlock(t, ctx)

	pools, err := repo.LoadPools(ctx, 999)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var found *outbound.CurvePoolRow
	for i := range pools {
		if pools[i].ID == poolID {
			found = &pools[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("pool id=%d with NULL deploy_block not returned by LoadPools", poolID)
	}
	if found.DeployBlock != 0 {
		t.Errorf("DeployBlock = %d, want 0 for a NULL deploy_block", found.DeployBlock)
	}
}

// TestCurveRepository_LoadPools_LpTokenAddress verifies that LoadPools returns
// LpTokenAddress when the column is non-null and nil when it is null.
func TestCurveRepository_LoadPools_LpTokenAddress(t *testing.T) {
	ctx := context.Background()
	repo := newCurveRepo(t)

	lpAddr := common.HexToAddress("0x06325440D014e39736583c165C2963BA99fAf14E")
	withLpPoolID := seedCurvePoolWithLpToken(t, ctx, lpAddr)
	noLpPoolID := seedCurvePool(t, ctx)

	pools, err := repo.LoadPools(ctx, 999)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var withLp, noLp *outbound.CurvePoolRow
	for i := range pools {
		if pools[i].ID == withLpPoolID {
			withLp = &pools[i]
		}
		if pools[i].ID == noLpPoolID {
			noLp = &pools[i]
		}
	}

	if withLp == nil {
		t.Fatalf("pool id=%d (with lp_token_address) not found in LoadPools", withLpPoolID)
	}
	if withLp.LpTokenAddress == nil {
		t.Fatal("LpTokenAddress should be non-nil for pool seeded with lp_token_address")
	}
	if *withLp.LpTokenAddress != lpAddr {
		t.Errorf("LpTokenAddress = %s, want %s", withLp.LpTokenAddress, lpAddr)
	}

	if noLp == nil {
		t.Fatalf("pool id=%d (null lp_token_address) not found in LoadPools", noLpPoolID)
	}
	if noLp.LpTokenAddress != nil {
		t.Errorf("LpTokenAddress should be nil for pool with no lp_token_address, got %s", noLp.LpTokenAddress)
	}
}

// seedCurvePoolWithLpToken inserts a 2-coin pre-NG pool with a non-null
// lp_token_address and returns its id. Uses a distinct pool/token address
// to avoid conflicts with seedCurvePool.
func seedCurvePoolWithLpToken(t *testing.T, ctx context.Context, lpAddr common.Address) int64 {
	t.Helper()

	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (999, 'testchain')
		 ON CONFLICT (chain_id) DO NOTHING`,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	var protoID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (999, '\xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC'::bytea, 'Curve', 'dex', 0, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&protoID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}

	var tokenID0, tokenID1 int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (999, '\xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA10'::bytea, 'TOKP', 18)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
	).Scan(&tokenID0); err != nil {
		t.Fatalf("seed token0: %v", err)
	}
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (999, '\xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA11'::bytea, 'TOKQ', 18)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
	).Scan(&tokenID1); err != nil {
		t.Fatalf("seed token1: %v", err)
	}

	var poolID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, deploy_block, lp_token_address)
		 VALUES (999, $1, '\xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE'::bytea, 'plain_pre_ng', 2, 100, $2)
		 ON CONFLICT (chain_id, pool_address) DO UPDATE SET lp_token_address = EXCLUDED.lp_token_address
		 RETURNING id`,
		protoID, lpAddr.Bytes(),
	).Scan(&poolID); err != nil {
		t.Fatalf("seed curve_pool with lp_token: %v", err)
	}

	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id)
		 VALUES ($1, 0, $2), ($1, 1, $3)
		 ON CONFLICT (curve_pool_id, coin_index) DO NOTHING`,
		poolID, tokenID0, tokenID1,
	); err != nil {
		t.Fatalf("seed curve_pool_coin: %v", err)
	}

	return poolID
}

// seedCurvePoolWithNullDeployBlock inserts a 2-coin pool with deploy_block = NULL
// and returns its id. Idempotent: re-running forces deploy_block back to NULL.
func seedCurvePoolWithNullDeployBlock(t *testing.T, ctx context.Context) int64 {
	t.Helper()

	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (999, 'testchain')
		 ON CONFLICT (chain_id) DO NOTHING`,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	var protoID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (999, '\xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC'::bytea, 'Curve', 'dex', 0, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&protoID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}

	var tokenID0, tokenID1 int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (999, '\xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA02'::bytea, 'TOKN', 18)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
	).Scan(&tokenID0); err != nil {
		t.Fatalf("seed token0: %v", err)
	}
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (999, '\xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA03'::bytea, 'TOKM', 6)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
	).Scan(&tokenID1); err != nil {
		t.Fatalf("seed token1: %v", err)
	}

	var poolID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, deploy_block)
		 VALUES (999, $1, '\xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD'::bytea, 'plain_pre_ng', 2, NULL)
		 ON CONFLICT (chain_id, pool_address) DO UPDATE SET deploy_block = NULL
		 RETURNING id`,
		protoID,
	).Scan(&poolID); err != nil {
		t.Fatalf("seed curve_pool: %v", err)
	}

	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id)
		 VALUES ($1, 0, $2), ($1, 1, $3)
		 ON CONFLICT (curve_pool_id, coin_index) DO NOTHING`,
		poolID, tokenID0, tokenID1,
	); err != nil {
		t.Fatalf("seed curve_pool_coin: %v", err)
	}

	return poolID
}

// setCurvePoolCoinPrecision sets the precision column for the two coins of a pool.
func setCurvePoolCoinPrecision(t *testing.T, ctx context.Context, poolID int64, prec0, prec1 *big.Int) {
	t.Helper()
	if _, err := curveTestPool.Exec(ctx,
		`UPDATE curve_pool_coin SET precision = $2 WHERE curve_pool_id = $1 AND coin_index = 0`,
		poolID, prec0.String(),
	); err != nil {
		t.Fatalf("set precision coin 0: %v", err)
	}
	if _, err := curveTestPool.Exec(ctx,
		`UPDATE curve_pool_coin SET precision = $2 WHERE curve_pool_id = $1 AND coin_index = 1`,
		poolID, prec1.String(),
	); err != nil {
		t.Fatalf("set precision coin 1: %v", err)
	}
}

// saveBlockCommitted runs SaveBlock inside a committed transaction and returns
// the state-row count.
func saveBlockCommitted(t *testing.T, ctx context.Context, repo *CurveRepository, w outbound.BlockWrites) int64 {
	t.Helper()
	tx, err := curveTestPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	n, err := repo.SaveBlock(ctx, tx, w)
	if err != nil {
		t.Fatalf("SaveBlock: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return n
}

// TestCurveRepository_LoadPools_Precisions verifies that LoadPools populates the
// Precisions slice index-aligned to the coins (same ordering as CoinDecimals).
func TestCurveRepository_LoadPools_Precisions(t *testing.T) {
	ctx := context.Background()
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	prec0 := big.NewInt(1)                                  // 10^(18-18) for an 18-decimal coin
	prec1, _ := new(big.Int).SetString("1000000000000", 10) // 10^(18-6)
	setCurvePoolCoinPrecision(t, ctx, poolID, prec0, prec1)

	pools, err := repo.LoadPools(ctx, 999)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var found *outbound.CurvePoolRow
	for i := range pools {
		if pools[i].ID == poolID {
			found = &pools[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("seeded pool id=%d not found", poolID)
	}
	if len(found.Precisions) != 2 {
		t.Fatalf("len(Precisions) = %d, want 2", len(found.Precisions))
	}
	if len(found.Precisions) != len(found.CoinDecimals) {
		t.Fatalf("Precisions (%d) not index-aligned with CoinDecimals (%d)",
			len(found.Precisions), len(found.CoinDecimals))
	}
	if found.Precisions[0] == nil || found.Precisions[0].Cmp(prec0) != 0 {
		t.Errorf("Precisions[0] = %v, want %s", found.Precisions[0], prec0)
	}
	if found.Precisions[1] == nil || found.Precisions[1].Cmp(prec1) != 0 {
		t.Errorf("Precisions[1] = %v, want %s", found.Precisions[1], prec1)
	}
}

// TestCurveRepository_LoadPools_Precisions_Null verifies that a coin with a NULL
// precision column maps to a nil entry in the index-aligned Precisions slice.
func TestCurveRepository_LoadPools_Precisions_Null(t *testing.T) {
	ctx := context.Background()
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	if _, err := curveTestPool.Exec(ctx,
		`UPDATE curve_pool_coin SET precision = '1' WHERE curve_pool_id = $1 AND coin_index = 0`,
		poolID,
	); err != nil {
		t.Fatalf("set precision coin 0: %v", err)
	}
	if _, err := curveTestPool.Exec(ctx,
		`UPDATE curve_pool_coin SET precision = NULL WHERE curve_pool_id = $1 AND coin_index = 1`,
		poolID,
	); err != nil {
		t.Fatalf("null precision coin 1: %v", err)
	}

	pools, err := repo.LoadPools(ctx, 999)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var found *outbound.CurvePoolRow
	for i := range pools {
		if pools[i].ID == poolID {
			found = &pools[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("seeded pool id=%d not found", poolID)
	}
	if len(found.Precisions) != 2 {
		t.Fatalf("len(Precisions) = %d, want 2", len(found.Precisions))
	}
	if found.Precisions[0] == nil || found.Precisions[0].Cmp(big.NewInt(1)) != 0 {
		t.Errorf("Precisions[0] = %v, want 1", found.Precisions[0])
	}
	if found.Precisions[1] != nil {
		t.Errorf("Precisions[1] = %v, want nil for a NULL precision column", found.Precisions[1])
	}
}

// TestCurveRepository_SaveStableswapState_ExtendedColumns verifies the extended
// nullable columns round-trip and that a nil array field is stored as SQL NULL
// (not an empty array).
func TestCurveRepository_SaveStableswapState_ExtendedColumns(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	st, err := entity.NewCurveStableswapState(entity.CurveStableswapStateParams{
		CurvePoolID:     poolID,
		BlockNumber:     600,
		BlockVersion:    0,
		Timestamp:       time.Unix(1700010000, 0).UTC(),
		Balances:        []*big.Int{big.NewInt(10), big.NewInt(11)},
		VirtualPrice:    big.NewInt(1),
		TotalSupply:     big.NewInt(21),
		A:               big.NewInt(900),
		Fee:             big.NewInt(1000000),
		SpotDy:          []*big.Int{big.NewInt(1), big.NewInt(1)},
		APrecise:        big.NewInt(90000),
		AdminBalances:   []*big.Int{big.NewInt(2), big.NewInt(3)},
		CalcTokenAmount: big.NewInt(12345),
		// StoredRates and CalcWithdrawOneCoin left nil: must be SQL NULL, not empty arrays.
	})
	if err != nil {
		t.Fatalf("NewCurveStableswapState: %v", err)
	}

	if n := saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		StableStates: []*entity.CurveStableswapState{st},
	}); n != 1 {
		t.Fatalf("state rows = %d, want 1", n)
	}

	var (
		aPrecise        string
		adminBalances0  string
		calcTokenAmount string
		storedRatesNull bool
		calcWithdrawNil bool
	)
	if err := curveTestPool.QueryRow(ctx,
		`SELECT a_precise::text, admin_balances[1]::text, calc_token_amount::text,
		        stored_rates IS NULL, calc_withdraw_one_coin IS NULL
		 FROM curve_stableswap_state WHERE curve_pool_id=$1 AND block_number=600`,
		poolID,
	).Scan(&aPrecise, &adminBalances0, &calcTokenAmount, &storedRatesNull, &calcWithdrawNil); err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if aPrecise != "90000" {
		t.Errorf("a_precise = %q, want 90000", aPrecise)
	}
	if adminBalances0 != "2" {
		t.Errorf("admin_balances[0] = %q, want 2", adminBalances0)
	}
	if calcTokenAmount != "12345" {
		t.Errorf("calc_token_amount = %q, want 12345", calcTokenAmount)
	}
	if !storedRatesNull {
		t.Error("stored_rates should be SQL NULL for a nil slice, not an empty array")
	}
	if !calcWithdrawNil {
		t.Error("calc_withdraw_one_coin should be SQL NULL for a nil slice")
	}
}

// TestCurveRepository_SaveCryptoswapState_ExtendedColumns verifies the extended
// cryptoswap columns round-trip, including the nullable last_prices_timestamp
// and SQL NULL for nil array fields.
func TestCurveRepository_SaveCryptoswapState_ExtendedColumns(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	ts := int64(1700009999)
	st, err := entity.NewCurveCryptoswapState(entity.CurveCryptoswapStateParams{
		CurvePoolID:         poolID,
		BlockNumber:         700,
		BlockVersion:        0,
		Timestamp:           time.Unix(1700011000, 0).UTC(),
		Balances:            []*big.Int{big.NewInt(1000000), big.NewInt(2000000)},
		VirtualPrice:        big.NewInt(1000000000000000000),
		TotalSupply:         big.NewInt(2000000000000000000),
		A:                   big.NewInt(2700000),
		Gamma:               big.NewInt(145000000000000000),
		Fee:                 big.NewInt(4000000),
		PriceScale:          []*big.Int{big.NewInt(1234567890)},
		PriceOracle:         []*big.Int{big.NewInt(1234560000)},
		LastPrices:          []*big.Int{big.NewInt(1234500000)},
		SpotDy:              []*big.Int{big.NewInt(990000000000000000)},
		LpPrice:             big.NewInt(5555),
		XcpProfitA:          big.NewInt(1000200000000000000),
		LastPricesTimestamp: &ts,
		AdminBalances:       []*big.Int{big.NewInt(7), big.NewInt(8)},
		// GetDx and CalcWithdrawOneCoin left nil: must be SQL NULL.
	})
	if err != nil {
		t.Fatalf("NewCurveCryptoswapState: %v", err)
	}

	if n := saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		CryptoStates: []*entity.CurveCryptoswapState{st},
	}); n != 1 {
		t.Fatalf("state rows = %d, want 1", n)
	}

	var (
		lpPrice        string
		xcpProfitA     string
		lastPricesTs   int64
		adminBalances0 string
		getDxNull      bool
		calcWithdraw   bool
	)
	if err := curveTestPool.QueryRow(ctx,
		`SELECT lp_price::text, xcp_profit_a::text, last_prices_timestamp,
		        admin_balances[1]::text, get_dx IS NULL, calc_withdraw_one_coin IS NULL
		 FROM curve_cryptoswap_state WHERE curve_pool_id=$1 AND block_number=700`,
		poolID,
	).Scan(&lpPrice, &xcpProfitA, &lastPricesTs, &adminBalances0, &getDxNull, &calcWithdraw); err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if lpPrice != "5555" {
		t.Errorf("lp_price = %q, want 5555", lpPrice)
	}
	if xcpProfitA != "1000200000000000000" {
		t.Errorf("xcp_profit_a = %q, want 1000200000000000000", xcpProfitA)
	}
	if lastPricesTs != ts {
		t.Errorf("last_prices_timestamp = %d, want %d", lastPricesTs, ts)
	}
	if adminBalances0 != "7" {
		t.Errorf("admin_balances[0] = %q, want 7", adminBalances0)
	}
	if !getDxNull {
		t.Error("get_dx should be SQL NULL for a nil slice")
	}
	if !calcWithdraw {
		t.Error("calc_withdraw_one_coin should be SQL NULL for a nil slice")
	}
}

// TestCurveRepository_StableswapConfig_AppendOnChange verifies the
// append-on-change semantics: identical config repeats do not add rows, a
// changed field appends a new row, and a NG-shaped config (nil FutureAdminFee /
// OracleMethod) is written once and not duplicated on an unchanged repeat.
func TestCurveRepository_StableswapConfig_AppendOnChange(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	countRows := func() int {
		var n int
		if err := curveTestPool.QueryRow(ctx,
			`SELECT count(*) FROM curve_stableswap_config WHERE curve_pool_id=$1`, poolID,
		).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	maExp := int64(866)
	makeConfig := func(block int64, futureA *big.Int) *entity.CurveStableswapConfig {
		cfg, err := entity.NewCurveStableswapConfig(entity.CurveStableswapConfigParams{
			CurvePoolID:    poolID,
			BlockNumber:    block,
			BlockVersion:   0,
			Timestamp:      time.Unix(1700020000+block, 0).UTC(),
			InitialA:       big.NewInt(1000),
			InitialATime:   100,
			FutureA:        futureA,
			FutureATime:    200,
			AdminFee:       big.NewInt(5000000000),
			FutureFee:      big.NewInt(4000000),
			FutureAdminFee: nil,
			MaExpTime:      &maExp,
			OracleMethod:   nil,
		})
		if err != nil {
			t.Fatalf("NewCurveStableswapConfig: %v", err)
		}
		return cfg
	}

	// (a) first write -> exactly 1 row.
	saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		StableswapConfigs: []*entity.CurveStableswapConfig{makeConfig(1000, big.NewInt(2000))},
	})
	if got := countRows(); got != 1 {
		t.Fatalf("after first write: rows = %d, want 1", got)
	}

	// (b) same values at a later block -> still 1 row (no change).
	saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		StableswapConfigs: []*entity.CurveStableswapConfig{makeConfig(1001, big.NewInt(2000))},
	})
	if got := countRows(); got != 1 {
		t.Fatalf("after unchanged repeat: rows = %d, want 1", got)
	}

	// (c) a changed field -> 2 rows.
	saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		StableswapConfigs: []*entity.CurveStableswapConfig{makeConfig(1002, big.NewInt(2500))},
	})
	if got := countRows(); got != 2 {
		t.Fatalf("after changed field: rows = %d, want 2", got)
	}

	// (d) an unchanged repeat of the latest NG-shaped values -> still 2 rows.
	saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		StableswapConfigs: []*entity.CurveStableswapConfig{makeConfig(1003, big.NewInt(2500))},
	})
	if got := countRows(); got != 2 {
		t.Fatalf("after second unchanged repeat: rows = %d, want 2", got)
	}
}

// TestCurveRepository_CryptoswapConfig_AppendOnChange verifies append-on-change
// for cryptoswap configs across first-write, unchanged-repeat, and changed-field.
func TestCurveRepository_CryptoswapConfig_AppendOnChange(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	countRows := func() int {
		var n int
		if err := curveTestPool.QueryRow(ctx,
			`SELECT count(*) FROM curve_cryptoswap_config WHERE curve_pool_id=$1`, poolID,
		).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	makeConfig := func(block int64, midFee *big.Int) *entity.CurveCryptoswapConfig {
		cfg, err := entity.NewCurveCryptoswapConfig(entity.CurveCryptoswapConfigParams{
			CurvePoolID:        poolID,
			BlockNumber:        block,
			BlockVersion:       0,
			Timestamp:          time.Unix(1700030000+block, 0).UTC(),
			InitialAGamma:      big.NewInt(123456),
			FutureAGamma:       big.NewInt(654321),
			InitialAGammaTime:  10,
			FutureAGammaTime:   20,
			MidFee:             midFee,
			OutFee:             big.NewInt(40000000),
			FeeGamma:           big.NewInt(10000000000000000),
			AllowedExtraProfit: big.NewInt(2000000000000),
			AdjustmentStep:     big.NewInt(146000000000000),
			MaTime:             big.NewInt(866),
			AdminFee:           big.NewInt(5000000000),
		})
		if err != nil {
			t.Fatalf("NewCurveCryptoswapConfig: %v", err)
		}
		return cfg
	}

	saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		CryptoswapConfigs: []*entity.CurveCryptoswapConfig{makeConfig(2000, big.NewInt(3000000))},
	})
	if got := countRows(); got != 1 {
		t.Fatalf("after first write: rows = %d, want 1", got)
	}

	saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		CryptoswapConfigs: []*entity.CurveCryptoswapConfig{makeConfig(2001, big.NewInt(3000000))},
	})
	if got := countRows(); got != 1 {
		t.Fatalf("after unchanged repeat: rows = %d, want 1", got)
	}

	saveBlockCommitted(t, ctx, repo, outbound.BlockWrites{
		CryptoswapConfigs: []*entity.CurveCryptoswapConfig{makeConfig(2002, big.NewInt(3500000))},
	})
	if got := countRows(); got != 2 {
		t.Fatalf("after changed field: rows = %d, want 2", got)
	}
}

// TestCurveRepository_ParameterEvent_RoundTrip verifies a parameter event round
// trips including the JSONB params, and that a redelivery does not duplicate.
func TestCurveRepository_ParameterEvent_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	ev, err := entity.NewCurveParameterEvent(entity.CurveParameterEventParams{
		CurvePoolID:  poolID,
		BlockNumber:  800,
		BlockVersion: 0,
		Timestamp:    time.Unix(1700040000, 0).UTC(),
		TxHash:       common.HexToHash("0xaa11bb22cc33dd44ee55ff66aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"),
		LogIndex:     2,
		EventName:    "ramp_a",
		Params:       json.RawMessage(`{"old_A":1000,"new_A":2000,"initial_time":1,"future_time":2}`),
	})
	if err != nil {
		t.Fatalf("NewCurveParameterEvent: %v", err)
	}

	w := outbound.BlockWrites{ParameterEvents: []*entity.CurveParameterEvent{ev}}
	saveBlockCommitted(t, ctx, repo, w)
	saveBlockCommitted(t, ctx, repo, w) // redelivery

	var count int
	if err := curveTestPool.QueryRow(ctx,
		`SELECT count(*) FROM curve_parameter_event WHERE curve_pool_id=$1 AND block_number=800`,
		poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1 (idempotent)", count)
	}

	var (
		eventName string
		params    []byte
	)
	if err := curveTestPool.QueryRow(ctx,
		`SELECT event_name, params
		 FROM curve_parameter_event WHERE curve_pool_id=$1 AND block_number=800`,
		poolID,
	).Scan(&eventName, &params); err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if eventName != "ramp_a" {
		t.Errorf("event_name = %q, want ramp_a", eventName)
	}
	var got map[string]any
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if got["new_A"] != float64(2000) {
		t.Errorf("params.new_A = %v, want 2000", got["new_A"])
	}
}

// TestCurveRepository_LpTokenEvent_RoundTrip verifies an LP token event round
// trips including addresses and value, and that a redelivery does not duplicate.
func TestCurveRepository_LpTokenEvent_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	from := common.HexToAddress("0x3333333333333333333333333333333333333333")
	to := common.HexToAddress("0x4444444444444444444444444444444444444444")
	ev, err := entity.NewCurveLpTokenEvent(entity.CurveLpTokenEventParams{
		CurvePoolID:  poolID,
		BlockNumber:  900,
		BlockVersion: 0,
		Timestamp:    time.Unix(1700050000, 0).UTC(),
		TxHash:       common.HexToHash("0xbb11cc22dd33ee44ff55aa66bb11cc22dd33ee44ff55aa66bb11cc22dd33ee44"),
		LogIndex:     3,
		EventName:    "transfer",
		From:         from,
		To:           to,
		Value:        big.NewInt(123456789),
	})
	if err != nil {
		t.Fatalf("NewCurveLpTokenEvent: %v", err)
	}

	w := outbound.BlockWrites{LpTokenEvents: []*entity.CurveLpTokenEvent{ev}}
	saveBlockCommitted(t, ctx, repo, w)
	saveBlockCommitted(t, ctx, repo, w) // redelivery

	var count int
	if err := curveTestPool.QueryRow(ctx,
		`SELECT count(*) FROM curve_lp_token_event WHERE curve_pool_id=$1 AND block_number=900`,
		poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1 (idempotent)", count)
	}

	var (
		eventName   string
		fromAddr    []byte
		toAddr      []byte
		valueString string
	)
	if err := curveTestPool.QueryRow(ctx,
		`SELECT event_name, from_address, to_address, value::text
		 FROM curve_lp_token_event WHERE curve_pool_id=$1 AND block_number=900`,
		poolID,
	).Scan(&eventName, &fromAddr, &toAddr, &valueString); err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if eventName != "transfer" {
		t.Errorf("event_name = %q, want transfer", eventName)
	}
	if common.BytesToAddress(fromAddr) != from {
		t.Errorf("from_address = %s, want %s", common.BytesToAddress(fromAddr), from)
	}
	if common.BytesToAddress(toAddr) != to {
		t.Errorf("to_address = %s, want %s", common.BytesToAddress(toAddr), to)
	}
	if valueString != "123456789" {
		t.Errorf("value = %q, want 123456789", valueString)
	}
}

// TestCurveRepository_SaveBlock_MixedBatchDrainOrder guards the invariant that
// sendCurveBatch drains br.Exec() results in the exact order queueCurveBatch
// queued them (swaps, liquidity, stableswap, cryptoswap, parameter, lp). The
// state-row count SaveBlock returns is summed only over the stableswap+cryptoswap
// drain positions, so if the queue and drain orders ever drift, the count is read
// off the wrong statements. We make that observable: the non-state rows are
// inserted once up front so they conflict (RowsAffected 0) in the measured save,
// while the 3 state rows are new (RowsAffected 1 each). A correct drain returns
// exactly 3; a mismatched drain would read the zero tags and return something else.
func TestCurveRepository_SaveBlock_MixedBatchDrainOrder(t *testing.T) {
	ctx := context.Background()
	truncateCurveFactTables(t, ctx)
	repo := newCurveRepo(t)
	poolID := seedCurvePool(t, ctx)

	ts := time.Unix(1700050000, 0).UTC()
	swap := outbound.SwapInput{
		CurvePoolID: poolID, BlockNumber: 1000, BlockVersion: 0, BlockTimestamp: ts,
		LogIndex: 0, TxHash: common.HexToHash("0xa1"),
		Buyer:  common.HexToAddress("0x1111111111111111111111111111111111111111"),
		SoldID: 0, BoughtID: 1, TokensSold: big.NewInt(1), TokensBought: big.NewInt(1),
	}
	liq := outbound.LiquidityInput{
		CurvePoolID: poolID, BlockNumber: 1000, BlockVersion: 0, BlockTimestamp: ts,
		LogIndex: 1, TxHash: common.HexToHash("0xb2"),
		Provider: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Kind:     "add", TokenAmounts: []*big.Int{big.NewInt(1), big.NewInt(1)},
		Invariant: big.NewInt(2), TokenSupply: big.NewInt(2),
	}
	param, err := entity.NewCurveParameterEvent(entity.CurveParameterEventParams{
		CurvePoolID: poolID, BlockNumber: 1000, BlockVersion: 0, Timestamp: ts,
		TxHash: common.HexToHash("0xc3"), LogIndex: 2, EventName: "ramp_a",
		Params: json.RawMessage(`{"old_A":1,"new_A":2,"initial_time":1,"future_time":2}`),
	})
	if err != nil {
		t.Fatalf("NewCurveParameterEvent: %v", err)
	}
	lp, err := entity.NewCurveLpTokenEvent(entity.CurveLpTokenEventParams{
		CurvePoolID: poolID, BlockNumber: 1000, BlockVersion: 0, Timestamp: ts,
		TxHash: common.HexToHash("0xd4"), LogIndex: 3, EventName: "transfer",
		From:  common.HexToAddress("0x3333333333333333333333333333333333333333"),
		To:    common.HexToAddress("0x4444444444444444444444444444444444444444"),
		Value: big.NewInt(5),
	})
	if err != nil {
		t.Fatalf("NewCurveLpTokenEvent: %v", err)
	}

	nonState := outbound.BlockWrites{
		Swaps:           []outbound.SwapInput{swap},
		Liquidity:       []outbound.LiquidityInput{liq},
		ParameterEvents: []*entity.CurveParameterEvent{param},
		LpTokenEvents:   []*entity.CurveLpTokenEvent{lp},
	}
	// Insert the non-state rows once so they conflict (RowsAffected 0) below.
	saveBlockCommitted(t, ctx, repo, nonState)

	newStable := func(bn int64) *entity.CurveStableswapState {
		st, stErr := entity.NewCurveStableswapState(entity.CurveStableswapStateParams{
			CurvePoolID: poolID, BlockNumber: bn, BlockVersion: 0, Timestamp: ts,
			Balances: []*big.Int{big.NewInt(10), big.NewInt(11)}, VirtualPrice: big.NewInt(1),
			TotalSupply: big.NewInt(21), A: big.NewInt(900), Fee: big.NewInt(1000000),
			SpotDy: []*big.Int{big.NewInt(1), big.NewInt(1)},
		})
		if stErr != nil {
			t.Fatalf("NewCurveStableswapState(%d): %v", bn, stErr)
		}
		return st
	}
	crypto, err := entity.NewCurveCryptoswapState(entity.CurveCryptoswapStateParams{
		CurvePoolID: poolID, BlockNumber: 1003, BlockVersion: 0, Timestamp: ts,
		Balances:     []*big.Int{big.NewInt(1000000), big.NewInt(2000000)},
		VirtualPrice: big.NewInt(1000000000000000000), TotalSupply: big.NewInt(2000000000000000000),
		A: big.NewInt(2700000), Gamma: big.NewInt(145000000000000000), Fee: big.NewInt(4000000),
		PriceScale: []*big.Int{big.NewInt(1234567890)}, PriceOracle: []*big.Int{big.NewInt(1234560000)},
		LastPrices: []*big.Int{big.NewInt(1234500000)}, SpotDy: []*big.Int{big.NewInt(990000000000000000)},
	})
	if err != nil {
		t.Fatalf("NewCurveCryptoswapState: %v", err)
	}

	// Measured save: same non-state rows (now conflict) + 3 genuinely-new state rows.
	mixed := nonState
	mixed.StableStates = []*entity.CurveStableswapState{newStable(1001), newStable(1002)}
	mixed.CryptoStates = []*entity.CurveCryptoswapState{crypto}

	if stateRows := saveBlockCommitted(t, ctx, repo, mixed); stateRows != 3 {
		t.Fatalf("stateRows = %d, want 3 (only the 3 new state rows count; the pre-existing swap/liquidity/parameter/lp rows conflict to 0). A wrong count means queueCurveBatch and sendCurveBatch iterate the batch groups in different orders.", stateRows)
	}

	// Data is bound at Queue time, so every row lands in its own table regardless
	// of drain order; assert the queued inserts populated each table.
	counts := []struct {
		query string
		want  int
		name  string
	}{
		{`SELECT count(*) FROM curve_swap WHERE curve_pool_id=$1`, 1, "curve_swap"},
		{`SELECT count(*) FROM curve_liquidity_event WHERE curve_pool_id=$1`, 1, "curve_liquidity_event"},
		{`SELECT count(*) FROM curve_stableswap_state WHERE curve_pool_id=$1`, 2, "curve_stableswap_state"},
		{`SELECT count(*) FROM curve_cryptoswap_state WHERE curve_pool_id=$1`, 1, "curve_cryptoswap_state"},
		{`SELECT count(*) FROM curve_parameter_event WHERE curve_pool_id=$1`, 1, "curve_parameter_event"},
		{`SELECT count(*) FROM curve_lp_token_event WHERE curve_pool_id=$1`, 1, "curve_lp_token_event"},
	}
	for _, c := range counts {
		var n int
		if err := curveTestPool.QueryRow(ctx, c.query, poolID).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", c.name, err)
		}
		if n != c.want {
			t.Errorf("%s rows = %d, want %d", c.name, n, c.want)
		}
	}
}
