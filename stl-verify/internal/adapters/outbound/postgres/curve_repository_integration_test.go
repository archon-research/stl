//go:build integration

package postgres

import (
	"context"
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
