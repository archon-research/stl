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
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
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

	save := func() {
		tx, err := curveTestPool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer tx.Rollback(ctx)
		if err := repo.SaveStableswapState(ctx, tx, st); err != nil {
			t.Fatalf("SaveStableswapState: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	save()
	save() // redelivery; same build -> trigger reuses pv -> ON CONFLICT DO NOTHING

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
	if err := repo.SaveCryptoswapState(ctx, tx, st); err != nil {
		t.Fatalf("SaveCryptoswapState: %v", err)
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
		if err := repo.SaveSwap(ctx, tx, in); err != nil {
			t.Fatalf("SaveSwap: %v", err)
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
		if err := repo.SaveLiquidityEvent(ctx, tx, in); err != nil {
			t.Fatalf("SaveLiquidityEvent: %v", err)
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
// pool with correct coin token IDs in coin_index order.
func TestCurveRepository_LoadPools(t *testing.T) {
	ctx := context.Background()
	repo := newCurveRepo(t)
	poolID, tokenID0, tokenID1 := seedCurvePoolWithTokens(t, ctx)

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
	if found.Kind != "plain_pre_ng" {
		t.Errorf("Kind = %q, want plain_pre_ng", found.Kind)
	}
	if found.NCoins != 2 {
		t.Errorf("NCoins = %d, want 2", found.NCoins)
	}
	if len(found.CoinTokenIDs) != 2 {
		t.Errorf("len(CoinTokenIDs) = %d, want 2", len(found.CoinTokenIDs))
	}

	// Assert CoinTokenIDs are ordered by coin_index (tokenID0 at [0], tokenID1 at [1]).
	// seedCurvePool inserts: coin_index 0 -> tokenID0 (TOKA), coin_index 1 -> tokenID1 (TOKB).
	if found.CoinTokenIDs[0] != tokenID0 {
		t.Errorf("CoinTokenIDs[0] = %d, want %d (coin_index 0 token)", found.CoinTokenIDs[0], tokenID0)
	}
	if found.CoinTokenIDs[1] != tokenID1 {
		t.Errorf("CoinTokenIDs[1] = %d, want %d (coin_index 1 token)", found.CoinTokenIDs[1], tokenID1)
	}

	if len(found.CoinDecimals) != 2 {
		t.Errorf("len(CoinDecimals) = %d, want 2", len(found.CoinDecimals))
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
