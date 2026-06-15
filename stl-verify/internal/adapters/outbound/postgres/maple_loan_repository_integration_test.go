//go:build integration

package postgres

import (
	"context"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity/maple"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const mapleSchemaName = "test_maple_graphql"

var maplePool *pgxpool.Pool

func init() {
	registerTestFileSetup(mapleSchemaName, func() {
		maplePool = testutil.SetupSchemaForMain(sharedDSN, mapleSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, maplePool, mapleSchemaName)
	})
}

// truncateMaple clears maple-related tables for test isolation. The protocol
// table keeps the migration-seeded maple row.
func truncateMaple(t *testing.T, ctx context.Context) {
	t.Helper()
	tables := []string{
		`maple_loan_collateral`,
		`maple_loan_state`,
		`maple_pool_state`,
		`maple_sky_strategy_state`,
		`maple_syrup_global_state`,
		`maple_loan`,
		`maple_sky_strategy`,
		`maple_pool`,
	}
	for _, table := range tables {
		if _, err := maplePool.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("failed to truncate %s: %v", table, err)
		}
	}
	if _, err := maplePool.Exec(ctx, `DELETE FROM "user"`); err != nil {
		t.Fatalf("failed to truncate user: %v", err)
	}
}

func newMapleRepo(t *testing.T, buildID buildregistry.BuildID) *MapleGraphQLRepository {
	t.Helper()
	repo, err := NewMapleGraphQLRepository(maplePool, nil, buildID, 0)
	if err != nil {
		t.Fatalf("NewMapleGraphQLRepository: %v", err)
	}
	return repo
}

// inMapleTx runs fn inside a committed transaction.
func inMapleTx(t *testing.T, ctx context.Context, fn func(tx pgx.Tx) error) {
	t.Helper()
	tx, err := maplePool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("tx fn: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func mapleAddr(b byte) []byte {
	a := make([]byte, 20)
	for i := range a {
		a[i] = b
	}
	return a
}

func mapleSyncedAt() time.Time {
	return time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
}

// mapleProtocolID resolves the migration-seeded maple protocol row.
func mapleProtocolID(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository) int64 {
	t.Helper()
	id, err := repo.GetMapleProtocolID(ctx, 1)
	if err != nil {
		t.Fatalf("GetMapleProtocolID: %v", err)
	}
	return id
}

// upsertTestAssetToken resolves a token id for a pool asset, creating the
// token row if needed.
func upsertTestAssetToken(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, tx pgx.Tx, addrByte byte, symbol string) int64 {
	t.Helper()
	asset := outbound.MapleAssetToken{Address: common.BytesToAddress(mapleAddr(addrByte)), Symbol: symbol, Decimals: 6}
	ids, err := repo.GetOrCreateAssetTokens(ctx, tx, 1, []outbound.MapleAssetToken{asset})
	if err != nil {
		t.Fatalf("GetOrCreateAssetTokens: %v", err)
	}
	id, ok := ids[asset.Address]
	if !ok {
		t.Fatalf("asset token id missing from map: %v", ids)
	}
	return id
}

func upsertTestPool(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, addrByte byte) int64 {
	t.Helper()
	protocolID := mapleProtocolID(t, ctx, repo)

	var ids map[common.Address]int64
	var poolAddr common.Address
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		assetTokenID := upsertTestAssetToken(t, ctx, repo, tx, 0xee, "USDC")
		pool, err := maple.NewPool(1, protocolID, mapleAddr(addrByte), "Test Pool", assetTokenID, true)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		poolAddr = common.BytesToAddress(pool.Address)
		ids, err = repo.UpsertPools(ctx, tx, []*maple.Pool{pool})
		return err
	})
	id, ok := ids[poolAddr]
	if !ok {
		t.Fatalf("pool id missing from map: %v", ids)
	}
	return id
}

func upsertTestLoan(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, poolID int64, addrByte byte, meta *maple.LoanMeta) int64 {
	t.Helper()
	protocolID := mapleProtocolID(t, ctx, repo)

	var loanID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		users, err := repo.GetOrCreateBorrowerUsers(ctx, tx, 1, []common.Address{common.BytesToAddress(mapleAddr(0xab))})
		if err != nil {
			return err
		}
		loan, err := maple.NewLoan(1, protocolID, mapleAddr(addrByte), poolID, users[common.BytesToAddress(mapleAddr(0xab))], meta)
		if err != nil {
			return err
		}
		ids, err := repo.UpsertLoans(ctx, tx, []*maple.Loan{loan})
		if err != nil {
			return err
		}
		loanID = ids[common.BytesToAddress(loan.LoanAddress)]
		return nil
	})
	if loanID == 0 {
		t.Fatal("loan id not resolved")
	}
	return loanID
}

func TestMapleGetMapleProtocolID(t *testing.T) {
	ctx := context.Background()
	repo := newMapleRepo(t, 0)

	id, err := repo.GetMapleProtocolID(ctx, 1)
	if err != nil {
		t.Fatalf("GetMapleProtocolID: %v", err)
	}
	if id <= 0 {
		t.Errorf("id = %d, want positive", id)
	}

	if _, err := repo.GetMapleProtocolID(ctx, 999); err == nil {
		t.Error("expected error for unknown chain, got nil")
	}
}

func TestMapleGetOrCreateBorrowerUsers(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)

	borrowerA := common.BytesToAddress(mapleAddr(0x01))
	borrowerB := common.BytesToAddress(mapleAddr(0x02))

	var first map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		// Duplicate input must be deduplicated.
		first, err = repo.GetOrCreateBorrowerUsers(ctx, tx, 1, []common.Address{borrowerA, borrowerB, borrowerA})
		return err
	})
	if len(first) != 2 {
		t.Fatalf("len(first) = %d, want 2", len(first))
	}

	// New borrower users must have NULL first_seen_block.
	var fsb *int64
	if err := maplePool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE chain_id = 1 AND address = $1`,
		borrowerA.Bytes()).Scan(&fsb); err != nil {
		t.Fatalf("querying user: %v", err)
	}
	if fsb != nil {
		t.Errorf("first_seen_block = %v, want NULL", *fsb)
	}

	// Re-upserting returns the same ids.
	var second map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		second, err = repo.GetOrCreateBorrowerUsers(ctx, tx, 1, []common.Address{borrowerA, borrowerB})
		return err
	})
	if second[borrowerA] != first[borrowerA] || second[borrowerB] != first[borrowerB] {
		t.Errorf("ids changed across upserts: %v vs %v", first, second)
	}

	// An existing user created by an on-chain indexer keeps its
	// first_seen_block (the shared GetOrCreateUser LEAST() merge would have
	// clobbered it to 0).
	existing := common.BytesToAddress(mapleAddr(0x03))
	var existingID int64
	if err := maplePool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block, created_at, updated_at, metadata)
		 VALUES (1, $1, 12345, NOW(), NOW(), '{}'::jsonb) RETURNING id`,
		existing.Bytes()).Scan(&existingID); err != nil {
		t.Fatalf("seeding existing user: %v", err)
	}

	var third map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		third, err = repo.GetOrCreateBorrowerUsers(ctx, tx, 1, []common.Address{existing})
		return err
	})
	if third[existing] != existingID {
		t.Errorf("existing user id = %d, want %d", third[existing], existingID)
	}
	var preserved int64
	if err := maplePool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE id = $1`, existingID).Scan(&preserved); err != nil {
		t.Fatalf("querying preserved user: %v", err)
	}
	if preserved != 12345 {
		t.Errorf("first_seen_block = %d, want 12345 (must not be clobbered)", preserved)
	}
}

func TestMapleGetOrCreateAssetTokens(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)

	usdc := outbound.MapleAssetToken{Address: common.BytesToAddress(mapleAddr(0xe1)), Symbol: "USDC", Decimals: 6}
	usdt := outbound.MapleAssetToken{Address: common.BytesToAddress(mapleAddr(0xe2)), Symbol: "USDT", Decimals: 6}

	var first map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		// Duplicate input must be deduplicated.
		first, err = repo.GetOrCreateAssetTokens(ctx, tx, 1, []outbound.MapleAssetToken{usdc, usdt, usdc})
		return err
	})
	if len(first) != 2 {
		t.Fatalf("len(first) = %d, want 2", len(first))
	}

	// New asset tokens must have NULL created_at_block and the API metadata.
	var cab *int64
	var symbol string
	var decimals int16
	if err := maplePool.QueryRow(ctx,
		`SELECT created_at_block, symbol, decimals FROM token WHERE chain_id = 1 AND address = $1`,
		usdc.Address.Bytes()).Scan(&cab, &symbol, &decimals); err != nil {
		t.Fatalf("querying token: %v", err)
	}
	if cab != nil {
		t.Errorf("created_at_block = %v, want NULL", *cab)
	}
	if symbol != "USDC" || decimals != 6 {
		t.Errorf("symbol/decimals = %s/%d, want USDC/6", symbol, decimals)
	}

	// Re-upserting returns the same ids.
	var second map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		second, err = repo.GetOrCreateAssetTokens(ctx, tx, 1, []outbound.MapleAssetToken{usdc, usdt})
		return err
	})
	if second[usdc.Address] != first[usdc.Address] || second[usdt.Address] != first[usdt.Address] {
		t.Errorf("ids changed across upserts: %v vs %v", first, second)
	}

	// An existing token created by a migration or on-chain indexer keeps its
	// created_at_block, symbol, and decimals (the shared GetOrCreateTokens
	// LEAST() merge would have clobbered created_at_block to 0). A differing
	// API symbol only warns (the registry may hold a canonical symbol), so the
	// upsert still succeeds and the stored symbol wins.
	existing := common.BytesToAddress(mapleAddr(0xe3))
	var existingID int64
	if err := maplePool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
		 VALUES (1, $1, 'WETH', 18, 12345, '{}'::jsonb, NOW()) RETURNING id`,
		existing.Bytes()).Scan(&existingID); err != nil {
		t.Fatalf("seeding existing token: %v", err)
	}

	var third map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		third, err = repo.GetOrCreateAssetTokens(ctx, tx, 1,
			[]outbound.MapleAssetToken{{Address: existing, Symbol: "DIFFERENT", Decimals: 18}})
		return err
	})
	if third[existing] != existingID {
		t.Errorf("existing token id = %d, want %d", third[existing], existingID)
	}
	var preservedCAB int64
	var preservedSymbol string
	var preservedDecimals int16
	if err := maplePool.QueryRow(ctx,
		`SELECT created_at_block, symbol, decimals FROM token WHERE id = $1`,
		existingID).Scan(&preservedCAB, &preservedSymbol, &preservedDecimals); err != nil {
		t.Fatalf("querying preserved token: %v", err)
	}
	if preservedCAB != 12345 || preservedSymbol != "WETH" || preservedDecimals != 18 {
		t.Errorf("token = %d/%s/%d, want 12345/WETH/18 (must not be clobbered)",
			preservedCAB, preservedSymbol, preservedDecimals)
	}
}

func TestMapleGetOrCreateAssetTokens_DecimalsDriftFails(t *testing.T) {
	// Decimals are immutable and safety-critical (they scale every USD
	// computation), so a stored value differing from the API's must fail the
	// call instead of returning an id keyed to stale scaling.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)

	existing := common.BytesToAddress(mapleAddr(0xe4))
	if _, err := maplePool.Exec(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, metadata, updated_at)
		 VALUES (1, $1, 'USDC', 6, '{}'::jsonb, NOW())`,
		existing.Bytes()); err != nil {
		t.Fatalf("seeding existing token: %v", err)
	}

	tx, err := maplePool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, driftErr := repo.GetOrCreateAssetTokens(ctx, tx, 1,
		[]outbound.MapleAssetToken{{Address: existing, Symbol: "USDC", Decimals: 18}})
	if driftErr == nil {
		t.Fatal("expected decimals-drift error, got nil")
	}
	if !strings.Contains(driftErr.Error(), "decimals changed") {
		t.Errorf("error %q should report the decimals drift", driftErr.Error())
	}
}

func TestMapleUpsertPools_RoundTripAndRefresh(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)

	var ids map[common.Address]int64
	var poolA *maple.Pool
	var usdtTokenID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		usdcTokenID := upsertTestAssetToken(t, ctx, repo, tx, 0xee, "USDC")
		usdtTokenID = upsertTestAssetToken(t, ctx, repo, tx, 0xef, "USDT")

		var err error
		poolA, err = maple.NewPool(1, protocolID, mapleAddr(0x10), "Pool A", usdcTokenID, false)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		poolB, err := maple.NewPool(1, protocolID, mapleAddr(0x11), "Pool B", usdtTokenID, true)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		ids, err = repo.UpsertPools(ctx, tx, []*maple.Pool{poolA, poolB})
		return err
	})
	if len(ids) != 2 {
		t.Fatalf("len(ids) = %d, want 2", len(ids))
	}

	// Re-upsert with changed name/is_syrup/asset refreshes and keeps the
	// same id.
	poolA.Name = "Pool A renamed"
	poolA.IsSyrup = true
	poolA.AssetTokenID = usdtTokenID
	var again map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		again, err = repo.UpsertPools(ctx, tx, []*maple.Pool{poolA})
		return err
	})
	if again[common.BytesToAddress(poolA.Address)] != ids[common.BytesToAddress(poolA.Address)] {
		t.Errorf("pool id changed on re-upsert")
	}

	var name string
	var isSyrup bool
	var assetTokenID int64
	if err := maplePool.QueryRow(ctx,
		`SELECT name, is_syrup, asset_token_id FROM maple_pool WHERE chain_id = 1 AND address = $1`,
		poolA.Address).Scan(&name, &isSyrup, &assetTokenID); err != nil {
		t.Fatalf("querying pool: %v", err)
	}
	if name != "Pool A renamed" || !isSyrup || assetTokenID != usdtTokenID {
		t.Errorf("name/is_syrup/asset_token_id = %q/%v/%d, want refreshed values", name, isSyrup, assetTokenID)
	}
}

func TestMaplePoolStates_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x20)

	state, err := maple.NewPoolState(maple.PoolStateParams{
		PoolID: poolID, SyncedAt: mapleSyncedAt(),
		TVL: big.NewInt(1000), LiquidAssets: big.NewInt(400), CollateralValueUSD: big.NewInt(500),
		PrincipalOut: big.NewInt(600), MonthlyAPY: big.NewInt(123),
	})
	if err != nil {
		t.Fatalf("NewPoolState: %v", err)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, []*maple.PoolState{state})
	})

	var tvl, utilization string
	var spotAPY *string
	if err := maplePool.QueryRow(ctx,
		`SELECT tvl::text, utilization::text, spot_apy::text FROM maple_pool_state WHERE maple_pool_id = $1`,
		poolID).Scan(&tvl, &utilization, &spotAPY); err != nil {
		t.Fatalf("querying pool state: %v", err)
	}
	if tvl != "1000" {
		t.Errorf("tvl = %s, want 1000", tvl)
	}
	if utilization != "0.6" {
		t.Errorf("utilization = %s, want 0.6", utilization)
	}
	if spotAPY != nil {
		t.Errorf("spot_apy = %v, want NULL", *spotAPY)
	}
}

func TestMaplePoolStates_NullTVLAndCollateralValueRoundTrip(t *testing.T) {
	// tvl and collateralValue are nullable in the Maple API schema; nil
	// entity values persist as SQL NULL.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x21)

	state, err := maple.NewPoolState(maple.PoolStateParams{
		PoolID: poolID, SyncedAt: mapleSyncedAt(),
		LiquidAssets: big.NewInt(400), PrincipalOut: big.NewInt(600),
	})
	if err != nil {
		t.Fatalf("NewPoolState: %v", err)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, []*maple.PoolState{state})
	})

	var tvl, collateralValueUSD *string
	var utilization string
	if err := maplePool.QueryRow(ctx,
		`SELECT tvl::text, collateral_value_usd::text, utilization::text FROM maple_pool_state WHERE maple_pool_id = $1`,
		poolID).Scan(&tvl, &collateralValueUSD, &utilization); err != nil {
		t.Fatalf("querying pool state: %v", err)
	}
	if tvl != nil {
		t.Errorf("tvl = %v, want NULL", *tvl)
	}
	if collateralValueUSD != nil {
		t.Errorf("collateral_value_usd = %v, want NULL", *collateralValueUSD)
	}
	if utilization != "0.6" {
		t.Errorf("utilization = %s, want 0.6", utilization)
	}
}

func TestMaplePoolStates_DedupWarnsOnConflict(t *testing.T) {
	// Re-inserting the same state at the same synced_at and build dedupes
	// via the processing-version trigger + ON CONFLICT DO NOTHING (the
	// Temporal-retry path) and must be surfaced by the RowsAffected warn.
	ctx := context.Background()
	truncateMaple(t, ctx)
	recorder := &testutil.SlogRecorder{}
	repo, err := NewMapleGraphQLRepository(maplePool, slog.New(recorder), 0, 0)
	if err != nil {
		t.Fatalf("NewMapleGraphQLRepository: %v", err)
	}
	poolID := upsertTestPool(t, ctx, repo, 0x23)

	state, err := maple.NewPoolState(maple.PoolStateParams{
		PoolID: poolID, SyncedAt: mapleSyncedAt(),
		TVL: big.NewInt(1000), LiquidAssets: big.NewInt(400),
		CollateralValueUSD: big.NewInt(500), PrincipalOut: big.NewInt(600),
	})
	if err != nil {
		t.Fatalf("NewPoolState: %v", err)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, []*maple.PoolState{state})
	})
	if got := recorder.CountWarn("deduplicated"); got != 0 {
		t.Fatalf("dedup warn fired %d times on first insert, want 0", got)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, []*maple.PoolState{state})
	})
	if got := recorder.CountWarn("deduplicated"); got != 1 {
		t.Errorf("dedup warn fired %d times after duplicate insert, want 1", got)
	}

	var count int
	if err := maplePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_pool_state WHERE maple_pool_id = $1`, poolID).Scan(&count); err != nil {
		t.Fatalf("counting pool states: %v", err)
	}
	if count != 1 {
		t.Errorf("pool state count = %d, want 1 (duplicate must dedup)", count)
	}
}

func TestMaplePoolStates_PartialDedupFailsAndRollsBack(t *testing.T) {
	// A batch where one row collides (same pool, synced_at, and build) while
	// a sibling does not must fail instead of committing: committing would
	// silently drop the collided row from the snapshot.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x24)

	first, err := maple.NewPoolState(maple.PoolStateParams{
		PoolID: poolID, SyncedAt: mapleSyncedAt(),
		TVL: big.NewInt(1000), LiquidAssets: big.NewInt(400),
		CollateralValueUSD: big.NewInt(500), PrincipalOut: big.NewInt(600),
	})
	if err != nil {
		t.Fatalf("NewPoolState: %v", err)
	}
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, []*maple.PoolState{first})
	})

	fresh, err := maple.NewPoolState(maple.PoolStateParams{
		PoolID: poolID, SyncedAt: mapleSyncedAt().Add(time.Minute),
		TVL: big.NewInt(1100), LiquidAssets: big.NewInt(450),
		CollateralValueUSD: big.NewInt(550), PrincipalOut: big.NewInt(650),
	})
	if err != nil {
		t.Fatalf("NewPoolState: %v", err)
	}

	tx, err := maplePool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	saveErr := repo.SavePoolStates(ctx, tx, []*maple.PoolState{first, fresh})
	if saveErr == nil {
		t.Fatal("expected partial-dedup error, got nil")
	}
	if !strings.Contains(saveErr.Error(), "partially deduplicated") {
		t.Errorf("error %q should report the partial dedup", saveErr.Error())
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	var count int
	if err := maplePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_pool_state WHERE maple_pool_id = $1`, poolID).Scan(&count); err != nil {
		t.Fatalf("counting pool states: %v", err)
	}
	if count != 1 {
		t.Errorf("pool state count = %d, want 1 (partial dedup must roll back the batch)", count)
	}
}

func TestMaplePoolStates_PartialDedupAcrossChunksFails(t *testing.T) {
	// batchSize 1 puts the collided row and the fresh rows in separate
	// chunks; the dedup check must judge the whole save, not each chunk —
	// per-chunk it would read as one full dedup plus clean inserts.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo, err := NewMapleGraphQLRepository(maplePool, nil, 0, 1)
	if err != nil {
		t.Fatalf("NewMapleGraphQLRepository: %v", err)
	}
	poolID := upsertTestPool(t, ctx, repo, 0x25)

	newState := func(offset time.Duration) *maple.PoolState {
		t.Helper()
		state, err := maple.NewPoolState(maple.PoolStateParams{
			PoolID: poolID, SyncedAt: mapleSyncedAt().Add(offset),
			TVL: big.NewInt(1000), LiquidAssets: big.NewInt(400),
			CollateralValueUSD: big.NewInt(500), PrincipalOut: big.NewInt(600),
		})
		if err != nil {
			t.Fatalf("NewPoolState: %v", err)
		}
		return state
	}

	first := newState(0)
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, []*maple.PoolState{first})
	})

	inMapleTxExpectErr(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, []*maple.PoolState{first, newState(time.Minute), newState(2 * time.Minute)})
	})

	var count int
	if err := maplePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_pool_state WHERE maple_pool_id = $1`, poolID).Scan(&count); err != nil {
		t.Fatalf("counting pool states: %v", err)
	}
	if count != 1 {
		t.Errorf("pool state count = %d, want 1 (cross-chunk partial dedup must roll back)", count)
	}
}

func TestMaplePoolStates_MultiChunkBatch(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)

	// batchSize 2 with 5 states exercises the chunked-insert path (3 chunks,
	// the last one partial) that the default batch size of 1000 never hits.
	repo, err := NewMapleGraphQLRepository(maplePool, nil, 0, 2)
	if err != nil {
		t.Fatalf("NewMapleGraphQLRepository: %v", err)
	}
	poolID := upsertTestPool(t, ctx, repo, 0x22)

	const stateCount = 5
	states := make([]*maple.PoolState, 0, stateCount)
	for i := range stateCount {
		state, err := maple.NewPoolState(maple.PoolStateParams{
			PoolID: poolID, SyncedAt: mapleSyncedAt().Add(time.Duration(i) * time.Minute),
			TVL: big.NewInt(1000), LiquidAssets: big.NewInt(400),
			CollateralValueUSD: big.NewInt(500), PrincipalOut: big.NewInt(600),
		})
		if err != nil {
			t.Fatalf("NewPoolState: %v", err)
		}
		states = append(states, state)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SavePoolStates(ctx, tx, states)
	})

	var count int
	if err := maplePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_pool_state WHERE maple_pool_id = $1`, poolID).Scan(&count); err != nil {
		t.Fatalf("counting pool states: %v", err)
	}
	if count != stateCount {
		t.Errorf("pool state rows = %d, want %d", count, stateCount)
	}
}

func TestMapleLoans_FullRoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x21)

	internalLoanID := upsertTestLoan(t, ctx, repo, poolID, 0x30, &maple.LoanMeta{Type: "amm", DexName: "Uniswap"})
	externalLoanID := upsertTestLoan(t, ctx, repo, poolID, 0x31, nil)

	// is_internal generated column follows loan_meta_type.
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT is_internal FROM maple_loan WHERE id = $1`, internalLoanID).Scan(&isInternal); err != nil {
		t.Fatalf("querying loan: %v", err)
	}
	if !isInternal {
		t.Error("internal loan is_internal = false, want true")
	}
	if err := maplePool.QueryRow(ctx,
		`SELECT is_internal FROM maple_loan WHERE id = $1`, externalLoanID).Scan(&isInternal); err != nil {
		t.Fatalf("querying loan: %v", err)
	}
	if isInternal {
		t.Error("external loan is_internal = true, want false")
	}

	// States: one with acmRatio, one with NULL acmRatio (live API shape for
	// uncollateralized loans). Collateral row only for the first.
	withACM, err := maple.NewLoanState(internalLoanID, mapleSyncedAt(), "Active", big.NewInt(100), big.NewInt(1445731))
	if err != nil {
		t.Fatalf("NewLoanState: %v", err)
	}
	withoutACM, err := maple.NewLoanState(externalLoanID, mapleSyncedAt(), "Active", big.NewInt(200), nil)
	if err != nil {
		t.Fatalf("NewLoanState: %v", err)
	}
	collateral, err := maple.NewLoanCollateral(maple.LoanCollateralParams{
		LoanID: internalLoanID, SyncedAt: mapleSyncedAt(), AssetSymbol: "BTC",
		AssetAmount: big.NewInt(21510), AssetDecimals: 8, AssetValueUSD: big.NewInt(6357500000),
		State: "Deposited", Custodian: "ANCHORAGE", LiquidationLevel: big.NewInt(1020000),
	})
	if err != nil {
		t.Fatalf("NewLoanCollateral: %v", err)
	}
	// Pending collateral (live API shape during DepositPending): null amounts
	// round-trip as SQL NULL instead of dropping the row.
	pendingCollateral, err := maple.NewLoanCollateral(maple.LoanCollateralParams{
		LoanID: externalLoanID, SyncedAt: mapleSyncedAt(), AssetSymbol: "SOL",
		AssetDecimals: 9, State: "DepositPending", Custodian: "ANCHORAGE",
	})
	if err != nil {
		t.Fatalf("NewLoanCollateral (pending): %v", err)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		if err := repo.SaveLoanStates(ctx, tx, []*maple.LoanState{withACM, withoutACM}); err != nil {
			return err
		}
		return repo.SaveLoanCollaterals(ctx, tx, []*maple.LoanCollateral{collateral, pendingCollateral})
	})

	var acm *string
	if err := maplePool.QueryRow(ctx,
		`SELECT acm_ratio::text FROM maple_loan_state WHERE maple_loan_id = $1`, externalLoanID).Scan(&acm); err != nil {
		t.Fatalf("querying loan state: %v", err)
	}
	if acm != nil {
		t.Errorf("acm_ratio = %v, want NULL", *acm)
	}

	var collateralCount int
	if err := maplePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_loan_collateral`).Scan(&collateralCount); err != nil {
		t.Fatalf("counting collaterals: %v", err)
	}
	if collateralCount != 2 {
		t.Errorf("collateral count = %d, want 2", collateralCount)
	}

	var pendingAmount, pendingValue *string
	var pendingState string
	if err := maplePool.QueryRow(ctx,
		`SELECT asset_amount::text, asset_value_usd::text, state FROM maple_loan_collateral WHERE maple_loan_id = $1`,
		externalLoanID).Scan(&pendingAmount, &pendingValue, &pendingState); err != nil {
		t.Fatalf("querying pending collateral: %v", err)
	}
	if pendingAmount != nil || pendingValue != nil {
		t.Errorf("pending collateral amounts = %v/%v, want NULL/NULL", pendingAmount, pendingValue)
	}
	if pendingState != "DepositPending" {
		t.Errorf("pending collateral state = %s, want DepositPending", pendingState)
	}

	var custodian string
	var liquidationLevel string
	if err := maplePool.QueryRow(ctx,
		`SELECT custodian, liquidation_level::text FROM maple_loan_collateral WHERE maple_loan_id = $1`,
		internalLoanID).Scan(&custodian, &liquidationLevel); err != nil {
		t.Fatalf("querying collateral: %v", err)
	}
	if custodian != "ANCHORAGE" || liquidationLevel != "1020000" {
		t.Errorf("custodian/liquidation = %s/%s", custodian, liquidationLevel)
	}
}

func TestMapleUpsertLoans_RefreshesMeta(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x22)

	loanID := upsertTestLoan(t, ctx, repo, poolID, 0x32, nil)

	// Same loan reappears with meta — columns refresh, id stable.
	sameID := upsertTestLoan(t, ctx, repo, poolID, 0x32, &maple.LoanMeta{Type: "strategy", Location: "base"})
	if sameID != loanID {
		t.Fatalf("loan id changed on re-upsert: %d vs %d", sameID, loanID)
	}

	var metaType, location *string
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT loan_meta_type, loan_meta_location, is_internal FROM maple_loan WHERE id = $1`,
		loanID).Scan(&metaType, &location, &isInternal); err != nil {
		t.Fatalf("querying loan: %v", err)
	}
	if metaType == nil || *metaType != "strategy" || location == nil || *location != "base" {
		t.Errorf("meta not refreshed: type=%v location=%v", metaType, location)
	}
	if !isInternal {
		t.Error("is_internal = false, want true after meta refresh")
	}
}

func TestMapleUpsertLoans_RejectsBorrowerChange(t *testing.T) {
	// A loan contract's borrower is immutable; the upsert never refreshes
	// borrower_user_id and must fail loudly when the API contradicts the
	// stored value instead of silently keeping the stale association.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x24)
	protocolID := mapleProtocolID(t, ctx, repo)

	loanAddr := mapleAddr(0x34)
	upsertWithBorrower := func(borrowerByte byte) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		borrower := common.BytesToAddress(mapleAddr(borrowerByte))
		users, err := repo.GetOrCreateBorrowerUsers(ctx, tx, 1, []common.Address{borrower})
		if err != nil {
			t.Fatalf("GetOrCreateBorrowerUsers: %v", err)
		}
		loan, err := maple.NewLoan(1, protocolID, loanAddr, poolID, users[borrower], nil)
		if err != nil {
			t.Fatalf("NewLoan: %v", err)
		}
		if _, err := repo.UpsertLoans(ctx, tx, []*maple.Loan{loan}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := upsertWithBorrower(0xa1); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	// Same loan, same borrower: fine.
	if err := upsertWithBorrower(0xa1); err != nil {
		t.Fatalf("same-borrower re-upsert: %v", err)
	}
	// Same loan, different borrower: must fail.
	err := upsertWithBorrower(0xa2)
	if err == nil {
		t.Fatal("expected borrower-change error, got nil")
	}
	if !strings.Contains(err.Error(), "borrower changed") {
		t.Errorf("error %q should mention borrower change", err.Error())
	}
}

func TestMapleStates_IdempotencyAndReprocessing(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repoBuild0 := newMapleRepo(t, 0)
	repoBuild9 := newMapleRepo(t, 9)
	poolID := upsertTestPool(t, ctx, repoBuild0, 0x23)
	loanID := upsertTestLoan(t, ctx, repoBuild0, poolID, 0x33, nil)

	newState := func(principal int64) *maple.LoanState {
		s, err := maple.NewLoanState(loanID, mapleSyncedAt(), "Active", big.NewInt(principal), big.NewInt(1))
		if err != nil {
			t.Fatalf("NewLoanState: %v", err)
		}
		return s
	}

	// Same build twice: trigger reuses the version, conflict dedupes.
	for range 2 {
		inMapleTx(t, ctx, func(tx pgx.Tx) error {
			return repoBuild0.SaveLoanStates(ctx, tx, []*maple.LoanState{newState(100)})
		})
	}
	var count int
	if err := maplePool.QueryRow(ctx, `SELECT COUNT(*) FROM maple_loan_state`).Scan(&count); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after same-build retry = %d, want 1", count)
	}

	// Different build: trigger assigns processing_version 1.
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repoBuild9.SaveLoanStates(ctx, tx, []*maple.LoanState{newState(200)})
	})

	rows, err := maplePool.Query(ctx,
		`SELECT processing_version, build_id, principal_owed::text FROM maple_loan_state ORDER BY processing_version`)
	if err != nil {
		t.Fatalf("querying versions: %v", err)
	}
	defer rows.Close()

	type versionRow struct {
		version   int
		buildID   int
		principal string
	}
	var got []versionRow
	for rows.Next() {
		var vr versionRow
		if err := rows.Scan(&vr.version, &vr.buildID, &vr.principal); err != nil {
			t.Fatalf("scanning: %v", err)
		}
		got = append(got, vr)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating: %v", err)
	}
	want := []versionRow{{0, 0, "100"}, {1, 9, "200"}}
	if len(got) != len(want) {
		t.Fatalf("rows = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestMapleSkyStrategies_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x24)

	strategy, err := maple.NewSkyStrategy(1, mapleAddr(0x40), poolID, 100)
	if err != nil {
		t.Fatalf("NewSkyStrategy: %v", err)
	}

	var ids map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		ids, err = repo.UpsertSkyStrategies(ctx, tx, []*maple.SkyStrategy{strategy})
		return err
	})
	strategyID := ids[common.BytesToAddress(strategy.StrategyAddress)]
	if strategyID == 0 {
		t.Fatal("strategy id not resolved")
	}

	// Version refresh keeps the id.
	strategy.Version = 200
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		ids, err = repo.UpsertSkyStrategies(ctx, tx, []*maple.SkyStrategy{strategy})
		return err
	})
	if ids[common.BytesToAddress(strategy.StrategyAddress)] != strategyID {
		t.Error("strategy id changed on re-upsert")
	}
	var version int
	if err := maplePool.QueryRow(ctx,
		`SELECT version FROM maple_sky_strategy WHERE id = $1`, strategyID).Scan(&version); err != nil {
		t.Fatalf("querying strategy: %v", err)
	}
	if version != 200 {
		t.Errorf("version = %d, want 200", version)
	}

	state, err := maple.NewSkyStrategyState(maple.SkyStrategyStateParams{
		SkyStrategyID: strategyID, SyncedAt: mapleSyncedAt(), State: "Active",
		CurrentlyDeployed: big.NewInt(0), DepositedAssets: big.NewInt(100), WithdrawnAssets: big.NewInt(50),
	})
	if err != nil {
		t.Fatalf("NewSkyStrategyState: %v", err)
	}
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SaveSkyStrategyStates(ctx, tx, []*maple.SkyStrategyState{state})
	})

	var deposited string
	var feeRate *string
	if err := maplePool.QueryRow(ctx,
		`SELECT deposited_assets::text, strategy_fee_rate::text FROM maple_sky_strategy_state WHERE maple_sky_strategy_id = $1`,
		strategyID).Scan(&deposited, &feeRate); err != nil {
		t.Fatalf("querying strategy state: %v", err)
	}
	if deposited != "100" || feeRate != nil {
		t.Errorf("deposited/feeRate = %s/%v", deposited, feeRate)
	}
}

func TestMapleSyrupGlobalState_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)

	apy, _ := new(big.Int).SetString("46314953537216910976747498327", 10)
	state, err := maple.NewSyrupGlobalState(1, mapleSyncedAt(),
		big.NewInt(3563135115920200), apy, big.NewInt(1), big.NewInt(2), nil)
	if err != nil {
		t.Fatalf("NewSyrupGlobalState: %v", err)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SaveSyrupGlobalState(ctx, tx, state)
	})

	var gotAPY string
	var drips *string
	if err := maplePool.QueryRow(ctx,
		`SELECT apy::text, drips_yield_boost::text FROM maple_syrup_global_state WHERE chain_id = 1`).Scan(&gotAPY, &drips); err != nil {
		t.Fatalf("querying syrup global state: %v", err)
	}
	if gotAPY != apy.String() {
		t.Errorf("apy = %s, want %s (30-decimal value must round-trip exactly)", gotAPY, apy)
	}
	if drips != nil {
		t.Errorf("drips_yield_boost = %v, want NULL", *drips)
	}

	// Nil state is a hard error.
	inMapleTxExpectErr(t, ctx, func(tx pgx.Tx) error {
		return repo.SaveSyrupGlobalState(ctx, tx, nil)
	})
}

// inMapleTxExpectErr runs fn inside a transaction and requires it to fail.
func inMapleTxExpectErr(t *testing.T, ctx context.Context, fn func(tx pgx.Tx) error) {
	t.Helper()
	tx, err := maplePool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err == nil {
		t.Fatal("expected error, got nil")
	}
}
