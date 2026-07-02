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
		`maple_ftl_loan_state`,
		`maple_pool_state`,
		`maple_sky_strategy_state`,
		`maple_syrup_global_state`,
		`maple_loan_meta`,
		`maple_sky_strategy_meta`,
		`maple_pool_meta`,
		`maple_loan`,
		`maple_ftl_loan`,
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
// token row if needed. Pool asset tokens carry no block context, so the row
// is inserted with a NULL created_at_block (matching the maple service path).
func upsertTestAssetToken(t *testing.T, ctx context.Context, tx pgx.Tx, addrByte byte, symbol string) int64 {
	t.Helper()
	var id int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, metadata, updated_at)
		 VALUES (1, $1, $2, 6, '{}'::jsonb, NOW())
		 ON CONFLICT (chain_id, address) DO UPDATE SET id = token.id
		 RETURNING id`,
		mapleAddr(addrByte), symbol).Scan(&id); err != nil {
		t.Fatalf("seeding asset token: %v", err)
	}
	return id
}

// upsertTestBorrowerUser resolves a user id for a borrower, creating the row
// (with a NULL first_seen_block) if needed.
func upsertTestBorrowerUser(t *testing.T, ctx context.Context, tx pgx.Tx, addrByte byte) int64 {
	t.Helper()
	var id int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, created_at, updated_at, metadata)
		 VALUES (1, $1, NOW(), NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET id = "user".id
		 RETURNING id`,
		mapleAddr(addrByte)).Scan(&id); err != nil {
		t.Fatalf("seeding borrower user: %v", err)
	}
	return id
}

func recordTestPool(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, addrByte byte) int64 {
	t.Helper()
	protocolID := mapleProtocolID(t, ctx, repo)

	var ids map[common.Address]int64
	var poolAddr common.Address
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		assetTokenID := upsertTestAssetToken(t, ctx, tx, 0xee, "USDC")
		pool, err := maple.NewPool(1, protocolID, mapleAddr(addrByte), "Test Pool", assetTokenID, true)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		poolAddr = common.BytesToAddress(pool.Address)
		ids, err = repo.RecordPools(ctx, tx, mapleSyncedAt(), []*maple.Pool{pool})
		return err
	})
	id, ok := ids[poolAddr]
	if !ok {
		t.Fatalf("pool id missing from map: %v", ids)
	}
	return id
}

func recordTestLoan(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, poolID int64, addrByte byte, meta *maple.LoanMeta) int64 {
	t.Helper()
	protocolID := mapleProtocolID(t, ctx, repo)

	var loanID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		borrowerID := upsertTestBorrowerUser(t, ctx, tx, 0xab)
		loan, err := maple.NewLoan(1, protocolID, mapleAddr(addrByte), poolID, borrowerID, meta)
		if err != nil {
			return err
		}
		ids, err := repo.RecordLoans(ctx, tx, mapleSyncedAt(), []*maple.Loan{loan})
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

func TestMapleRecordPools_RoundTripAndNoOp(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)

	var ids map[common.Address]int64
	var poolA *maple.Pool
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		usdcTokenID := upsertTestAssetToken(t, ctx, tx, 0xee, "USDC")

		var err error
		poolA, err = maple.NewPool(1, protocolID, mapleAddr(0x10), "Pool A", usdcTokenID, false)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		poolB, err := maple.NewPool(1, protocolID, mapleAddr(0x11), "Pool B", usdcTokenID, true)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		ids, err = repo.RecordPools(ctx, tx, mapleSyncedAt(), []*maple.Pool{poolA, poolB})
		return err
	})
	if len(ids) != 2 {
		t.Fatalf("len(ids) = %d, want 2", len(ids))
	}

	// Re-record with all values unchanged is a clean no-op that keeps the same
	// id and appends no new satellite row (the hashdiff matches the latest).
	var again map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		again, err = repo.RecordPools(ctx, tx, mapleSyncedAt(), []*maple.Pool{poolA})
		return err
	})
	if again[common.BytesToAddress(poolA.Address)] != ids[common.BytesToAddress(poolA.Address)] {
		t.Errorf("pool id changed on unchanged re-record")
	}

	var metaRows int
	if err := maplePool.QueryRow(ctx,
		`SELECT count(*) FROM maple_pool_meta WHERE maple_pool_id = $1`,
		ids[common.BytesToAddress(poolA.Address)]).Scan(&metaRows); err != nil {
		t.Fatalf("counting satellite rows: %v", err)
	}
	if metaRows != 1 {
		t.Errorf("maple_pool_meta rows = %d, want 1 (unchanged re-record must not append)", metaRows)
	}

	// The current editorial values resolve through the convenience view.
	var name string
	var isSyrup bool
	if err := maplePool.QueryRow(ctx,
		`SELECT name, is_syrup FROM maple_pool_current WHERE chain_id = 1 AND address = $1`,
		poolA.Address).Scan(&name, &isSyrup); err != nil {
		t.Fatalf("querying pool: %v", err)
	}
	if name != "Pool A" || isSyrup {
		t.Errorf("name/is_syrup = %q/%v, want unchanged Pool A/false", name, isSyrup)
	}
}

func TestMapleRecordPools_RejectsIdentityChange(t *testing.T) {
	// Identity columns (protocol_id, asset_token_id) are immutable; a re-record
	// changing one must fail the run naming the field, never silently move it.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)

	var usdcID, usdtID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		usdcID = upsertTestAssetToken(t, ctx, tx, 0xee, "USDC")
		usdtID = upsertTestAssetToken(t, ctx, tx, 0xef, "USDT")
		return nil
	})

	baseline, err := maple.NewPool(1, protocolID, mapleAddr(0x10), "Pool A", usdcID, false)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	record := func(p *maple.Pool) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := repo.RecordPools(ctx, tx, mapleSyncedAt(), []*maple.Pool{p}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := record(baseline); err != nil {
		t.Fatalf("baseline record: %v", err)
	}

	changed := *baseline
	changed.AssetTokenID = usdtID
	err = record(&changed)
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "asset_token_id") || !strings.Contains(err.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming asset_token_id", err.Error())
	}
}

func TestMapleRecordPools_AppendsEditorialChange(t *testing.T) {
	// Editorial columns (name, is_syrup) are versioned, not immutable: a change
	// appends a new satellite row at the new synced_at; the hub id is unchanged
	// and the view resolves the latest values.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)

	var usdcID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		usdcID = upsertTestAssetToken(t, ctx, tx, 0xee, "USDC")
		return nil
	})

	t1 := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	v1, err := maple.NewPool(1, protocolID, mapleAddr(0x10), "Pool A", usdcID, false)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	var poolID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		ids, err := repo.RecordPools(ctx, tx, t1, []*maple.Pool{v1})
		poolID = ids[common.BytesToAddress(v1.Address)]
		return err
	})

	v2 := *v1
	v2.Name = "Pool A renamed"
	v2.IsSyrup = true
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		_, err := repo.RecordPools(ctx, tx, t2, []*maple.Pool{&v2})
		return err
	})

	var rows int
	if err := maplePool.QueryRow(ctx,
		`SELECT count(*) FROM maple_pool_meta WHERE maple_pool_id = $1`, poolID).Scan(&rows); err != nil {
		t.Fatalf("counting satellite rows: %v", err)
	}
	if rows != 2 {
		t.Fatalf("maple_pool_meta rows = %d, want 2 (one per editorial version)", rows)
	}

	// The view resolves the latest version.
	var name string
	var isSyrup bool
	if err := maplePool.QueryRow(ctx,
		`SELECT name, is_syrup FROM maple_pool_current WHERE id = $1`, poolID).Scan(&name, &isSyrup); err != nil {
		t.Fatalf("querying view: %v", err)
	}
	if name != "Pool A renamed" || !isSyrup {
		t.Errorf("current name/is_syrup = %q/%v, want renamed/true", name, isSyrup)
	}

	// The as-of read at t1 still resolves the original values.
	var asOfName string
	if err := maplePool.QueryRow(ctx,
		`SELECT name FROM maple_pool_meta WHERE maple_pool_id = $1 AND synced_at <= $2 ORDER BY synced_at DESC LIMIT 1`,
		poolID, t1).Scan(&asOfName); err != nil {
		t.Fatalf("as-of query: %v", err)
	}
	if asOfName != "Pool A" {
		t.Errorf("as-of(t1) name = %q, want Pool A", asOfName)
	}
}

func TestMapleSatellite_BackfillRecipeMatchesGo(t *testing.T) {
	// The migration backfill computes hashdiff in SQL; the repo computes it in
	// Go (metaHashdiff). They must be byte-identical or the first post-deploy
	// sync would see a false change and append a duplicate satellite row per
	// hub. This asserts the stored (Go) hashdiff equals the SQL backfill recipe
	// for every satellite, covering each editorial encoding: pool (bool), loan
	// (7-field *string null-sentinel) and strategy (int::text) — the recipe
	// strings below are byte-for-byte the ones in the migration's backfill.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)

	var poolID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		assetID := upsertTestAssetToken(t, ctx, tx, 0xee, "USDC")
		pool, err := maple.NewPool(1, protocolID, mapleAddr(0x10), "Recipe Pool", assetID, true)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		ids, err := repo.RecordPools(ctx, tx, mapleSyncedAt(), []*maple.Pool{pool})
		poolID = ids[common.BytesToAddress(pool.Address)]
		return err
	})

	// Meta mixes a populated field (Type, Location) with absent ones (asset
	// symbol, dex, wallet_*) so the *string null-sentinel (E'\x1e') path is
	// exercised, not just non-null text.
	loanID := recordTestLoan(t, ctx, repo, poolID, 0x11, &maple.LoanMeta{Type: "strategy", Location: "base"})

	var strategyID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		strategy, err := maple.NewSkyStrategy(1, mapleAddr(0x12), poolID, 100)
		if err != nil {
			t.Fatalf("NewSkyStrategy: %v", err)
		}
		ids, err := repo.RecordSkyStrategies(ctx, tx, mapleSyncedAt(), []*maple.SkyStrategy{strategy})
		strategyID = ids[common.BytesToAddress(strategy.StrategyAddress)]
		return err
	})

	tests := []struct {
		name   string
		recipe string
		id     int64
	}{
		{"pool", `SELECT hashdiff = decode(md5(
		     COALESCE(name, '') || E'\x1f' ||
		     (CASE WHEN is_syrup THEN 'true' ELSE 'false' END)
		 ), 'hex')
		 FROM maple_pool_meta WHERE maple_pool_id = $1`, poolID},
		{"loan", `SELECT hashdiff = decode(md5(
		     loan_type || E'\x1f' ||
		     COALESCE(loan_meta_type, E'\x1e') || E'\x1f' ||
		     COALESCE(loan_meta_asset_symbol, E'\x1e') || E'\x1f' ||
		     COALESCE(loan_meta_dex, E'\x1e') || E'\x1f' ||
		     COALESCE(loan_meta_wallet_address, E'\x1e') || E'\x1f' ||
		     COALESCE(loan_meta_wallet_type, E'\x1e') || E'\x1f' ||
		     COALESCE(loan_meta_location, E'\x1e')
		 ), 'hex')
		 FROM maple_loan_meta WHERE maple_loan_id = $1`, loanID},
		{"strategy", `SELECT hashdiff = decode(md5(
		     COALESCE(version::text, '0')
		 ), 'hex')
		 FROM maple_sky_strategy_meta WHERE maple_sky_strategy_id = $1`, strategyID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var equal bool
			if err := maplePool.QueryRow(ctx, tt.recipe, tt.id).Scan(&equal); err != nil {
				t.Fatalf("comparing hashdiff: %v", err)
			}
			if !equal {
				t.Error("stored Go hashdiff does not match the SQL backfill recipe")
			}
		})
	}
}

func TestMapleSatellite_TombstoneHidesFromCurrentView(t *testing.T) {
	// A tombstone (latest satellite row with is_present = false) drops the
	// entity from the *_current view: "current" means still live.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)

	t1 := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	var poolID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		assetID := upsertTestAssetToken(t, ctx, tx, 0xee, "USDC")
		pool, err := maple.NewPool(1, protocolID, mapleAddr(0x10), "Doomed Pool", assetID, false)
		if err != nil {
			t.Fatalf("NewPool: %v", err)
		}
		ids, err := repo.RecordPools(ctx, tx, t1, []*maple.Pool{pool})
		poolID = ids[common.BytesToAddress(pool.Address)]
		return err
	})

	var visibleBefore int
	if err := maplePool.QueryRow(ctx,
		`SELECT count(*) FROM maple_pool_current WHERE id = $1`, poolID).Scan(&visibleBefore); err != nil {
		t.Fatalf("counting view before: %v", err)
	}
	if visibleBefore != 1 {
		t.Fatalf("pool visible in view before tombstone = %d, want 1", visibleBefore)
	}

	if _, err := maplePool.Exec(ctx,
		`INSERT INTO maple_pool_meta (maple_pool_id, synced_at, name, is_syrup, hashdiff, is_present, build_id)
		 VALUES ($1, $2, 'Doomed Pool', false, decode(md5('tombstone'), 'hex'), false, 0)`,
		poolID, t2); err != nil {
		t.Fatalf("writing tombstone: %v", err)
	}

	var visibleAfter int
	if err := maplePool.QueryRow(ctx,
		`SELECT count(*) FROM maple_pool_current WHERE id = $1`, poolID).Scan(&visibleAfter); err != nil {
		t.Fatalf("counting view after: %v", err)
	}
	if visibleAfter != 0 {
		t.Errorf("pool visible in view after tombstone = %d, want 0", visibleAfter)
	}
}

func TestMaplePoolStates_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x20)

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
	poolID := recordTestPool(t, ctx, repo, 0x21)

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
	poolID := recordTestPool(t, ctx, repo, 0x23)

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
	poolID := recordTestPool(t, ctx, repo, 0x24)

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
	poolID := recordTestPool(t, ctx, repo, 0x25)

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
	poolID := recordTestPool(t, ctx, repo, 0x22)

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
	poolID := recordTestPool(t, ctx, repo, 0x21)

	internalLoanID := recordTestLoan(t, ctx, repo, poolID, 0x30, &maple.LoanMeta{Type: "amm", DexName: "Uniswap"})
	externalLoanID := recordTestLoan(t, ctx, repo, poolID, 0x31, nil)

	// is_internal is derived in the view from the current loan_meta_type.
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT is_internal FROM maple_loan_current WHERE id = $1`, internalLoanID).Scan(&isInternal); err != nil {
		t.Fatalf("querying loan: %v", err)
	}
	if !isInternal {
		t.Error("internal loan is_internal = false, want true")
	}
	if err := maplePool.QueryRow(ctx,
		`SELECT is_internal FROM maple_loan_current WHERE id = $1`, externalLoanID).Scan(&isInternal); err != nil {
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

func TestMapleRecordLoans_NoOpOnUnchanged(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x22)

	meta := &maple.LoanMeta{Type: "strategy", Location: "base"}
	loanID := recordTestLoan(t, ctx, repo, poolID, 0x32, meta)

	// Same loan with all values unchanged is a clean no-op, id stable, and
	// the stored meta is intact (nothing is refreshed).
	sameID := recordTestLoan(t, ctx, repo, poolID, 0x32, meta)
	if sameID != loanID {
		t.Fatalf("loan id changed on unchanged re-record: %d vs %d", sameID, loanID)
	}

	var metaType, location *string
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT loan_meta_type, loan_meta_location, is_internal FROM maple_loan_current WHERE id = $1`,
		loanID).Scan(&metaType, &location, &isInternal); err != nil {
		t.Fatalf("querying loan: %v", err)
	}
	if metaType == nil || *metaType != "strategy" || location == nil || *location != "base" {
		t.Errorf("meta changed: type=%v location=%v, want strategy/base", metaType, location)
	}
	if !isInternal {
		t.Error("is_internal = false, want true")
	}

	var metaRows int
	if err := maplePool.QueryRow(ctx,
		`SELECT count(*) FROM maple_loan_meta WHERE maple_loan_id = $1`, loanID).Scan(&metaRows); err != nil {
		t.Fatalf("counting satellite rows: %v", err)
	}
	if metaRows != 1 {
		t.Errorf("maple_loan_meta rows = %d, want 1 (unchanged re-record must not append)", metaRows)
	}
}

func TestMapleRecordLoans_RejectsPoolReassignment(t *testing.T) {
	// maple_pool_id is immutable identity; a re-record moving the loan to a
	// different pool must fail the run naming the field, not silently reassign.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)
	poolA := recordTestPool(t, ctx, repo, 0x20)
	poolB := recordTestPool(t, ctx, repo, 0x21)

	var borrowerID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		borrowerID = upsertTestBorrowerUser(t, ctx, tx, 0xab)
		return nil
	})

	loanAddr := mapleAddr(0x30)
	record := func(poolID int64, meta *maple.LoanMeta) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		loan, err := maple.NewLoan(1, protocolID, loanAddr, poolID, borrowerID, meta)
		if err != nil {
			t.Fatalf("NewLoan: %v", err)
		}
		if _, err := repo.RecordLoans(ctx, tx, mapleSyncedAt(), []*maple.Loan{loan}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := record(poolA, nil); err != nil {
		t.Fatalf("baseline record: %v", err)
	}
	err := record(poolB, nil)
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "maple_pool_id") || !strings.Contains(err.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming maple_pool_id", err.Error())
	}
}

func TestMapleRecordLoans_AppendsMetaChange(t *testing.T) {
	// loanMeta columns are versioned editorial, not immutable: enrichment
	// (null->value) and clearing (value->null) each append a satellite row at
	// the new synced_at. The as-of read recovers each historical version.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)
	poolID := recordTestPool(t, ctx, repo, 0x20)

	var borrowerID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		borrowerID = upsertTestBorrowerUser(t, ctx, tx, 0xab)
		return nil
	})

	loanAddr := mapleAddr(0x30)
	recordAt := func(syncedAt time.Time, meta *maple.LoanMeta) int64 {
		var loanID int64
		inMapleTx(t, ctx, func(tx pgx.Tx) error {
			loan, err := maple.NewLoan(1, protocolID, loanAddr, poolID, borrowerID, meta)
			if err != nil {
				t.Fatalf("NewLoan: %v", err)
			}
			ids, err := repo.RecordLoans(ctx, tx, syncedAt, []*maple.Loan{loan})
			loanID = ids[common.BytesToAddress(loanAddr)]
			return err
		})
		return loanID
	}

	t1 := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)

	loanID := recordAt(t1, nil)                                    // v1: all meta NULL
	recordAt(t2, &maple.LoanMeta{Type: "amm", Location: "Cayman"}) // v2: enriched
	recordAt(t3, &maple.LoanMeta{Type: "amm"})                     // v3: Location cleared

	var rows int
	if err := maplePool.QueryRow(ctx,
		`SELECT count(*) FROM maple_loan_meta WHERE maple_loan_id = $1`, loanID).Scan(&rows); err != nil {
		t.Fatalf("counting satellite rows: %v", err)
	}
	if rows != 3 {
		t.Fatalf("maple_loan_meta rows = %d, want 3", rows)
	}

	// Current view: latest version (type=amm, location cleared, internal).
	var metaType, location *string
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT loan_meta_type, loan_meta_location, is_internal FROM maple_loan_current WHERE id = $1`,
		loanID).Scan(&metaType, &location, &isInternal); err != nil {
		t.Fatalf("querying view: %v", err)
	}
	if metaType == nil || *metaType != "amm" || location != nil || !isInternal {
		t.Errorf("current = type %v / location %v / internal %v, want amm/NULL/true", metaType, location, isInternal)
	}

	// As-of t2 recovers the enriched-with-location version.
	var asOfLocation *string
	if err := maplePool.QueryRow(ctx,
		`SELECT loan_meta_location FROM maple_loan_meta WHERE maple_loan_id = $1 AND synced_at <= $2 ORDER BY synced_at DESC LIMIT 1`,
		loanID, t2).Scan(&asOfLocation); err != nil {
		t.Fatalf("as-of query: %v", err)
	}
	if asOfLocation == nil || *asOfLocation != "Cayman" {
		t.Errorf("as-of(t2) location = %v, want Cayman", asOfLocation)
	}
}

func TestMapleRecordLoans_NullMetaTypeIsNotInternal(t *testing.T) {
	// Live API drift documented in CLAUDE.md: loanMeta present but type null.
	// The empty Type maps to a NULL loan_meta_type, so the view's derived
	// is_internal stays false while the other meta fields persist.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x33)

	loanID := recordTestLoan(t, ctx, repo, poolID, 0x34, &maple.LoanMeta{Type: "", Location: "Cayman"})

	var metaType, location *string
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT loan_meta_type, loan_meta_location, is_internal FROM maple_loan_current WHERE id = $1`,
		loanID).Scan(&metaType, &location, &isInternal); err != nil {
		t.Fatalf("querying loan: %v", err)
	}
	if metaType != nil {
		t.Errorf("loan_meta_type = %v, want NULL", *metaType)
	}
	if location == nil || *location != "Cayman" {
		t.Errorf("loan_meta_location = %v, want Cayman", location)
	}
	if isInternal {
		t.Error("is_internal = true, want false for null meta type")
	}
}

func TestMapleRecordLoans_RejectsBorrowerChange(t *testing.T) {
	// A loan contract's borrower is immutable; the record never refreshes
	// borrower_user_id and must fail loudly when the API contradicts the
	// stored value instead of silently keeping the stale association.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x24)
	protocolID := mapleProtocolID(t, ctx, repo)

	loanAddr := mapleAddr(0x34)
	recordWithBorrower := func(borrowerByte byte) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		borrowerID := upsertTestBorrowerUser(t, ctx, tx, borrowerByte)
		loan, err := maple.NewLoan(1, protocolID, loanAddr, poolID, borrowerID, nil)
		if err != nil {
			t.Fatalf("NewLoan: %v", err)
		}
		if _, err := repo.RecordLoans(ctx, tx, mapleSyncedAt(), []*maple.Loan{loan}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := recordWithBorrower(0xa1); err != nil {
		t.Fatalf("first record: %v", err)
	}
	// Same loan, same borrower: fine.
	if err := recordWithBorrower(0xa1); err != nil {
		t.Fatalf("same-borrower re-record: %v", err)
	}
	// Same loan, different borrower: must fail.
	err := recordWithBorrower(0xa2)
	if err == nil {
		t.Fatal("expected borrower-change error, got nil")
	}
	if !strings.Contains(err.Error(), "borrower_user_id") || !strings.Contains(err.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming borrower_user_id", err.Error())
	}
}

func TestMapleStates_IdempotencyAndReprocessing(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repoBuild0 := newMapleRepo(t, 0)
	repoBuild9 := newMapleRepo(t, 9)
	poolID := recordTestPool(t, ctx, repoBuild0, 0x23)
	loanID := recordTestLoan(t, ctx, repoBuild0, poolID, 0x33, nil)

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

// upsertTestTokenWithDecimals seeds a token row with explicit decimals and
// returns its id (the FTL collateral/funds FKs need two distinct tokens).
func upsertTestTokenWithDecimals(t *testing.T, ctx context.Context, tx pgx.Tx, addrByte byte, symbol string, decimals int) int64 {
	t.Helper()
	var id int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, metadata, updated_at)
		 VALUES (1, $1, $2, $3, '{}'::jsonb, NOW())
		 ON CONFLICT (chain_id, address) DO UPDATE SET id = token.id
		 RETURNING id`,
		mapleAddr(addrByte), symbol, decimals).Scan(&id); err != nil {
		t.Fatalf("seeding token %s: %v", symbol, err)
	}
	return id
}

// recordTestFTLLoan seeds an FTL registry row and returns its id, resolving the
// pool, borrower, and the two asset tokens it needs.
func recordTestFTLLoan(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, poolID int64, addrByte byte) (loanID, collateralTokenID, fundsTokenID int64) {
	t.Helper()
	protocolID := mapleProtocolID(t, ctx, repo)

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		borrowerID := upsertTestBorrowerUser(t, ctx, tx, 0xac)
		collateralTokenID = upsertTestTokenWithDecimals(t, ctx, tx, 0xc0, "WBTC", 8)
		fundsTokenID = upsertTestTokenWithDecimals(t, ctx, tx, 0xf0, "USDC", 6)
		loan, err := maple.NewFTLLoan(1, protocolID, mapleAddr(addrByte), poolID, borrowerID, collateralTokenID, fundsTokenID)
		if err != nil {
			return err
		}
		ids, err := repo.RecordFixedTermLoans(ctx, tx, []*maple.FTLLoan{loan})
		if err != nil {
			return err
		}
		loanID = ids[common.BytesToAddress(loan.LoanAddress)]
		return nil
	})
	if loanID == 0 {
		t.Fatal("ftl loan id not resolved")
	}
	return loanID, collateralTokenID, fundsTokenID
}

func TestMapleFTLLoans_FullRoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x50)
	loanID, collateralTokenID, fundsTokenID := recordTestFTLLoan(t, ctx, repo, poolID, 0x60)

	// Registry FK columns round-trip.
	var gotCollateral, gotFunds int64
	if err := maplePool.QueryRow(ctx,
		`SELECT collateral_token_id, funds_token_id FROM maple_ftl_loan WHERE id = $1`,
		loanID).Scan(&gotCollateral, &gotFunds); err != nil {
		t.Fatalf("querying ftl loan: %v", err)
	}
	if gotCollateral != collateralTokenID || gotFunds != fundsTokenID {
		t.Errorf("token FKs = %d/%d, want %d/%d", gotCollateral, gotFunds, collateralTokenID, fundsTokenID)
	}

	// A funded loan: full field set, with stateDetail/acmRatio set and both
	// epoch dates present.
	maturity := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	nextDue := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	funded, err := maple.NewFTLLoanState(maple.FTLLoanStateParams{
		LoanID: loanID, SyncedAt: mapleSyncedAt(), State: "Active", StateDetail: "ActiveInArrears",
		PrincipalOwed: big.NewInt(10000000000000), InterestRate: big.NewInt(182000), InterestPaid: big.NewInt(5000),
		PaymentsRemaining: 6, PaymentIntervalDays: 30, TermDays: 180,
		MaturityDate: &maturity, NextPaymentDue: &nextDue,
		CollateralAmount: big.NewInt(21510), CollateralRequired: big.NewInt(20000), CollateralRatio: big.NewInt(1500000),
		DrawdownAmount: big.NewInt(16917002739727), ClaimableAmount: big.NewInt(0),
		AcmRatio: big.NewInt(1445731), IsImpaired: true,
	})
	if err != nil {
		t.Fatalf("NewFTLLoanState (funded): %v", err)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SaveFixedTermLoanStates(ctx, tx, []*maple.FTLLoanState{funded})
	})

	var state, stateDetail string
	var interestRate, collateralRatio, acmRatio string
	var paymentsRemaining, termDays int64
	var maturityDate, nextPaymentDue *time.Time
	var isImpaired bool
	if err := maplePool.QueryRow(ctx,
		`SELECT state, state_detail, interest_rate::text, collateral_ratio::text, acm_ratio::text,
		        payments_remaining, term_days, maturity_date, next_payment_due, is_impaired
		 FROM maple_ftl_loan_state WHERE maple_ftl_loan_id = $1`,
		loanID).Scan(&state, &stateDetail, &interestRate, &collateralRatio, &acmRatio,
		&paymentsRemaining, &termDays, &maturityDate, &nextPaymentDue, &isImpaired); err != nil {
		t.Fatalf("querying ftl loan state: %v", err)
	}
	if state != "Active" || stateDetail != "ActiveInArrears" {
		t.Errorf("state/detail = %s/%s", state, stateDetail)
	}
	if interestRate != "182000" || collateralRatio != "1500000" || acmRatio != "1445731" {
		t.Errorf("rates = %s/%s/%s", interestRate, collateralRatio, acmRatio)
	}
	if paymentsRemaining != 6 || termDays != 180 {
		t.Errorf("counts = %d/%d, want 6/180", paymentsRemaining, termDays)
	}
	if maturityDate == nil || !maturityDate.Equal(maturity) || nextPaymentDue == nil || !nextPaymentDue.Equal(nextDue) {
		t.Errorf("dates = %v/%v, want %v/%v", maturityDate, nextPaymentDue, maturity, nextDue)
	}
	if !isImpaired {
		t.Error("is_impaired = false, want true")
	}
}

func TestMapleFTLLoanStates_PreFundingNullsRoundTrip(t *testing.T) {
	// A pre-funding state (WaitingForAcceptance) reports zero amounts, null
	// stateDetail/acmRatio, and zero epoch dates that map to SQL NULL.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x51)
	loanID, _, _ := recordTestFTLLoan(t, ctx, repo, poolID, 0x61)

	pending, err := maple.NewFTLLoanState(maple.FTLLoanStateParams{
		LoanID: loanID, SyncedAt: mapleSyncedAt(), State: "WaitingForAcceptance", StateDetail: "",
		PrincipalOwed: big.NewInt(0), InterestRate: big.NewInt(0), InterestPaid: big.NewInt(0),
		PaymentsRemaining: 0, PaymentIntervalDays: 0, TermDays: 0,
		MaturityDate: nil, NextPaymentDue: nil,
		CollateralAmount: big.NewInt(0), CollateralRequired: big.NewInt(0), CollateralRatio: big.NewInt(0),
		DrawdownAmount: big.NewInt(0), ClaimableAmount: big.NewInt(0),
		AcmRatio: nil, IsImpaired: false,
	})
	if err != nil {
		t.Fatalf("NewFTLLoanState (pending): %v", err)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SaveFixedTermLoanStates(ctx, tx, []*maple.FTLLoanState{pending})
	})

	var stateDetail, acmRatio *string
	var maturityDate, nextPaymentDue *time.Time
	var principalOwed string
	if err := maplePool.QueryRow(ctx,
		`SELECT state_detail, acm_ratio::text, maturity_date, next_payment_due, principal_owed::text
		 FROM maple_ftl_loan_state WHERE maple_ftl_loan_id = $1`,
		loanID).Scan(&stateDetail, &acmRatio, &maturityDate, &nextPaymentDue, &principalOwed); err != nil {
		t.Fatalf("querying ftl loan state: %v", err)
	}
	if stateDetail != nil || acmRatio != nil {
		t.Errorf("state_detail/acm_ratio = %v/%v, want NULL/NULL", stateDetail, acmRatio)
	}
	if maturityDate != nil || nextPaymentDue != nil {
		t.Errorf("dates = %v/%v, want NULL/NULL", maturityDate, nextPaymentDue)
	}
	if principalOwed != "0" {
		t.Errorf("principal_owed = %s, want 0 (zero is a valid pre-funding value)", principalOwed)
	}
}

func TestMapleRecordFTLLoans_NoOpOnUnchanged(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x52)

	loanID, _, _ := recordTestFTLLoan(t, ctx, repo, poolID, 0x62)
	sameID, _, _ := recordTestFTLLoan(t, ctx, repo, poolID, 0x62)
	if sameID != loanID {
		t.Fatalf("ftl loan id changed on unchanged re-record: %d vs %d", sameID, loanID)
	}
}

func TestMapleRecordFTLLoans_RejectsFieldChange(t *testing.T) {
	// maple_pool_id, borrower_user_id, collateral_token_id and funds_token_id
	// are immutable; a re-record with any changed value must fail naming the
	// field instead of refreshing the row.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)
	poolA := recordTestPool(t, ctx, repo, 0x53)
	poolB := recordTestPool(t, ctx, repo, 0x54)

	var borrowerA, borrowerB, wbtc, usdc, weth int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		borrowerA = upsertTestBorrowerUser(t, ctx, tx, 0xa1)
		borrowerB = upsertTestBorrowerUser(t, ctx, tx, 0xa2)
		wbtc = upsertTestTokenWithDecimals(t, ctx, tx, 0xc0, "WBTC", 8)
		usdc = upsertTestTokenWithDecimals(t, ctx, tx, 0xf0, "USDC", 6)
		weth = upsertTestTokenWithDecimals(t, ctx, tx, 0xf1, "WETH", 18)
		return nil
	})

	loanAddr := mapleAddr(0x63)
	record := func(poolID, borrowerID, collateralToken, fundsToken int64) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		loan, err := maple.NewFTLLoan(1, protocolID, loanAddr, poolID, borrowerID, collateralToken, fundsToken)
		if err != nil {
			t.Fatalf("NewFTLLoan: %v", err)
		}
		if _, err := repo.RecordFixedTermLoans(ctx, tx, []*maple.FTLLoan{loan}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := record(poolA, borrowerA, wbtc, usdc); err != nil {
		t.Fatalf("baseline record: %v", err)
	}
	if err := record(poolA, borrowerA, wbtc, usdc); err != nil {
		t.Fatalf("unchanged re-record: %v", err)
	}

	cases := []struct {
		name                                            string
		poolID, borrowerID, collateralToken, fundsToken int64
		wantField                                       string
	}{
		{"pool reassignment", poolB, borrowerA, wbtc, usdc, "maple_pool_id"},
		{"borrower change", poolA, borrowerB, wbtc, usdc, "borrower_user_id"},
		{"collateral token change", poolA, borrowerA, weth, usdc, "collateral_token_id"},
		{"funds token change", poolA, borrowerA, wbtc, weth, "funds_token_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := record(tc.poolID, tc.borrowerID, tc.collateralToken, tc.fundsToken)
			if err == nil {
				t.Fatal("expected mismatch error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantField) || !strings.Contains(err.Error(), "registry fields changed") {
				t.Errorf("error %q should report a registry mismatch naming %q", err.Error(), tc.wantField)
			}
		})
	}
}

func TestMapleFTLLoanStates_IdempotencyAndReprocessing(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repoBuild0 := newMapleRepo(t, 0)
	repoBuild9 := newMapleRepo(t, 9)
	poolID := recordTestPool(t, ctx, repoBuild0, 0x55)
	loanID, _, _ := recordTestFTLLoan(t, ctx, repoBuild0, poolID, 0x64)

	newState := func(principal int64) *maple.FTLLoanState {
		s, err := maple.NewFTLLoanState(maple.FTLLoanStateParams{
			LoanID: loanID, SyncedAt: mapleSyncedAt(), State: "Active",
			PrincipalOwed: big.NewInt(principal), InterestRate: big.NewInt(1), InterestPaid: big.NewInt(0),
			CollateralAmount: big.NewInt(0), CollateralRequired: big.NewInt(0), CollateralRatio: big.NewInt(0),
			DrawdownAmount: big.NewInt(0), ClaimableAmount: big.NewInt(0),
		})
		if err != nil {
			t.Fatalf("NewFTLLoanState: %v", err)
		}
		return s
	}

	for range 2 {
		inMapleTx(t, ctx, func(tx pgx.Tx) error {
			return repoBuild0.SaveFixedTermLoanStates(ctx, tx, []*maple.FTLLoanState{newState(100)})
		})
	}
	var count int
	if err := maplePool.QueryRow(ctx, `SELECT COUNT(*) FROM maple_ftl_loan_state`).Scan(&count); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if count != 1 {
		t.Fatalf("count after same-build retry = %d, want 1", count)
	}

	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repoBuild9.SaveFixedTermLoanStates(ctx, tx, []*maple.FTLLoanState{newState(200)})
	})

	var maxVersion int
	if err := maplePool.QueryRow(ctx, `SELECT MAX(processing_version) FROM maple_ftl_loan_state`).Scan(&maxVersion); err != nil {
		t.Fatalf("max processing_version: %v", err)
	}
	if maxVersion != 1 {
		t.Errorf("max processing_version = %d, want 1 (new build bumps)", maxVersion)
	}
}

func TestMapleSkyStrategies_RoundTrip(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x24)

	strategy, err := maple.NewSkyStrategy(1, mapleAddr(0x40), poolID, 100)
	if err != nil {
		t.Fatalf("NewSkyStrategy: %v", err)
	}

	var ids map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		ids, err = repo.RecordSkyStrategies(ctx, tx, mapleSyncedAt(), []*maple.SkyStrategy{strategy})
		return err
	})
	strategyID := ids[common.BytesToAddress(strategy.StrategyAddress)]
	if strategyID == 0 {
		t.Fatal("strategy id not resolved")
	}

	// Unchanged re-record is a clean no-op that keeps the id (nothing is
	// refreshed).
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		ids, err = repo.RecordSkyStrategies(ctx, tx, mapleSyncedAt(), []*maple.SkyStrategy{strategy})
		return err
	})
	if ids[common.BytesToAddress(strategy.StrategyAddress)] != strategyID {
		t.Error("strategy id changed on unchanged re-record")
	}
	var version int
	if err := maplePool.QueryRow(ctx,
		`SELECT version FROM maple_sky_strategy_current WHERE id = $1`, strategyID).Scan(&version); err != nil {
		t.Fatalf("querying strategy: %v", err)
	}
	if version != 100 {
		t.Errorf("version = %d, want unchanged 100", version)
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

func TestMapleRecordSkyStrategies_RejectsPoolReassignment(t *testing.T) {
	// maple_pool_id is immutable identity; a re-record moving the strategy to a
	// different pool must fail the run naming the field, not silently reassign.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolA := recordTestPool(t, ctx, repo, 0x26)
	poolB := recordTestPool(t, ctx, repo, 0x27)

	strategyAddr := mapleAddr(0x41)
	record := func(poolID int64, version int) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		strategy, err := maple.NewSkyStrategy(1, strategyAddr, poolID, version)
		if err != nil {
			t.Fatalf("NewSkyStrategy: %v", err)
		}
		if _, err := repo.RecordSkyStrategies(ctx, tx, mapleSyncedAt(), []*maple.SkyStrategy{strategy}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := record(poolA, 100); err != nil {
		t.Fatalf("baseline record: %v", err)
	}
	err := record(poolB, 100)
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "maple_pool_id") || !strings.Contains(err.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming maple_pool_id", err.Error())
	}
}

func TestMapleRecordSkyStrategies_AppendsVersionChange(t *testing.T) {
	// version is versioned editorial (live mutation path: Governor proxy
	// upgrade): a change appends a satellite row at the new synced_at; the view
	// resolves the latest version and the as-of read recovers the prior one.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := recordTestPool(t, ctx, repo, 0x26)

	strategyAddr := mapleAddr(0x41)
	recordAt := func(syncedAt time.Time, version int) int64 {
		var id int64
		inMapleTx(t, ctx, func(tx pgx.Tx) error {
			strategy, err := maple.NewSkyStrategy(1, strategyAddr, poolID, version)
			if err != nil {
				t.Fatalf("NewSkyStrategy: %v", err)
			}
			ids, err := repo.RecordSkyStrategies(ctx, tx, syncedAt, []*maple.SkyStrategy{strategy})
			id = ids[common.BytesToAddress(strategyAddr)]
			return err
		})
		return id
	}

	t1 := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	strategyID := recordAt(t1, 100)
	recordAt(t2, 200)

	var rows int
	if err := maplePool.QueryRow(ctx,
		`SELECT count(*) FROM maple_sky_strategy_meta WHERE maple_sky_strategy_id = $1`, strategyID).Scan(&rows); err != nil {
		t.Fatalf("counting satellite rows: %v", err)
	}
	if rows != 2 {
		t.Fatalf("maple_sky_strategy_meta rows = %d, want 2", rows)
	}

	var version int
	if err := maplePool.QueryRow(ctx,
		`SELECT version FROM maple_sky_strategy_current WHERE id = $1`, strategyID).Scan(&version); err != nil {
		t.Fatalf("querying view: %v", err)
	}
	if version != 200 {
		t.Errorf("current version = %d, want 200", version)
	}

	var asOfVersion int
	if err := maplePool.QueryRow(ctx,
		`SELECT version FROM maple_sky_strategy_meta WHERE maple_sky_strategy_id = $1 AND synced_at <= $2 ORDER BY synced_at DESC LIMIT 1`,
		strategyID, t1).Scan(&asOfVersion); err != nil {
		t.Fatalf("as-of query: %v", err)
	}
	if asOfVersion != 100 {
		t.Errorf("as-of(t1) version = %d, want 100", asOfVersion)
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
