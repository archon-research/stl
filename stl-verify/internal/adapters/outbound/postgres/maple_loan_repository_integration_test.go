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

func upsertTestPool(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, addrByte byte) int64 {
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
		ids, err = repo.UpsertPools(ctx, tx, []*maple.Pool{pool})
		return err
	})
	id, ok := ids[poolAddr]
	if !ok {
		t.Fatalf("pool id missing from map: %v", ids)
	}
	return id
}

func upsertTestLoan(t *testing.T, ctx context.Context, repo *MapleGraphQLRepository, poolID int64, addrByte byte, meta *maple.LoanMeta, syncedAt time.Time) int64 {
	t.Helper()
	protocolID := mapleProtocolID(t, ctx, repo)

	var loanID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		borrowerID := upsertTestBorrowerUser(t, ctx, tx, 0xab)
		loan, err := maple.NewLoan(1, protocolID, mapleAddr(addrByte), poolID, borrowerID, meta)
		if err != nil {
			return err
		}
		ids, err := repo.UpsertLoans(ctx, tx, []*maple.Loan{loan}, syncedAt)
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

func TestMapleUpsertPools_RoundTripAndNoOp(t *testing.T) {
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
		ids, err = repo.UpsertPools(ctx, tx, []*maple.Pool{poolA, poolB})
		return err
	})
	if len(ids) != 2 {
		t.Fatalf("len(ids) = %d, want 2", len(ids))
	}

	// Re-upsert with all values unchanged is a clean no-op that keeps the
	// same id (nothing is refreshed).
	var again map[common.Address]int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		again, err = repo.UpsertPools(ctx, tx, []*maple.Pool{poolA})
		return err
	})
	if again[common.BytesToAddress(poolA.Address)] != ids[common.BytesToAddress(poolA.Address)] {
		t.Errorf("pool id changed on unchanged re-upsert")
	}

	var name string
	var isSyrup bool
	if err := maplePool.QueryRow(ctx,
		`SELECT name, is_syrup FROM maple_pool WHERE chain_id = 1 AND address = $1`,
		poolA.Address).Scan(&name, &isSyrup); err != nil {
		t.Fatalf("querying pool: %v", err)
	}
	if name != "Pool A" || isSyrup {
		t.Errorf("name/is_syrup = %q/%v, want unchanged Pool A/false", name, isSyrup)
	}
}

func TestMapleUpsertPools_RejectsFieldChange(t *testing.T) {
	// Every pool attribute is immutable; a re-upsert with any changed value
	// must fail the run, naming the field, instead of refreshing the row.
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

	upsert := func(p *maple.Pool) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := repo.UpsertPools(ctx, tx, []*maple.Pool{p}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := upsert(baseline); err != nil {
		t.Fatalf("baseline upsert: %v", err)
	}
	// Unchanged re-upsert is a clean no-op.
	if err := upsert(baseline); err != nil {
		t.Fatalf("unchanged re-upsert: %v", err)
	}

	cases := []struct {
		name      string
		mutate    func(p *maple.Pool)
		wantField string
	}{
		{"name", func(p *maple.Pool) { p.Name = "Pool A renamed" }, "name"},
		{"asset_token_id", func(p *maple.Pool) { p.AssetTokenID = usdtID }, "asset_token_id"},
		{"is_syrup", func(p *maple.Pool) { p.IsSyrup = true }, "is_syrup"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := *baseline
			tc.mutate(&changed)
			err := upsert(&changed)
			if err == nil {
				t.Fatal("expected mismatch error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantField) || !strings.Contains(err.Error(), "registry fields changed") {
				t.Errorf("error %q should report a registry mismatch naming %q", err.Error(), tc.wantField)
			}
		})
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

	internalLoanID := upsertTestLoan(t, ctx, repo, poolID, 0x30, &maple.LoanMeta{Type: "amm", DexName: "Uniswap"}, mapleSyncedAt())
	externalLoanID := upsertTestLoan(t, ctx, repo, poolID, 0x31, nil, mapleSyncedAt())

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

func TestMapleUpsertLoans_NoOpOnUnchangedMeta(t *testing.T) {
	// Re-reading a loan with identical metadata reuses the existing row: no new
	// version is appended and the same id is returned.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x22)

	meta := &maple.LoanMeta{Type: "strategy", Location: "base"}
	loanID := upsertTestLoan(t, ctx, repo, poolID, 0x32, meta, mapleSyncedAt())

	// A later cycle re-reading the same metadata reuses the row (no new version),
	// even though its synced_at is newer.
	sameID := upsertTestLoan(t, ctx, repo, poolID, 0x32, meta, mapleSyncedAt().Add(time.Hour))
	if sameID != loanID {
		t.Fatalf("unchanged re-upsert appended a new row: %d vs %d", sameID, loanID)
	}

	var count int
	if err := maplePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_loan WHERE chain_id = 1 AND loan_address = $1`,
		mapleAddr(0x32)).Scan(&count); err != nil {
		t.Fatalf("counting loan rows: %v", err)
	}
	if count != 1 {
		t.Errorf("loan row count = %d, want 1 (no new version on unchanged meta)", count)
	}

	var metaType, location *string
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT loan_meta_type, loan_meta_location, is_internal FROM maple_loan WHERE id = $1`,
		loanID).Scan(&metaType, &location, &isInternal); err != nil {
		t.Fatalf("querying loan: %v", err)
	}
	if metaType == nil || *metaType != "strategy" || location == nil || *location != "base" {
		t.Errorf("meta changed: type=%v location=%v, want strategy/base", metaType, location)
	}
	if !isInternal {
		t.Error("is_internal = false, want true")
	}
}

// latestMapleLoanID returns the id of the current (latest) maple_loan version
// for a test loan address.
func latestMapleLoanID(t *testing.T, ctx context.Context, addrByte byte) int64 {
	t.Helper()
	var id int64
	if err := maplePool.QueryRow(ctx,
		`SELECT id FROM maple_loan WHERE chain_id = 1 AND loan_address = $1
		 ORDER BY synced_at DESC, id DESC LIMIT 1`,
		mapleAddr(addrByte)).Scan(&id); err != nil {
		t.Fatalf("latest loan id: %v", err)
	}
	return id
}

// mapleLoanRowCount counts the maple_loan version rows for a test loan address.
func mapleLoanRowCount(t *testing.T, ctx context.Context, addrByte byte) int {
	t.Helper()
	var count int
	if err := maplePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_loan WHERE chain_id = 1 AND loan_address = $1`,
		mapleAddr(addrByte)).Scan(&count); err != nil {
		t.Fatalf("counting loan rows: %v", err)
	}
	return count
}

func TestMapleUpsertLoans_RejectsPoolChange(t *testing.T) {
	// maple_pool_id is strictly immutable; the upsert never refreshes it and a
	// re-upsert reassigning the loan to a different pool must fail the run
	// naming the field, instead of refreshing the row.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)
	poolA := upsertTestPool(t, ctx, repo, 0x20)
	poolB := upsertTestPool(t, ctx, repo, 0x21)

	var borrowerID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		borrowerID = upsertTestBorrowerUser(t, ctx, tx, 0xab)
		return nil
	})

	loanAddr := mapleAddr(0x30)
	upsert := func(poolID int64) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		loan, err := maple.NewLoan(1, protocolID, loanAddr, poolID, borrowerID, nil)
		if err != nil {
			t.Fatalf("NewLoan: %v", err)
		}
		if _, err := repo.UpsertLoans(ctx, tx, []*maple.Loan{loan}, mapleSyncedAt()); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := upsert(poolA); err != nil {
		t.Fatalf("baseline upsert: %v", err)
	}
	// Unchanged re-upsert is a clean no-op.
	if err := upsert(poolA); err != nil {
		t.Fatalf("unchanged re-upsert: %v", err)
	}
	// Pool reassignment must fail.
	err := upsert(poolB)
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "maple_pool_id") || !strings.Contains(err.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming maple_pool_id", err.Error())
	}
}

func TestMapleUpsertLoans_AppendsRowOnMetaChange(t *testing.T) {
	// maple_loan is append-only: loanMeta is off-chain editorial data Maple
	// enriches after origination. Any loanMeta difference — null->value
	// enrichment, value->value reclassification, value->null clear — appends a
	// NEW row instead of mutating the stored one, so prior metadata versions stay
	// queryable for reproducible historical risk calculations.
	ctx := context.Background()
	repo := newMapleRepo(t, 0)

	cases := []struct {
		name       string
		addrByte   byte
		from, to   *maple.LoanMeta
		wantIntern bool // is_internal of the new (latest) row
	}{
		{"null->value enrichment (intercompany)", 0x40, nil, &maple.LoanMeta{Type: "intercompany"}, false},
		{"null->value enrichment flips is_internal (amm)", 0x41, nil, &maple.LoanMeta{Type: "amm"}, true},
		{"value->value reclassification internal->external", 0x42, &maple.LoanMeta{Type: "amm"}, &maple.LoanMeta{Type: "intercompany"}, false},
		{"value->null clear", 0x43, &maple.LoanMeta{Type: "amm", Location: "Cayman"}, &maple.LoanMeta{Type: "amm"}, true},
		{"multi-field enrichment", 0x44, nil, &maple.LoanMeta{Type: "amm", DexName: "Uniswap", WalletAddress: "0xabc", WalletType: "BASE"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			truncateMaple(t, ctx)
			poolID := upsertTestPool(t, ctx, repo, 0x20)

			oldID := upsertTestLoan(t, ctx, repo, poolID, tc.addrByte, tc.from, mapleSyncedAt())
			newID := upsertTestLoan(t, ctx, repo, poolID, tc.addrByte, tc.to, mapleSyncedAt().Add(time.Hour))
			if newID == oldID {
				t.Fatalf("meta change reused id %d; expected a new appended row", oldID)
			}

			var count int
			if err := maplePool.QueryRow(ctx,
				`SELECT COUNT(*) FROM maple_loan WHERE chain_id = 1 AND loan_address = $1`,
				mapleAddr(tc.addrByte)).Scan(&count); err != nil {
				t.Fatalf("counting loan rows: %v", err)
			}
			if count != 2 {
				t.Fatalf("loan row count = %d, want 2 (original version + appended version)", count)
			}

			readMeta := func(id int64) (loanMetaCols, bool) {
				t.Helper()
				var m loanMetaCols
				var internal bool
				if err := maplePool.QueryRow(ctx,
					`SELECT loan_meta_type, loan_meta_asset_symbol, loan_meta_dex,
					        loan_meta_wallet_address, loan_meta_wallet_type, loan_meta_location, is_internal
					   FROM maple_loan WHERE id = $1`, id).
					Scan(&m.typ, &m.assetSymbol, &m.dex, &m.walletAddress, &m.walletType, &m.location, &internal); err != nil {
					t.Fatalf("reading loan id=%d: %v", id, err)
				}
				return m, internal
			}

			// History intact: the original version still carries its old meta.
			if got, _ := readMeta(oldID); !loanMetaEqual(got, loanMetaColsOf(&maple.Loan{LoanMeta: tc.from})) {
				t.Errorf("original row meta mutated; want the pre-change values preserved")
			}
			// Latest version carries the new meta and a recomputed is_internal.
			gotNew, internal := readMeta(newID)
			if !loanMetaEqual(gotNew, loanMetaColsOf(&maple.Loan{LoanMeta: tc.to})) {
				t.Errorf("appended row meta mismatch")
			}
			if internal != tc.wantIntern {
				t.Errorf("is_internal = %v, want %v", internal, tc.wantIntern)
			}
			// UpsertLoans returns the latest (appended) row id.
			if latest := latestMapleLoanID(t, ctx, tc.addrByte); latest != newID {
				t.Errorf("latest row id = %d, but UpsertLoans returned %d", latest, newID)
			}
		})
	}
}

func TestMapleUpsertLoans_StateSnapshotsPinToMetaVersion(t *testing.T) {
	// Reproducibility guarantee: each loan state snapshot FKs the maple_loan
	// version current at its sync cycle, so a later metadata change never
	// rewrites the metadata a historical snapshot was taken under.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x20)

	// Cycle 1: external loan (type null). Snapshot S1 points to version v1.
	v1 := upsertTestLoan(t, ctx, repo, poolID, 0x50, nil, mapleSyncedAt())
	s1, err := maple.NewLoanState(v1, mapleSyncedAt(), "Active", big.NewInt(100), big.NewInt(1000000))
	if err != nil {
		t.Fatalf("NewLoanState s1: %v", err)
	}
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SaveLoanStates(ctx, tx, []*maple.LoanState{s1})
	})

	// Cycle 2: Maple reclassifies the loan internal ("amm"). New version v2;
	// snapshot S2 points to it.
	v2 := upsertTestLoan(t, ctx, repo, poolID, 0x50, &maple.LoanMeta{Type: "amm"}, mapleSyncedAt().Add(time.Hour))
	if v2 == v1 {
		t.Fatal("expected a new version row for the reclassified loan")
	}
	s2, err := maple.NewLoanState(v2, mapleSyncedAt().Add(time.Hour), "Active", big.NewInt(90), big.NewInt(1100000))
	if err != nil {
		t.Fatalf("NewLoanState s2: %v", err)
	}
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		return repo.SaveLoanStates(ctx, tx, []*maple.LoanState{s2})
	})

	rows, err := maplePool.Query(ctx,
		`SELECT ls.maple_loan_id, l.is_internal
		   FROM maple_loan_state ls JOIN maple_loan l ON l.id = ls.maple_loan_id
		  ORDER BY ls.synced_at`)
	if err != nil {
		t.Fatalf("join query: %v", err)
	}
	defer rows.Close()

	type snap struct {
		loanID     int64
		isInternal bool
	}
	var got []snap
	for rows.Next() {
		var s snap
		if err := rows.Scan(&s.loanID, &s.isInternal); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	want := []snap{{v1, false}, {v2, true}}
	if len(got) != len(want) {
		t.Fatalf("snapshots = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("snapshot %d = %+v, want %+v (each state must resolve to the meta version live at its cycle)", i, got[i], want[i])
		}
	}
}

func TestMapleUpsertLoans_BatchAppendsOnlyChangedLoan(t *testing.T) {
	// One UpsertLoans call carrying several loans appends a new version only for
	// the loan whose metadata changed; the unchanged sibling reuses its row. The
	// returned map must carry the right id per loan. Exercises the per-loan lock
	// loop across more than one loan.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)
	poolID := upsertTestPool(t, ctx, repo, 0x20)

	addrA := mapleAddr(0x60)
	addrB := mapleAddr(0x61)
	keyA := common.BytesToAddress(addrA)
	keyB := common.BytesToAddress(addrB)

	upsertBatch := func(metaA, metaB *maple.LoanMeta, syncedAt time.Time) map[common.Address]int64 {
		t.Helper()
		var ids map[common.Address]int64
		inMapleTx(t, ctx, func(tx pgx.Tx) error {
			borrowerID := upsertTestBorrowerUser(t, ctx, tx, 0xab)
			loanA, err := maple.NewLoan(1, protocolID, addrA, poolID, borrowerID, metaA)
			if err != nil {
				return err
			}
			loanB, err := maple.NewLoan(1, protocolID, addrB, poolID, borrowerID, metaB)
			if err != nil {
				return err
			}
			ids, err = repo.UpsertLoans(ctx, tx, []*maple.Loan{loanA, loanB}, syncedAt)
			return err
		})
		return ids
	}

	first := upsertBatch(nil, &maple.LoanMeta{Type: "amm"}, mapleSyncedAt())
	// Cycle 2: A enriched null->intercompany; B unchanged.
	second := upsertBatch(&maple.LoanMeta{Type: "intercompany"}, &maple.LoanMeta{Type: "amm"}, mapleSyncedAt().Add(time.Hour))

	if second[keyA] == first[keyA] {
		t.Errorf("loan A meta changed but reused id %d; expected a new version", first[keyA])
	}
	if second[keyB] != first[keyB] {
		t.Errorf("loan B unchanged but got new id %d (was %d)", second[keyB], first[keyB])
	}
	if got := mapleLoanRowCount(t, ctx, 0x60); got != 2 {
		t.Errorf("loan A row count = %d, want 2 (a new version was appended)", got)
	}
	if got := mapleLoanRowCount(t, ctx, 0x61); got != 1 {
		t.Errorf("loan B row count = %d, want 1 (unchanged, no append)", got)
	}
}

func TestMapleUpsertLoans_MetaRoundTripAppendsDistinctVersions(t *testing.T) {
	// append-only compares only against the latest row, never resurrecting an
	// older one: amm -> intercompany -> amm yields three distinct version rows
	// (the third matches the first's metadata but is a fresh row).
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x20)

	id1 := upsertTestLoan(t, ctx, repo, poolID, 0x62, &maple.LoanMeta{Type: "amm"}, mapleSyncedAt())
	id2 := upsertTestLoan(t, ctx, repo, poolID, 0x62, &maple.LoanMeta{Type: "intercompany"}, mapleSyncedAt().Add(time.Hour))
	id3 := upsertTestLoan(t, ctx, repo, poolID, 0x62, &maple.LoanMeta{Type: "amm"}, mapleSyncedAt().Add(2*time.Hour))

	if id1 == id2 || id2 == id3 || id1 == id3 {
		t.Fatalf("expected three distinct version ids, got %d, %d, %d", id1, id2, id3)
	}
	if got := mapleLoanRowCount(t, ctx, 0x62); got != 3 {
		t.Errorf("row count = %d, want 3 (round-trip must not reuse the original version)", got)
	}
	if latest := latestMapleLoanID(t, ctx, 0x62); latest != id3 {
		t.Errorf("latest id = %d, want the last appended %d", latest, id3)
	}
}

func TestMapleUpsertLoans_PoolChangeWithMetaChangeStillRejected(t *testing.T) {
	// The immutable maple_pool_id guard runs before the loanMeta append decision:
	// a loan changing BOTH its pool and its metadata fails on the pool change and
	// appends nothing, instead of recording a forbidden pool reassignment as a
	// new version.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)
	poolA := upsertTestPool(t, ctx, repo, 0x20)
	poolB := upsertTestPool(t, ctx, repo, 0x21)

	loanAddr := mapleAddr(0x63)
	upsert := func(poolID int64, meta *maple.LoanMeta) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		borrowerID := upsertTestBorrowerUser(t, ctx, tx, 0xab)
		loan, err := maple.NewLoan(1, protocolID, loanAddr, poolID, borrowerID, meta)
		if err != nil {
			t.Fatalf("NewLoan: %v", err)
		}
		if _, err := repo.UpsertLoans(ctx, tx, []*maple.Loan{loan}, mapleSyncedAt()); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := upsert(poolA, &maple.LoanMeta{Type: "amm"}); err != nil {
		t.Fatalf("baseline upsert: %v", err)
	}
	// Pool reassignment together with a metadata change must fail on the pool.
	err := upsert(poolB, &maple.LoanMeta{Type: "intercompany"})
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "maple_pool_id") || !strings.Contains(err.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming maple_pool_id", err.Error())
	}
	if got := mapleLoanRowCount(t, ctx, 0x63); got != 1 {
		t.Errorf("row count = %d, want 1 (rejected pool change must not append a version)", got)
	}
}

func TestMapleUpsertPools_RejectsNullStoredName(t *testing.T) {
	// name is a nullable column. A row seeded with NULL name (e.g. by another
	// writer) that re-upserts with a concrete name must surface as a named
	// registry mismatch, not a misattributed scan error.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	protocolID := mapleProtocolID(t, ctx, repo)

	var assetID int64
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		assetID = upsertTestAssetToken(t, ctx, tx, 0xee, "USDC")
		return nil
	})
	if _, err := maplePool.Exec(ctx,
		`INSERT INTO maple_pool (chain_id, protocol_id, address, name, asset_token_id, is_syrup)
		 VALUES (1, $1, $2, NULL, $3, false)`,
		protocolID, mapleAddr(0x12), assetID); err != nil {
		t.Fatalf("seeding NULL-name pool: %v", err)
	}

	pool, err := maple.NewPool(1, protocolID, mapleAddr(0x12), "Pool A", assetID, false)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	tx, err := maplePool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, upsertErr := repo.UpsertPools(ctx, tx, []*maple.Pool{pool})
	if upsertErr == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	if !strings.Contains(upsertErr.Error(), "name") || !strings.Contains(upsertErr.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming name (stored NULL)", upsertErr.Error())
	}
}

func TestMapleUpsertSkyStrategies_RejectsNullStoredVersion(t *testing.T) {
	// version is a nullable column; a stored NULL re-upserted with a concrete
	// version must surface as a named registry mismatch, not a scan error.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x29)

	if _, err := maplePool.Exec(ctx,
		`INSERT INTO maple_sky_strategy (chain_id, strategy_address, maple_pool_id, version)
		 VALUES (1, $1, $2, NULL)`,
		mapleAddr(0x42), poolID); err != nil {
		t.Fatalf("seeding NULL-version strategy: %v", err)
	}

	strategy, err := maple.NewSkyStrategy(1, mapleAddr(0x42), poolID, 100)
	if err != nil {
		t.Fatalf("NewSkyStrategy: %v", err)
	}
	tx, err := maplePool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, upsertErr := repo.UpsertSkyStrategies(ctx, tx, []*maple.SkyStrategy{strategy})
	if upsertErr == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	if !strings.Contains(upsertErr.Error(), "version") || !strings.Contains(upsertErr.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming version (stored NULL)", upsertErr.Error())
	}
}

func TestMapleUpsertLoans_NullMetaTypeIsNotInternal(t *testing.T) {
	// Live API drift documented in CLAUDE.md: loanMeta present but type null.
	// The empty Type maps to a NULL loan_meta_type, so the is_internal
	// generated column stays false while the other meta fields persist.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolID := upsertTestPool(t, ctx, repo, 0x33)

	loanID := upsertTestLoan(t, ctx, repo, poolID, 0x34, &maple.LoanMeta{Type: "", Location: "Cayman"}, mapleSyncedAt())

	var metaType, location *string
	var isInternal bool
	if err := maplePool.QueryRow(ctx,
		`SELECT loan_meta_type, loan_meta_location, is_internal FROM maple_loan WHERE id = $1`,
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

		borrowerID := upsertTestBorrowerUser(t, ctx, tx, borrowerByte)
		loan, err := maple.NewLoan(1, protocolID, loanAddr, poolID, borrowerID, nil)
		if err != nil {
			t.Fatalf("NewLoan: %v", err)
		}
		if _, err := repo.UpsertLoans(ctx, tx, []*maple.Loan{loan}, mapleSyncedAt()); err != nil {
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
	if !strings.Contains(err.Error(), "borrower_user_id") || !strings.Contains(err.Error(), "registry fields changed") {
		t.Errorf("error %q should report a registry mismatch naming borrower_user_id", err.Error())
	}
}

func TestMapleStates_IdempotencyAndReprocessing(t *testing.T) {
	ctx := context.Background()
	truncateMaple(t, ctx)
	repoBuild0 := newMapleRepo(t, 0)
	repoBuild9 := newMapleRepo(t, 9)
	poolID := upsertTestPool(t, ctx, repoBuild0, 0x23)
	loanID := upsertTestLoan(t, ctx, repoBuild0, poolID, 0x33, nil, mapleSyncedAt())

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

	// Unchanged re-upsert is a clean no-op that keeps the id (nothing is
	// refreshed).
	inMapleTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		ids, err = repo.UpsertSkyStrategies(ctx, tx, []*maple.SkyStrategy{strategy})
		return err
	})
	if ids[common.BytesToAddress(strategy.StrategyAddress)] != strategyID {
		t.Error("strategy id changed on unchanged re-upsert")
	}
	var version int
	if err := maplePool.QueryRow(ctx,
		`SELECT version FROM maple_sky_strategy WHERE id = $1`, strategyID).Scan(&version); err != nil {
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

func TestMapleUpsertSkyStrategies_RejectsFieldChange(t *testing.T) {
	// maple_pool_id and version are immutable; a re-upsert with any changed
	// value must fail the run naming the field, instead of refreshing the row.
	// version is the field with a live mutation path (proxy upgrade), so its
	// guard firing is the expected first-observed-mismatch signal.
	ctx := context.Background()
	truncateMaple(t, ctx)
	repo := newMapleRepo(t, 0)
	poolA := upsertTestPool(t, ctx, repo, 0x26)
	poolB := upsertTestPool(t, ctx, repo, 0x27)

	strategyAddr := mapleAddr(0x41)
	upsert := func(poolID int64, version int) error {
		tx, err := maplePool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		strategy, err := maple.NewSkyStrategy(1, strategyAddr, poolID, version)
		if err != nil {
			t.Fatalf("NewSkyStrategy: %v", err)
		}
		if _, err := repo.UpsertSkyStrategies(ctx, tx, []*maple.SkyStrategy{strategy}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := upsert(poolA, 100); err != nil {
		t.Fatalf("baseline upsert: %v", err)
	}
	// Unchanged re-upsert is a clean no-op.
	if err := upsert(poolA, 100); err != nil {
		t.Fatalf("unchanged re-upsert: %v", err)
	}

	cases := []struct {
		name      string
		poolID    int64
		version   int
		wantField string
	}{
		{"pool reassignment", poolB, 100, "maple_pool_id"},
		{"version upgrade", poolA, 200, "version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := upsert(tc.poolID, tc.version)
			if err == nil {
				t.Fatal("expected mismatch error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantField) || !strings.Contains(err.Error(), "registry fields changed") {
				t.Errorf("error %q should report a registry mismatch naming %q", err.Error(), tc.wantField)
			}
		})
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
