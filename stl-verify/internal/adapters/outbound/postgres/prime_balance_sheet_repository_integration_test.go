//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func balanceSheetSnapshot(primeID int64, observedAt time.Time, buildID int) entity.PrimeBalanceSheetSnapshot {
	return entity.PrimeBalanceSheetSnapshot{
		PrimeID:            primeID,
		ObservedAt:         observedAt,
		TreasuryBalanceUSD: "48142491.085806286854722044",
		AssetsUSD:          "3224022323.40",
		AllocatedAssetsUSD: "2718840719.96",
		IdleAssetsUSD:      "505181603.43",
		DebtUSD:            "2642147590.40",
		BackstopCapitalUSD: "25000000",
		Source:             entity.ReferenceDataSource,
		BuildID:            buildID,
	}
}

func TestPrimeBalanceSheetRepositoryPreservesEighteenDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pbs-precision")
	repo := NewPrimeBalanceSheetRepository(pool, newReferenceRepoTxm(t, pool), nil)
	observedAt := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)

	inserted, newDays, err := repo.SaveBalanceSheetSnapshots(ctx, []entity.PrimeBalanceSheetSnapshot{
		balanceSheetSnapshot(primeID, observedAt, 1),
	})
	if err != nil {
		t.Fatalf("SaveBalanceSheetSnapshots() = %v", err)
	}
	if inserted != 1 {
		t.Fatalf("inserted = %d, want 1", inserted)
	}
	if newDays != 1 {
		t.Fatalf("newDays = %d, want 1 — the day's first-ever insert", newDays)
	}

	var treasury string
	if err := pool.QueryRow(ctx, `
		SELECT treasury_balance_usd::text FROM prime_reference_balance_sheet WHERE prime_id = $1`, primeID).Scan(&treasury); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if treasury != "48142491.085806286854722044" {
		t.Errorf("treasury_balance_usd = %s, want the 18-decimal value unrounded", treasury)
	}
}

// SaveBalanceSheetSnapshots returns the rows actually inserted, not the batch
// size: the indexer's 3-day lookback re-sends already-persisted days, which
// conflict away, so a replayed save of the same day must report 0.
func TestPrimeBalanceSheetRepositoryReportsZeroInsertedOnAReplayedSave(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pbs-replay")
	repo := NewPrimeBalanceSheetRepository(pool, newReferenceRepoTxm(t, pool), nil)
	snapshot := balanceSheetSnapshot(primeID, time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC), 1)

	first, _, err := repo.SaveBalanceSheetSnapshots(ctx, []entity.PrimeBalanceSheetSnapshot{snapshot})
	if err != nil {
		t.Fatalf("SaveBalanceSheetSnapshots() = %v", err)
	}
	if first != 1 {
		t.Fatalf("first save inserted = %d, want 1", first)
	}

	second, secondNewDays, err := repo.SaveBalanceSheetSnapshots(ctx, []entity.PrimeBalanceSheetSnapshot{snapshot})
	if err != nil {
		t.Fatalf("SaveBalanceSheetSnapshots() = %v", err)
	}
	if second != 0 {
		t.Errorf("replayed save inserted = %d, want 0 — the row conflicted away, not landed twice", second)
	}
	if secondNewDays != 0 {
		t.Errorf("replayed save newDays = %d, want 0", secondNewDays)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_reference_balance_sheet WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("rows in table = %d, want 1", rows)
	}
}

// The lookback window re-sends an already-persisted day alongside a genuinely
// new one in the same batch; the count must reflect only the new row.
func TestPrimeBalanceSheetRepositoryCountsOnlyTheNewRowsInAMixedBatch(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pbs-partial")
	repo := NewPrimeBalanceSheetRepository(pool, newReferenceRepoTxm(t, pool), nil)
	day1 := balanceSheetSnapshot(primeID, time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), 1)
	day2 := balanceSheetSnapshot(primeID, time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC), 1)

	if _, _, err := repo.SaveBalanceSheetSnapshots(ctx, []entity.PrimeBalanceSheetSnapshot{day1}); err != nil {
		t.Fatalf("seeding day1: %v", err)
	}

	inserted, newDays, err := repo.SaveBalanceSheetSnapshots(ctx, []entity.PrimeBalanceSheetSnapshot{day1, day2})
	if err != nil {
		t.Fatalf("SaveBalanceSheetSnapshots() = %v", err)
	}
	if inserted != 1 {
		t.Errorf("inserted = %d, want 1 — day1 conflicts away, only day2 is new", inserted)
	}
	if newDays != 1 {
		t.Errorf("newDays = %d, want 1 — day2 is a fresh day", newDays)
	}
}

// A new build reprocessing the same day appends a correction rather than
// overwriting (ADR-0002); that correction row still counts as inserted, but
// must not count as a newDays — a deploy replaying the lookback must not be
// able to mask a genuine write-path stall.
func TestPrimeBalanceSheetRepositoryCountsACorrectionForANewBuildAsInsertedButNotNew(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pbs-correction")
	repo := NewPrimeBalanceSheetRepository(pool, newReferenceRepoTxm(t, pool), nil)
	observedAt := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)

	for i, buildID := range []int{1, 2} {
		wantNewDays := 0
		if i == 0 {
			wantNewDays = 1
		}
		inserted, newDays, err := repo.SaveBalanceSheetSnapshots(ctx, []entity.PrimeBalanceSheetSnapshot{
			balanceSheetSnapshot(primeID, observedAt, buildID),
		})
		if err != nil {
			t.Fatalf("SaveBalanceSheetSnapshots(build=%d) = %v", buildID, err)
		}
		if inserted != 1 {
			t.Errorf("build=%d inserted = %d, want 1", buildID, inserted)
		}
		if newDays != wantNewDays {
			t.Errorf("build=%d newDays = %d, want %d", buildID, newDays, wantNewDays)
		}
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_reference_balance_sheet WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 2 {
		t.Errorf("rows in table = %d, want 2 — original plus correction", rows)
	}
}

// A mid-batch failure (here, an FK violation on a bogus prime_id) must roll
// back the whole batch and report zero inserted, not the rows that landed
// before the failing one — the transaction, not the loop, owns atomicity.
func TestPrimeBalanceSheetRepositoryRollsBackWholeBatchOnAForeignKeyViolation(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pbs-fk-violation")
	repo := NewPrimeBalanceSheetRepository(pool, newReferenceRepoTxm(t, pool), nil)
	valid := balanceSheetSnapshot(primeID, time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), 1)
	invalid := balanceSheetSnapshot(999999999, time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC), 1)

	inserted, newDays, err := repo.SaveBalanceSheetSnapshots(ctx, []entity.PrimeBalanceSheetSnapshot{valid, invalid})
	if err == nil {
		t.Fatal("SaveBalanceSheetSnapshots() = nil, want a foreign key violation error")
	}
	if inserted != 0 {
		t.Errorf("inserted = %d, want 0 — a batch error must report nothing landed", inserted)
	}
	if newDays != 0 {
		t.Errorf("newDays = %d, want 0 — a batch error must report nothing landed", newDays)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_reference_balance_sheet WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 0 {
		t.Errorf("rows in table = %d, want 0 — the valid row must not survive the rolled-back batch", rows)
	}
}
