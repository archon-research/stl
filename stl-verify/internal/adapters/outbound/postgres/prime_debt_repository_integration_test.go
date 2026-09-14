//go:build integration

package postgres

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
)

func TestPrimeDebtSaveDebtSnapshots_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prime-debt-run")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewPrimeDebtRepository(pool, newReferenceRepoTxm(t, pool), nil, buildID, runID)

	vatID, err := repo.ProtocolIDByAddress(ctx, 1, common.HexToAddress("0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"))
	if err != nil {
		t.Fatalf("resolving the seeded Vat row: %v", err)
	}

	err = repo.SaveDebtSnapshots(ctx, []*entity.PrimeDebt{{
		PrimeID: primeID, ProtocolID: vatID, IlkName: "ALLOCATOR-SPARK-A", DebtWad: big.NewInt(1_000_000),
		BlockNumber: 24_000_000, BlockVersion: 0, SyncedAt: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
	}})
	if err != nil {
		t.Fatalf("SaveDebtSnapshots: %v", err)
	}

	var gotRunID *int64
	if err := pool.QueryRow(ctx, `SELECT run_id FROM prime_debt WHERE prime_id = $1`, primeID).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
}

// The write path and the projection have to agree about protocol_id: the column is nullable, so a
// writer that omits it stores NULL, and materialize_sky_prime_debt() then refuses every run for a
// row nothing can repair. This is the test the fixtures could not give, because they all seed
// protocol_id by hand.
func TestPrimeDebtWrittenRowsAreProjectable(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prime-debt-projectable")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewPrimeDebtRepository(pool, newReferenceRepoTxm(t, pool), nil, buildID, runID)

	vatID, err := repo.ProtocolIDByAddress(ctx, 1, common.HexToAddress("0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"))
	if err != nil {
		t.Fatalf("resolving the seeded Vat row: %v", err)
	}
	if err := repo.SaveDebtSnapshots(ctx, []*entity.PrimeDebt{{
		PrimeID: primeID, ProtocolID: vatID, IlkName: "ALLOCATOR-SPARK-B", DebtWad: big.NewInt(2_500_000),
		BlockNumber: 24_100_000, BlockVersion: 0, SyncedAt: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
	}}); err != nil {
		t.Fatalf("SaveDebtSnapshots: %v", err)
	}

	var appended int64
	if err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt(7, 9182)`).Scan(&appended); err != nil {
		t.Fatalf("the projection refused rows this repository wrote: %v", err)
	}
	if appended != 1 {
		t.Errorf("appended %d observations, want 1", appended)
	}

	// And a snapshot with no protocol row never reaches the table at all.
	err = repo.SaveDebtSnapshots(ctx, []*entity.PrimeDebt{{
		PrimeID: primeID, IlkName: "ALLOCATOR-SPARK-C", DebtWad: big.NewInt(1),
		BlockNumber: 24_200_000, BlockVersion: 0, SyncedAt: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
	}})
	if err == nil {
		t.Error("a snapshot without a protocol row was accepted; the projection would refuse every later run")
	}
}
