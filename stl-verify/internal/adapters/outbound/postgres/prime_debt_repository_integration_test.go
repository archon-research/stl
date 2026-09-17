//go:build integration

package postgres

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestPrimeDebtSaveDebtSnapshots_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prime-debt-run")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewPrimeDebtRepository(pool, newReferenceRepoTxm(t, pool), nil, buildID, runID)

	err := repo.SaveDebtSnapshots(ctx, []*entity.PrimeDebt{{
		PrimeID: primeID, IlkName: "ALLOCATOR-SPARK-A", DebtWad: big.NewInt(1_000_000),
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
