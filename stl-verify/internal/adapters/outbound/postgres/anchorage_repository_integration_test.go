//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func newAnchorageRunFixture(t *testing.T, ctx context.Context) (*pgxpool.Pool, *AnchorageRepository, int64, buildregistry.RunID) {
	t.Helper()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	primeID := seedReferencePrime(t, ctx, pool, "spark-anchorage-run")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewAnchorageRepository(pool, newReferenceRepoTxm(t, pool), nil, buildID, runID)
	return pool, repo, primeID, runID
}

func TestAnchorageSaveSnapshots_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, repo, primeID, runID := newAnchorageRunFixture(t, ctx)
	snapshotTime := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	err := repo.SaveSnapshots(ctx, []entity.AnchoragePackageSnapshot{{
		PrimeID: primeID, PackageID: "pkg-1", PledgorID: "pledgor", SecuredPartyID: "party",
		Active: true, State: "ACTIVE",
		CurrentLTV: "0.5", ExposureValue: "100", PackageValue: "200",
		MarginCallLTV: "0.7", CriticalLTV: "0.8", MarginReturnLTV: "0.4",
		AssetType: "BTC", CustodyType: "ANCHORAGECUSTODY", AssetPrice: "60000", AssetQuantity: "1", AssetWeightedValue: "200",
		LTVTimestamp: snapshotTime, SnapshotTime: snapshotTime,
	}})
	if err != nil {
		t.Fatalf("SaveSnapshots: %v", err)
	}

	var gotRunID *int64
	if err := pool.QueryRow(ctx,
		`SELECT run_id FROM anchorage_package_snapshot WHERE prime_id = $1 AND package_id = 'pkg-1'`, primeID,
	).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
}

func TestAnchorageSaveOperations_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, repo, primeID, runID := newAnchorageRunFixture(t, ctx)

	err := repo.SaveOperations(ctx, []entity.AnchorageOperation{{
		PrimeID: primeID, OperationID: "op-1", Action: "TOP_UP", OperationType: "COLLATERAL_PACKAGE", TypeID: "pkg-1",
		AssetType: "BTC", CustodyType: "ANCHORAGECUSTODY", Quantity: "1", Notes: "",
		CreatedAt: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
	}})
	if err != nil {
		t.Fatalf("SaveOperations: %v", err)
	}

	var gotRunID *int64
	if err := pool.QueryRow(ctx,
		`SELECT run_id FROM anchorage_operation WHERE prime_id = $1 AND operation_id = 'op-1'`, primeID,
	).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
}
