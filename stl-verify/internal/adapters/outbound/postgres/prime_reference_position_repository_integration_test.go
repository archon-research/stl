//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func referencePosition(primeID int64, syncedAt time.Time, buildID int) entity.PrimeReferencePosition {
	chainID := int64(1)
	name := "Spark USDS"
	allocated := "700000000.10"
	idle := "82710914.02"
	return entity.PrimeReferencePosition{
		PrimeID:            primeID,
		SyncedAt:           syncedAt,
		ProtocolName:       "sparklend",
		Network:            "ethereum",
		ChainID:            &chainID,
		TokenSymbol:        "spUSDS",
		TokenName:          &name,
		TokenAddress:       "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359",
		WalletAddress:      "0x1111111111111111111111111111111111111111",
		AssetsUSD:          "782710914.129541047405509005",
		AllocatedAssetsUSD: &allocated,
		IdleAssetsUSD:      &idle,
		Source:             entity.ReferenceDataSource,
		BuildID:            buildID,
	}
}

// savePositions wraps SaveReferencePositions in a transaction, matching how
// the service calls it in production.
func savePositions(
	t *testing.T,
	ctx context.Context,
	txm *TxManager,
	repo *PrimeReferencePositionRepository,
	positions []entity.PrimeReferencePosition,
) error {
	t.Helper()
	return txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return repo.SaveReferencePositions(ctx, tx, positions)
	})
}

func TestPrimeReferencePositionRepositoryPreservesEighteenDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prp-precision")
	txm := newReferenceRepoTxm(t, pool)
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewPrimeReferencePositionRepository(pool, nil, runID)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	if err := savePositions(t, ctx, txm, repo, []entity.PrimeReferencePosition{
		referencePosition(primeID, syncedAt, int(buildID)),
	}); err != nil {
		t.Fatalf("SaveReferencePositions() = %v", err)
	}

	var assets string
	var gotRunID *int64
	if err := pool.QueryRow(ctx, `
		SELECT assets_usd::text, run_id FROM prime_reference_position WHERE prime_id = $1`, primeID).Scan(&assets, &gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if assets != "782710914.129541047405509005" {
		t.Errorf("assets_usd = %s, want the 18-decimal value unrounded", assets)
	}
	if gotRunID == nil || *gotRunID != int64(runID) {
		t.Errorf("run_id = %v, want %d", gotRunID, runID)
	}
}

func TestPrimeReferencePositionRepositoryKeepsOptionalFieldsNull(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prp-null")
	txm := newReferenceRepoTxm(t, pool)
	repo := NewPrimeReferencePositionRepository(pool, nil, 0)
	position := referencePosition(primeID, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 1)
	position.ChainID = nil
	position.TokenName = nil
	position.AllocatedAssetsUSD = nil
	position.IdleAssetsUSD = nil

	if err := savePositions(t, ctx, txm, repo, []entity.PrimeReferencePosition{position}); err != nil {
		t.Fatalf("SaveReferencePositions() = %v", err)
	}

	var nulls int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM prime_reference_position
		WHERE prime_id = $1 AND chain_id IS NULL AND token_name IS NULL
		  AND allocated_assets_usd IS NULL AND idle_assets_usd IS NULL`, primeID).Scan(&nulls); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if nulls != 1 {
		t.Errorf("null round-trip rows = %d, want 1 — an omitted figure must not become a value", nulls)
	}
}

// A re-run under the same build_id must reuse its processing_version and
// conflict away, so a Temporal retry cannot duplicate a cycle.
func TestPrimeReferencePositionRepositoryIsIdempotentWithinABuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prp-idem")
	txm := newReferenceRepoTxm(t, pool)
	repo := NewPrimeReferencePositionRepository(pool, nil, 0)
	position := referencePosition(primeID, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 1)

	for range 2 {
		if err := savePositions(t, ctx, txm, repo, []entity.PrimeReferencePosition{position}); err != nil {
			t.Fatalf("SaveReferencePositions() = %v", err)
		}
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_reference_position WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("wrote %d rows for one cycle re-run under one build, want 1", rows)
	}
}

// A new build reprocessing the same cycle appends a correction rather than
// overwriting, so history stays auditable (ADR-0002).
func TestPrimeReferencePositionRepositoryAppendsACorrectionForANewBuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prp-correction")
	txm := newReferenceRepoTxm(t, pool)
	repo := NewPrimeReferencePositionRepository(pool, nil, 0)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	for _, buildID := range []int{1, 2} {
		if err := savePositions(t, ctx, txm, repo, []entity.PrimeReferencePosition{
			referencePosition(primeID, syncedAt, buildID),
		}); err != nil {
			t.Fatalf("SaveReferencePositions(build=%d) = %v", buildID, err)
		}
	}

	var versions []int32
	rows, err := pool.Query(ctx, `
		SELECT processing_version FROM prime_reference_position
		WHERE prime_id = $1 ORDER BY processing_version`, primeID)
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int32
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scanning: %v", err)
		}
		versions = append(versions, v)
	}
	if len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Errorf("processing_versions = %v, want [0 1]", versions)
	}
}

// wallet_address is a PK component alongside network and token_address:
// grove legitimately reports the same token on the same network under two
// proxy wallets with different balances, and a key regression collapsing to
// (network, token_address) would still pass every single-row test above.
func TestPrimeReferencePositionRepositoryKeepsSameTokenUnderTwoWalletsDistinct(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "grove-prp-two-wallets")
	txm := newReferenceRepoTxm(t, pool)
	repo := NewPrimeReferencePositionRepository(pool, nil, 0)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	proxyOne := referencePosition(primeID, syncedAt, 1)
	proxyTwo := referencePosition(primeID, syncedAt, 1)
	proxyTwo.WalletAddress = "0x000000005ce4e5e4e5e4e5e4e5e4e5e4e5e4e5e4"

	if err := savePositions(t, ctx, txm, repo, []entity.PrimeReferencePosition{proxyOne, proxyTwo}); err != nil {
		t.Fatalf("SaveReferencePositions() = %v", err)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_reference_position WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 2 {
		t.Errorf("wrote %d rows for the same token under two proxy wallets, want 2", rows)
	}
}
