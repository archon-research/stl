//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

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
		AssetsUSD:          "782710914.129541047405509005",
		AllocatedAssetsUSD: &allocated,
		IdleAssetsUSD:      &idle,
		Source:             entity.ReferenceDataSource,
		BuildID:            buildID,
	}
}

func TestPrimeReferencePositionRepositoryPreservesEighteenDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prp-precision")
	repo := NewPrimeReferencePositionRepository(pool, newReferenceRepoTxm(t, pool), nil)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	if err := repo.SaveReferencePositions(ctx, []entity.PrimeReferencePosition{
		referencePosition(primeID, syncedAt, 1),
	}); err != nil {
		t.Fatalf("SaveReferencePositions() = %v", err)
	}

	var assets string
	if err := pool.QueryRow(ctx, `
		SELECT assets_usd::text FROM prime_reference_position WHERE prime_id = $1`, primeID).Scan(&assets); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if assets != "782710914.129541047405509005" {
		t.Errorf("assets_usd = %s, want the 18-decimal value unrounded", assets)
	}
}

func TestPrimeReferencePositionRepositoryKeepsOptionalFieldsNull(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-prp-null")
	repo := NewPrimeReferencePositionRepository(pool, newReferenceRepoTxm(t, pool), nil)
	position := referencePosition(primeID, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 1)
	position.ChainID = nil
	position.TokenName = nil
	position.AllocatedAssetsUSD = nil
	position.IdleAssetsUSD = nil

	if err := repo.SaveReferencePositions(ctx, []entity.PrimeReferencePosition{position}); err != nil {
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
	repo := NewPrimeReferencePositionRepository(pool, newReferenceRepoTxm(t, pool), nil)
	position := referencePosition(primeID, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 1)

	for range 2 {
		if err := repo.SaveReferencePositions(ctx, []entity.PrimeReferencePosition{position}); err != nil {
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
	repo := NewPrimeReferencePositionRepository(pool, newReferenceRepoTxm(t, pool), nil)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	for _, buildID := range []int{1, 2} {
		if err := repo.SaveReferencePositions(ctx, []entity.PrimeReferencePosition{
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
