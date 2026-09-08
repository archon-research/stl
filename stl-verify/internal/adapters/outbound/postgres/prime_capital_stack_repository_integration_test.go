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

func capitalStackSnapshot(primeID int64, syncedAt time.Time, buildID int) entity.PrimeCapitalStackSnapshot {
	ratio := "0.3705"
	return entity.PrimeCapitalStackSnapshot{
		PrimeID:                       primeID,
		SyncedAt:                      syncedAt,
		ExposureUSD:                   "2098090654.811942249063867795",
		RequiredRiskCapitalUSD:        "17837860.437905393198969414",
		TotalRiskCapitalUSD:           "48142491.085806286854722044",
		JuniorRiskCapitalUSD:          "48142491.085806286854722044",
		SeniorRiskCapitalUSD:          "0",
		InternalJuniorRiskCapitalUSD:  "48142491.085806286854722044",
		ExternalJuniorRiskCapitalUSD:  "0",
		TokenizedJuniorRiskCapitalUSD: "0",
		InternalSeniorRiskCapitalUSD:  "0",
		ExternalSeniorRiskCapitalUSD:  "0",
		EncumbranceRatio:              &ratio,
		ExposureShare:                 "0.0084",
		EPIUtilization:                "0",
		SPJUtilization:                "0",
		Source:                        "skyeco:star-monitoring:risk-capital",
		BuildID:                       buildID,
	}
}

func TestPrimeCapitalStackRepositoryPreservesEighteenDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	var primeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO prime (name, vault_address) VALUES ('spark-pcs-precision', decode('aabbccddeeff00112233445566778899aabbccdd','hex'))
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`).Scan(&primeID); err != nil {
		t.Fatalf("seeding prime: %v", err)
	}

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewPrimeCapitalStackRepository(pool, nil)
	syncedAt := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	if err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return repo.SavePrimeCapitalSnapshots(ctx, tx, []entity.PrimeCapitalStackSnapshot{
			capitalStackSnapshot(primeID, syncedAt, 1),
		})
	}); err != nil {
		t.Fatalf("SavePrimeCapitalSnapshots() = %v", err)
	}

	var exposure, junior string
	if err := pool.QueryRow(ctx, `
		SELECT exposure_usd::text, junior_risk_capital_usd::text
		FROM prime_capital_stack WHERE prime_id = $1`, primeID).Scan(&exposure, &junior); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if exposure != "2098090654.811942249063867795" {
		t.Errorf("exposure_usd = %s, want the 18-decimal value unrounded", exposure)
	}
	if junior != "48142491.085806286854722044" {
		t.Errorf("junior_risk_capital_usd = %s, want the 18-decimal value unrounded", junior)
	}
}

// A re-run under the same build_id must reuse its processing_version and
// conflict away, so a Temporal retry cannot duplicate a cycle.
func TestPrimeCapitalStackRepositoryIsIdempotentWithinABuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	var primeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO prime (name, vault_address) VALUES ('spark-pcs-idem', decode('aabbccddeeff00112233445566778899aabbccde','hex'))
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`).Scan(&primeID); err != nil {
		t.Fatalf("seeding prime: %v", err)
	}

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewPrimeCapitalStackRepository(pool, nil)
	syncedAt := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	snapshot := capitalStackSnapshot(primeID, syncedAt, 1)

	for range 2 {
		if err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
			return repo.SavePrimeCapitalSnapshots(ctx, tx, []entity.PrimeCapitalStackSnapshot{snapshot})
		}); err != nil {
			t.Fatalf("SavePrimeCapitalSnapshots() = %v", err)
		}
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_capital_stack WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("wrote %d rows for one cycle re-run under one build, want 1", rows)
	}
}

// A new build reprocessing the same cycle appends a correction rather than
// overwriting, so history stays auditable (ADR-0002).
func TestPrimeCapitalStackRepositoryAppendsACorrectionForANewBuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	var primeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO prime (name, vault_address) VALUES ('spark-pcs-correction', decode('aabbccddeeff00112233445566778899aabbccdf','hex'))
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`).Scan(&primeID); err != nil {
		t.Fatalf("seeding prime: %v", err)
	}

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewPrimeCapitalStackRepository(pool, nil)
	syncedAt := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	for _, buildID := range []int{1, 2} {
		if err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
			return repo.SavePrimeCapitalSnapshots(ctx, tx, []entity.PrimeCapitalStackSnapshot{
				capitalStackSnapshot(primeID, syncedAt, buildID),
			})
		}); err != nil {
			t.Fatalf("SavePrimeCapitalSnapshots(build=%d) = %v", buildID, err)
		}
	}

	var versions []int32
	rows, err := pool.Query(ctx, `
		SELECT processing_version FROM prime_capital_stack
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
