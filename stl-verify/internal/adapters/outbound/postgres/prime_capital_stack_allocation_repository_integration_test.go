//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func capitalStackAllocation(primeID int64, syncedAt time.Time, buildID int) entity.PrimeCapitalStackAllocation {
	chainID := int64(1)
	name := "Spark USDS"
	loanAddress := "0xdc035d45d973e3ec169d2276ddab16f1e407384f"
	loanSymbol := "USDS"
	return entity.PrimeCapitalStackAllocation{
		PrimeID:                primeID,
		SyncedAt:               syncedAt,
		ProtocolName:           "sparklend",
		Network:                "ethereum",
		ChainID:                &chainID,
		Symbol:                 "spUSDS",
		Name:                   &name,
		TokenAddress:           "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359",
		LoanTokenAddress:       &loanAddress,
		LoanTokenSymbol:        &loanSymbol,
		ExposureUSD:            "782710914.129541047405509005",
		RequiredRiskCapitalUSD: "23308466.81",
		CRR:                    "0.0447",
		Source:                 entity.ReferenceDataSource,
		BuildID:                buildID,
	}
}

func TestPrimeCapitalStackAllocationRepositoryPreservesEighteenDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pcsa-precision")
	repo := NewPrimeCapitalStackAllocationRepository(pool, newReferenceRepoTxm(t, pool), nil)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	if err := repo.SaveCapitalStackAllocations(ctx, []entity.PrimeCapitalStackAllocation{
		capitalStackAllocation(primeID, syncedAt, 1),
	}); err != nil {
		t.Fatalf("SaveCapitalStackAllocations() = %v", err)
	}

	var exposure, crr string
	var chainID *int64
	if err := pool.QueryRow(ctx, `
		SELECT exposure_usd::text, crr::text, chain_id
		FROM prime_capital_stack_allocation WHERE prime_id = $1`, primeID).Scan(&exposure, &crr, &chainID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if exposure != "782710914.129541047405509005" {
		t.Errorf("exposure_usd = %s, want the 18-decimal value unrounded", exposure)
	}
	if crr != "0.0447" {
		t.Errorf("crr = %s, want the raw 0-1 fraction", crr)
	}
	if chainID == nil || *chainID != 1 {
		t.Errorf("chain_id = %v, want 1", chainID)
	}
}

func TestPrimeCapitalStackAllocationRepositoryKeepsOptionalFieldsNull(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pcsa-null")
	repo := NewPrimeCapitalStackAllocationRepository(pool, newReferenceRepoTxm(t, pool), nil)
	allocation := capitalStackAllocation(primeID, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 1)
	allocation.ChainID = nil
	allocation.Name = nil
	allocation.LoanTokenAddress = nil
	allocation.LoanTokenSymbol = nil

	if err := repo.SaveCapitalStackAllocations(ctx, []entity.PrimeCapitalStackAllocation{allocation}); err != nil {
		t.Fatalf("SaveCapitalStackAllocations() = %v", err)
	}

	var nulls int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM prime_capital_stack_allocation
		WHERE prime_id = $1 AND chain_id IS NULL AND name IS NULL
		  AND loan_token_address IS NULL AND loan_token_symbol IS NULL`, primeID).Scan(&nulls); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if nulls != 1 {
		t.Errorf("null round-trip rows = %d, want 1 — an omitted figure must not become a value", nulls)
	}
}

// A re-run under the same build_id must reuse its processing_version and
// conflict away, so a Temporal retry cannot duplicate a cycle.
func TestPrimeCapitalStackAllocationRepositoryIsIdempotentWithinABuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pcsa-idem")
	repo := NewPrimeCapitalStackAllocationRepository(pool, newReferenceRepoTxm(t, pool), nil)
	allocation := capitalStackAllocation(primeID, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 1)

	for range 2 {
		if err := repo.SaveCapitalStackAllocations(ctx, []entity.PrimeCapitalStackAllocation{allocation}); err != nil {
			t.Fatalf("SaveCapitalStackAllocations() = %v", err)
		}
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_capital_stack_allocation WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("wrote %d rows for one cycle re-run under one build, want 1", rows)
	}
}

// A new build reprocessing the same cycle appends a correction rather than
// overwriting, so history stays auditable (ADR-0002).
func TestPrimeCapitalStackAllocationRepositoryAppendsACorrectionForANewBuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pcsa-correction")
	repo := NewPrimeCapitalStackAllocationRepository(pool, newReferenceRepoTxm(t, pool), nil)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	for _, buildID := range []int{1, 2} {
		if err := repo.SaveCapitalStackAllocations(ctx, []entity.PrimeCapitalStackAllocation{
			capitalStackAllocation(primeID, syncedAt, buildID),
		}); err != nil {
			t.Fatalf("SaveCapitalStackAllocations(build=%d) = %v", buildID, err)
		}
	}

	var versions []int32
	rows, err := pool.Query(ctx, `
		SELECT processing_version FROM prime_capital_stack_allocation
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

// network is a PK component alongside token_address, unlike the sibling
// prime_capital_stack table — a key regression collapsing to token_address
// would still pass every single-row test above.
func TestPrimeCapitalStackAllocationRepositoryKeepsSameTokenOnTwoNetworksDistinct(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	primeID := seedReferencePrime(t, ctx, pool, "spark-pcsa-two-networks")
	repo := NewPrimeCapitalStackAllocationRepository(pool, newReferenceRepoTxm(t, pool), nil)
	syncedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	ethereum := capitalStackAllocation(primeID, syncedAt, 1)
	base := capitalStackAllocation(primeID, syncedAt, 1)
	base.Network = "base"

	if err := repo.SaveCapitalStackAllocations(ctx, []entity.PrimeCapitalStackAllocation{ethereum, base}); err != nil {
		t.Fatalf("SaveCapitalStackAllocations() = %v", err)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_capital_stack_allocation WHERE prime_id = $1`, primeID).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 2 {
		t.Errorf("wrote %d rows for the same token on two networks, want 2", rows)
	}
}
