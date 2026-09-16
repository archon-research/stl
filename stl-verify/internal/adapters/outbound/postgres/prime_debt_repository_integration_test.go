//go:build integration

package postgres

import (
	"context"
	"math/big"
	"slices"
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

// A vault is deployed on one chain, and the tracker for another chain must not see it. The unscoped
// reader still returns both, because a prime's positions reach chains its vault is not on.
func TestGetPrimesOnChainReturnsOnlyThatChainsVaults(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	if _, err := pool.Exec(ctx, `
		INSERT INTO chain (chain_id, name) VALUES (8453, 'base') ON CONFLICT DO NOTHING;
		INSERT INTO prime (external_id, name, vault_address, chain_id)
		VALUES (gen_random_uuid(), 'scoped-eth', '\x9101', 1),
		       (gen_random_uuid(), 'scoped-base', '\x9102', 8453)`); err != nil {
		t.Fatalf("seed primes on two chains: %v", err)
	}

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewPrimeDebtRepository(pool, newReferenceRepoTxm(t, pool), nil, buildID, runID)

	onEth, err := repo.GetPrimesOnChain(ctx, 1)
	if err != nil {
		t.Fatalf("GetPrimesOnChain(1): %v", err)
	}
	for _, p := range onEth {
		if p.Name == "scoped-base" {
			t.Error("a chain-1 tracker was handed the Base vault; debt would be read from the wrong Vat")
		}
	}
	if !slices.ContainsFunc(onEth, func(p entity.Prime) bool { return p.Name == "scoped-eth" }) {
		t.Fatal("the chain-1 vault is missing, so the assertion above would hold for any reason")
	}

	all, err := repo.GetPrimes(ctx)
	if err != nil {
		t.Fatalf("GetPrimes: %v", err)
	}
	for _, name := range []string{"scoped-eth", "scoped-base"} {
		if !slices.ContainsFunc(all, func(p entity.Prime) bool { return p.Name == name }) {
			t.Errorf("the unscoped reader lost %s; consumers of cross-chain positions need every vault", name)
		}
	}
}
