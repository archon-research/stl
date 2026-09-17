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

func coreMarketResult(uid string, syncedAt time.Time, buildID int) entity.ReferenceCoreMarketResult {
	chainID := int64(1)
	return entity.ReferenceCoreMarketResult{
		Network:              "ethereum",
		ChainID:              &chainID,
		ProtocolName:         "sparklend",
		MarketUID:            uid,
		MarketSymbol:         "spUSDS",
		LoanTokenSymbol:      "USDS",
		LoanTokenAddress:     "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
		ModelDate:            time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC),
		SyncedAt:             syncedAt,
		NScenarios:           10000,
		HorizonDays:          15,
		EffectiveHorizonDays: 1,
		TotalSupplyUSD:       "1309071023.791548000000000000",
		ProbNoBadDebt:        "0.694299999999999900",
		CRREL:                "0.001214738329317449",
		CRRVaR:               "0.010683027397348378",
		CRRES:                "0.030818438149105926",
		CRRELSE:              "0.000077018244182130",
		CRRVaRSE:             "0.000694722112615200",
		CRRESSE:              "0.001220932121971280",
		CRRFloor:             "0.020000000000000000",
		ExternalFlowEnabled:  true,
		Source:               entity.ReferenceCoreDataSource,
		BuildID:              buildID,
	}
}

func saveCoreMarkets(t *testing.T, ctx context.Context, txm *TxManager, repo *ReferenceCoreMarketResultRepository, rows ...entity.ReferenceCoreMarketResult) {
	t.Helper()
	if err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return repo.SaveMarketResults(ctx, tx, rows)
	}); err != nil {
		t.Fatalf("SaveMarketResults() = %v", err)
	}
}

func TestReferenceCoreMarketResultRepositoryPreservesEighteenDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewReferenceCoreMarketResultRepository(nil, runID)
	syncedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	saveCoreMarkets(t, ctx, txm, repo, coreMarketResult("0xaaa", syncedAt, int(buildID)))

	var supply, crrEL, crrFloor, modelDate string
	var chainID *int64
	var gotRunID *int64
	var externalFlow bool
	if err := pool.QueryRow(ctx, `
		SELECT total_supply_usd::text, crr_el::text, crr_floor::text, model_date::text, chain_id, run_id, external_flow_enabled
		FROM reference_core_market_result WHERE market_uid = '0xaaa'`).Scan(&supply, &crrEL, &crrFloor, &modelDate, &chainID, &gotRunID, &externalFlow); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
	if supply != "1309071023.791548000000000000" {
		t.Errorf("total_supply_usd = %s, want the 18-decimal value unrounded", supply)
	}
	if crrEL != "0.001214738329317449" || crrFloor != "0.020000000000000000" {
		t.Errorf("crr_el/crr_floor = %s/%s, want the literals unrounded and the floor NOT added", crrEL, crrFloor)
	}
	if modelDate != "2026-09-17" || chainID == nil || *chainID != 1 || !externalFlow {
		t.Errorf("model_date/chain_id/external_flow = %s/%v/%v, want 2026-09-17/1/true", modelDate, chainID, externalFlow)
	}
}

// The same cycle written twice under one build_id must reuse its
// processing_version and conflict away rather than duplicate.
func TestReferenceCoreMarketResultRepositoryIsIdempotentWithinABuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewReferenceCoreMarketResultRepository(nil, 0)
	syncedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	row := coreMarketResult("0xaaa", syncedAt, 1)

	for range 2 {
		saveCoreMarkets(t, ctx, txm, repo, row)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM reference_core_market_result WHERE market_uid = '0xaaa'`).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("wrote %d rows for one cycle re-run under one build, want 1", rows)
	}
}

// A new build reprocessing the same cycle appends a correction rather than
// overwriting, so history stays auditable (ADR-0002).
func TestReferenceCoreMarketResultRepositoryAppendsACorrectionForANewBuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewReferenceCoreMarketResultRepository(nil, 0)
	syncedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	saveCoreMarkets(t, ctx, txm, repo, coreMarketResult("0xaaa", syncedAt, 1))
	corrected := coreMarketResult("0xaaa", syncedAt, 2)
	corrected.CRREL = "0.002"
	saveCoreMarkets(t, ctx, txm, repo, corrected)

	rows, err := pool.Query(ctx, `
		SELECT processing_version, crr_el::text FROM reference_core_market_result
		WHERE market_uid = '0xaaa' ORDER BY processing_version`)
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	defer rows.Close()
	var versions []int
	var crrs []string
	for rows.Next() {
		var v int
		var crr string
		if err := rows.Scan(&v, &crr); err != nil {
			t.Fatalf("scan: %v", err)
		}
		versions = append(versions, v)
		crrs = append(crrs, crr)
	}
	if len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Fatalf("processing_versions = %v, want [0 1]", versions)
	}
	if crrs[1] != "0.002" {
		t.Errorf("corrected crr_el = %s, want 0.002 on version 1", crrs[1])
	}
}

// Two protocols share a placeholder market_uid on ethereum (anchorage and
// galaxy); the identity must keep them apart.
func TestReferenceCoreMarketResultRepositoryKeepsOneUIDUnderTwoProtocolsApart(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewReferenceCoreMarketResultRepository(nil, 0)
	syncedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	anchorage := coreMarketResult("0x0000000000000000000000000000000000000000", syncedAt, 1)
	anchorage.ProtocolName = "anchorage"
	galaxy := coreMarketResult("0x0000000000000000000000000000000000000000", syncedAt, 1)
	galaxy.ProtocolName = "galaxy"

	saveCoreMarkets(t, ctx, txm, repo, anchorage, galaxy)

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM reference_core_market_result`).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 2 {
		t.Errorf("wrote %d rows, want 2: protocol_name is part of the identity", rows)
	}
}
