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

func coreVaultResult(address string, syncedAt time.Time, buildID int) entity.CoreModelReferenceVaultResult {
	chainID := int64(8453)
	return entity.CoreModelReferenceVaultResult{
		Network:          "base",
		ChainID:          &chainID,
		ProtocolName:     "morpho",
		VaultAddress:     address,
		VaultSymbol:      "steakUSDC",
		VaultName:        "Steakhouse Prime USDC",
		VersionLabel:     "v2",
		LoanTokenSymbol:  "USDC",
		LoanTokenAddress: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
		Method:           "model",
		ModelDate:        time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC),
		SyncedAt:         syncedAt,
		NMarkets:         5,
		TotalAssetsUSD:   "434521303.920751150000000000",
		IdleAssetsUSD:    "3201.013246119022400000",
		CRREL:            "0.000288698101040592",
		CRRELSE:          new("0.000028834308921715"),
		CRRES:            new("0.009880342305281255"),
		Source:           entity.CoreModelReferenceDataSource,
		BuildID:          buildID,
	}
}

func saveCoreVaults(t *testing.T, ctx context.Context, txm *TxManager, repo *CoreModelReferenceVaultResultRepository, rows ...entity.CoreModelReferenceVaultResult) {
	t.Helper()
	if err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return repo.SaveVaultResults(ctx, tx, rows)
	}); err != nil {
		t.Fatalf("SaveVaultResults() = %v", err)
	}
}

func TestCoreModelReferenceVaultResultRepositoryPreservesEighteenDecimalPrecision(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewCoreModelReferenceVaultResultRepository(nil, runID)
	syncedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	saveCoreVaults(t, ctx, txm, repo, coreVaultResult("0xbeef", syncedAt, int(buildID)))

	var assets, idle, crrEL, method, version string
	var gotRunID *int64
	if err := pool.QueryRow(ctx, `
		SELECT total_assets_usd::text, idle_assets_usd::text, crr_el::text, method, version_label, run_id
		FROM core_model_reference_vault_result WHERE vault_address = '0xbeef'`).Scan(&assets, &idle, &crrEL, &method, &version, &gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
	if assets != "434521303.920751150000000000" || idle != "3201.013246119022400000" {
		t.Errorf("total_assets_usd/idle_assets_usd = %s/%s, want the 18-decimal values unrounded", assets, idle)
	}
	if crrEL != "0.000288698101040592" || method != "model" || version != "v2" {
		t.Errorf("crr_el/method/version_label = %s/%s/%s, want the literals", crrEL, method, version)
	}
}

// Upstream serialises a very small figure in exponent notation (sxsRLUSD's
// crr_el_se arrived as 4.27099736914E-7, verified live 17 Sep 2026); NUMERIC
// must take it as the same number, not reject the literal.
func TestCoreModelReferenceVaultResultRepositoryAcceptsAnExponentNotationFigure(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewCoreModelReferenceVaultResultRepository(nil, 0)
	row := coreVaultResult("0xbeef", time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC), 1)
	row.CRRELSE = new("4.27099736914E-7")

	saveCoreVaults(t, ctx, txm, repo, row)

	var se string
	if err := pool.QueryRow(ctx, `
		SELECT crr_el_se::text FROM core_model_reference_vault_result WHERE vault_address = '0xbeef'`).Scan(&se); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if se != "0.000000427099736914" {
		t.Errorf("crr_el_se = %s, want 0.000000427099736914 (the exponent literal as a plain numeric)", se)
	}
}

// An override vault has no simulation behind it, so its standard error and
// expected shortfall are structurally absent and land as NULL.
func TestCoreModelReferenceVaultResultRepositoryStoresAnOverrideVaultWithoutSimulationFigures(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewCoreModelReferenceVaultResultRepository(nil, 0)
	row := coreVaultResult("0xbeef", time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC), 1)
	row.Method = "override"
	row.CRRELSE, row.CRRES = nil, nil

	saveCoreVaults(t, ctx, txm, repo, row)

	var se, es *string
	if err := pool.QueryRow(ctx, `
		SELECT crr_el_se::text, crr_es::text FROM core_model_reference_vault_result WHERE vault_address = '0xbeef'`).Scan(&se, &es); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if se != nil || es != nil {
		t.Errorf("crr_el_se/crr_es = %v/%v, want both NULL on an override vault", se, es)
	}
}

// The table refuses the same NULLs on a modelled vault: there they mean a
// broken payload, and the client should have failed the cycle first.
func TestCoreModelReferenceVaultResultRepositoryRejectsAModelVaultWithoutSimulationFigures(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewCoreModelReferenceVaultResultRepository(nil, 0)
	row := coreVaultResult("0xbeef", time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC), 1)
	row.CRRES = nil

	err = txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return repo.SaveVaultResults(ctx, tx, []entity.CoreModelReferenceVaultResult{row})
	})
	if err == nil {
		t.Fatal("SaveVaultResults() = nil, want the CHECK to reject a model vault with a NULL crr_es")
	}
}

func TestCoreModelReferenceVaultResultRepositoryIsIdempotentWithinABuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewCoreModelReferenceVaultResultRepository(nil, 0)
	syncedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	row := coreVaultResult("0xbeef", syncedAt, 1)

	for range 2 {
		saveCoreVaults(t, ctx, txm, repo, row)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM core_model_reference_vault_result WHERE vault_address = '0xbeef'`).Scan(&rows); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("wrote %d rows for one cycle re-run under one build, want 1", rows)
	}
}

func TestCoreModelReferenceVaultResultRepositoryAppendsACorrectionForANewBuild(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	repo := NewCoreModelReferenceVaultResultRepository(nil, 0)
	syncedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	saveCoreVaults(t, ctx, txm, repo, coreVaultResult("0xbeef", syncedAt, 1))
	saveCoreVaults(t, ctx, txm, repo, coreVaultResult("0xbeef", syncedAt, 2))

	var maxVersion, rows int
	if err := pool.QueryRow(ctx, `
		SELECT max(processing_version), count(*) FROM core_model_reference_vault_result WHERE vault_address = '0xbeef'`).Scan(&maxVersion, &rows); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if rows != 2 || maxVersion != 1 {
		t.Errorf("rows/max version = %d/%d, want 2/1: a new build appends a correction", rows, maxVersion)
	}
}
