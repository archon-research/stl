//go:build integration

package postgres

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const fluidSchemaName = "test_fluid"

var fluidPool *pgxpool.Pool

func init() {
	registerTestFileSetup(fluidSchemaName, func() {
		fluidPool = testutil.SetupSchemaForMain(sharedDSN, fluidSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, fluidPool, fluidSchemaName)
	})
}

// truncateFluid clears only the fluid-owned tables. The shared protocol and
// token registries are NOT truncated here: with schema-per-file isolation,
// unqualified protocol/token resolve to public (CREATE TABLE IF NOT EXISTS
// finds the public copy first), so a TRUNCATE here wipes rows sibling test
// files (e.g. maple) rely on. The fixtures below seed protocol/token
// idempotently instead.
func truncateFluid(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := fluidPool.Exec(ctx, `DELETE FROM fluid_vault_state`); err != nil {
		t.Fatalf("failed to truncate fluid_vault_state: %v", err)
	}
	if _, err := fluidPool.Exec(ctx, `DELETE FROM fluid_vault`); err != nil {
		t.Fatalf("failed to truncate fluid_vault: %v", err)
	}
}

type fluidTestFixture struct {
	repo        *FluidVaultRepository
	pool        *pgxpool.Pool
	protocolID  int64
	collTokenID int64
	debtTokenID int64
}

func setupFluidTest(t *testing.T) *fluidTestFixture {
	t.Helper()
	ctx := context.Background()

	truncateFluid(t, ctx)

	repo, err := NewFluidVaultRepository(fluidPool, nil, 0, 0)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	f := &fluidTestFixture{repo: repo, pool: fluidPool}
	f.createTestFixtures(t, ctx)
	return f
}

func (f *fluidTestFixture) createTestFixtures(t *testing.T, ctx context.Context) {
	t.Helper()

	err := f.pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, '\x52Aa899454998Be5b000Ad077a46Bbe360F4e497'::bytea, 'fluid', 'lending', 19239106, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&f.protocolID)
	if err != nil {
		t.Fatalf("failed to create fluid protocol: %v", err)
	}

	err = f.pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (chain_id, address) DO UPDATE SET id = token.id
		 RETURNING id`,
		1, bytes20(0x01), "WETH", 18,
	).Scan(&f.collTokenID)
	if err != nil {
		t.Fatalf("failed to create collateral token: %v", err)
	}

	err = f.pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (chain_id, address) DO UPDATE SET id = token.id
		 RETURNING id`,
		1, bytes20(0x02), "USDC", 6,
	).Scan(&f.debtTokenID)
	if err != nil {
		t.Fatalf("failed to create debt token: %v", err)
	}
}

// newTestVault returns a valid FluidVault for the fixture, with the given
// address first byte; pass a closure to override fields.
func (f *fluidTestFixture) newTestVault(firstByte byte) *entity.FluidVault {
	return &entity.FluidVault{
		ChainID:           1,
		ProtocolID:        f.protocolID,
		Address:           bytes20(firstByte),
		VaultType:         "T1",
		CollateralTokenID: f.collTokenID,
		DebtTokenID:       f.debtTokenID,
		CreatedAtBlock:    19500000,
	}
}

// createVault records one vault in its own committed tx and returns its id.
func (f *fluidTestFixture) createVault(t *testing.T, ctx context.Context, v *entity.FluidVault) int64 {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	ids, err := f.repo.RecordVaults(ctx, tx, []*entity.FluidVault{v})
	if err != nil {
		t.Fatalf("RecordVaults: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return ids[common.BytesToAddress(v.Address)]
}

func bytes20(first byte) []byte {
	b := make([]byte, 20)
	b[0] = first
	return b
}

// --- Registry tests ---

func TestFluidRecordVaults_CreateAndGetAll(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	id := f.createVault(t, ctx, f.newTestVault(0xaa))
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	vaults, err := f.repo.GetAllVaults(ctx, 1)
	if err != nil {
		t.Fatalf("GetAllVaults: %v", err)
	}
	if len(vaults) != 1 {
		t.Fatalf("expected 1 vault, got %d", len(vaults))
	}
	got := vaults[common.BytesToAddress(bytes20(0xaa))]
	if got == nil {
		t.Fatal("expected vault keyed by address")
	}
	if got.ID != id {
		t.Errorf("ID = %d, want %d", got.ID, id)
	}
	if got.VaultType != "T1" {
		t.Errorf("VaultType = %q, want T1", got.VaultType)
	}
	if got.CollateralTokenID != f.collTokenID || got.DebtTokenID != f.debtTokenID {
		t.Errorf("token ids = (%d,%d), want (%d,%d)", got.CollateralTokenID, got.DebtTokenID, f.collTokenID, f.debtTokenID)
	}
}

func TestFluidRecordVaults_MixedChainsFails(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	// Result map is keyed by address alone, so a batch mixing chains with the
	// same address would silently drop the colliding entry. Reject it instead.
	v1 := f.newTestVault(0xf1)
	v2 := f.newTestVault(0xf1)
	v2.ChainID = 137

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = f.repo.RecordVaults(ctx, tx, []*entity.FluidVault{v1, v2})
	if err == nil {
		t.Fatal("expected error on mixed-chain batch, got nil")
	}
	if !strings.Contains(err.Error(), "mixed chain IDs") {
		t.Errorf("error %q should mention mixed chain IDs", err.Error())
	}
}

func TestFluidGetAllVaults_Empty(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	vaults, err := f.repo.GetAllVaults(ctx, 1)
	if err != nil {
		t.Fatalf("GetAllVaults: %v", err)
	}
	if len(vaults) != 0 {
		t.Errorf("expected 0 vaults, got %d", len(vaults))
	}
}

func TestFluidRecordVaults_Idempotent(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	v := f.newTestVault(0xbb)
	id1 := f.createVault(t, ctx, v)
	id2 := f.createVault(t, ctx, v)
	if id1 != id2 {
		t.Errorf("RecordVaults not idempotent: first=%d second=%d", id1, id2)
	}
}

func TestFluidRecordVaults_ImmutableFieldChangeFails(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	f.createVault(t, ctx, f.newTestVault(0xcc))

	// Same address, different debt token — must fail rather than overwrite.
	changed := f.newTestVault(0xcc)
	changed.DebtTokenID = f.collTokenID

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = f.repo.RecordVaults(ctx, tx, []*entity.FluidVault{changed})
	if err == nil {
		t.Fatal("expected error on immutable field change, got nil")
	}
	if !strings.Contains(err.Error(), "debt_token_id") {
		t.Errorf("error %q should mention debt_token_id", err.Error())
	}
}

func TestFluidRecordVaults_ProtocolIDChangeFails(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	f.createVault(t, ctx, f.newTestVault(0xc1))

	// Seed a second protocol so the incoming protocol_id is a real, distinct id.
	var otherProtocolID int64
	if err := f.pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, '\x000000000000000000000000000000000000c1c1'::bytea, 'fluid-other', 'lending', 19239106, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&otherProtocolID); err != nil {
		t.Fatalf("failed to seed second protocol: %v", err)
	}

	// Same address, different protocol_id — must fail rather than overwrite.
	changed := f.newTestVault(0xc1)
	changed.ProtocolID = otherProtocolID

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = f.repo.RecordVaults(ctx, tx, []*entity.FluidVault{changed})
	if err == nil {
		t.Fatal("expected error on protocol_id change, got nil")
	}
	if !strings.Contains(err.Error(), "protocol_id") {
		t.Errorf("error %q should mention protocol_id", err.Error())
	}
}

func TestFluidRecordVaults_CreatedAtBlockChangeFails(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	f.createVault(t, ctx, f.newTestVault(0xc2))

	// Same address, different created_at_block — must fail rather than overwrite.
	changed := f.newTestVault(0xc2)
	changed.CreatedAtBlock = 19500001

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = f.repo.RecordVaults(ctx, tx, []*entity.FluidVault{changed})
	if err == nil {
		t.Fatal("expected error on created_at_block change, got nil")
	}
	if !strings.Contains(err.Error(), "created_at_block") {
		t.Errorf("error %q should mention created_at_block", err.Error())
	}
}

// --- State tests ---

func newFluidState(vaultID, block int64, version int, ts time.Time) *entity.FluidVaultState {
	return &entity.FluidVaultState{
		FluidVaultID:    vaultID,
		BlockNumber:     block,
		BlockVersion:    version,
		Timestamp:       ts,
		TotalCollateral: big.NewInt(1_000_000),
		TotalDebt:       big.NewInt(400_000),
	}
}

func TestFluidSaveVaultStates_BasicWithOptionalFields(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xd1))

	ts := time.Unix(1700000000, 0).UTC()
	s := newFluidState(vaultID, 19600000, 0, ts)
	s.SupplyExchangePrice = big.NewInt(1_000_000_000_123)
	s.BorrowRate = big.NewInt(550)

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := f.repo.SaveVaultStates(ctx, tx, []*entity.FluidVaultState{s}); err != nil {
		t.Fatalf("SaveVaultStates: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var totalCollateral, totalDebt, supplyExchangePrice, borrowRate string
	var borrowExchangePrice, supplyRate *string
	err = f.pool.QueryRow(ctx,
		`SELECT total_collateral, total_debt, supply_exchange_price, borrow_exchange_price, supply_rate, borrow_rate
		 FROM fluid_vault_state WHERE fluid_vault_id = $1 AND block_number = $2`,
		vaultID, int64(19600000),
	).Scan(&totalCollateral, &totalDebt, &supplyExchangePrice, &borrowExchangePrice, &supplyRate, &borrowRate)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	if totalCollateral != "1000000" || totalDebt != "400000" {
		t.Errorf("totals = (%s,%s), want (1000000,400000)", totalCollateral, totalDebt)
	}
	if supplyExchangePrice != "1000000000123" {
		t.Errorf("supply_exchange_price = %s, want 1000000000123", supplyExchangePrice)
	}
	if borrowRate != "550" {
		t.Errorf("borrow_rate = %s, want 550", borrowRate)
	}
	if borrowExchangePrice != nil || supplyRate != nil {
		t.Errorf("expected nil borrow_exchange_price and supply_rate, got %v / %v", borrowExchangePrice, supplyRate)
	}
}

// TestFluidSaveVaultStates_IdempotentReusesProcessingVersion verifies that a
// same-build re-save of the identical snapshot key reuses processing_version 0
// (the trigger) and dedupes via ON CONFLICT DO NOTHING — leaving exactly one
// row with the first-written values.
func TestFluidSaveVaultStates_IdempotentReusesProcessingVersion(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xd2))
	ts := time.Unix(1700001000, 0).UTC()

	first := newFluidState(vaultID, 19600100, 0, ts)
	saveState(t, ctx, f, first)

	// Re-save same key with different values — must be ignored (DO NOTHING).
	second := newFluidState(vaultID, 19600100, 0, ts)
	second.TotalCollateral = big.NewInt(9_999_999)
	saveState(t, ctx, f, second)

	var count int
	var totalCollateral string
	var processingVersion int
	if err := f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fluid_vault_state WHERE fluid_vault_id = $1 AND block_number = $2`,
		vaultID, int64(19600100),
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after idempotent re-save, got %d", count)
	}
	if err := f.pool.QueryRow(ctx,
		`SELECT total_collateral, processing_version FROM fluid_vault_state WHERE fluid_vault_id = $1 AND block_number = $2`,
		vaultID, int64(19600100),
	).Scan(&totalCollateral, &processingVersion); err != nil {
		t.Fatalf("select: %v", err)
	}
	if totalCollateral != "1000000" {
		t.Errorf("expected first write preserved (1000000), got %s", totalCollateral)
	}
	if processingVersion != 0 {
		t.Errorf("expected processing_version 0, got %d", processingVersion)
	}
}

// TestFluidSaveVaultStates_NewBuildBumpsProcessingVersion verifies a different
// build_id at the same snapshot key inserts a new row at processing_version 1
// rather than deduping (a re-index by a newer build is preserved for audit).
func TestFluidSaveVaultStates_NewBuildBumpsProcessingVersion(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xd3))
	ts := time.Unix(1700002000, 0).UTC()

	repo0, err := NewFluidVaultRepository(f.pool, nil, 0, 0)
	if err != nil {
		t.Fatalf("new repo0: %v", err)
	}
	repo1, err := NewFluidVaultRepository(f.pool, nil, 1, 0)
	if err != nil {
		t.Fatalf("new repo1: %v", err)
	}

	saveStateWith(t, ctx, f, repo0, newFluidState(vaultID, 19600200, 0, ts))
	saveStateWith(t, ctx, f, repo1, newFluidState(vaultID, 19600200, 0, ts))

	versions := make(map[int]bool)
	rows, err := f.pool.Query(ctx,
		`SELECT processing_version, build_id FROM fluid_vault_state WHERE fluid_vault_id = $1 AND block_number = $2 ORDER BY processing_version`,
		vaultID, int64(19600200))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var pv, bid int
		if err := rows.Scan(&pv, &bid); err != nil {
			t.Fatalf("scan: %v", err)
		}
		versions[pv] = true
	}
	if !versions[0] || !versions[1] {
		t.Errorf("expected processing_versions {0,1}, got %v", versions)
	}
}

// TestFluidSaveVaultStates_ReorgNewBlockVersion verifies a reorg (same block,
// new block_version) inserts a separate row rather than deduping.
func TestFluidSaveVaultStates_ReorgNewBlockVersion(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xd4))
	ts := time.Unix(1700003000, 0).UTC()

	saveState(t, ctx, f, newFluidState(vaultID, 19600300, 0, ts))
	reorged := newFluidState(vaultID, 19600300, 1, ts)
	reorged.TotalDebt = big.NewInt(123_456)
	saveState(t, ctx, f, reorged)

	var count int
	if err := f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fluid_vault_state WHERE fluid_vault_id = $1 AND block_number = $2`,
		vaultID, int64(19600300),
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows (block_version 0 and 1), got %d", count)
	}
}

func TestFluidSaveVaultStates_BatchMultipleVaults(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	v1 := f.createVault(t, ctx, f.newTestVault(0xe1))
	v2 := f.createVault(t, ctx, f.newTestVault(0xe2))
	ts := time.Unix(1700004000, 0).UTC()

	states := []*entity.FluidVaultState{
		newFluidState(v1, 19600400, 0, ts),
		newFluidState(v2, 19600400, 0, ts),
	}

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := f.repo.SaveVaultStates(ctx, tx, states); err != nil {
		t.Fatalf("SaveVaultStates: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var count int
	if err := f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fluid_vault_state WHERE block_number = $1`, int64(19600400),
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestFluidSaveVaultStates_PartialDedupFails verifies that a batch where one
// row is a same-key duplicate and one is fresh fails the save (rather than
// silently committing only the fresh row).
func TestFluidSaveVaultStates_PartialDedupFails(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xe3))
	ts := time.Unix(1700005000, 0).UTC()

	// Pre-commit one row.
	saveState(t, ctx, f, newFluidState(vaultID, 19600500, 0, ts))

	// Now save a batch: the already-present row + a fresh row.
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	batch := []*entity.FluidVaultState{
		newFluidState(vaultID, 19600500, 0, ts), // collides
		newFluidState(vaultID, 19600501, 0, ts), // fresh
	}
	err = f.repo.SaveVaultStates(ctx, tx, batch)
	if err == nil {
		t.Fatal("expected partial-dedup error, got nil")
	}
	if !strings.Contains(err.Error(), "partially deduplicated") {
		t.Errorf("error %q should mention partial dedup", err.Error())
	}
}

// TestFluidSaveVaultStates_CrossChunkPartialDedupFails forces batchSize=1 so a
// two-row save spans two chunks, with one row colliding against a pre-committed
// row and one fresh. checkDedupedStateRows runs once on the summed count, so the
// partial dedup is detected across chunk boundaries and the save fails (rather
// than reading the colliding chunk as a clean full-dedup).
func TestFluidSaveVaultStates_CrossChunkPartialDedupFails(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xe6))
	ts := time.Unix(1700008000, 0).UTC()

	repo1, err := NewFluidVaultRepository(f.pool, nil, 0, 1) // batchSize=1
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}

	saveStateWith(t, ctx, f, repo1, newFluidState(vaultID, 19600800, 0, ts))

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	batch := []*entity.FluidVaultState{
		newFluidState(vaultID, 19600800, 0, ts), // collides, lands in chunk 1
		newFluidState(vaultID, 19600801, 0, ts), // fresh, lands in chunk 2
	}
	err = repo1.SaveVaultStates(ctx, tx, batch)
	if err == nil {
		t.Fatal("expected partial-dedup error across chunks, got nil")
	}
	if !strings.Contains(err.Error(), "partially deduplicated") {
		t.Errorf("error %q should mention partial dedup", err.Error())
	}
}

func TestFluidSaveVaultStates_Rollback(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xe4))
	ts := time.Unix(1700006000, 0).UTC()

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := f.repo.SaveVaultStates(ctx, tx, []*entity.FluidVaultState{newFluidState(vaultID, 19600600, 0, ts)}); err != nil {
		t.Fatalf("SaveVaultStates: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	var count int
	if err := f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fluid_vault_state WHERE fluid_vault_id = $1 AND block_number = $2`,
		vaultID, int64(19600600),
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}
}

func TestFluidSaveVaultStates_LargeBigIntPrecision(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()
	vaultID := f.createVault(t, ctx, f.newTestVault(0xe5))
	ts := time.Unix(1700007000, 0).UTC()

	maxUint256, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	s := newFluidState(vaultID, 19600700, 0, ts)
	s.TotalCollateral = maxUint256

	saveState(t, ctx, f, s)

	var totalCollateral string
	if err := f.pool.QueryRow(ctx,
		`SELECT total_collateral FROM fluid_vault_state WHERE fluid_vault_id = $1 AND block_number = $2`,
		vaultID, int64(19600700),
	).Scan(&totalCollateral); err != nil {
		t.Fatalf("query: %v", err)
	}
	if totalCollateral != maxUint256.String() {
		t.Errorf("precision lost: got %s, want %s", totalCollateral, maxUint256.String())
	}
}

func TestFluidSaveVaultStates_Empty(t *testing.T) {
	f := setupFluidTest(t)
	ctx := context.Background()

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	if err := f.repo.SaveVaultStates(ctx, tx, nil); err != nil {
		t.Errorf("SaveVaultStates(nil) should be a no-op, got %v", err)
	}
}

// saveState commits one state via the fixture's default (build 0) repo.
func saveState(t *testing.T, ctx context.Context, f *fluidTestFixture, s *entity.FluidVaultState) {
	t.Helper()
	saveStateWith(t, ctx, f, f.repo, s)
}

func saveStateWith(t *testing.T, ctx context.Context, f *fluidTestFixture, repo *FluidVaultRepository, s *entity.FluidVaultState) {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := repo.SaveVaultStates(ctx, tx, []*entity.FluidVaultState{s}); err != nil {
		t.Fatalf("SaveVaultStates: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
