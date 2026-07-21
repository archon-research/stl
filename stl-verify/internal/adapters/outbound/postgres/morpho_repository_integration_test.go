//go:build integration

package postgres

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const morphoSchemaName = "test_morpho"

var morphoPool *pgxpool.Pool

func init() {
	registerTestFileSetup(morphoSchemaName, func() {
		morphoPool = testutil.SetupSchemaForMain(sharedDSN, morphoSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, morphoPool, morphoSchemaName)
	})
}

// truncateMorpho clears morpho-related tables for test isolation.
func truncateMorpho(t *testing.T, ctx context.Context) {
	t.Helper()
	// Delete children before parents: morpho_adapter_state FKs morpho_adapter,
	// morpho_vault_cap and morpho_adapter FK morpho_vault.
	tables := []string{
		`morpho_market_state`,
		`morpho_market_position`,
		`morpho_vault_state`,
		`morpho_vault_position`,
		`morpho_adapter_state`,
		`morpho_vault_cap`,
		`morpho_market`,
		`morpho_adapter`,
		`morpho_vault`,
	}
	for _, table := range tables {
		if _, err := morphoPool.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("failed to truncate %s: %v", table, err)
		}
	}
	// protocol is referenced by morpho_market; CASCADE handles any remaining refs.
	if _, err := morphoPool.Exec(ctx, `TRUNCATE protocol CASCADE`); err != nil {
		t.Fatalf("failed to truncate protocol: %v", err)
	}
	// "user" is referenced by morpho_market_position/morpho_vault_position; CASCADE handles them.
	if _, err := morphoPool.Exec(ctx, `TRUNCATE "user" CASCADE`); err != nil {
		t.Fatalf("failed to truncate user: %v", err)
	}
	// token has FK references from many tables; use CASCADE to clear dependents.
	if _, err := morphoPool.Exec(ctx, `TRUNCATE token CASCADE`); err != nil {
		t.Fatalf("failed to truncate token: %v", err)
	}
}

// morphoTestFixture holds test dependencies for morpho repository tests.
type morphoTestFixture struct {
	repo *MorphoRepository
	pool *pgxpool.Pool
	// Pre-created IDs for foreign key references
	protocolID  int64
	loanTokenID int64
	collTokenID int64
	userID      int64
}

// setupMorphoTest returns a connected MorphoRepository using the schema-specific pool.
func setupMorphoTest(t *testing.T) *morphoTestFixture {
	t.Helper()
	ctx := context.Background()

	truncateMorpho(t, ctx)

	repo, err := NewMorphoRepository(morphoPool, nil, 0)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	fixture := &morphoTestFixture{
		repo: repo,
		pool: morphoPool,
	}

	fixture.createTestFixtures(t, ctx)

	return fixture
}

// createTestFixtures creates the required chain, user, protocol, and token records.
func (f *morphoTestFixture) createTestFixtures(t *testing.T, ctx context.Context) {
	t.Helper()

	// Create the Morpho Blue protocol (previously seeded by migration, now created at runtime by GetOrCreateProtocol)
	err := f.pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, '\xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb'::bytea, 'Morpho Blue', 'lending', 18883124, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&f.protocolID)
	if err != nil {
		t.Fatalf("failed to create Morpho Blue protocol: %v", err)
	}

	// Create test tokens (loan and collateral)
	err = f.pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4) RETURNING id`,
		1, []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xab, 0xcd, 0xef, 0x01}, "USDC", 6,
	).Scan(&f.loanTokenID)
	if err != nil {
		t.Fatalf("failed to create loan token: %v", err)
	}

	err = f.pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4) RETURNING id`,
		1, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x12, 0x34, 0x56, 0x78}, "WETH", 18,
	).Scan(&f.collTokenID)
	if err != nil {
		t.Fatalf("failed to create collateral token: %v", err)
	}

	// Create a test user
	err = f.pool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block) VALUES ($1, $2, $3) RETURNING id`,
		1, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14}, 18883124,
	).Scan(&f.userID)
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}
}

// createTestMarket creates a morpho market via the repository and returns its DB ID.
func (f *morphoTestFixture) createTestMarket(t *testing.T, ctx context.Context, marketIDBytes []byte) int64 {
	t.Helper()

	market := &entity.MorphoMarket{
		ChainID:           1,
		ProtocolID:        f.protocolID,
		MarketID:          common.BytesToHash(marketIDBytes),
		LoanTokenID:       f.loanTokenID,
		CollateralTokenID: f.collTokenID,
		OracleAddress:     common.Address{},
		IrmAddress:        common.Address{},
		LLTV:              big.NewInt(860000000000000000),
		CreatedAtBlock:    18883124,
	}

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := f.repo.GetOrCreateMarket(ctx, tx, market)
	if err != nil {
		t.Fatalf("failed to create market: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	return id
}

// createTestVault creates a morpho vault via the repository and returns its DB ID.
func (f *morphoTestFixture) createTestVault(t *testing.T, ctx context.Context, address []byte) int64 {
	t.Helper()

	vault := &entity.MorphoVault{
		ChainID:        1,
		ProtocolID:     f.protocolID,
		Address:        address,
		Name:           "Gauntlet USDC Core",
		Symbol:         "gtUSDCcore",
		AssetTokenID:   f.loanTokenID,
		VaultVersion:   entity.MorphoVaultV1,
		CreatedAtBlock: 19000000,
	}

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := f.repo.GetOrCreateVault(ctx, tx, vault)
	if err != nil {
		t.Fatalf("failed to create vault: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	return id
}

// --- Market Tests ---

func TestGetOrCreateMarket_CreateNew(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketID := common.BytesToHash([]byte("test-market-id-1234567890abcdef"))

	market := &entity.MorphoMarket{
		ChainID:           1,
		ProtocolID:        fixture.protocolID,
		MarketID:          marketID,
		LoanTokenID:       fixture.loanTokenID,
		CollateralTokenID: fixture.collTokenID,
		OracleAddress:     common.Address{},
		IrmAddress:        common.Address{},
		LLTV:              big.NewInt(860000000000000000),
		CreatedAtBlock:    18883124,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := fixture.repo.GetOrCreateMarket(ctx, tx, market)
	if err != nil {
		t.Fatalf("GetOrCreateMarket failed: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive id, got %d", id)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify via GetMarketByMarketID
	got, err := fixture.repo.GetMarketByMarketID(ctx, 1, marketID)
	if err != nil {
		t.Fatalf("GetMarketByMarketID failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected market, got nil")
	}
	if got.ID != id {
		t.Errorf("ID mismatch: got %d, want %d", got.ID, id)
	}
	if got.ProtocolID != fixture.protocolID {
		t.Errorf("ProtocolID mismatch: got %d, want %d", got.ProtocolID, fixture.protocolID)
	}
	if got.LLTV.Cmp(big.NewInt(860000000000000000)) != 0 {
		t.Errorf("LLTV mismatch: got %s, want 860000000000000000", got.LLTV)
	}
}

func TestGetOrCreateMarket_Idempotent(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketID := common.BytesToHash([]byte("idempotent-market-test-12345678"))

	market := &entity.MorphoMarket{
		ChainID:           1,
		ProtocolID:        fixture.protocolID,
		MarketID:          marketID,
		LoanTokenID:       fixture.loanTokenID,
		CollateralTokenID: fixture.collTokenID,
		OracleAddress:     common.Address{},
		IrmAddress:        common.Address{},
		LLTV:              big.NewInt(945000000000000000),
		CreatedAtBlock:    18883124,
	}

	// Create first time
	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	id1, err := fixture.repo.GetOrCreateMarket(ctx, tx1, market)
	if err != nil {
		t.Fatalf("first GetOrCreateMarket failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}

	// Create second time - should return same ID
	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	id2, err := fixture.repo.GetOrCreateMarket(ctx, tx2, market)
	if err != nil {
		t.Fatalf("second GetOrCreateMarket failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}

	if id1 != id2 {
		t.Errorf("GetOrCreateMarket not idempotent: first=%d, second=%d", id1, id2)
	}
}

func TestGetMarketByMarketID_NotFound(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()

	got, err := fixture.repo.GetMarketByMarketID(ctx, 1, common.BytesToHash([]byte("this-market-does-not-exist-1234")))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for non-existent market, got: %+v", got)
	}
}

// --- Market State Tests ---

func TestSaveMarketState_Basic(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketDBID := fixture.createTestMarket(t, ctx, []byte("state-test-market-id-12345678ab"))

	state := &entity.MorphoMarketState{
		MorphoMarketID:    marketDBID,
		BlockNumber:       19000000,
		BlockVersion:      0,
		TotalSupplyAssets: big.NewInt(1000000000000),
		TotalSupplyShares: big.NewInt(1000000000000000000),
		TotalBorrowAssets: big.NewInt(500000000000),
		TotalBorrowShares: big.NewInt(500000000000000000),
		LastUpdate:        1700000000,
		Fee:               big.NewInt(100000000000000000),
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveMarketState(ctx, tx, state)
	if err != nil {
		t.Fatalf("SaveMarketState failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify by querying directly
	var totalSupplyAssets, totalBorrowAssets, fee string
	err = fixture.pool.QueryRow(ctx,
		`SELECT total_supply_assets, total_borrow_assets, fee FROM morpho_market_state WHERE morpho_market_id = $1 AND block_number = $2 AND block_version = 0`,
		marketDBID, int64(19000000),
	).Scan(&totalSupplyAssets, &totalBorrowAssets, &fee)
	if err != nil {
		t.Fatalf("failed to query market state: %v", err)
	}
	if totalSupplyAssets != "1000000000000" {
		t.Errorf("totalSupplyAssets mismatch: got %s, want 1000000000000", totalSupplyAssets)
	}
	if totalBorrowAssets != "500000000000" {
		t.Errorf("totalBorrowAssets mismatch: got %s, want 500000000000", totalBorrowAssets)
	}
}

func TestSaveMarketState_WithAccrueInterest(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketDBID := fixture.createTestMarket(t, ctx, []byte("accrue-interest-test-market-1234"))

	state := &entity.MorphoMarketState{
		MorphoMarketID:    marketDBID,
		BlockNumber:       19000100,
		BlockVersion:      0,
		TotalSupplyAssets: big.NewInt(2000000000000),
		TotalSupplyShares: big.NewInt(2000000000000000000),
		TotalBorrowAssets: big.NewInt(1000000000000),
		TotalBorrowShares: big.NewInt(1000000000000000000),
		LastUpdate:        1700001000,
		Fee:               big.NewInt(100000000000000000),
	}
	state.WithAccrueInterest(
		big.NewInt(3170979198376458),
		big.NewInt(1234567890),
		big.NewInt(9876543210),
	)

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveMarketState(ctx, tx, state)
	if err != nil {
		t.Fatalf("SaveMarketState failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify AccrueInterest fields
	var prevBorrowRate, interestAccrued, feeShares *string
	err = fixture.pool.QueryRow(ctx,
		`SELECT prev_borrow_rate, interest_accrued, fee_shares FROM morpho_market_state WHERE morpho_market_id = $1 AND block_number = $2 AND block_version = 0`,
		marketDBID, int64(19000100),
	).Scan(&prevBorrowRate, &interestAccrued, &feeShares)
	if err != nil {
		t.Fatalf("failed to query market state: %v", err)
	}
	if prevBorrowRate == nil || *prevBorrowRate != "3170979198376458" {
		t.Errorf("prevBorrowRate mismatch: got %v", prevBorrowRate)
	}
	if interestAccrued == nil || *interestAccrued != "1234567890" {
		t.Errorf("interestAccrued mismatch: got %v", interestAccrued)
	}
	if feeShares == nil || *feeShares != "9876543210" {
		t.Errorf("feeShares mismatch: got %v", feeShares)
	}
}

func TestSaveMarketState_DuplicateIgnored(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketDBID := fixture.createTestMarket(t, ctx, []byte("upsert-state-test-market-123456"))

	// Insert first state
	state1 := &entity.MorphoMarketState{
		MorphoMarketID:    marketDBID,
		BlockNumber:       19000200,
		BlockVersion:      0,
		TotalSupplyAssets: big.NewInt(1000),
		TotalSupplyShares: big.NewInt(1000),
		TotalBorrowAssets: big.NewInt(500),
		TotalBorrowShares: big.NewInt(500),
		LastUpdate:        1700000000,
		Fee:               big.NewInt(0),
	}

	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	if err := fixture.repo.SaveMarketState(ctx, tx1, state1); err != nil {
		t.Fatalf("first SaveMarketState failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}

	// Insert duplicate with different values at same key — should be ignored
	state2 := &entity.MorphoMarketState{
		MorphoMarketID:    marketDBID,
		BlockNumber:       19000200,
		BlockVersion:      0,
		TotalSupplyAssets: big.NewInt(9999),
		TotalSupplyShares: big.NewInt(9999),
		TotalBorrowAssets: big.NewInt(7777),
		TotalBorrowShares: big.NewInt(7777),
		LastUpdate:        1700001111,
		Fee:               big.NewInt(50000000000000000),
	}

	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	if err := fixture.repo.SaveMarketState(ctx, tx2, state2); err != nil {
		t.Fatalf("duplicate SaveMarketState failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}

	// Verify first write preserved (DO NOTHING semantics)
	var totalSupplyAssets string
	err = fixture.pool.QueryRow(ctx,
		`SELECT total_supply_assets FROM morpho_market_state WHERE morpho_market_id = $1 AND block_number = $2 AND block_version = 0`,
		marketDBID, int64(19000200),
	).Scan(&totalSupplyAssets)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if totalSupplyAssets != "1000" {
		t.Errorf("expected first write preserved (1000), got %s", totalSupplyAssets)
	}
}

// --- Position Tests ---

func TestSaveMarketPosition_Basic(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketDBID := fixture.createTestMarket(t, ctx, []byte("position-test-market-id-1234567"))

	position := &entity.MorphoMarketPosition{
		UserID:         fixture.userID,
		MorphoMarketID: marketDBID,
		BlockNumber:    19000300,
		BlockVersion:   0,
		SupplyShares:   big.NewInt(500000000000000000),
		BorrowShares:   big.NewInt(0),
		Collateral:     big.NewInt(1000000000000000000),
		SupplyAssets:   big.NewInt(500000),
		BorrowAssets:   big.NewInt(0),
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveMarketPosition(ctx, tx, position)
	if err != nil {
		t.Fatalf("SaveMarketPosition failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify
	var supplyShares, borrowShares, collateral, supplyAssets, borrowAssets string
	err = fixture.pool.QueryRow(ctx,
		`SELECT supply_shares, borrow_shares, collateral, supply_assets, borrow_assets
		 FROM morpho_market_position WHERE user_id = $1 AND morpho_market_id = $2 AND block_number = $3 AND block_version = 0`,
		fixture.userID, marketDBID, int64(19000300),
	).Scan(&supplyShares, &borrowShares, &collateral, &supplyAssets, &borrowAssets)
	if err != nil {
		t.Fatalf("failed to query position: %v", err)
	}
	if supplyShares != "500000000000000000" {
		t.Errorf("supplyShares mismatch: got %s", supplyShares)
	}
	if borrowShares != "0" {
		t.Errorf("borrowShares mismatch: got %s", borrowShares)
	}
}

func TestSaveMarketPosition_DuplicateIgnored(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketDBID := fixture.createTestMarket(t, ctx, []byte("pos-upsert-test-market-12345678"))

	// Insert initial position
	pos1 := &entity.MorphoMarketPosition{
		UserID:         fixture.userID,
		MorphoMarketID: marketDBID,
		BlockNumber:    19000400,
		BlockVersion:   0,
		SupplyShares:   big.NewInt(100),
		BorrowShares:   big.NewInt(0),
		Collateral:     big.NewInt(0),
		SupplyAssets:   big.NewInt(100),
		BorrowAssets:   big.NewInt(0),
	}

	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	if err := fixture.repo.SaveMarketPosition(ctx, tx1, pos1); err != nil {
		t.Fatalf("first SaveMarketPosition failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}

	// Insert duplicate with different values — should be ignored
	pos2 := &entity.MorphoMarketPosition{
		UserID:         fixture.userID,
		MorphoMarketID: marketDBID,
		BlockNumber:    19000400,
		BlockVersion:   0,
		SupplyShares:   big.NewInt(999),
		BorrowShares:   big.NewInt(50),
		Collateral:     big.NewInt(200),
		SupplyAssets:   big.NewInt(999),
		BorrowAssets:   big.NewInt(50),
	}

	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	if err := fixture.repo.SaveMarketPosition(ctx, tx2, pos2); err != nil {
		t.Fatalf("duplicate SaveMarketPosition failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}

	// Verify first write preserved (DO NOTHING semantics)
	var supplyShares string
	err = fixture.pool.QueryRow(ctx,
		`SELECT supply_shares FROM morpho_market_position WHERE user_id = $1 AND morpho_market_id = $2 AND block_number = $3 AND block_version = 0`,
		fixture.userID, marketDBID, int64(19000400),
	).Scan(&supplyShares)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if supplyShares != "100" {
		t.Errorf("expected first write preserved (supply_shares 100), got %s", supplyShares)
	}
}

func TestSaveMarketPosition_Rollback(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketDBID := fixture.createTestMarket(t, ctx, []byte("pos-rollback-test-market-1234567"))

	position := &entity.MorphoMarketPosition{
		UserID:         fixture.userID,
		MorphoMarketID: marketDBID,
		BlockNumber:    19000500,
		BlockVersion:   0,
		SupplyShares:   big.NewInt(100),
		BorrowShares:   big.NewInt(0),
		Collateral:     big.NewInt(0),
		SupplyAssets:   big.NewInt(100),
		BorrowAssets:   big.NewInt(0),
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	err = fixture.repo.SaveMarketPosition(ctx, tx, position)
	if err != nil {
		t.Fatalf("SaveMarketPosition failed: %v", err)
	}

	// Rollback instead of commit
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify no records exist after rollback
	var count int
	err = fixture.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM morpho_market_position WHERE morpho_market_id = $1 AND block_number = $2`,
		marketDBID, int64(19000500),
	).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records after rollback, got %d", count)
	}
}

func TestSaveMarketPosition_LargeBigIntPrecision(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	marketDBID := fixture.createTestMarket(t, ctx, []byte("large-int-test-market-1234567890"))

	// max uint256
	maxUint256, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)

	position := &entity.MorphoMarketPosition{
		UserID:         fixture.userID,
		MorphoMarketID: marketDBID,
		BlockNumber:    19000600,
		BlockVersion:   0,
		SupplyShares:   maxUint256,
		BorrowShares:   big.NewInt(0),
		Collateral:     big.NewInt(0),
		SupplyAssets:   maxUint256,
		BorrowAssets:   big.NewInt(0),
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveMarketPosition(ctx, tx, position)
	if err != nil {
		t.Fatalf("SaveMarketPosition with large values failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify precision preserved
	var supplyShares string
	err = fixture.pool.QueryRow(ctx,
		`SELECT supply_shares FROM morpho_market_position WHERE user_id = $1 AND morpho_market_id = $2 AND block_number = $3 AND block_version = 0`,
		fixture.userID, marketDBID, int64(19000600),
	).Scan(&supplyShares)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if supplyShares != maxUint256.String() {
		t.Errorf("precision lost: got %s, want %s", supplyShares, maxUint256.String())
	}
}

// --- Vault Tests ---

func TestGetOrCreateVault_CreateNew(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	vaultAddr := []byte("vault-addr-123456789")

	vault := &entity.MorphoVault{
		ChainID:        1,
		ProtocolID:     fixture.protocolID,
		Address:        vaultAddr,
		Name:           "Gauntlet USDC Core",
		Symbol:         "gtUSDCcore",
		AssetTokenID:   fixture.loanTokenID,
		VaultVersion:   entity.MorphoVaultV1,
		CreatedAtBlock: 19000000,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := fixture.repo.GetOrCreateVault(ctx, tx, vault)
	if err != nil {
		t.Fatalf("GetOrCreateVault failed: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive id, got %d", id)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify via GetVaultByAddress
	got, err := fixture.repo.GetVaultByAddress(ctx, 1, common.BytesToAddress(vaultAddr))
	if err != nil {
		t.Fatalf("GetVaultByAddress failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected vault, got nil")
	}
	if got.ID != id {
		t.Errorf("ID mismatch: got %d, want %d", got.ID, id)
	}
	if got.Name != "Gauntlet USDC Core" {
		t.Errorf("Name mismatch: got %s", got.Name)
	}
	if got.Symbol != "gtUSDCcore" {
		t.Errorf("Symbol mismatch: got %s", got.Symbol)
	}
	if got.VaultVersion != entity.MorphoVaultV1 {
		t.Errorf("VaultVersion mismatch: got %d, want %d", got.VaultVersion, entity.MorphoVaultV1)
	}
}

func TestGetOrCreateVault_Idempotent(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	vaultAddr := []byte("vault-idemp-12345678")

	vault := &entity.MorphoVault{
		ChainID:        1,
		ProtocolID:     fixture.protocolID,
		Address:        vaultAddr,
		Name:           "Test Vault",
		Symbol:         "tVLT",
		AssetTokenID:   fixture.loanTokenID,
		VaultVersion:   entity.MorphoVaultV1_1,
		CreatedAtBlock: 19100000,
	}

	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	id1, err := fixture.repo.GetOrCreateVault(ctx, tx1, vault)
	if err != nil {
		t.Fatalf("first GetOrCreateVault failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}

	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	id2, err := fixture.repo.GetOrCreateVault(ctx, tx2, vault)
	if err != nil {
		t.Fatalf("second GetOrCreateVault failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}

	if id1 != id2 {
		t.Errorf("GetOrCreateVault not idempotent: first=%d, second=%d", id1, id2)
	}
}

func TestGetVaultByAddress_NotFound(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	nonExistentAddr := common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

	got, err := fixture.repo.GetVaultByAddress(ctx, 1, nonExistentAddr)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for non-existent vault, got: %+v", got)
	}
}

func TestGetAllVaults_Empty(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()

	vaults, err := fixture.repo.GetAllVaults(ctx, 1)
	if err != nil {
		t.Fatalf("GetAllVaults failed: %v", err)
	}
	if len(vaults) != 0 {
		t.Errorf("expected 0 vaults, got %d", len(vaults))
	}
}

func TestGetAllVaults_MultipleVaults(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()

	// Create 3 vaults
	for i := 0; i < 3; i++ {
		addr := make([]byte, 20)
		addr[0] = byte(i + 1)
		fixture.createTestVault(t, ctx, addr)
	}

	vaults, err := fixture.repo.GetAllVaults(ctx, 1)
	if err != nil {
		t.Fatalf("GetAllVaults failed: %v", err)
	}
	if len(vaults) != 3 {
		t.Errorf("expected 3 vaults, got %d", len(vaults))
	}

	// Verify vault details are populated
	for addr, vault := range vaults {
		if vault.ID == 0 {
			t.Errorf("vault %s has zero ID", addr.Hex())
		}
		if vault.Name == "" {
			t.Errorf("vault %s has empty name", addr.Hex())
		}
	}
}

// --- Vault State Tests ---

func TestSaveVaultState_Basic(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	vaultDBID := fixture.createTestVault(t, ctx, []byte("vstate-test-12345678"))

	state := &entity.MorphoVaultState{
		MorphoVaultID: vaultDBID,
		BlockNumber:   19100000,
		BlockVersion:  0,
		TotalAssets:   big.NewInt(5000000000000),
		TotalShares:   big.NewInt(5000000000000000000),
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveVaultState(ctx, tx, state)
	if err != nil {
		t.Fatalf("SaveVaultState failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify
	var totalAssets, totalShares string
	var feeShares, newTotalAssets *string
	err = fixture.pool.QueryRow(ctx,
		`SELECT total_assets, total_shares, fee_shares, new_total_assets FROM morpho_vault_state WHERE morpho_vault_id = $1 AND block_number = $2 AND block_version = 0`,
		vaultDBID, int64(19100000),
	).Scan(&totalAssets, &totalShares, &feeShares, &newTotalAssets)
	if err != nil {
		t.Fatalf("failed to query vault state: %v", err)
	}
	if totalAssets != "5000000000000" {
		t.Errorf("totalAssets mismatch: got %s", totalAssets)
	}
	if totalShares != "5000000000000000000" {
		t.Errorf("totalShares mismatch: got %s", totalShares)
	}
	if feeShares != nil {
		t.Errorf("expected nil feeShares, got %v", feeShares)
	}
	if newTotalAssets != nil {
		t.Errorf("expected nil newTotalAssets, got %v", newTotalAssets)
	}
}

func TestSaveVaultState_WithAccrueInterest(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	vaultDBID := fixture.createTestVault(t, ctx, []byte("vs-accrue-test-12345"))

	state := &entity.MorphoVaultState{
		MorphoVaultID: vaultDBID,
		BlockNumber:   19100100,
		BlockVersion:  0,
		TotalAssets:   big.NewInt(6000000000000),
		TotalShares:   big.NewInt(6000000000000000000),
	}
	state.WithAccrueInterest(big.NewInt(12345678), big.NewInt(6000100000000), nil, nil) // V1: no previousTotalAssets or managementFeeShares

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveVaultState(ctx, tx, state)
	if err != nil {
		t.Fatalf("SaveVaultState failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	var feeShares, newTotalAssets *string
	err = fixture.pool.QueryRow(ctx,
		`SELECT fee_shares, new_total_assets FROM morpho_vault_state WHERE morpho_vault_id = $1 AND block_number = $2 AND block_version = 0`,
		vaultDBID, int64(19100100),
	).Scan(&feeShares, &newTotalAssets)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if feeShares == nil || *feeShares != "12345678" {
		t.Errorf("feeShares mismatch: got %v", feeShares)
	}
	if newTotalAssets == nil || *newTotalAssets != "6000100000000" {
		t.Errorf("newTotalAssets mismatch: got %v", newTotalAssets)
	}
}

// --- Vault Position Tests ---

func TestSaveVaultPosition_Basic(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	vaultDBID := fixture.createTestVault(t, ctx, []byte("vpos-test-1234567890"))

	position := &entity.MorphoVaultPosition{
		UserID:        fixture.userID,
		MorphoVaultID: vaultDBID,
		BlockNumber:   19200000,
		BlockVersion:  0,
		Shares:        big.NewInt(1000000000000000000),
		Assets:        big.NewInt(1000000),
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveVaultPosition(ctx, tx, position)
	if err != nil {
		t.Fatalf("SaveVaultPosition failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify
	var shares, assets string
	err = fixture.pool.QueryRow(ctx,
		`SELECT shares, assets FROM morpho_vault_position WHERE user_id = $1 AND morpho_vault_id = $2 AND block_number = $3 AND block_version = 0`,
		fixture.userID, vaultDBID, int64(19200000),
	).Scan(&shares, &assets)
	if err != nil {
		t.Fatalf("failed to query vault position: %v", err)
	}
	if shares != "1000000000000000000" {
		t.Errorf("shares mismatch: got %s", shares)
	}
	if assets != "1000000" {
		t.Errorf("assets mismatch: got %s", assets)
	}
}

func TestSaveVaultPosition_DuplicateIgnored(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()
	vaultDBID := fixture.createTestVault(t, ctx, []byte("vpos-upsert-12345678"))

	// Insert initial
	pos1 := &entity.MorphoVaultPosition{
		UserID:        fixture.userID,
		MorphoVaultID: vaultDBID,
		BlockNumber:   19200100,
		BlockVersion:  0,
		Shares:        big.NewInt(100),
		Assets:        big.NewInt(100),
	}

	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	if err := fixture.repo.SaveVaultPosition(ctx, tx1, pos1); err != nil {
		t.Fatalf("first SaveVaultPosition failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}

	// Insert duplicate with different values — should be ignored
	pos2 := &entity.MorphoVaultPosition{
		UserID:        fixture.userID,
		MorphoVaultID: vaultDBID,
		BlockNumber:   19200100,
		BlockVersion:  0,
		Shares:        big.NewInt(999),
		Assets:        big.NewInt(999),
	}

	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	if err := fixture.repo.SaveVaultPosition(ctx, tx2, pos2); err != nil {
		t.Fatalf("duplicate SaveVaultPosition failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}

	// Verify first write preserved (DO NOTHING semantics)
	var shares string
	err = fixture.pool.QueryRow(ctx,
		`SELECT shares FROM morpho_vault_position WHERE user_id = $1 AND morpho_vault_id = $2 AND block_number = $3 AND block_version = 0`,
		fixture.userID, vaultDBID, int64(19200100),
	).Scan(&shares)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if shares != "100" {
		t.Errorf("expected first write preserved (shares 100), got %s", shares)
	}
}

// --- Cross-table Transaction Tests ---

func TestTransactionAcrossMultipleTables(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()

	// Create a market and vault, then save state + position in single tx
	marketDBID := fixture.createTestMarket(t, ctx, []byte("cross-table-tx-test-market-12345"))
	vaultDBID := fixture.createTestVault(t, ctx, []byte("cross-table-vault-12"))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Save market state
	marketState := &entity.MorphoMarketState{
		MorphoMarketID:    marketDBID,
		BlockNumber:       19300000,
		BlockVersion:      0,
		TotalSupplyAssets: big.NewInt(1000),
		TotalSupplyShares: big.NewInt(1000),
		TotalBorrowAssets: big.NewInt(500),
		TotalBorrowShares: big.NewInt(500),
		LastUpdate:        1700000000,
		Fee:               big.NewInt(0),
	}
	if err := fixture.repo.SaveMarketState(ctx, tx, marketState); err != nil {
		t.Fatalf("SaveMarketState in tx failed: %v", err)
	}

	// Save position
	position := &entity.MorphoMarketPosition{
		UserID:         fixture.userID,
		MorphoMarketID: marketDBID,
		BlockNumber:    19300000,
		BlockVersion:   0,
		SupplyShares:   big.NewInt(100),
		BorrowShares:   big.NewInt(0),
		Collateral:     big.NewInt(0),
		SupplyAssets:   big.NewInt(100),
		BorrowAssets:   big.NewInt(0),
	}
	if err := fixture.repo.SaveMarketPosition(ctx, tx, position); err != nil {
		t.Fatalf("SaveMarketPosition in tx failed: %v", err)
	}

	// Save vault state
	vaultState := &entity.MorphoVaultState{
		MorphoVaultID: vaultDBID,
		BlockNumber:   19300000,
		BlockVersion:  0,
		TotalAssets:   big.NewInt(2000),
		TotalShares:   big.NewInt(2000),
	}
	if err := fixture.repo.SaveVaultState(ctx, tx, vaultState); err != nil {
		t.Fatalf("SaveVaultState in tx failed: %v", err)
	}

	// Save vault position
	vaultPosition := &entity.MorphoVaultPosition{
		UserID:        fixture.userID,
		MorphoVaultID: vaultDBID,
		BlockNumber:   19300000,
		BlockVersion:  0,
		Shares:        big.NewInt(500),
		Assets:        big.NewInt(500),
	}
	if err := fixture.repo.SaveVaultPosition(ctx, tx, vaultPosition); err != nil {
		t.Fatalf("SaveVaultPosition in tx failed: %v", err)
	}

	// Commit the whole batch
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit multi-table transaction: %v", err)
	}

	// Verify all records exist
	var msCount, mpCount, vsCount, vpCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_market_state WHERE block_number = 19300000`).Scan(&msCount); err != nil {
		t.Fatalf("scanning market state count: %v", err)
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_market_position WHERE block_number = 19300000`).Scan(&mpCount); err != nil {
		t.Fatalf("scanning market position count: %v", err)
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_vault_state WHERE block_number = 19300000`).Scan(&vsCount); err != nil {
		t.Fatalf("scanning vault state count: %v", err)
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_vault_position WHERE block_number = 19300000`).Scan(&vpCount); err != nil {
		t.Fatalf("scanning vault position count: %v", err)
	}

	if msCount != 1 {
		t.Errorf("expected 1 market state, got %d", msCount)
	}
	if mpCount != 1 {
		t.Errorf("expected 1 position, got %d", mpCount)
	}
	if vsCount != 1 {
		t.Errorf("expected 1 vault state, got %d", vsCount)
	}
	if vpCount != 1 {
		t.Errorf("expected 1 vault position, got %d", vpCount)
	}
}

func TestTransactionRollbackAcrossMultipleTables(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()

	marketDBID := fixture.createTestMarket(t, ctx, []byte("rollback-cross-table-test-123456"))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Save market state and position
	marketState := &entity.MorphoMarketState{
		MorphoMarketID:    marketDBID,
		BlockNumber:       19400000,
		BlockVersion:      0,
		TotalSupplyAssets: big.NewInt(1000),
		TotalSupplyShares: big.NewInt(1000),
		TotalBorrowAssets: big.NewInt(500),
		TotalBorrowShares: big.NewInt(500),
		LastUpdate:        1700000000,
		Fee:               big.NewInt(0),
	}
	if err := fixture.repo.SaveMarketState(ctx, tx, marketState); err != nil {
		t.Fatalf("SaveMarketState failed: %v", err)
	}

	position := &entity.MorphoMarketPosition{
		UserID:         fixture.userID,
		MorphoMarketID: marketDBID,
		BlockNumber:    19400000,
		BlockVersion:   0,
		SupplyShares:   big.NewInt(100),
		BorrowShares:   big.NewInt(0),
		Collateral:     big.NewInt(0),
		SupplyAssets:   big.NewInt(100),
		BorrowAssets:   big.NewInt(0),
	}
	if err := fixture.repo.SaveMarketPosition(ctx, tx, position); err != nil {
		t.Fatalf("SaveMarketPosition failed: %v", err)
	}

	// Rollback
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify nothing was persisted
	var msCount, mpCount int
	fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_market_state WHERE block_number = 19400000`).Scan(&msCount)
	fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM morpho_market_position WHERE block_number = 19400000`).Scan(&mpCount)

	if msCount != 0 {
		t.Errorf("expected 0 market states after rollback, got %d", msCount)
	}
	if mpCount != 0 {
		t.Errorf("expected 0 positions after rollback, got %d", mpCount)
	}
}

// --- Concurrency Tests ---

// TestConcurrentWorkers_AllTablesAppendOnly simulates 10 concurrent workers indexing
// the same block for the same market/vault/user. All workers write to every table
// (market state, market position, vault state, vault position) with the same key but
// different values. Under DO NOTHING semantics, exactly one write wins per table and
// the first-written values are preserved. Under no-op DO UPDATE (GetOrCreate*),
// all workers get the same ID back.
func TestConcurrentWorkers_AllTablesAppendOnly(t *testing.T) {
	fixture := setupMorphoTest(t)

	ctx := context.Background()

	const workers = 10
	const blockNumber = int64(20000000)
	const blockVersion = 0

	// Pre-create market and vault (these are dimension rows, not per-block data)
	marketDBID := fixture.createTestMarket(t, ctx, []byte("concurrent-test-market-12345678"))
	vaultDBID := fixture.createTestVault(t, ctx, []byte("concurrent-vault-12"))

	type result struct {
		marketID int64
		vaultID  int64
		err      error
	}

	results := make([]result, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start

			tx, err := fixture.pool.Begin(ctx)
			if err != nil {
				results[idx].err = fmt.Errorf("begin tx: %w", err)
				return
			}
			defer tx.Rollback(ctx)

			// Each worker tries GetOrCreateMarket with the same market
			mid, err := fixture.repo.GetOrCreateMarket(ctx, tx, &entity.MorphoMarket{
				ChainID:           1,
				ProtocolID:        fixture.protocolID,
				MarketID:          common.BytesToHash([]byte("concurrent-test-market-12345678")),
				LoanTokenID:       fixture.loanTokenID,
				CollateralTokenID: fixture.collTokenID,
				OracleAddress:     common.Address{},
				IrmAddress:        common.Address{},
				LLTV:              big.NewInt(860000000000000000),
				CreatedAtBlock:    18883124,
			})
			if err != nil {
				results[idx].err = fmt.Errorf("GetOrCreateMarket: %w", err)
				return
			}
			results[idx].marketID = mid

			// Each worker tries GetOrCreateVault with the same vault
			vid, err := fixture.repo.GetOrCreateVault(ctx, tx, &entity.MorphoVault{
				ChainID:        1,
				ProtocolID:     fixture.protocolID,
				Address:        []byte("concurrent-vault-12"),
				Name:           "Gauntlet USDC Core",
				Symbol:         "gtUSDCcore",
				AssetTokenID:   fixture.loanTokenID,
				VaultVersion:   entity.MorphoVaultV1,
				CreatedAtBlock: 19000000,
			})
			if err != nil {
				results[idx].err = fmt.Errorf("GetOrCreateVault: %w", err)
				return
			}
			results[idx].vaultID = vid

			// Each worker writes market state with a unique value per worker
			val := big.NewInt(int64(1000 + idx))
			if err := fixture.repo.SaveMarketState(ctx, tx, &entity.MorphoMarketState{
				MorphoMarketID:    marketDBID,
				BlockNumber:       blockNumber,
				BlockVersion:      blockVersion,
				TotalSupplyAssets: val,
				TotalSupplyShares: val,
				TotalBorrowAssets: big.NewInt(500),
				TotalBorrowShares: big.NewInt(500),
				LastUpdate:        1700000000,
				Fee:               big.NewInt(0),
			}); err != nil {
				results[idx].err = fmt.Errorf("SaveMarketState: %w", err)
				return
			}

			// Each worker writes market position with a unique value per worker
			if err := fixture.repo.SaveMarketPosition(ctx, tx, &entity.MorphoMarketPosition{
				UserID:         fixture.userID,
				MorphoMarketID: marketDBID,
				BlockNumber:    blockNumber,
				BlockVersion:   blockVersion,
				SupplyShares:   val,
				BorrowShares:   big.NewInt(0),
				Collateral:     big.NewInt(0),
				SupplyAssets:   val,
				BorrowAssets:   big.NewInt(0),
			}); err != nil {
				results[idx].err = fmt.Errorf("SaveMarketPosition: %w", err)
				return
			}

			// Each worker writes vault state with a unique value per worker
			if err := fixture.repo.SaveVaultState(ctx, tx, &entity.MorphoVaultState{
				MorphoVaultID: vaultDBID,
				BlockNumber:   blockNumber,
				BlockVersion:  blockVersion,
				TotalAssets:   val,
				TotalShares:   val,
			}); err != nil {
				results[idx].err = fmt.Errorf("SaveVaultState: %w", err)
				return
			}

			// Each worker writes vault position with a unique value per worker
			if err := fixture.repo.SaveVaultPosition(ctx, tx, &entity.MorphoVaultPosition{
				UserID:        fixture.userID,
				MorphoVaultID: vaultDBID,
				BlockNumber:   blockNumber,
				BlockVersion:  blockVersion,
				Shares:        val,
				Assets:        val,
			}); err != nil {
				results[idx].err = fmt.Errorf("SaveVaultPosition: %w", err)
				return
			}

			if err := tx.Commit(ctx); err != nil {
				results[idx].err = fmt.Errorf("commit: %w", err)
				return
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// All workers must succeed
	for i, r := range results {
		if r.err != nil {
			t.Errorf("worker %d failed: %v", i, r.err)
		}
	}

	// All workers must get the same market ID and vault ID back
	for i, r := range results {
		if r.marketID != marketDBID {
			t.Errorf("worker %d: GetOrCreateMarket returned %d, want %d", i, r.marketID, marketDBID)
		}
		if r.vaultID != vaultDBID {
			t.Errorf("worker %d: GetOrCreateVault returned %d, want %d", i, r.vaultID, vaultDBID)
		}
	}

	// Exactly one row per table for this block (DO NOTHING means no duplicates)
	tables := []string{"morpho_market_state", "morpho_market_position", "morpho_vault_state", "morpho_vault_position"}
	for _, table := range tables {
		var count int
		err := fixture.pool.QueryRow(ctx,
			fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE block_number = $1`, table),
			blockNumber,
		).Scan(&count)
		if err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("%s: expected 1 row, got %d", table, count)
		}
	}

	// The surviving row's value must be one of the workers' values (1000..1009)
	var totalSupplyAssets string
	err := fixture.pool.QueryRow(ctx,
		`SELECT total_supply_assets FROM morpho_market_state WHERE morpho_market_id = $1 AND block_number = $2 AND block_version = 0`,
		marketDBID, blockNumber,
	).Scan(&totalSupplyAssets)
	if err != nil {
		t.Fatalf("querying market state: %v", err)
	}
	// Parse and verify it's in the valid range
	got, ok := new(big.Int).SetString(totalSupplyAssets, 10)
	if !ok {
		t.Fatalf("invalid total_supply_assets: %s", totalSupplyAssets)
	}
	if got.Int64() < 1000 || got.Int64() > 1000+workers-1 {
		t.Errorf("total_supply_assets = %s, expected value in range [1000, %d]", totalSupplyAssets, 1000+workers-1)
	}
}

// --- VaultV2 Adapter helpers ---

// morphoBlockTime is a fixed snapshot timestamp for adapter-state / cap tests.
var morphoBlockTime = time.Unix(1700000000, 0).UTC()

// adapterAddr builds a distinct 20-byte adapter address from a seed byte.
func adapterAddr(seed byte) []byte {
	addr := make([]byte, 20)
	addr[0] = seed
	return addr
}

// createTestAdapter registers an active adapter on the given vault via the
// repository and returns its DB ID.
func (f *morphoTestFixture) createTestAdapter(t *testing.T, ctx context.Context, vaultID int64, address []byte, addedAtBlock int64) int64 {
	t.Helper()

	adapter := &entity.MorphoAdapter{
		MorphoVaultID: vaultID,
		Address:       address,
		AssetTokenID:  f.loanTokenID,
		AdapterType:   entity.MorphoAdapterTypeMarketV1,
		AddedAtBlock:  addedAtBlock,
	}

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := f.repo.GetOrCreateAdapter(ctx, tx, adapter)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	return id
}

// --- GetOrCreateAdapter Tests ---

func TestGetOrCreateAdapter_CreateNew(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x30))

	adapter := &entity.MorphoAdapter{
		MorphoVaultID: vaultID,
		Address:       adapterAddr(0x01),
		AssetTokenID:  fixture.loanTokenID,
		AdapterType:   entity.MorphoAdapterTypeMarketV1,
		AddedAtBlock:  24481834,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := fixture.repo.GetOrCreateAdapter(ctx, tx, adapter)
	if err != nil {
		t.Fatalf("GetOrCreateAdapter failed: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive id, got %d", id)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := fixture.repo.GetActiveAdapter(ctx, vaultID, adapterAddr(0x01))
	if err != nil {
		t.Fatalf("GetActiveAdapter failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected adapter, got nil")
	}
	if got.ID != id {
		t.Errorf("ID mismatch: got %d, want %d", got.ID, id)
	}
	if got.AdapterType != entity.MorphoAdapterTypeMarketV1 {
		t.Errorf("AdapterType mismatch: got %d", got.AdapterType)
	}
	if got.AddedAtBlock != 24481834 {
		t.Errorf("AddedAtBlock mismatch: got %d", got.AddedAtBlock)
	}
	if got.RemovedAtBlock != nil {
		t.Errorf("expected nil RemovedAtBlock, got %v", *got.RemovedAtBlock)
	}
}

func TestGetOrCreateAdapter_Idempotent(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x10))

	id1 := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x02), 24481834)
	id2 := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x02), 24481834)

	if id1 != id2 {
		t.Errorf("GetOrCreateAdapter not idempotent: first=%d, second=%d", id1, id2)
	}
}

func TestGetOrCreateAdapter_DifferentAddedBlockNewRow(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x11))

	// Same vault + address but a different added_at_block is a distinct
	// registry row (removed-then-re-added adapter).
	id1 := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x03), 24481834)
	id2 := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x03), 24500000)

	if id1 == id2 {
		t.Errorf("expected distinct ids for different added_at_block, got %d twice", id1)
	}
}

// --- MarkAdapterRemoved Tests ---

func TestMarkAdapterRemoved_SetsBlock(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x12))
	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x04), 24481834)

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, adapterAddr(0x04), 24600000); err != nil {
		t.Fatalf("MarkAdapterRemoved failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var removed *int64
	err = fixture.pool.QueryRow(ctx,
		`SELECT removed_at_block FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`,
		vaultID, adapterAddr(0x04),
	).Scan(&removed)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if removed == nil || *removed != 24600000 {
		t.Errorf("removed_at_block mismatch: got %v, want 24600000", removed)
	}
}

func TestMarkAdapterRemoved_ReplaySameBlockNoop(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x13))
	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x05), 24481834)

	mark := func() error {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, adapterAddr(0x05), 24600000); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := mark(); err != nil {
		t.Fatalf("first MarkAdapterRemoved failed: %v", err)
	}
	// Replay at the same block must be an idempotent success (backfill re-run).
	if err := mark(); err != nil {
		t.Fatalf("replay MarkAdapterRemoved at same block should succeed, got: %v", err)
	}
}

func TestMarkAdapterRemoved_UnknownAddressErrors(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x14))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, adapterAddr(0x06), 24600000)
	if err == nil {
		t.Fatal("expected error for unknown adapter address, got nil")
	}
}

func TestMarkAdapterRemoved_AlreadyRemovedDifferentBlockErrors(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x15))
	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x07), 24481834)

	mark := func(block int64) error {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, adapterAddr(0x07), block); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := mark(24600000); err != nil {
		t.Fatalf("first MarkAdapterRemoved failed: %v", err)
	}
	// A second removal at a different block is a data bug: no active row and no
	// row removed at this block, so it must surface an error.
	if err := mark(24700000); err == nil {
		t.Fatal("expected error removing already-removed adapter at a different block, got nil")
	}
}

// --- GetActiveAdapter / GetActiveAdaptersByVault Tests ---

func TestGetActiveAdapter_Found(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x16))
	id := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x08), 24481834)

	got, err := fixture.repo.GetActiveAdapter(ctx, vaultID, adapterAddr(0x08))
	if err != nil {
		t.Fatalf("GetActiveAdapter failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected adapter, got nil")
	}
	if got.ID != id {
		t.Errorf("ID mismatch: got %d, want %d", got.ID, id)
	}
}

func TestGetActiveAdapter_RemovedReturnsNil(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x17))
	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x09), 24481834)

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, adapterAddr(0x09), 24600000); err != nil {
		t.Fatalf("MarkAdapterRemoved failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := fixture.repo.GetActiveAdapter(ctx, vaultID, adapterAddr(0x09))
	if err != nil {
		t.Fatalf("GetActiveAdapter failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for removed adapter, got %+v", got)
	}
}

func TestGetActiveAdapter_NotFound(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x18))

	got, err := fixture.repo.GetActiveAdapter(ctx, vaultID, adapterAddr(0x0a))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown adapter, got %+v", got)
	}
}

func TestGetActiveAdaptersByVault_ReturnsActiveExcludesRemoved(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x19))

	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x0b), 24481834)
	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x0c), 24481900)
	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x0d), 24482000)

	// Remove one of the three.
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, adapterAddr(0x0d), 24600000); err != nil {
		t.Fatalf("MarkAdapterRemoved failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := fixture.repo.GetActiveAdaptersByVault(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetActiveAdaptersByVault failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 active adapters, got %d", len(got))
	}
	for _, a := range got {
		if a.RemovedAtBlock != nil {
			t.Errorf("active adapter %d has non-nil RemovedAtBlock", a.ID)
		}
		if a.MorphoVaultID != vaultID {
			t.Errorf("adapter %d has wrong vault id %d", a.ID, a.MorphoVaultID)
		}
	}
}

func TestGetActiveAdaptersByVault_Empty(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x1a))

	got, err := fixture.repo.GetActiveAdaptersByVault(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetActiveAdaptersByVault failed: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 adapters, got %d", len(got))
	}
}

// --- SaveAdapterState Tests ---

func TestSaveAdapterState_RoundTrip(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x1b))
	adapterID := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x0e), 24481834)

	maxUint256, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	state := &entity.MorphoAdapterState{
		MorphoAdapterID: adapterID,
		BlockNumber:     24500000,
		BlockVersion:    0,
		Timestamp:       morphoBlockTime,
		RealAssets:      maxUint256,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := fixture.repo.SaveAdapterState(ctx, tx, state); err != nil {
		t.Fatalf("SaveAdapterState failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var realAssets string
	err = fixture.pool.QueryRow(ctx,
		`SELECT real_assets FROM morpho_adapter_state WHERE morpho_adapter_id = $1 AND block_number = $2 AND block_version = 0`,
		adapterID, int64(24500000),
	).Scan(&realAssets)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if realAssets != maxUint256.String() {
		t.Errorf("real_assets precision lost: got %s, want %s", realAssets, maxUint256.String())
	}
}

func TestSaveAdapterState_DuplicateSameBuildDeduped(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x1c))
	adapterID := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x0f), 24481834)

	save := func(realAssets *big.Int) {
		state := &entity.MorphoAdapterState{
			MorphoAdapterID: adapterID,
			BlockNumber:     24500100,
			BlockVersion:    0,
			Timestamp:       morphoBlockTime,
			RealAssets:      realAssets,
		}
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := fixture.repo.SaveAdapterState(ctx, tx, state); err != nil {
			t.Fatalf("SaveAdapterState failed: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Both saves use the same repo (build_id 0) → trigger reuses
	// processing_version 0 and ON CONFLICT DO NOTHING dedupes to one row.
	save(big.NewInt(1000))
	save(big.NewInt(9999))

	var count int
	err := fixture.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM morpho_adapter_state WHERE morpho_adapter_id = $1 AND block_number = $2`,
		adapterID, int64(24500100),
	).Scan(&count)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 deduped row, got %d", count)
	}
}

func TestSaveAdapterState_DifferentBuildNewVersion(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x1d))
	adapterID := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x1e), 24481834)

	repoBuild1, err := NewMorphoRepository(morphoPool, nil, 1)
	if err != nil {
		t.Fatalf("NewMorphoRepository build 1: %v", err)
	}

	save := func(repo *MorphoRepository, realAssets *big.Int) {
		state := &entity.MorphoAdapterState{
			MorphoAdapterID: adapterID,
			BlockNumber:     24500200,
			BlockVersion:    0,
			Timestamp:       morphoBlockTime,
			RealAssets:      realAssets,
		}
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := repo.SaveAdapterState(ctx, tx, state); err != nil {
			t.Fatalf("SaveAdapterState failed: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Different build_id → reprocessing → a new processing_version, so both rows
	// survive.
	save(fixture.repo, big.NewInt(1000))
	save(repoBuild1, big.NewInt(2000))

	var count, maxVer int
	err = fixture.pool.QueryRow(ctx,
		`SELECT COUNT(*), MAX(processing_version) FROM morpho_adapter_state WHERE morpho_adapter_id = $1 AND block_number = $2`,
		adapterID, int64(24500200),
	).Scan(&count, &maxVer)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows for distinct builds, got %d", count)
	}
	if maxVer != 1 {
		t.Errorf("expected max processing_version 1, got %d", maxVer)
	}
}

// --- SaveVaultCap / GetLatestVaultCap Tests ---

func capID(seed byte) []byte {
	id := make([]byte, 32)
	id[0] = seed
	id[31] = seed
	return id
}

func TestSaveVaultCap_RoundTrip(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x20))

	cid := capID(0x42)
	cap := &entity.MorphoVaultCap{
		MorphoVaultID: vaultID,
		CapID:         cid,
		IDData:        []byte{0x01, 0x02, 0x03, 0x04},
		AbsoluteCap:   big.NewInt(1000000000000),
		RelativeCap:   big.NewInt(500000000000000000),
		BlockNumber:   24500000,
		BlockVersion:  0,
		Timestamp:     morphoBlockTime,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fixture.repo.SaveVaultCap(ctx, tx, cap); err != nil {
		t.Fatalf("SaveVaultCap failed: %v", err)
	}
	got, err := fixture.repo.GetLatestVaultCap(ctx, tx, vaultID, cid)
	if err != nil {
		t.Fatalf("GetLatestVaultCap failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got == nil {
		t.Fatal("expected cap, got nil")
	}
	if len(got.CapID) != 32 {
		t.Errorf("cap_id length: got %d, want 32", len(got.CapID))
	}
	if !bytes.Equal(got.CapID, cid) {
		t.Errorf("cap_id round-trip mismatch")
	}
	if got.AbsoluteCap.Cmp(big.NewInt(1000000000000)) != 0 {
		t.Errorf("absolute_cap mismatch: got %s", got.AbsoluteCap)
	}
	if got.RelativeCap.Cmp(big.NewInt(500000000000000000)) != 0 {
		t.Errorf("relative_cap mismatch: got %s", got.RelativeCap)
	}
}

func TestGetLatestVaultCap_LatestAcrossBlocks(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x21))
	cid := capID(0x43)

	saveCap := func(block int64, abs *big.Int) {
		cap := &entity.MorphoVaultCap{
			MorphoVaultID: vaultID,
			CapID:         cid,
			IDData:        []byte{0xaa},
			AbsoluteCap:   abs,
			RelativeCap:   big.NewInt(0),
			BlockNumber:   block,
			BlockVersion:  0,
			Timestamp:     morphoBlockTime.Add(time.Duration(block) * time.Second),
		}
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := fixture.repo.SaveVaultCap(ctx, tx, cap); err != nil {
			t.Fatalf("SaveVaultCap failed: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	saveCap(24500000, big.NewInt(100))
	saveCap(24500002, big.NewInt(300)) // latest block
	saveCap(24500001, big.NewInt(200))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	got, err := fixture.repo.GetLatestVaultCap(ctx, tx, vaultID, cid)
	if err != nil {
		t.Fatalf("GetLatestVaultCap failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected cap, got nil")
	}
	if got.BlockNumber != 24500002 {
		t.Errorf("expected latest block 24500002, got %d", got.BlockNumber)
	}
	if got.AbsoluteCap.Cmp(big.NewInt(300)) != 0 {
		t.Errorf("expected absolute_cap 300 from latest block, got %s", got.AbsoluteCap)
	}
}

func TestGetLatestVaultCap_LatestAcrossBlockVersions(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x22))
	cid := capID(0x44)

	saveCap := func(blockVersion int, abs *big.Int) {
		cap := &entity.MorphoVaultCap{
			MorphoVaultID: vaultID,
			CapID:         cid,
			IDData:        []byte{0xbb},
			AbsoluteCap:   abs,
			RelativeCap:   big.NewInt(0),
			BlockNumber:   24500000,
			BlockVersion:  blockVersion,
			Timestamp:     morphoBlockTime,
		}
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := fixture.repo.SaveVaultCap(ctx, tx, cap); err != nil {
			t.Fatalf("SaveVaultCap failed: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Same block, reorg produced a higher block_version with a corrected value.
	saveCap(0, big.NewInt(100))
	saveCap(1, big.NewInt(999))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	got, err := fixture.repo.GetLatestVaultCap(ctx, tx, vaultID, cid)
	if err != nil {
		t.Fatalf("GetLatestVaultCap failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected cap, got nil")
	}
	if got.BlockVersion != 1 {
		t.Errorf("expected latest block_version 1, got %d", got.BlockVersion)
	}
	if got.AbsoluteCap.Cmp(big.NewInt(999)) != 0 {
		t.Errorf("expected absolute_cap 999 from latest block_version, got %s", got.AbsoluteCap)
	}
}

func TestGetLatestVaultCap_NotFound(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x23))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	got, err := fixture.repo.GetLatestVaultCap(ctx, tx, vaultID, capID(0x45))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown cap, got %+v", got)
	}
}

// --- UpdateVaultFeeConfig Tests ---

func TestUpdateVaultFeeConfig_PartialLeavesOthersNull(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x24))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	err = fixture.repo.UpdateVaultFeeConfig(ctx, tx, vaultID, entity.MorphoVaultFeeUpdate{
		PerformanceFee: big.NewInt(500000000000000000),
	})
	if err != nil {
		t.Fatalf("UpdateVaultFeeConfig failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var perfFee *string
	var mgmtFee, perfRecipient, mgmtRecipient *string
	err = fixture.pool.QueryRow(ctx,
		`SELECT performance_fee::text, management_fee::text, encode(performance_fee_recipient, 'hex'), encode(management_fee_recipient, 'hex')
		 FROM morpho_vault WHERE id = $1`,
		vaultID,
	).Scan(&perfFee, &mgmtFee, &perfRecipient, &mgmtRecipient)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if perfFee == nil || *perfFee != "500000000000000000" {
		t.Errorf("performance_fee mismatch: got %v", perfFee)
	}
	if mgmtFee != nil {
		t.Errorf("expected management_fee NULL, got %v", *mgmtFee)
	}
	if perfRecipient != nil {
		t.Errorf("expected performance_fee_recipient NULL, got %v", *perfRecipient)
	}
	if mgmtRecipient != nil {
		t.Errorf("expected management_fee_recipient NULL, got %v", *mgmtRecipient)
	}
}

func TestUpdateVaultFeeConfig_SecondPartialPreservesFirst(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x25))

	update := func(u entity.MorphoVaultFeeUpdate) {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := fixture.repo.UpdateVaultFeeConfig(ctx, tx, vaultID, u); err != nil {
			t.Fatalf("UpdateVaultFeeConfig failed: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	recipient := adapterAddr(0x99)
	update(entity.MorphoVaultFeeUpdate{PerformanceFee: big.NewInt(111)})
	// A later event sets only the management fee + its recipient; the earlier
	// performance fee must be preserved.
	update(entity.MorphoVaultFeeUpdate{ManagementFee: big.NewInt(222), ManagementFeeRecipient: recipient})

	var perfFee, mgmtFee string
	var mgmtRecipient string
	err := fixture.pool.QueryRow(ctx,
		`SELECT performance_fee::text, management_fee::text, encode(management_fee_recipient, 'hex')
		 FROM morpho_vault WHERE id = $1`,
		vaultID,
	).Scan(&perfFee, &mgmtFee, &mgmtRecipient)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if perfFee != "111" {
		t.Errorf("expected performance_fee preserved (111), got %s", perfFee)
	}
	if mgmtFee != "222" {
		t.Errorf("expected management_fee 222, got %s", mgmtFee)
	}
	if mgmtRecipient != fmt.Sprintf("%x", recipient) {
		t.Errorf("management_fee_recipient mismatch: got %s", mgmtRecipient)
	}
}

func TestUpdateVaultFeeConfig_AllNilRejected(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x26))

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.UpdateVaultFeeConfig(ctx, tx, vaultID, entity.MorphoVaultFeeUpdate{})
	if err == nil {
		t.Fatal("expected error for all-nil fee update, got nil")
	}
}

func TestUpdateVaultFeeConfig_UnknownVaultErrors(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.UpdateVaultFeeConfig(ctx, tx, 999999, entity.MorphoVaultFeeUpdate{
		PerformanceFee: big.NewInt(1),
	})
	if err == nil {
		t.Fatal("expected error for unknown vault, got nil")
	}
}
