//go:build integration

package postgres

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
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
	// morpho_vault_cap / morpho_vault_fee and morpho_adapter FK morpho_vault.
	tables := []string{
		`morpho_market_state`,
		`morpho_market_position`,
		`morpho_vault_state`,
		`morpho_vault_position`,
		`morpho_adapter_state`,
		`morpho_vault_cap`,
		`morpho_vault_fee`,
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

// TestGetOrCreateVault_CreatedAtBlockConvergesDownward mirrors GetOrCreateToken's
// first-observed semantics. A vault first seen inside a narrowed backfill range
// (or on the live stream) records that block as created_at_block; without downward
// convergence the wrong deploy block would persist forever, because the upsert's
// only conflict action is a no-op SET.
func TestGetOrCreateVault_CreatedAtBlockConvergesDownward(t *testing.T) {
	tests := []struct {
		name          string
		firstBlock    int64
		secondBlock   int64
		wantConverged int64
	}{
		{"an earlier observation wins", 24500000, 19000000, 19000000},
		{"a later observation is ignored", 19000000, 24500000, 19000000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			address := adapterAddr(0x3d)

			upsert := func(block int64) int64 {
				t.Helper()
				vault := &entity.MorphoVault{
					ChainID: 1, ProtocolID: fixture.protocolID, Address: address,
					Name: "Gauntlet USDC Core", Symbol: "gtUSDCcore",
					AssetTokenID: fixture.loanTokenID, VaultVersion: entity.MorphoVaultV1,
					CreatedAtBlock: block,
				}
				tx, err := fixture.pool.Begin(ctx)
				if err != nil {
					t.Fatalf("begin: %v", err)
				}
				defer tx.Rollback(ctx)
				id, err := fixture.repo.GetOrCreateVault(ctx, tx, vault)
				if err != nil {
					t.Fatalf("GetOrCreateVault at block %d: %v", block, err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("commit: %v", err)
				}
				return id
			}

			id1 := upsert(tt.firstBlock)
			if id2 := upsert(tt.secondBlock); id2 != id1 {
				t.Fatalf("upsert must reuse the vault row: first=%d second=%d", id1, id2)
			}

			got, err := fixture.repo.GetVaultByAddress(ctx, 1, common.BytesToAddress(address))
			if err != nil {
				t.Fatalf("GetVaultByAddress: %v", err)
			}
			if got == nil {
				t.Fatal("expected the vault row")
			}
			if got.CreatedAtBlock != tt.wantConverged {
				t.Errorf("created_at_block = %d, want %d (LEAST of the two observations)", got.CreatedAtBlock, tt.wantConverged)
			}
		})
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

// createTestAdapter registers an active MarketV1 adapter on the given vault via
// the repository and returns its DB ID.
func (f *morphoTestFixture) createTestAdapter(t *testing.T, ctx context.Context, vaultID int64, address []byte, addedAtBlock int64) int64 {
	t.Helper()
	return f.createTestAdapterOfType(t, ctx, vaultID, address, addedAtBlock, entity.MorphoAdapterTypeMarketV1)
}

// createTestAdapterOfType is createTestAdapter with an explicit classification, for
// the adapter_type curation cases.
func (f *morphoTestFixture) createTestAdapterOfType(t *testing.T, ctx context.Context, vaultID int64, address []byte, addedAtBlock int64, adapterType entity.MorphoAdapterType) int64 {
	t.Helper()

	adapter := &entity.MorphoAdapter{
		MorphoVaultID: vaultID,
		Address:       address,
		AssetTokenID:  f.loanTokenID,
		AdapterType:   adapterType,
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

// adapterTypeOf reads the recorded classification of one adapter row.
func (f *morphoTestFixture) adapterTypeOf(t *testing.T, ctx context.Context, id int64) entity.MorphoAdapterType {
	t.Helper()
	var adapterType entity.MorphoAdapterType
	if err := f.pool.QueryRow(ctx, `SELECT adapter_type FROM morpho_adapter WHERE id = $1`, id).Scan(&adapterType); err != nil {
		t.Fatalf("reading adapter_type for id %d: %v", id, err)
	}
	return adapterType
}

// seedAdapterStateAt writes one adapter_state snapshot for the given adapter at
// blockNumber, so a test can own state rows inside (or outside) a lifetime window.
func (f *morphoTestFixture) seedAdapterStateAt(t *testing.T, ctx context.Context, adapterID, blockNumber int64) {
	t.Helper()
	f.seedAdapterStateAtVersion(t, ctx, adapterID, blockNumber, 0)
}

// seedAdapterStateAtVersion writes one adapter_state snapshot at a specific
// block_version, so a test can distinguish a snapshot the canonical chain owns from
// dead-chain residue a reorg left behind.
func (f *morphoTestFixture) seedAdapterStateAtVersion(t *testing.T, ctx context.Context, adapterID, blockNumber int64, blockVersion int) {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	state := &entity.MorphoAdapterState{
		MorphoAdapterID: adapterID,
		BlockNumber:     blockNumber,
		BlockVersion:    blockVersion,
		Timestamp:       morphoBlockTime,
		RealAssets:      big.NewInt(1_000_000),
	}
	if err := f.repo.SaveAdapterState(ctx, tx, state); err != nil {
		t.Fatalf("SaveAdapterState at block %d version %d: %v", blockNumber, blockVersion, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// markAdapterRemoved runs MarkAdapterRemoved in its own transaction, committing on
// success and rolling back on error.
func (f *morphoTestFixture) markAdapterRemoved(t *testing.T, ctx context.Context, vaultID int64, address []byte, block int64) error {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := f.repo.MarkAdapterRemoved(ctx, tx, vaultID, address, block); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// removeAdapter runs the whole RemoveAdapter path the worker runs, in ONE
// transaction: guarantee an incarnation to close, then close it. The lifecycle
// tests below drive this rather than MarkAdapterRemoved alone, because the
// registry shape a removal leaves behind is a property of the composition.
//
// adapterType is the classification the on-chain probe supplied, used only if the
// registry turns out to have no incarnation to close.
func (f *morphoTestFixture) removeAdapter(ctx context.Context, vaultID int64, address []byte, block int64, adapterType entity.MorphoAdapterType) error {
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	candidate := &entity.MorphoAdapter{
		MorphoVaultID:  vaultID,
		Address:        address,
		AssetTokenID:   f.loanTokenID,
		AdapterType:    adapterType,
		AddedAtBlock:   block,
		RemovedAtBlock: &block,
	}
	if _, err := f.repo.EnsureIncarnationToClose(ctx, tx, vaultID, address, block, candidate); err != nil {
		return err
	}
	if err := f.repo.MarkAdapterRemoved(ctx, tx, vaultID, address, block); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// adapterIncarnationAt runs GetAdapterIncarnationAt (which reads through the
// caller's tx) in a short read transaction that is rolled back afterwards.
func (f *morphoTestFixture) adapterIncarnationAt(t *testing.T, ctx context.Context, vaultID int64, address []byte, atBlock int64) *entity.MorphoAdapter {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	got, err := f.repo.getAdapterIncarnationAt(ctx, tx, vaultID, address, atBlock)
	if err != nil {
		t.Fatalf("GetAdapterIncarnationAt(%d): %v", atBlock, err)
	}
	return got
}

// describeIncarnations renders every recorded incarnation of one adapter as
// "id=N [added,removed]", oldest first, so a lifecycle assertion can print the
// whole registry state it is unhappy about instead of one column of it.
func (f *morphoTestFixture) describeIncarnations(t *testing.T, ctx context.Context, vaultID int64, address []byte) string {
	t.Helper()
	rows, err := f.pool.Query(ctx,
		`SELECT id, added_at_block, removed_at_block FROM morpho_adapter
		 WHERE morpho_vault_id = $1 AND address = $2 ORDER BY added_at_block`,
		vaultID, address)
	if err != nil {
		t.Fatalf("listing incarnations: %v", err)
	}
	defer rows.Close()

	var described []string
	for rows.Next() {
		var (
			id      int64
			added   int64
			removed *int64
		)
		if err := rows.Scan(&id, &added, &removed); err != nil {
			t.Fatalf("scanning incarnation: %v", err)
		}
		closedAt := "ACTIVE"
		if removed != nil {
			closedAt = fmt.Sprintf("%d", *removed)
		}
		described = append(described, fmt.Sprintf("id=%d [%d,%s]", id, added, closedAt))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating incarnations: %v", err)
	}
	return strings.Join(described, " ")
}

// getActiveAdapter runs GetActiveAdapter (which reads through the caller's tx) in
// a short read transaction that is rolled back afterwards.
func (f *morphoTestFixture) getActiveAdapter(t *testing.T, ctx context.Context, vaultID int64, address []byte) (*entity.MorphoAdapter, error) {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	return f.repo.GetActiveAdapter(ctx, tx, vaultID, address)
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

	got, err := fixture.getActiveAdapter(t, ctx, vaultID, adapterAddr(0x01))
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

// TestGetOrCreateAdapter_ActiveRowConverges verifies the active-row-keyed
// semantics: a second GetOrCreateAdapter for the same (vault, address) while the
// first incarnation is still active does NOT create a second active row. It
// reuses the existing row's id and converges added_at_block to the earliest
// on-chain observation (LEAST). This is what lets the backfiller replay the TRUE
// AddAdapter@X after a live lazy-registration at first-seen block Y>X collapse to
// a single active row keyed at X, rather than leaving two active rows.
//
// A genuine re-add (a NEW row) only happens after a removal closes the prior
// incarnation — covered by TestMarkAdapterRemoved_ReplayOldRemovalSparesReAddedRow.
func TestGetOrCreateAdapter_ActiveRowConverges(t *testing.T) {
	tests := []struct {
		name          string
		firstBlock    int64
		secondBlock   int64
		wantConverged int64
	}{
		{"backfill replays the earlier true AddAdapter block", 24500000, 24481834, 24481834},
		{"a later observation keeps the earlier block", 24481834, 24500000, 24481834},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x11))

			id1 := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x03), tt.firstBlock)
			id2 := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x03), tt.secondBlock)

			if id1 != id2 {
				t.Errorf("active-row convergence must reuse the row: first=%d, second=%d", id1, id2)
			}

			got, err := fixture.getActiveAdapter(t, ctx, vaultID, adapterAddr(0x03))
			if err != nil {
				t.Fatalf("GetActiveAdapter: %v", err)
			}
			if got == nil {
				t.Fatal("expected one active adapter row")
			}
			if got.AddedAtBlock != tt.wantConverged {
				t.Errorf("added_at_block = %d, want %d (LEAST of the two observations)", got.AddedAtBlock, tt.wantConverged)
			}

			var count int
			if err := fixture.pool.QueryRow(ctx,
				`SELECT count(*) FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`,
				vaultID, adapterAddr(0x03)).Scan(&count); err != nil {
				t.Fatalf("counting adapter rows: %v", err)
			}
			if count != 1 {
				t.Errorf("want exactly 1 adapter row (no duplicate active rows), got %d", count)
			}
		})
	}
}

// TestGetOrCreateAdapter_BackfilledAddBeforeRemovalConvergesOntoClosedRow guards
// against resurrecting a de-registered adapter. The live stream can lazily
// register an adapter at block X and remove it at the same block (row: added X,
// removed X). When the backfiller later replays the TRUE AddAdapter@W with W < X,
// there is no active row — but the candidate belongs to the ALREADY-CLOSED
// incarnation whose window covers it, so GetOrCreateAdapter must converge onto
// that row (UPDATE added_at_block down to W, same id, removed_at_block still X),
// NOT insert a second, spuriously-active incarnation that would feed
// GetActiveAdaptersByVault / realAssets a de-registered adapter forever.
func TestGetOrCreateAdapter_BackfilledAddBeforeRemovalConvergesOntoClosedRow(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x2a))
	addr := adapterAddr(0x2b)

	const (
		liveBlock     = int64(24600000) // lazy register + removal both land here
		backfillBlock = int64(24481834) // the true AddAdapter, strictly earlier
	)

	// Live: lazily register at liveBlock, then remove at the same block.
	id1 := fixture.createTestAdapter(t, ctx, vaultID, addr, liveBlock)
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, addr, liveBlock); err != nil {
		t.Fatalf("MarkAdapterRemoved: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Backfill replays the true AddAdapter earlier than the removal: it must
	// converge onto the closed incarnation, not create a new active row.
	id2 := fixture.createTestAdapter(t, ctx, vaultID, addr, backfillBlock)

	if id1 != id2 {
		t.Errorf("backfilled add before the removal must converge onto the closed incarnation: id1=%d id2=%d", id1, id2)
	}

	var count int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`,
		vaultID, addr).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("want exactly 1 row (no resurrected active incarnation), got %d", count)
	}

	var added int64
	var removed *int64
	if err := fixture.pool.QueryRow(ctx,
		`SELECT added_at_block, removed_at_block FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`,
		vaultID, addr).Scan(&added, &removed); err != nil {
		t.Fatalf("query: %v", err)
	}
	if added != backfillBlock {
		t.Errorf("added_at_block = %d, want %d (converged down to the true AddAdapter)", added, backfillBlock)
	}
	if removed == nil || *removed != liveBlock {
		t.Errorf("removed_at_block = %v, want %d (incarnation stays closed)", removed, liveBlock)
	}

	got, err := fixture.getActiveAdapter(t, ctx, vaultID, addr)
	if err != nil {
		t.Fatalf("GetActiveAdapter: %v", err)
	}
	if got != nil {
		t.Errorf("expected NO active adapter (must stay removed), got %+v", got)
	}
}

// TestGetOrCreateAdapter_BackfilledAddConvergesClosedWindowNotActiveRow pins the
// decision order for a removed-then-re-added adapter. Two incarnations coexist: a
// CLOSED window (added 150, removed 200) and a later ACTIVE one (added 300, NULL).
// A backfilled true AddAdapter@80 belongs to the FIRST incarnation, and the closed
// window covers it (removed 200 >= 80), so it must converge onto the CLOSED row and
// leave the re-added active row's lifetime intact. Converging the active row first
// (matching regardless of window) would corrupt the live incarnation's
// added_at_block down to 80 and never converge the closed one, silently wrecking
// VEC-219's adapter-lifetime data.
func TestGetOrCreateAdapter_BackfilledAddConvergesClosedWindowNotActiveRow(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x2c))
	addr := adapterAddr(0x2d)

	const (
		firstAdd    = int64(150)
		removeBlock = int64(200)
		reAdd       = int64(300)
		backfill    = int64(80) // true AddAdapter of the FIRST incarnation, replayed late
	)

	// Seed the two incarnations via the repo's own methods: add→remove→re-add.
	closedID := fixture.createTestAdapter(t, ctx, vaultID, addr, firstAdd)
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, addr, removeBlock); err != nil {
		t.Fatalf("MarkAdapterRemoved: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	activeID := fixture.createTestAdapter(t, ctx, vaultID, addr, reAdd)

	// Backfill the true AddAdapter of the closed incarnation, arriving late.
	gotID := fixture.createTestAdapter(t, ctx, vaultID, addr, backfill)

	if gotID != closedID {
		t.Errorf("backfilled add@%d must converge onto the CLOSED incarnation id=%d, got id=%d (active row is id=%d)",
			backfill, closedID, gotID, activeID)
	}

	readRow := func(id int64) (int64, *int64) {
		var added int64
		var removed *int64
		if err := fixture.pool.QueryRow(ctx,
			`SELECT added_at_block, removed_at_block FROM morpho_adapter WHERE id = $1`,
			id).Scan(&added, &removed); err != nil {
			t.Fatalf("reading row id=%d: %v", id, err)
		}
		return added, removed
	}

	closedAdded, closedRemoved := readRow(closedID)
	if closedAdded != backfill {
		t.Errorf("closed incarnation added_at_block = %d, want %d (converged down to the backfilled add)", closedAdded, backfill)
	}
	if closedRemoved == nil || *closedRemoved != removeBlock {
		t.Errorf("closed incarnation removed_at_block = %v, want %d (window stays closed)", closedRemoved, removeBlock)
	}

	activeAdded, activeRemoved := readRow(activeID)
	if activeAdded != reAdd {
		t.Errorf("re-added incarnation added_at_block = %d, want %d (must stay untouched)", activeAdded, reAdd)
	}
	if activeRemoved != nil {
		t.Errorf("re-added incarnation removed_at_block = %d, want NULL (must stay active)", *activeRemoved)
	}

	var count int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`,
		vaultID, addr).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("want exactly 2 rows (closed + active, no new row), got %d", count)
	}
}

// TestGetOrCreateAdapter_SameBlockReAddOpensANewIncarnation covers a governance
// multicall that removes and immediately re-adds an adapter in ONE block: the logs
// are processed in order, so the re-add's added_at_block equals the removal block.
// The closed-window match must therefore be STRICT — an inclusive
// removed_at_block >= candidate swallowed the re-add into the row it had just
// closed, leaving the adapter with no active row on-DB while it is active on-chain.
func TestGetOrCreateAdapter_SameBlockReAddOpensANewIncarnation(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x35))
	addr := adapterAddr(0x36)

	const (
		firstAdd    = int64(100)
		sameBlockAt = int64(500)
	)

	closedID := fixture.createTestAdapter(t, ctx, vaultID, addr, firstAdd)
	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, sameBlockAt); err != nil {
		t.Fatalf("MarkAdapterRemoved: %v", err)
	}

	reAddID := fixture.createTestAdapter(t, ctx, vaultID, addr, sameBlockAt)
	if reAddID == closedID {
		t.Fatalf("the same-block re-add must open a NEW incarnation, got the just-closed row id=%d", closedID)
	}

	active, err := fixture.getActiveAdapter(t, ctx, vaultID, addr)
	if err != nil {
		t.Fatalf("GetActiveAdapter: %v", err)
	}
	if active == nil {
		t.Fatal("expected an ACTIVE adapter row after the same-block re-add")
	}
	if active.ID != reAddID || active.AddedAtBlock != sameBlockAt {
		t.Errorf("active row = (id %d, added %d), want (id %d, added %d)", active.ID, active.AddedAtBlock, reAddID, sameBlockAt)
	}
}

// TestGetOrCreateAdapter_BackfilledAddAtExactRemovalBlockStaysClosed is the other
// side of the strict closed-window boundary: when the live stream lazily registered
// AND removed an adapter in one block, a backfilled AddAdapter for that same block
// is a late observation of the incarnation that already exists, not a new one — the
// UNIQUE (vault, address, added_at_block) key folds it onto the closed row, so the
// adapter stays de-registered.
func TestGetOrCreateAdapter_BackfilledAddAtExactRemovalBlockStaysClosed(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x37))
	addr := adapterAddr(0x38)

	const liveBlock = int64(24600000)

	id1 := fixture.createTestAdapter(t, ctx, vaultID, addr, liveBlock)
	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, liveBlock); err != nil {
		t.Fatalf("MarkAdapterRemoved: %v", err)
	}

	if id2 := fixture.createTestAdapter(t, ctx, vaultID, addr, liveBlock); id2 != id1 {
		t.Errorf("backfilled add at the removal block must fold onto the existing row: got %d, want %d", id2, id1)
	}

	active, err := fixture.getActiveAdapter(t, ctx, vaultID, addr)
	if err != nil {
		t.Fatalf("GetActiveAdapter: %v", err)
	}
	if active != nil {
		t.Errorf("expected NO active adapter, got %+v", active)
	}
}

// adapterTypeConvergenceCases pins the curation rule shared by both convergence
// paths: a row recorded as Unknown (the forward-compatible sentinel written when
// the on-chain probe cannot classify an adapter) is upgraded when a replay supplies
// a real type, and a known type is never overwritten. Without this, an adapter that
// probed Unknown once stayed Unknown forever, and replay — the curation path the
// schema comment promises — could not fix it.
var adapterTypeConvergenceCases = []struct {
	name     string
	existing entity.MorphoAdapterType
	replayed entity.MorphoAdapterType
	want     entity.MorphoAdapterType
}{
	{"unknown is upgraded by a replayed known type", entity.MorphoAdapterTypeUnknown, entity.MorphoAdapterTypeMarketV1, entity.MorphoAdapterTypeMarketV1},
	{"a known type is never downgraded to unknown", entity.MorphoAdapterTypeMarketV1, entity.MorphoAdapterTypeUnknown, entity.MorphoAdapterTypeMarketV1},
	{"a known type is never replaced by another known type", entity.MorphoAdapterTypeVaultV1, entity.MorphoAdapterTypeMarketV1, entity.MorphoAdapterTypeVaultV1},
}

func TestGetOrCreateAdapter_ActiveRowConvergenceCuratesAdapterType(t *testing.T) {
	for _, tt := range adapterTypeConvergenceCases {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x39))
			addr := adapterAddr(0x3a)

			id := fixture.createTestAdapterOfType(t, ctx, vaultID, addr, 200, tt.existing)
			fixture.createTestAdapterOfType(t, ctx, vaultID, addr, 100, tt.replayed)

			if got := fixture.adapterTypeOf(t, ctx, id); got != tt.want {
				t.Errorf("adapter_type = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGetOrCreateAdapter_ClosedWindowConvergenceCuratesAdapterType(t *testing.T) {
	for _, tt := range adapterTypeConvergenceCases {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x3b))
			addr := adapterAddr(0x3c)

			id := fixture.createTestAdapterOfType(t, ctx, vaultID, addr, 200, tt.existing)
			if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, 500); err != nil {
				t.Fatalf("MarkAdapterRemoved: %v", err)
			}
			fixture.createTestAdapterOfType(t, ctx, vaultID, addr, 100, tt.replayed)

			if got := fixture.adapterTypeOf(t, ctx, id); got != tt.want {
				t.Errorf("adapter_type = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestMorphoAdapter_UniqueActiveIncarnationIndexRejectsSecondActiveRow is the
// database-level backstop for a writer that reaches morpho_adapter WITHOUT the
// per-(vault, address) advisory lock both GetOrCreateAdapter and MarkAdapterRemoved
// take — a future code path, a manual INSERT, or a migration. Unlocked, under READ
// COMMITTED a registration can pass the closed-window check, then have its
// active-row UPDATE re-checked by EvalPlanQual against a concurrently committed
// removed_at_block, match 0 rows, and fall through to the INSERT — resurrecting a
// de-registered adapter as a second ACTIVE row. The partial UNIQUE index makes that
// INSERT abort so the retried event re-runs and lands in the closed-window path
// instead. The raw INSERTs below stand in for such a lockless writer.
func TestMorphoAdapter_UniqueActiveIncarnationIndexRejectsSecondActiveRow(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x2e))
	addr := adapterAddr(0x2f)

	fixture.createTestAdapter(t, ctx, vaultID, addr, 100)

	insertActive := func(addedAtBlock int64) error {
		_, err := fixture.pool.Exec(ctx,
			`INSERT INTO morpho_adapter (morpho_vault_id, address, asset_token_id, adapter_type, added_at_block, removed_at_block)
			 VALUES ($1, $2, $3, 1, $4, NULL)`,
			vaultID, addr, fixture.loanTokenID, addedAtBlock)
		return err
	}

	if err := insertActive(200); err == nil {
		t.Fatal("a second ACTIVE row for the same (vault, address) must violate the partial unique index")
	}

	// The index must not block the legitimate shape: one CLOSED incarnation plus
	// one ACTIVE one, which is exactly what a removed-then-re-added adapter looks
	// like.
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, addr, 150); err != nil {
		t.Fatalf("MarkAdapterRemoved: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := insertActive(200); err != nil {
		t.Fatalf("closed + active incarnations must coexist: %v", err)
	}
}

// --- MarkAdapterRemoved Tests ---

// TestMarkAdapterRemoved_ClosesAndConverges is the authoritative matrix for which
// block a removal closes an incarnation at. One behaviour, one row per input shape.
//
// The axes are whether the incarnation is still OPEN (this is its first close) or
// already CLOSED (the removal is being re-observed), and how far the observed block
// sits from the recorded one. A re-observation within maxRemovalRelocationDistance of
// the recorded close, in EITHER direction, is the SAME on-chain removal that a reorg
// relocated or a backfill replayed, so it converges to the earliest observation.
// Beyond that window — either way — it cannot be the same removal: the row has
// conflated two of the adapter's lifetimes, and converging would rewrite one of two
// real de-registrations.
//
// The orphan guard (snapshots stranded outside the closed window) has its own tests
// below; it asserts on a refused write rather than on a converged block.
func TestMarkAdapterRemoved_ClosesAndConverges(t *testing.T) {
	const (
		addedAt    = int64(24481834)
		firstClose = int64(24600000)
	)

	tests := []struct {
		name        string
		closeFirst  bool
		removeAt    int64
		wantErr     string
		wantRemoved int64
	}{
		{
			name:     "an open incarnation closes at the observed block",
			removeAt: firstClose, wantRemoved: firstClose,
		},
		{
			name:       "a replay of the recorded removal is an idempotent no-op",
			closeFirst: true, removeAt: firstClose, wantRemoved: firstClose,
		},
		{
			name:       "a removal relocated one block later keeps the earliest observation",
			closeFirst: true, removeAt: firstClose + 1, wantRemoved: firstClose,
		},
		{
			name:       "a removal relocated one block earlier converges down",
			closeFirst: true, removeAt: firstClose - 1, wantRemoved: firstClose - 1,
		},
		{
			name:       "a removal at the far upper edge of the reorg window still converges",
			closeFirst: true, removeAt: firstClose + maxRemovalRelocationDistance, wantRemoved: firstClose,
		},
		{
			name:       "a removal at the far lower edge of the reorg window still converges",
			closeFirst: true, removeAt: firstClose - maxRemovalRelocationDistance,
			wantRemoved: firstClose - maxRemovalRelocationDistance,
		},
		{
			name:       "a removal above the reorg window is a conflated incarnation, not a relocation",
			closeFirst: true, removeAt: firstClose + maxRemovalRelocationDistance + 1,
			wantErr: "conflated incarnation", wantRemoved: firstClose,
		},
		{
			name:       "a removal below the reorg window is a conflated incarnation, not a relocation",
			closeFirst: true, removeAt: firstClose - maxRemovalRelocationDistance - 1,
			wantErr: "conflated incarnation", wantRemoved: firstClose,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x12))
			addr := adapterAddr(0x04)
			fixture.createTestAdapter(t, ctx, vaultID, addr, addedAt)

			if tt.closeFirst {
				if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, firstClose); err != nil {
					t.Fatalf("recording the first close at %d: %v", firstClose, err)
				}
			}

			err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, tt.removeAt)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("removal at %d must not be swallowed as a relocation of the close at %d", tt.removeAt, firstClose)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q should name the conflated-incarnation hypothesis (%q)", err.Error(), tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("MarkAdapterRemoved(%d): %v", tt.removeAt, err)
			}

			var removed *int64
			if err := fixture.pool.QueryRow(ctx,
				`SELECT removed_at_block FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`,
				vaultID, addr).Scan(&removed); err != nil {
				t.Fatalf("query: %v", err)
			}
			if removed == nil || *removed != tt.wantRemoved {
				t.Errorf("removed_at_block = %v, want %d", removed, tt.wantRemoved)
			}
		})
	}
}

// TestCreateAdapterIncarnation_HealingAnUnobservedRemovalSparesALaterIncarnation
// walks the two steps a RemoveAdapter takes when the adapter has no recorded
// incarnation covering its block (ensureIncarnationToClose registers one, then
// MarkAdapterRemoved closes it), and pins that a LATER incarnation of the same
// address is untouched by both.
//
// Registering through the converging GetOrCreateAdapter is what made this unsafe: its
// active-row match LEAST-converges added_at_block, so healing a historical removal at
// 1000 dragged an on-chain-ACTIVE [1100, NULL] row down to added=1000 and the close
// then de-registered it — an adapter still allocating on-chain, silently gone from the
// registry. With no state rows on that row the orphan guard has nothing to refuse, so
// this shape is precisely the one convergence cannot be allowed to touch.
func TestCreateAdapterIncarnation_HealingAnUnobservedRemovalSparesALaterIncarnation(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x52))
	addr := adapterAddr(0x53)

	const (
		healedRemoval = int64(1000)
		laterAdd      = int64(1100)
	)

	activeID := fixture.createTestAdapter(t, ctx, vaultID, addr, laterAdd)
	if covering := fixture.adapterIncarnationAt(t, ctx, vaultID, addr, healedRemoval); covering != nil {
		t.Fatalf("block %d must have no covering incarnation, so the removal takes the heal path", healedRemoval)
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	synthetic := &entity.MorphoAdapter{
		MorphoVaultID: vaultID,
		Address:       addr,
		AssetTokenID:  fixture.loanTokenID,
		AdapterType:   entity.MorphoAdapterTypeMarketV1,
		AddedAtBlock:  healedRemoval,
	}
	syntheticID, err := fixture.repo.createAdapterIncarnation(ctx, tx, synthetic, healedRemoval)
	if err != nil {
		t.Fatalf("CreateAdapterIncarnation: %v", err)
	}
	if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, addr, healedRemoval); err != nil {
		t.Fatalf("MarkAdapterRemoved after the heal: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if syntheticID == activeID {
		t.Errorf("the heal reused the later incarnation (id=%d) instead of inserting its own row", activeID)
	}
	want := fmt.Sprintf("id=%d [%d,%d] id=%d [%d,ACTIVE]", syntheticID, healedRemoval, healedRemoval, activeID, laterAdd)
	if got := fixture.describeIncarnations(t, ctx, vaultID, addr); got != want {
		t.Errorf("registry = %q, want %q", got, want)
	}
}

// TestCreateAdapterIncarnation_ExactKeyConflictLeavesTheRecordedRowAlone pins the only
// tolerated conflict. A row already recorded at the same (vault, address, added block)
// is returned as-is: the DO UPDATE must not write the requested removed_at_block onto
// it, or a heal racing an AddAdapter would close an incarnation the chain still has
// open.
func TestCreateAdapterIncarnation_ExactKeyConflictLeavesTheRecordedRowAlone(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x54))
	addr := adapterAddr(0x55)

	const addedAt = int64(900)
	openID := fixture.createTestAdapter(t, ctx, vaultID, addr, addedAt)

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	got, err := fixture.repo.createAdapterIncarnation(ctx, tx, &entity.MorphoAdapter{
		MorphoVaultID: vaultID,
		Address:       addr,
		AssetTokenID:  fixture.loanTokenID,
		AdapterType:   entity.MorphoAdapterTypeMarketV1,
		AddedAtBlock:  addedAt,
	}, addedAt)
	if err != nil {
		t.Fatalf("CreateAdapterIncarnation: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got != openID {
		t.Errorf("id = %d, want the existing row %d", got, openID)
	}
	got2 := fixture.describeIncarnations(t, ctx, vaultID, addr)
	if want := fmt.Sprintf("id=%d [%d,ACTIVE]", openID, addedAt); got2 != want {
		t.Errorf("registry = %q, want %q: the conflict must not close the existing row", got2, want)
	}
}

// TestEnsureIncarnationToClose_DecidesAndRegistersUnderOneLock pins the read-then-write
// serialization ADR-0002 §3 requires: the "is there an incarnation to close" read and the
// registration it authorises must both happen under the adapter's advisory lock, or two
// overlapping writers each decide "nothing on record" and mint their own incarnation for
// one on-chain lifetime.
//
// The interleaving below is the live-worker-vs-backfiller one. An AddAdapter@900 holds the
// lock uncommitted while a RemoveAdapter@1000 for the same adapter starts: with the
// decisive read taken BEFORE the lock, the removal sees an empty registry, waits for the
// lock only to insert, and lands a second [1000,1000] row beside the add's — leaving the
// added row ACTIVE forever. Taking the lock first makes the removal read the add and close
// IT, whichever order the two writers are granted the lock in.
func TestEnsureIncarnationToClose_DecidesAndRegistersUnderOneLock(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x58))
	addr := adapterAddr(0x59)

	const (
		addAt    = int64(900)
		removeAt = int64(1000)
	)

	addTx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the AddAdapter transaction: %v", err)
	}
	defer addTx.Rollback(ctx)
	addedID, err := fixture.repo.GetOrCreateAdapter(ctx, addTx, &entity.MorphoAdapter{
		MorphoVaultID: vaultID,
		Address:       addr,
		AssetTokenID:  fixture.loanTokenID,
		AdapterType:   entity.MorphoAdapterTypeMarketV1,
		AddedAtBlock:  addAt,
	})
	if err != nil {
		t.Fatalf("registering the adapter at %d: %v", addAt, err)
	}

	removed := make(chan error, 1)
	go func() {
		removed <- fixture.removeAdapter(ctx, vaultID, addr, removeAt, entity.MorphoAdapterTypeMarketV1)
	}()

	// Long enough for the removal to reach its decisive read; it can only get past the
	// registration by waiting for the lock this transaction holds.
	time.Sleep(500 * time.Millisecond)
	if err := addTx.Commit(ctx); err != nil {
		t.Fatalf("commit the AddAdapter transaction: %v", err)
	}
	if err := <-removed; err != nil {
		t.Fatalf("RemoveAdapter@%d: %v", removeAt, err)
	}

	want := fmt.Sprintf("id=%d [%d,%d]", addedID, addAt, removeAt)
	if got := fixture.describeIncarnations(t, ctx, vaultID, addr); got != want {
		t.Errorf("registry = %q, want %q: the removal decided before it held the lock, so it minted its own incarnation instead of closing the one being added", got, want)
	}
}

// TestMarkAdapterRemoved_ConflatedIncarnationsFromABoundedReplayAreRefused walks
// the full sequence a bounded historical replay puts an adapter through when the
// registry has conflated two of its incarnations, and pins that it stops loudly
// instead of erasing the recorded de-registration.
//
// True on-chain history is add@1000 / remove@1050 / add@1100 / remove@1200, but the
// live stream discovered the vault mid-life and recorded only [1100, 1200], with its
// realAssets snapshots hanging off that row. An operator then replays 1000-1150 — a
// range that covers the FIRST incarnation's whole life but stops inside the second's.
// Step by step: the replayed AddAdapter@1000 folds onto the recorded row (legitimate:
// with no incarnation-sequence key, a mid-life-discovered row is exactly what a
// backfilled add is supposed to converge), which makes the row cover 1050, so the
// replayed RemoveAdapter@1050 registers nothing and lands on MarkAdapterRemoved with
// a recorded close 150 blocks above it. Converging there would erase the real
// removal at 1200 and leave an adapter that is de-registered on-chain permanently
// ACTIVE in the registry, its two snapshots outside the window. A replay range that
// also covered 1200 would repair the row, but nothing enforces that it does, so the
// close block is a recorded fact this cannot silently rewrite.
func TestMarkAdapterRemoved_ConflatedIncarnationsFromABoundedReplayAreRefused(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x50))
	addr := adapterAddr(0x51)

	const (
		firstAdd     = int64(1000)
		firstRemove  = int64(1050)
		secondAdd    = int64(1100)
		midLifeState = int64(1150)
		secondRemove = int64(1200)
	)

	liveID := fixture.createTestAdapter(t, ctx, vaultID, addr, secondAdd)
	fixture.seedAdapterStateAt(t, ctx, liveID, secondAdd)
	fixture.seedAdapterStateAt(t, ctx, liveID, midLifeState)
	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, secondRemove); err != nil {
		t.Fatalf("recording the live removal at %d: %v", secondRemove, err)
	}

	if got := fixture.createTestAdapter(t, ctx, vaultID, addr, firstAdd); got != liveID {
		t.Fatalf("replayed AddAdapter@%d created id=%d; this scenario needs it to fold onto the live row %d", firstAdd, got, liveID)
	}
	if covering := fixture.adapterIncarnationAt(t, ctx, vaultID, addr, firstRemove); covering == nil {
		t.Fatalf("the folded row must cover block %d, so the removal reaches MarkAdapterRemoved without registering anything", firstRemove)
	}

	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, firstRemove); err == nil {
		t.Errorf("removal at %d must be refused as a conflated incarnation, not converged onto the close recorded at %d; registry is now %s",
			firstRemove, secondRemove, fixture.describeIncarnations(t, ctx, vaultID, addr))
	} else if !strings.Contains(err.Error(), "conflated incarnation") {
		t.Errorf("error %q should name the conflated-incarnation hypothesis", err.Error())
	}

	if got := fixture.describeIncarnations(t, ctx, vaultID, addr); got != fmt.Sprintf("id=%d [%d,%d]", liveID, firstAdd, secondRemove) {
		t.Errorf("registry = %q, want the folded row still closed at %d: the recorded de-registration must survive the replay", got, secondRemove)
	}
}

// TestMarkAdapterRemoved_ConvergingCloseRunsTheOrphanGuard pins that a downward
// convergence is guarded too, not exempt: it narrows the window, so it can strand
// snapshots the initial close legitimately admitted. Exempting this arm is what let a
// conflated replay inside the reorg window erase a recorded de-registration.
func TestMarkAdapterRemoved_ConvergingCloseRunsTheOrphanGuard(t *testing.T) {
	const (
		addedAt     = int64(100)
		recorded    = int64(500)
		relocatedTo = int64(499)
	)

	tests := []struct {
		name        string
		stateAt     int64
		wantRemoved int64
	}{
		{name: "a snapshot the narrowed window would strand blocks it", stateAt: recorded, wantRemoved: recorded},
		{name: "a snapshot still inside the narrowed window does not", stateAt: relocatedTo, wantRemoved: relocatedTo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x46))
			addr := adapterAddr(0x47)

			adapterID := fixture.createTestAdapter(t, ctx, vaultID, addr, addedAt)
			if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, recorded); err != nil {
				t.Fatalf("recording the removal at %d: %v", recorded, err)
			}
			fixture.seedAdapterStateAt(t, ctx, adapterID, tt.stateAt)

			err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, relocatedTo)
			if wantRefused := tt.wantRemoved == recorded; wantRefused != (err != nil) {
				t.Errorf("converging down to %d: err = %v, want refused = %v", relocatedTo, err, wantRefused)
			}

			var removed *int64
			if err := fixture.pool.QueryRow(ctx,
				`SELECT removed_at_block FROM morpho_adapter WHERE id = $1`, adapterID).Scan(&removed); err != nil {
				t.Fatalf("query: %v", err)
			}
			if removed == nil || *removed != tt.wantRemoved {
				t.Errorf("removed_at_block = %v, want %d", removed, tt.wantRemoved)
			}
		})
	}
}

// TestMarkAdapterRemoved_ConflatedIncarnationsInsideTheReorgWindowAreRefused is the F1
// corruption at a distance the relocation bound permits, and the reason the orphan
// guard may not reason about block_version at all.
//
// True history is add@900 / remove@1000 / add@1010 / remove@1030 — the two lifetimes are
// only 30 blocks apart, so a replay that conflates them converges INSIDE the reorg
// window and the symmetric bound cannot refuse it. The orphan guard is then the only
// thing standing between the replay and an erased de-registration, and the snapshots it
// must catch sit in exactly the band a relocation would have vacated. Excluding them by
// version — for any reason — hands the replay a silent pass.
func TestMarkAdapterRemoved_ConflatedIncarnationsInsideTheReorgWindowAreRefused(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x56))
	addr := adapterAddr(0x57)

	const (
		firstAdd     = int64(900)
		firstRemove  = int64(1000)
		secondAdd    = int64(1010)
		midLifeState = int64(1020)
		secondRemove = int64(1030)
	)

	liveID := fixture.createTestAdapter(t, ctx, vaultID, addr, secondAdd)
	fixture.seedAdapterStateAtVersion(t, ctx, liveID, secondAdd, 0)
	fixture.seedAdapterStateAtVersion(t, ctx, liveID, midLifeState, 0)
	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, secondRemove); err != nil {
		t.Fatalf("recording the live removal at %d: %v", secondRemove, err)
	}

	if got := fixture.createTestAdapter(t, ctx, vaultID, addr, firstAdd); got != liveID {
		t.Fatalf("replayed AddAdapter@%d created id=%d; this scenario needs it to fold onto the live row %d", firstAdd, got, liveID)
	}

	// The replayed removal's own block was reorged once, so it carries a higher version
	// than the untouched blocks its snapshots live in.
	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, firstRemove); err == nil {
		t.Errorf("converging down to %d must be refused: it is %d blocks below the recorded close, inside the reorg window, so only the stranded snapshots distinguish a relocation from a conflated replay; registry is now %s",
			firstRemove, secondRemove-firstRemove, fixture.describeIncarnations(t, ctx, vaultID, addr))
	}

	if got, want := fixture.describeIncarnations(t, ctx, vaultID, addr), fmt.Sprintf("id=%d [%d,%d]", liveID, firstAdd, secondRemove); got != want {
		t.Errorf("registry = %q, want %q: the recorded de-registration must survive the replay", got, want)
	}
}

// TestMarkAdapterRemoved_InitialCloseCountsEveryStateRowAboveIt pins that an
// incarnation's FIRST close compares block_number only, at every distance.
//
// An initial close relocated nothing: there is no prior recorded close for a reorg to
// have moved the removal away from, so the removal's own block_version says only "how
// many times this height was republished" and carries no information about the heights
// above it. Excluding lower-versioned snapshots there strands rows the canonical chain
// owns — the shape seedDiscoveredAdapters' bootstrap contract relies on this guard to
// catch, where a row seeded at a later discovery block is converged into an earlier
// replayed window. The distances below straddle the reorg window deliberately: a fixed
// 64-block band around the close is NOT a safe place to trust block_version either,
// because block_version is a per-block_number counter, not a chain epoch.
func TestMarkAdapterRemoved_InitialCloseCountsEveryStateRowAboveIt(t *testing.T) {
	const (
		trueAdd    = int64(100)
		trueRemove = int64(500)
	)

	tests := []struct {
		name        string
		discoveryAt int64
	}{
		{name: "a snapshot just above the close", discoveryAt: trueRemove + 1},
		{name: "a snapshot inside the reorg window", discoveryAt: trueRemove + maxRemovalRelocationDistance - 4},
		{name: "a snapshot beyond the reorg window", discoveryAt: trueRemove + maxRemovalRelocationDistance + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x4a))
			addr := adapterAddr(0x4b)

			adapterID := fixture.createTestAdapter(t, ctx, vaultID, addr, tt.discoveryAt)
			fixture.seedAdapterStateAtVersion(t, ctx, adapterID, tt.discoveryAt, 0)
			if got := fixture.createTestAdapter(t, ctx, vaultID, addr, trueAdd); got != adapterID {
				t.Fatalf("the replayed AddAdapter must fold onto the seeded row: got id=%d want %d", got, adapterID)
			}

			// The replayed removal's block was reorged once in history, so its receipt
			// version is 1 — higher than the untouched discovery block's.
			if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, trueRemove); err == nil {
				t.Errorf("closing at %d must be refused: the snapshot at (%d, v0) is %d blocks above it and no reorg relocated this close, so its lower version is not evidence of a dead chain; registry is now %s",
					trueRemove, tt.discoveryAt, tt.discoveryAt-trueRemove, fixture.describeIncarnations(t, ctx, vaultID, addr))
			}
		})
	}
}

// TestMarkAdapterRemoved_RedeliveryAboveTheRecordedCloseSkipsTheOrphanGuard pins the
// one exemption: a close that does not narrow the window. SQS delivers at least once,
// and a redelivered removal whose block a reorg nudged upward converges to the
// already-recorded close, leaving removed_at_block untouched. Re-asking the guard
// there would fail a write that changes nothing, turning every redelivery into a
// poison pill.
func TestMarkAdapterRemoved_RedeliveryAboveTheRecordedCloseSkipsTheOrphanGuard(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x48))
	addr := adapterAddr(0x49)

	const (
		addedAt  = int64(100)
		recorded = int64(500)
	)

	adapterID := fixture.createTestAdapter(t, ctx, vaultID, addr, addedAt)
	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, recorded); err != nil {
		t.Fatalf("recording the removal at %d: %v", recorded, err)
	}
	fixture.seedAdapterStateAtVersion(t, ctx, adapterID, recorded+10, 1)

	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, recorded+1); err != nil {
		t.Fatalf("a redelivery that leaves removed_at_block at %d must not be refused: %v", recorded, err)
	}

	var removed *int64
	if err := fixture.pool.QueryRow(ctx,
		`SELECT removed_at_block FROM morpho_adapter WHERE id = $1`, adapterID).Scan(&removed); err != nil {
		t.Fatalf("query: %v", err)
	}
	if removed == nil || *removed != recorded {
		t.Errorf("removed_at_block = %v, want %d unchanged", removed, recorded)
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

// TestMarkAdapterRemoved_OrphaningStateRowsIsHardError reproduces the
// multi-incarnation orphaning the reviewer found: AddAdapter@100 / RemoveAdapter@500
// / AddAdapter@600 on-chain, but the vault is discovered at 1000, so the registry
// seeds ONE row at added=1000 and hangs its adapter_state snapshots off it. The
// backfiller then converges that row down to added=100 and replays the removal at
// 500 — which would close a row that owns state rows at 1000+, stranding them
// inside a [100, 500] window: window-filtered queries drop them, window-ignoring
// queries double-count them against the second incarnation. The removal must fail
// loudly instead, so the event poison-pills and the snapshots get re-homed.
func TestMarkAdapterRemoved_OrphaningStateRowsIsHardError(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x31))
	addr := adapterAddr(0x32)

	const (
		trueAdd       = int64(100)
		trueRemove    = int64(500)
		discoveryAt   = int64(1000)
		laterSnapshot = int64(1010)
	)

	adapterID := fixture.createTestAdapter(t, ctx, vaultID, addr, discoveryAt)
	fixture.seedAdapterStateAt(t, ctx, adapterID, discoveryAt)
	fixture.seedAdapterStateAt(t, ctx, adapterID, laterSnapshot)

	// Backfill replays the true AddAdapter, converging the row down to block 100.
	if got := fixture.createTestAdapter(t, ctx, vaultID, addr, trueAdd); got != adapterID {
		t.Fatalf("backfilled add should converge onto the seeded row: got id=%d want %d", got, adapterID)
	}

	err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, trueRemove)
	if err == nil {
		t.Fatal("closing an incarnation that owns later adapter_state rows must be a hard error")
	}

	var removed *int64
	if err := fixture.pool.QueryRow(ctx,
		`SELECT removed_at_block FROM morpho_adapter WHERE id = $1`, adapterID).Scan(&removed); err != nil {
		t.Fatalf("query: %v", err)
	}
	if removed != nil {
		t.Errorf("removed_at_block = %d, want NULL (the failed close must not commit)", *removed)
	}
}

// TestMarkAdapterRemoved_StateRowAtRemovalBlockIsAllowed pins the guard's boundary:
// an allocation snapshot taken in the SAME block as the removal (the Deallocate log
// that precedes the RemoveAdapter log in one governance transaction) is inside the
// closed window, so it must not block the close.
func TestMarkAdapterRemoved_StateRowAtRemovalBlockIsAllowed(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x33))
	addr := adapterAddr(0x34)

	adapterID := fixture.createTestAdapter(t, ctx, vaultID, addr, 100)
	fixture.seedAdapterStateAt(t, ctx, adapterID, 500)

	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, 500); err != nil {
		t.Fatalf("a state row at the removal block must not block the close: %v", err)
	}
}

// TestMarkAdapterRemoved_ReplayOldRemovalSparesReAddedRow guards the added_at_block
// scope: an adapter added→removed@X→re-added at a2>X, then a replay of the old
// RemoveAdapter@X must re-match the originally-removed incarnation and leave the
// active re-added row untouched (rather than closing it with a block earlier than
// its own registration).
func TestMarkAdapterRemoved_ReplayOldRemovalSparesReAddedRow(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x1a))
	addr := adapterAddr(0x0e)

	const (
		firstAdd    = int64(24481834)
		removeBlock = int64(24600000)
		reAdd       = int64(24700000)
	)

	mark := func(block int64) error {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if err := fixture.repo.MarkAdapterRemoved(ctx, tx, vaultID, addr, block); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	removedAt := func(addedAtBlock int64) *int64 {
		var removed *int64
		if err := fixture.pool.QueryRow(ctx,
			`SELECT removed_at_block FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2 AND added_at_block = $3`,
			vaultID, addr, addedAtBlock).Scan(&removed); err != nil {
			t.Fatalf("reading removed_at_block for added_at_block %d: %v", addedAtBlock, err)
		}
		return removed
	}

	fixture.createTestAdapter(t, ctx, vaultID, addr, firstAdd)
	if err := mark(removeBlock); err != nil {
		t.Fatalf("removing first incarnation: %v", err)
	}
	fixture.createTestAdapter(t, ctx, vaultID, addr, reAdd)

	// Replay the old removal at removeBlock.
	if err := mark(removeBlock); err != nil {
		t.Fatalf("replaying removal at %d: %v", removeBlock, err)
	}

	if got := removedAt(reAdd); got != nil {
		t.Errorf("re-added incarnation (added %d) was wrongly closed at %d; must stay active", reAdd, *got)
	}
	if got := removedAt(firstAdd); got == nil || *got != removeBlock {
		t.Errorf("originally-removed incarnation removed_at_block = %v, want %d", got, removeBlock)
	}
}

// --- GetAdapterIncarnationAt Tests ---

// TestGetAdapterIncarnationAt pins the lookup a RemoveAdapter uses to decide whether
// it already has a row to close. The load-bearing case is the last two rows: an
// incarnation closed AT the block covers it (so a replayed removal is idempotent
// against that row instead of minting a zero-length duplicate), while one closed
// BELOW it does not (that is a later incarnation whose AddAdapter we never saw, and
// the caller must register it).
func TestGetAdapterIncarnationAt(t *testing.T) {
	const (
		addedAt  = int64(100)
		closedAt = int64(500)
	)

	tests := []struct {
		name    string
		close   bool
		atBlock int64
		wantHit bool
	}{
		{name: "an open incarnation covers a later block", atBlock: 900, wantHit: true},
		{name: "an open incarnation covers its own registration block", atBlock: addedAt, wantHit: true},
		{name: "no incarnation covers a block before the registration", atBlock: addedAt - 1},
		{name: "a closed incarnation covers a block inside its window", close: true, atBlock: 300, wantHit: true},
		{name: "a closed incarnation covers the block it closed at", close: true, atBlock: closedAt, wantHit: true},
		{name: "a closed incarnation does not cover a block above its close", close: true, atBlock: closedAt + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupMorphoTest(t)
			ctx := context.Background()
			vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x42))
			addr := adapterAddr(0x43)

			adapterID := fixture.createTestAdapter(t, ctx, vaultID, addr, addedAt)
			if tt.close {
				if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, closedAt); err != nil {
					t.Fatalf("MarkAdapterRemoved: %v", err)
				}
			}

			tx, err := fixture.pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer tx.Rollback(ctx)

			got, err := fixture.repo.getAdapterIncarnationAt(ctx, tx, vaultID, addr, tt.atBlock)
			if err != nil {
				t.Fatalf("GetAdapterIncarnationAt(%d): %v", tt.atBlock, err)
			}
			if !tt.wantHit {
				if got != nil {
					t.Fatalf("expected no incarnation covering block %d, got id=%d", tt.atBlock, got.ID)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected the incarnation covering block %d", tt.atBlock)
			}
			if got.ID != adapterID || got.AddedAtBlock != addedAt {
				t.Errorf("incarnation = (id %d, added %d), want (id %d, added %d)", got.ID, got.AddedAtBlock, adapterID, addedAt)
			}
		})
	}
}

// TestGetAdapterIncarnationAt_PicksTheLatestCoveringIncarnation guards the
// ORDER BY added_at_block DESC: with a closed [100, 500] and an active [600, …] row
// coexisting, a removal at 700 must resolve to the active incarnation.
func TestGetAdapterIncarnationAt_PicksTheLatestCoveringIncarnation(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x44))
	addr := adapterAddr(0x45)

	fixture.createTestAdapter(t, ctx, vaultID, addr, 100)
	if err := fixture.markAdapterRemoved(t, ctx, vaultID, addr, 500); err != nil {
		t.Fatalf("MarkAdapterRemoved: %v", err)
	}
	reAddID := fixture.createTestAdapter(t, ctx, vaultID, addr, 600)

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	got, err := fixture.repo.getAdapterIncarnationAt(ctx, tx, vaultID, addr, 700)
	if err != nil {
		t.Fatalf("GetAdapterIncarnationAt: %v", err)
	}
	if got == nil || got.ID != reAddID {
		t.Fatalf("incarnation = %+v, want the re-added row id=%d", got, reAddID)
	}
}

// --- GetActiveAdapter / GetActiveAdaptersByVault Tests ---

func TestGetActiveAdapter_Found(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x16))
	id := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x08), 24481834)

	got, err := fixture.getActiveAdapter(t, ctx, vaultID, adapterAddr(0x08))
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

	got, err := fixture.getActiveAdapter(t, ctx, vaultID, adapterAddr(0x09))
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

	got, err := fixture.getActiveAdapter(t, ctx, vaultID, adapterAddr(0x0a))
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

// --- SaveVaultCap Tests ---

// capIDFor returns the on-chain cap id for a pre-image: id = keccak256(idData).
// The entity enforces this pairing, so tests must derive the id, not invent one.
func capIDFor(idData []byte) []byte {
	return crypto.Keccak256(idData)
}

// saveCap persists one MorphoVaultCap in its own committed transaction.
func (f *morphoTestFixture) saveCap(t *testing.T, ctx context.Context, c *entity.MorphoVaultCap) {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := f.repo.SaveVaultCap(ctx, tx, c); err != nil {
		t.Fatalf("SaveVaultCap failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// latestCap reads the current (absolute, relative) pair for a cap id straight
// from the table using the ADR-0002 latest-row ordering — the read shape the
// downstream consumer uses now that the repo exposes no GetLatestVaultCap.
func (f *morphoTestFixture) latestCap(t *testing.T, ctx context.Context, vaultID int64, cid []byte) (absolute, relative *big.Int, found bool) {
	t.Helper()
	var absStr, relStr string
	err := f.pool.QueryRow(ctx,
		`SELECT absolute_cap::text, relative_cap::text FROM morpho_vault_cap
		 WHERE morpho_vault_id = $1 AND cap_id = $2
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC
		 LIMIT 1`, vaultID, cid).Scan(&absStr, &relStr)
	if err != nil {
		return nil, nil, false
	}
	a, ok := new(big.Int).SetString(absStr, 10)
	if !ok {
		t.Fatalf("absolute_cap %q not decimal", absStr)
	}
	r, ok := new(big.Int).SetString(relStr, 10)
	if !ok {
		t.Fatalf("relative_cap %q not decimal", relStr)
	}
	return a, r, true
}

func TestSaveVaultCap_RoundTrip(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x20))

	idData := []byte{0x01, 0x02, 0x03, 0x04}
	cid := capIDFor(idData)
	fixture.saveCap(t, ctx, &entity.MorphoVaultCap{
		MorphoVaultID: vaultID,
		CapID:         cid,
		IDData:        idData,
		AbsoluteCap:   big.NewInt(1000000000000),
		RelativeCap:   big.NewInt(500000000000000000),
		BlockNumber:   24500000,
		BlockVersion:  0,
		Timestamp:     morphoBlockTime,
	})

	abs, rel, found := fixture.latestCap(t, ctx, vaultID, cid)
	if !found {
		t.Fatal("expected cap, got none")
	}
	if abs.Cmp(big.NewInt(1000000000000)) != 0 {
		t.Errorf("absolute_cap mismatch: got %s", abs)
	}
	if rel.Cmp(big.NewInt(500000000000000000)) != 0 {
		t.Errorf("relative_cap mismatch: got %s", rel)
	}
}

// TestSaveVaultCap_SameBlockDedupesToIdenticalRow verifies the snapshot contract:
// two cap events in the same block (e.g. IncreaseAbsoluteCap + IncreaseRelativeCap)
// each read the same on-chain state and write a byte-identical row; the mvc
// trigger (same build → same processing_version) plus ON CONFLICT DO NOTHING
// collapse them to a single row.
func TestSaveVaultCap_SameBlockDedupesToIdenticalRow(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x24))

	idData := []byte{0xde, 0xad, 0xbe, 0xef}
	cid := capIDFor(idData)
	row := func() *entity.MorphoVaultCap {
		return &entity.MorphoVaultCap{
			MorphoVaultID: vaultID,
			CapID:         cid,
			IDData:        idData,
			AbsoluteCap:   big.NewInt(250000000000000),
			RelativeCap:   big.NewInt(1000000000000000000),
			BlockNumber:   24765623,
			BlockVersion:  0,
			Timestamp:     morphoBlockTime,
		}
	}
	fixture.saveCap(t, ctx, row())
	fixture.saveCap(t, ctx, row())

	var count int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_vault_cap WHERE morpho_vault_id = $1 AND cap_id = $2`,
		vaultID, cid).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("same-block identical caps: expected 1 row, got %d", count)
	}
}

// TestSaveVaultCap_MaxWidthRoundTrip round-trips the numeric column extremes:
// both absolute_cap and relative_cap are on-chain uint128 (NUMERIC(39,0)), so
// max uint128 must survive intact, guarding against a width/precision regression.
func TestSaveVaultCap_MaxWidthRoundTrip(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x25))

	maxU128 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

	idData := []byte{0xca, 0xfe}
	cid := capIDFor(idData)
	fixture.saveCap(t, ctx, &entity.MorphoVaultCap{
		MorphoVaultID: vaultID,
		CapID:         cid,
		IDData:        idData,
		AbsoluteCap:   maxU128,
		RelativeCap:   maxU128,
		BlockNumber:   24500000,
		BlockVersion:  0,
		Timestamp:     morphoBlockTime,
	})

	abs, rel, found := fixture.latestCap(t, ctx, vaultID, cid)
	if !found {
		t.Fatal("expected cap, got none")
	}
	if abs.Cmp(maxU128) != 0 {
		t.Errorf("absolute_cap max-uint128 round-trip: got %s", abs)
	}
	if rel.Cmp(maxU128) != 0 {
		t.Errorf("relative_cap max-uint128 round-trip: got %s", rel)
	}
}

// TestSaveVaultCap_DifferentBuildNewVersion mirrors the adapter-state correction
// path: two writes with the SAME natural key but different build_id are distinct
// reprocessings, so the mvc trigger assigns a new processing_version to the
// second rather than deduping it. Both rows survive (processing_version 0 and 1)
// and the ADR-0002 latest-row read returns the second (build-2) row.
func TestSaveVaultCap_DifferentBuildNewVersion(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x26))

	repoBuild1, err := NewMorphoRepository(morphoPool, nil, 1)
	if err != nil {
		t.Fatalf("NewMorphoRepository build 1: %v", err)
	}
	repoBuild2, err := NewMorphoRepository(morphoPool, nil, 2)
	if err != nil {
		t.Fatalf("NewMorphoRepository build 2: %v", err)
	}

	idData := []byte{0x0a, 0x0b, 0x0c}
	cid := capIDFor(idData)
	save := func(repo *MorphoRepository, absolute *big.Int) {
		c := &entity.MorphoVaultCap{
			MorphoVaultID: vaultID,
			CapID:         cid,
			IDData:        idData,
			AbsoluteCap:   absolute,
			RelativeCap:   big.NewInt(1000000000000000000),
			BlockNumber:   24500300,
			BlockVersion:  0,
			Timestamp:     morphoBlockTime,
		}
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := repo.SaveVaultCap(ctx, tx, c); err != nil {
			t.Fatalf("SaveVaultCap failed: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Different build_id → reprocessing → a new processing_version, so both rows
	// survive.
	save(repoBuild1, big.NewInt(1000))
	save(repoBuild2, big.NewInt(2000))

	var count, maxVer int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT COUNT(*), MAX(processing_version) FROM morpho_vault_cap WHERE morpho_vault_id = $1 AND cap_id = $2`,
		vaultID, cid).Scan(&count, &maxVer); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows for distinct builds, got %d", count)
	}
	if maxVer != 1 {
		t.Errorf("expected max processing_version 1, got %d", maxVer)
	}

	abs, _, found := fixture.latestCap(t, ctx, vaultID, cid)
	if !found {
		t.Fatal("expected cap, got none")
	}
	if abs.Cmp(big.NewInt(2000)) != 0 {
		t.Errorf("latest-read absolute_cap = %s, want 2000 (the build-2 correction row)", abs)
	}
}

// --- SaveVaultFee Tests ---

// saveFee persists one MorphoVaultFee in its own committed transaction.
func (f *morphoTestFixture) saveFee(t *testing.T, ctx context.Context, fee *entity.MorphoVaultFee) {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := f.repo.SaveVaultFee(ctx, tx, fee); err != nil {
		t.Fatalf("SaveVaultFee failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// latestFee reads the current full fee config for a vault via the ADR-0002
// latest-row ordering (block_number, block_version, processing_version DESC),
// never by build_id — the read shape the downstream consumer uses.
func (f *morphoTestFixture) latestFee(t *testing.T, ctx context.Context, vaultID int64) (perfFee, mgmtFee *big.Int, perfRecip, mgmtRecip []byte, found bool) {
	t.Helper()
	var perfStr, mgmtStr string
	err := f.pool.QueryRow(ctx,
		`SELECT performance_fee::text, management_fee::text, performance_fee_recipient, management_fee_recipient
		 FROM morpho_vault_fee
		 WHERE morpho_vault_id = $1
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC
		 LIMIT 1`, vaultID).Scan(&perfStr, &mgmtStr, &perfRecip, &mgmtRecip)
	if err != nil {
		return nil, nil, nil, nil, false
	}
	p, ok := new(big.Int).SetString(perfStr, 10)
	if !ok {
		t.Fatalf("performance_fee %q not decimal", perfStr)
	}
	m, ok := new(big.Int).SetString(mgmtStr, 10)
	if !ok {
		t.Fatalf("management_fee %q not decimal", mgmtStr)
	}
	return p, m, perfRecip, mgmtRecip, true
}

func TestSaveVaultFee_RoundTrip(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x30))

	perfRecip := adapterAddr(0x1a)
	mgmtRecip := make([]byte, 20) // zero-address recipient is the contract default
	fixture.saveFee(t, ctx, &entity.MorphoVaultFee{
		MorphoVaultID:           vaultID,
		PerformanceFee:          big.NewInt(100000000000000000),
		ManagementFee:           big.NewInt(0),
		PerformanceFeeRecipient: perfRecip,
		ManagementFeeRecipient:  mgmtRecip,
		BlockNumber:             24765805,
		BlockVersion:            0,
		Timestamp:               morphoBlockTime,
	})

	perf, mgmt, gotPerfRecip, gotMgmtRecip, found := fixture.latestFee(t, ctx, vaultID)
	if !found {
		t.Fatal("expected fee row, got none")
	}
	if perf.Cmp(big.NewInt(100000000000000000)) != 0 {
		t.Errorf("performance_fee mismatch: got %s", perf)
	}
	if mgmt.Sign() != 0 {
		t.Errorf("management_fee mismatch: got %s, want 0", mgmt)
	}
	if !bytes.Equal(gotPerfRecip, perfRecip) {
		t.Errorf("performance_fee_recipient mismatch: got %x, want %x", gotPerfRecip, perfRecip)
	}
	if !bytes.Equal(gotMgmtRecip, mgmtRecip) {
		t.Errorf("management_fee_recipient mismatch: got %x, want %x", gotMgmtRecip, mgmtRecip)
	}
}

// TestSaveVaultFee_SameBuildDedupesToOneRow verifies the snapshot contract: two
// same-block fee events (e.g. SetPerformanceFee + SetPerformanceFeeRecipient)
// each read the same on-chain config and write a byte-identical row; the mvf
// trigger (same build → same processing_version) plus ON CONFLICT DO NOTHING
// collapse them to a single row.
func TestSaveVaultFee_SameBuildDedupesToOneRow(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x31))

	row := func() *entity.MorphoVaultFee {
		return &entity.MorphoVaultFee{
			MorphoVaultID:           vaultID,
			PerformanceFee:          big.NewInt(100000000000000000),
			ManagementFee:           big.NewInt(0),
			PerformanceFeeRecipient: adapterAddr(0x1a),
			ManagementFeeRecipient:  make([]byte, 20),
			BlockNumber:             24765805,
			BlockVersion:            0,
			Timestamp:               morphoBlockTime,
		}
	}
	fixture.saveFee(t, ctx, row())
	fixture.saveFee(t, ctx, row())

	var count int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_vault_fee WHERE morpho_vault_id = $1`, vaultID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("same-build identical fees: expected 1 row, got %d", count)
	}
}

// TestSaveVaultFee_DifferentBuildNewVersion mirrors the cap-state correction
// path: two writes with the SAME natural key but different build_id are distinct
// reprocessings, so the mvf trigger assigns a new processing_version to the
// second rather than deduping it. Both rows survive (processing_version 0 and 1)
// and the ADR-0002 latest-row read returns the second (build-2) row — never
// ordered by build_id.
func TestSaveVaultFee_DifferentBuildNewVersion(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x32))

	repoBuild1, err := NewMorphoRepository(morphoPool, nil, 1)
	if err != nil {
		t.Fatalf("NewMorphoRepository build 1: %v", err)
	}
	repoBuild2, err := NewMorphoRepository(morphoPool, nil, 2)
	if err != nil {
		t.Fatalf("NewMorphoRepository build 2: %v", err)
	}

	save := func(repo *MorphoRepository, perfFee *big.Int) {
		fee := &entity.MorphoVaultFee{
			MorphoVaultID:           vaultID,
			PerformanceFee:          perfFee,
			ManagementFee:           big.NewInt(0),
			PerformanceFeeRecipient: adapterAddr(0x1a),
			ManagementFeeRecipient:  make([]byte, 20),
			BlockNumber:             24765900,
			BlockVersion:            0,
			Timestamp:               morphoBlockTime,
		}
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := repo.SaveVaultFee(ctx, tx, fee); err != nil {
			t.Fatalf("SaveVaultFee failed: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// build 2 (lower "latest" if ordered by build_id would still be 2 here, so use
	// a build-1 correction that is chronologically second to prove build_id is not
	// the ordering key): write build-2 first, then build-1 second.
	save(repoBuild2, big.NewInt(1000))
	save(repoBuild1, big.NewInt(2000))

	var count, maxVer int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT COUNT(*), MAX(processing_version) FROM morpho_vault_fee WHERE morpho_vault_id = $1`,
		vaultID).Scan(&count, &maxVer); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows for distinct builds, got %d", count)
	}
	if maxVer != 1 {
		t.Errorf("expected max processing_version 1, got %d", maxVer)
	}

	// Latest by processing_version is the build-1 row (perf 2000). Ordering by
	// build_id would wrongly pick the build-2 row (perf 1000).
	perf, _, _, _, found := fixture.latestFee(t, ctx, vaultID)
	if !found {
		t.Fatal("expected fee row, got none")
	}
	if perf.Cmp(big.NewInt(2000)) != 0 {
		t.Errorf("latest-read performance_fee = %s, want 2000 (highest processing_version, not highest build_id)", perf)
	}
}
