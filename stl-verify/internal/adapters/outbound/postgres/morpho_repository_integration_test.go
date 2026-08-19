//go:build integration

package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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
	// Delete children before parents: morpho_adapter_state and
	// morpho_adapter_membership FK morpho_adapter; morpho_vault_cap /
	// morpho_vault_fee and morpho_adapter FK morpho_vault.
	tables := []string{
		`morpho_market_state`,
		`morpho_market_position`,
		`morpho_vault_state`,
		`morpho_vault_position`,
		`morpho_adapter_state`,
		`morpho_adapter_membership`,
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

// adapterTypePtr is shorthand for taking the address of a classification literal.
func adapterTypePtr(t entity.MorphoAdapterType) *entity.MorphoAdapterType { return &t }

// addedAt / removedAt / assertedAt build the three observation shapes the tests use,
// so a test body reads as the sequence of observations it is describing.
func addedAt(block int64, version int, logIndex int32, adapterType entity.MorphoAdapterType) entity.MorphoAdapterMembership {
	return entity.MorphoAdapterMembership{
		BlockNumber: block, BlockVersion: version, LogIndex: logIndex, Timestamp: morphoBlockTime,
		IsMember: true, AdapterType: adapterTypePtr(adapterType), ObservedVia: entity.MembershipFromAddAdapter,
	}
}

func removedAt(block int64, version int, logIndex int32) entity.MorphoAdapterMembership {
	return entity.MorphoAdapterMembership{
		BlockNumber: block, BlockVersion: version, LogIndex: logIndex, Timestamp: morphoBlockTime,
		IsMember: false, AdapterType: nil, ObservedVia: entity.MembershipFromRemoveAdapter,
	}
}

func assertedAt(block int64, version int, logIndex int32, adapterType *entity.MorphoAdapterType, via entity.MembershipSource) entity.MorphoAdapterMembership {
	return entity.MorphoAdapterMembership{
		BlockNumber: block, BlockVersion: version, LogIndex: logIndex, Timestamp: morphoBlockTime,
		IsMember: true, AdapterType: adapterType, ObservedVia: via,
	}
}

// observe records one observation in its own committed transaction and returns the
// adapter's stable id and whether a row was appended.
func (f *morphoTestFixture) observe(t *testing.T, ctx context.Context, vaultID int64, address []byte, m entity.MorphoAdapterMembership) (int64, bool) {
	t.Helper()
	id, appended, err := f.observeErr(ctx, vaultID, address, m)
	if err != nil {
		t.Fatalf("ObserveAdapterMembership(%s@%d.%d): %v", m.ObservedVia, m.BlockNumber, m.LogIndex, err)
	}
	return id, appended
}

// observeErr is observe for the paths that are expected to fail: it rolls the
// transaction back on error, so a refused observation leaves nothing behind.
func (f *morphoTestFixture) observeErr(ctx context.Context, vaultID int64, address []byte, m entity.MorphoAdapterMembership) (int64, bool, error) {
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	id, appended, err := f.repo.ObserveAdapterMembership(ctx, tx, &entity.MorphoAdapterObservation{
		Identity: entity.MorphoAdapterIdentity{
			MorphoVaultID: vaultID, Address: address, AssetTokenID: f.loanTokenID,
		},
		Membership: m,
	})
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("commit: %w", err)
	}
	return id, appended, nil
}

// createTestAdapter records an AddAdapter observation for a MarketV1 adapter on the
// given vault and returns its stable id.
func (f *morphoTestFixture) createTestAdapter(t *testing.T, ctx context.Context, vaultID int64, address []byte, addedAtBlock int64) int64 {
	t.Helper()
	return f.createTestAdapterOfType(t, ctx, vaultID, address, addedAtBlock, entity.MorphoAdapterTypeMarketV1)
}

// createTestAdapterOfType is createTestAdapter with an explicit classification.
func (f *morphoTestFixture) createTestAdapterOfType(t *testing.T, ctx context.Context, vaultID int64, address []byte, addedAtBlock int64, adapterType entity.MorphoAdapterType) int64 {
	t.Helper()
	id, _ := f.observe(t, ctx, vaultID, address, addedAt(addedAtBlock, 0, 0, adapterType))
	return id
}

// seedAdapterStateAt writes one adapter_state snapshot for the given adapter at
// blockNumber, so a test can own state rows around a de-registration.
func (f *morphoTestFixture) seedAdapterStateAt(t *testing.T, ctx context.Context, adapterID, blockNumber int64) {
	t.Helper()
	f.seedAdapterStateAtVersion(t, ctx, adapterID, blockNumber, 0)
}

// seedAdapterStateAtVersion writes one adapter_state snapshot at a specific
// block_version.
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

// getActiveAdapter runs GetActiveAdapter (which reads through the caller's tx) in
// a short read transaction that is rolled back afterwards.
func (f *morphoTestFixture) getActiveAdapter(t *testing.T, ctx context.Context, vaultID int64, address []byte) (*entity.MorphoAdapterMember, error) {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	return f.repo.GetActiveAdapter(ctx, tx, vaultID, address)
}

// isMemberAt reports whether the log says the adapter was a member as of the END of a
// block on the latest chain we have indexed: the sentinel log index so every log in that
// block counts, and the maximum block_version so a re-indexed block wins over the version
// a reorg replaced. A CALLER passes its own position instead — an event being processed
// at block_version v must be answered about v, not about a version indexed later.
func (f *morphoTestFixture) isMemberAt(t *testing.T, ctx context.Context, vaultID int64, address []byte, block int64) bool {
	t.Helper()
	member, err := f.repo.GetActiveAdapterAt(ctx, vaultID, address, entity.BlockPosition{
		BlockNumber: block, BlockVersion: math.MaxInt32, LogIndex: entity.EndOfBlockLogIndex,
	})
	if err != nil {
		t.Fatalf("GetActiveAdapterAt(%d): %v", block, err)
	}
	return member != nil
}

// describeMembership renders an adapter's whole observation log in selection order
// (latest first), so a failure message shows what the registry actually holds.
func (f *morphoTestFixture) describeMembership(t *testing.T, ctx context.Context, adapterID int64) string {
	t.Helper()
	rows, err := f.pool.Query(ctx,
		`SELECT block_number, block_version, log_index, is_member, adapter_type, observed_via, processing_version
		 FROM morpho_adapter_membership WHERE morpho_adapter_id = $1
		 ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC`,
		adapterID)
	if err != nil {
		t.Fatalf("query membership: %v", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var (
			block, logIndex, version, pv int64
			isMember                     bool
			adapterType                  *int16
			via                          string
		)
		if err := rows.Scan(&block, &version, &logIndex, &isMember, &adapterType, &via, &pv); err != nil {
			t.Fatalf("scan membership: %v", err)
		}
		typeText := "nil"
		if adapterType != nil {
			typeText = fmt.Sprintf("%d", *adapterType)
		}
		out = append(out, fmt.Sprintf("%d.v%d.%d member=%t type=%s via=%s pv=%d",
			block, version, logIndex, isMember, typeText, via, pv))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate membership: %v", err)
	}
	return strings.Join(out, " | ")
}

// countMembership returns how many observations the log holds for an adapter.
func (f *morphoTestFixture) countMembership(t *testing.T, ctx context.Context, adapterID int64) int {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter_membership WHERE morpho_adapter_id = $1`, adapterID,
	).Scan(&count); err != nil {
		t.Fatalf("counting membership rows: %v", err)
	}
	return count
}

// firstAddBlock is "the block this adapter was added at": a MIN over the log, never a
// column. It is NULL until an AddAdapter has actually been observed.
func (f *morphoTestFixture) firstAddBlock(t *testing.T, ctx context.Context, adapterID int64) *int64 {
	t.Helper()
	var block *int64
	if err := f.pool.QueryRow(ctx,
		`SELECT MIN(block_number) FILTER (WHERE is_member AND observed_via = 'add_adapter_event')
		 FROM morpho_adapter_membership WHERE morpho_adapter_id = $1`, adapterID,
	).Scan(&block); err != nil {
		t.Fatalf("reading the first add block: %v", err)
	}
	return block
}

// countIdentityRows counts the identity rows for one (vault, address).
func (f *morphoTestFixture) countIdentityRows(t *testing.T, ctx context.Context, vaultID int64, address []byte) int {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter WHERE morpho_vault_id = $1 AND address = $2`, vaultID, address,
	).Scan(&count); err != nil {
		t.Fatalf("counting identity rows: %v", err)
	}
	return count
}

// --- ObserveAdapterMembership Tests ---

func TestObserveAdapterMembership_CreateNew(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x30))
	addr := adapterAddr(0x01)

	id, appended := fixture.observe(t, ctx, vaultID, addr, addedAt(24481834, 0, 7, entity.MorphoAdapterTypeMarketV1))
	if id <= 0 {
		t.Errorf("expected positive id, got %d", id)
	}
	if !appended {
		t.Error("a transition must always be recorded")
	}

	got, err := fixture.getActiveAdapter(t, ctx, vaultID, addr)
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
	if got.AsOfBlock != 24481834 {
		t.Errorf("AsOfBlock mismatch: got %d", got.AsOfBlock)
	}
	if got.ObservedVia != entity.MembershipFromAddAdapter {
		t.Errorf("ObservedVia mismatch: got %q", got.ObservedVia)
	}
	if got.AssetTokenID != fixture.loanTokenID {
		t.Errorf("AssetTokenID mismatch: got %d, want %d", got.AssetTokenID, fixture.loanTokenID)
	}
	if block := fixture.firstAddBlock(t, ctx, id); block == nil || *block != 24481834 {
		t.Errorf("first add block = %v, want 24481834", block)
	}
}

func TestObserveAdapterMembership_Idempotent(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x10))
	addr := adapterAddr(0x02)

	id1 := fixture.createTestAdapter(t, ctx, vaultID, addr, 24481834)
	id2 := fixture.createTestAdapter(t, ctx, vaultID, addr, 24481834)

	if id1 != id2 {
		t.Errorf("ObserveAdapterMembership not idempotent: first=%d, second=%d", id1, id2)
	}
	if got := fixture.countMembership(t, ctx, id1); got != 1 {
		t.Errorf("membership rows = %d, want 1: %s", got, fixture.describeMembership(t, ctx, id1))
	}
}

// TestObserveAdapterMembership_RemovalOfUnknownAdapterIsRecorded pins the behaviour
// change this redesign makes on the removal path. The old registry ERRORED on a removal
// for an address it had never seen ("no adapter incarnation registered at or before
// block N"), and the caller papered over that by probing the chain and healing a
// zero-length [R,R] row. There is no lifetime to heal any more: a removal is one
// observation, and the truthful record of "we first learned of this adapter when it was
// de-registered" is exactly one untyped is_member=false row.
func TestObserveAdapterMembership_RemovalOfUnknownAdapterIsRecorded(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x50))
	addr := adapterAddr(0x51)

	id, appended := fixture.observe(t, ctx, vaultID, addr, removedAt(24600000, 0, 3))
	if !appended {
		t.Error("a removal must always be recorded")
	}
	if got := fixture.describeMembership(t, ctx, id); got != "24600000.v0.3 member=false type=nil via=remove_adapter_event pv=0" {
		t.Errorf("membership log = %q", got)
	}
	active, err := fixture.getActiveAdapter(t, ctx, vaultID, addr)
	if err != nil {
		t.Fatalf("GetActiveAdapter: %v", err)
	}
	if active != nil {
		t.Errorf("expected no active adapter, got %+v", active)
	}
	// R3: an adapter first observed by its removal has no known type, and that is now
	// representable rather than a hard failure.
	if block := fixture.firstAddBlock(t, ctx, id); block != nil {
		t.Errorf("first add block = %d, want NULL: no AddAdapter has ever been observed", *block)
	}
}

// TestObserveAdapterMembership_LatestTransitionWinsUnderReorgVersions pins the ordering
// tuple. A re-indexed block is a HIGHER block_version at the same block_number, so it
// wins there without anything being edited — and a later block wins outright, whatever
// version either carries.
func TestObserveAdapterMembership_LatestTransitionWinsUnderReorgVersions(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x31))

	t.Run("a higher block_version at the same block wins", func(t *testing.T) {
		addr := adapterAddr(0x32)
		id, _ := fixture.observe(t, ctx, vaultID, addr, addedAt(1000, 0, 2, entity.MorphoAdapterTypeMarketV1))
		fixture.observe(t, ctx, vaultID, addr, removedAt(1000, 1, 2))

		if fixture.isMemberAt(t, ctx, vaultID, addr, 1000) {
			t.Errorf("the re-indexed block says the adapter is gone: %s", fixture.describeMembership(t, ctx, id))
		}
		if got := fixture.countMembership(t, ctx, id); got != 2 {
			t.Errorf("membership rows = %d, want 2 (nothing is overwritten)", got)
		}
	})

	t.Run("a higher block wins whatever version either carries", func(t *testing.T) {
		addr := adapterAddr(0x33)
		id, _ := fixture.observe(t, ctx, vaultID, addr, removedAt(1000, 3, 2))
		fixture.observe(t, ctx, vaultID, addr, addedAt(1001, 0, 1, entity.MorphoAdapterTypeVaultV1))

		if !fixture.isMemberAt(t, ctx, vaultID, addr, 1001) {
			t.Errorf("the later block must win: %s", fixture.describeMembership(t, ctx, id))
		}
		if fixture.isMemberAt(t, ctx, vaultID, addr, 1000) {
			t.Errorf("as of 1000 the adapter was still gone: %s", fixture.describeMembership(t, ctx, id))
		}
	})
}

// TestObserveAdapterMembership_SameBlockAddRemoveReAdd covers the shape the previous
// design documented as unrepresentable: a governance multicall that adds, removes and
// re-adds one adapter inside a single block collapsed onto one row, leaving the adapter
// de-registered on-DB while it was active on-chain. log_index in the key makes the three
// observations three rows, and the ordering resolves them.
func TestObserveAdapterMembership_SameBlockAddRemoveReAdd(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x34))
	addr := adapterAddr(0x35)

	id, _ := fixture.observe(t, ctx, vaultID, addr, addedAt(2000, 0, 3, entity.MorphoAdapterTypeMarketV1))
	fixture.observe(t, ctx, vaultID, addr, removedAt(2000, 0, 5))
	fixture.observe(t, ctx, vaultID, addr, addedAt(2000, 0, 9, entity.MorphoAdapterTypeMarketV1))

	if got := fixture.countMembership(t, ctx, id); got != 3 {
		t.Errorf("membership rows = %d, want 3: %s", got, fixture.describeMembership(t, ctx, id))
	}
	active, err := fixture.getActiveAdapter(t, ctx, vaultID, addr)
	if err != nil {
		t.Fatalf("GetActiveAdapter: %v", err)
	}
	if active == nil {
		t.Fatalf("the re-add at log index 9 is the last word in the block: %s", fixture.describeMembership(t, ctx, id))
	}
	// Between the removal and the re-add the adapter really was out of the set.
	between, err := fixture.repo.GetActiveAdapterAt(ctx, vaultID, addr, entity.BlockPosition{BlockNumber: 2000, BlockVersion: 0, LogIndex: 6})
	if err != nil {
		t.Fatalf("GetActiveAdapterAt: %v", err)
	}
	if between != nil {
		t.Errorf("at log index 6 the adapter was removed, got %+v", between)
	}
}

// TestObserveAdapterMembership_IdempotentReAppendAndNewBuild pins the redelivery and
// reprocess semantics: the same observation from the same build dedupes on the PK, while
// a deliberate reprocess (a new build_id) takes MAX+1 and orders LAST, so the reprocessed
// row is the one that wins without anything being updated.
func TestObserveAdapterMembership_IdempotentReAppendAndNewBuild(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x36))
	addr := adapterAddr(0x37)

	observation := addedAt(3000, 0, 4, entity.MorphoAdapterTypeMarketV1)
	id, _ := fixture.observe(t, ctx, vaultID, addr, observation)
	fixture.observe(t, ctx, vaultID, addr, observation)

	if got := fixture.countMembership(t, ctx, id); got != 1 {
		t.Fatalf("a same-build re-observation must dedupe, got %d rows: %s", got, fixture.describeMembership(t, ctx, id))
	}

	repoBuild1, err := NewMorphoRepository(morphoPool, nil, 1)
	if err != nil {
		t.Fatalf("NewMorphoRepository build 1: %v", err)
	}
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, _, err := repoBuild1.ObserveAdapterMembership(ctx, tx, &entity.MorphoAdapterObservation{
		Identity:   entity.MorphoAdapterIdentity{MorphoVaultID: vaultID, Address: addr, AssetTokenID: fixture.loanTokenID},
		Membership: observation,
	}); err != nil {
		t.Fatalf("reprocess under build 1: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var count, maxVer int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT count(*), MAX(processing_version) FROM morpho_adapter_membership WHERE morpho_adapter_id = $1`, id,
	).Scan(&count, &maxVer); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 2 || maxVer != 1 {
		t.Errorf("reprocess = %d rows / max processing_version %d, want 2 / 1: %s",
			count, maxVer, fixture.describeMembership(t, ctx, id))
	}
	if !fixture.isMemberAt(t, ctx, vaultID, addr, 3000) {
		t.Error("the reprocessed row carries the same answer, so membership is unchanged")
	}
}

// TestObserveAdapterMembership_RelocatedRemovalNeedsNoBound is the direct replacement for
// the ±64-block symmetric relocation bound and its 8-row semantic matrix. A removal
// re-observed at a different block is simply another row at its own position: both are
// retained, every as-of answer is decided by the ordering tuple, and nothing errors —
// including at distances the old bound refused outright.
func TestObserveAdapterMembership_RelocatedRemovalNeedsNoBound(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x38))
	addr := adapterAddr(0x39)

	id := fixture.createTestAdapter(t, ctx, vaultID, addr, 900)
	fixture.observe(t, ctx, vaultID, addr, removedAt(1000, 0, 1))
	fixture.observe(t, ctx, vaultID, addr, removedAt(990, 0, 1))

	if got := fixture.countMembership(t, ctx, id); got != 3 {
		t.Errorf("membership rows = %d, want 3 (both removals retained): %s", got, fixture.describeMembership(t, ctx, id))
	}
	if !fixture.isMemberAt(t, ctx, vaultID, addr, 950) {
		t.Error("as of 950 the adapter was still a member")
	}
	if fixture.isMemberAt(t, ctx, vaultID, addr, 995) {
		t.Error("as of 995 the relocated removal already applies")
	}
	if fixture.isMemberAt(t, ctx, vaultID, addr, 1005) {
		t.Error("as of 1005 the adapter is gone")
	}

	// A re-observation far outside the old ±64 reorg window is refused by nothing.
	fixture.observe(t, ctx, vaultID, addr, removedAt(500, 0, 1))
	if fixture.isMemberAt(t, ctx, vaultID, addr, 600) {
		t.Errorf("the 500 observation stands on its own: %s", fixture.describeMembership(t, ctx, id))
	}
}

// TestObserveAdapterMembership_CloseNeverOrphansSnapshots is the inverse of the deleted
// orphan guard. Snapshots hang off an identity id that no lifecycle observation can move,
// so a de-registration recorded BELOW existing snapshots is simply recorded: no refusal,
// no poison pill, and every snapshot keeps its adapter.
func TestObserveAdapterMembership_CloseNeverOrphansSnapshots(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x3a))
	addr := adapterAddr(0x3b)

	id := fixture.createTestAdapter(t, ctx, vaultID, addr, 400)
	fixture.seedAdapterStateAt(t, ctx, id, 1000)
	fixture.seedAdapterStateAt(t, ctx, id, 1010)

	if _, _, err := fixture.observeErr(ctx, vaultID, addr, removedAt(500, 0, 2)); err != nil {
		t.Fatalf("recording a removal below existing snapshots must not fail: %v", err)
	}

	var orphaned int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter_state WHERE morpho_adapter_id = $1`, id,
	).Scan(&orphaned); err != nil {
		t.Fatalf("counting snapshots: %v", err)
	}
	if orphaned != 2 {
		t.Errorf("snapshots = %d, want 2 still hanging off adapter %d", orphaned, id)
	}
	if fixture.isMemberAt(t, ctx, vaultID, addr, 1010) {
		t.Errorf("the removal is on record: %s", fixture.describeMembership(t, ctx, id))
	}
}

// TestObserveAdapterMembership_AddBlockConvergesInEitherArrivalOrder is the
// order-independence theorem, and the reason "when was it added" is a MIN over the log
// rather than a column some writer converges. A mid-life discovery ASSERTS membership at
// the discovery block; the true AddAdapter is a TRANSITION at its own, lower block. Both
// arrival orders land on the same two answers.
//
// The two orders do not produce the same ROWS, deliberately: replaying the add after a
// discovery adds the transition the log was missing, whereas a discovery after the add
// asserts an answer the log already gives and writes nothing. Only the answers are
// claimed to converge.
func TestObserveAdapterMembership_AddBlockConvergesInEitherArrivalOrder(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x3c))

	discovery := assertedAt(2000, 0, entity.EndOfBlockLogIndex, adapterTypePtr(entity.MorphoAdapterTypeMarketV1), entity.MembershipFromDiscovery)
	add := addedAt(1000, 0, 6, entity.MorphoAdapterTypeMarketV1)

	orders := []struct {
		name string
		addr []byte
		seq  []entity.MorphoAdapterMembership
	}{
		{"discovery then the replayed add", adapterAddr(0x3d), []entity.MorphoAdapterMembership{discovery, add}},
		{"the add then a later discovery", adapterAddr(0x3e), []entity.MorphoAdapterMembership{add, discovery}},
	}
	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			var id int64
			for _, m := range order.seq {
				id, _ = fixture.observe(t, ctx, vaultID, order.addr, m)
			}
			if block := fixture.firstAddBlock(t, ctx, id); block == nil || *block != 1000 {
				t.Errorf("first add block = %v, want 1000: %s", block, fixture.describeMembership(t, ctx, id))
			}
			if !fixture.isMemberAt(t, ctx, vaultID, order.addr, 2000) {
				t.Errorf("membership at 2000: %s", fixture.describeMembership(t, ctx, id))
			}
			if !fixture.isMemberAt(t, ctx, vaultID, order.addr, 1000) {
				t.Errorf("membership at 1000: %s", fixture.describeMembership(t, ctx, id))
			}
			if fixture.isMemberAt(t, ctx, vaultID, order.addr, 999) {
				t.Errorf("nothing is claimed below the add: %s", fixture.describeMembership(t, ctx, id))
			}
		})
	}
}

// TestObserveAdapterMembership_AssertionThatChangesNothingAppendsNothing pins the
// conditional that keeps a governance-rate table governance-rate. An Allocate proves
// membership but witnesses no change, so once the log already says "member" at that
// position, further allocations write nothing at all.
func TestObserveAdapterMembership_AssertionThatChangesNothingAppendsNothing(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x3f))
	addr := adapterAddr(0x40)

	id := fixture.createTestAdapter(t, ctx, vaultID, addr, 1000)

	for _, logIndex := range []int32{2, 11} {
		_, appended := fixture.observe(t, ctx, vaultID, addr,
			assertedAt(1500, 0, logIndex, adapterTypePtr(entity.MorphoAdapterTypeMarketV1), entity.MembershipFromAllocation))
		if appended {
			t.Errorf("allocation at log index %d appended although the log already said member", logIndex)
		}
	}
	if got := fixture.countMembership(t, ctx, id); got != 1 {
		t.Errorf("membership rows = %d, want 1: %s", got, fixture.describeMembership(t, ctx, id))
	}

	// It DOES append when it changes the answer: after a removal, an allocation is
	// evidence the adapter is back in the set.
	fixture.observe(t, ctx, vaultID, addr, removedAt(1600, 0, 1))
	_, appended := fixture.observe(t, ctx, vaultID, addr,
		assertedAt(1700, 0, 4, adapterTypePtr(entity.MorphoAdapterTypeMarketV1), entity.MembershipFromAllocation))
	if !appended {
		t.Errorf("an allocation after a removal must be recorded: %s", fixture.describeMembership(t, ctx, id))
	}
}

// TestObserveAdapterMembership_UnclassifiedMembershipAssertionIsRefused pins the one
// place ErrAdapterUnclassified survives. The caller probes the type only when its
// pre-transaction read says the adapter is NOT a member; if the in-transaction decision
// disagrees, recording membership would need a classification nobody has, and a defaulted
// type is worse than a failed event.
func TestObserveAdapterMembership_UnclassifiedMembershipAssertionIsRefused(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x41))
	addr := adapterAddr(0x42)

	_, _, err := fixture.observeErr(ctx, vaultID, addr,
		assertedAt(1200, 0, 3, nil, entity.MembershipFromAllocation))
	if !errors.Is(err, outbound.ErrAdapterUnclassified) {
		t.Fatalf("error = %v, want ErrAdapterUnclassified", err)
	}
	// Nothing is written: the transaction is rolled back, so not even the identity row
	// survives. The table's CHECK is the structural backstop behind this.
	if got := fixture.countIdentityRows(t, ctx, vaultID, addr); got != 0 {
		t.Errorf("identity rows = %d, want 0 after a refused observation", got)
	}
}

// TestObserveAdapterMembership_IdentityRowIsWrittenOnce pins the invariant that replaces
// the whole incarnation model: one identity row per (vault, address) forever, with a
// stable id, and at least one observation hanging off it (R5).
func TestObserveAdapterMembership_IdentityRowIsWrittenOnce(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x43))
	addr := adapterAddr(0x44)

	first, _ := fixture.observe(t, ctx, vaultID, addr, addedAt(1000, 0, 1, entity.MorphoAdapterTypeMarketV1))
	second, _ := fixture.observe(t, ctx, vaultID, addr, removedAt(1100, 0, 1))
	third, _ := fixture.observe(t, ctx, vaultID, addr, addedAt(1200, 0, 1, entity.MorphoAdapterTypeVaultV1))

	if first != second || second != third {
		t.Errorf("the identity id moved across observations: %d, %d, %d", first, second, third)
	}
	if got := fixture.countIdentityRows(t, ctx, vaultID, addr); got != 1 {
		t.Errorf("identity rows = %d, want exactly 1 forever", got)
	}

	var stranded int
	if err := fixture.pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter a
		 WHERE NOT EXISTS (SELECT 1 FROM morpho_adapter_membership m WHERE m.morpho_adapter_id = a.id)`,
	).Scan(&stranded); err != nil {
		t.Fatalf("checking the every-identity-has-an-observation invariant: %v", err)
	}
	if stranded != 0 {
		t.Errorf("%d identity rows carry no observation", stranded)
	}
}

// TestObserveAdapterMembership_ConcurrentAssertionsAppendOnce pins the surviving advisory
// lock. An assertion decides whether to append by reading the log, so two overlapping
// writers that both read "nothing here" would each decide to append for one on-chain
// fact — ON CONFLICT cannot catch that, because the decision precedes the insert
// (ADR-0002 §3). Taking the lock BEFORE the decisive read makes the second writer see the
// first's committed answer and append nothing.
//
// The adapter is deliberately seeded FIRST, with a committed removal. If the identity row
// did not exist yet, the two writers would serialize on its speculative insert instead —
// ON CONFLICT DO NOTHING waits out a conflicting inserter — and the test would pass with
// the lock deleted. What is under test is the decision, so the identity must be settled
// before the race starts, and the prior removal is what gives both writers something to
// change.
func TestObserveAdapterMembership_ConcurrentAssertionsAppendOnce(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x45))
	addr := adapterAddr(0x46)
	fixture.createTestAdapter(t, ctx, vaultID, addr, 1000)
	fixture.observe(t, ctx, vaultID, addr, removedAt(1100, 0, 1))
	assertion := assertedAt(1300, 0, 5, adapterTypePtr(entity.MorphoAdapterTypeMarketV1), entity.MembershipFromAllocation)

	firstTx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the first assertion: %v", err)
	}
	defer firstTx.Rollback(ctx)
	id, appended, err := fixture.repo.ObserveAdapterMembership(ctx, firstTx, &entity.MorphoAdapterObservation{
		Identity:   entity.MorphoAdapterIdentity{MorphoVaultID: vaultID, Address: addr, AssetTokenID: fixture.loanTokenID},
		Membership: assertion,
	})
	if err != nil {
		t.Fatalf("first assertion: %v", err)
	}
	if !appended {
		t.Fatal("the first assertion had nothing to go on, so it must append")
	}

	type result struct {
		appended bool
		err      error
	}
	second := make(chan result, 1)
	go func() {
		_, appended, err := fixture.observeErr(ctx, vaultID, addr, assertion)
		second <- result{appended, err}
	}()

	// Long enough for the concurrent writer to reach its decisive read; it can only get
	// past it by waiting for the lock this transaction holds.
	time.Sleep(500 * time.Millisecond)
	if err := firstTx.Commit(ctx); err != nil {
		t.Fatalf("commit the first assertion: %v", err)
	}

	got := <-second
	if got.err != nil {
		t.Fatalf("the concurrent assertion failed: %v", got.err)
	}
	if got.appended {
		t.Error("the concurrent assertion decided before it held the lock, so it recorded an observation for a fact the first writer had already recorded")
	}
	// Two adds/removes seeded above plus the one assertion under test.
	if rows := fixture.countMembership(t, ctx, id); rows != 3 {
		t.Errorf("membership rows = %d, want 3: %s", rows, fixture.describeMembership(t, ctx, id))
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
	addr := adapterAddr(0x09)
	fixture.createTestAdapter(t, ctx, vaultID, addr, 24481834)
	fixture.observe(t, ctx, vaultID, addr, removedAt(24600000, 0, 1))

	got, err := fixture.getActiveAdapter(t, ctx, vaultID, addr)
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
	fixture.observe(t, ctx, vaultID, adapterAddr(0x0d), removedAt(24600000, 0, 1))

	got, err := fixture.repo.GetActiveAdaptersByVault(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetActiveAdaptersByVault failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 active adapters, got %d", len(got))
	}
	for _, a := range got {
		if a.MorphoVaultID != vaultID {
			t.Errorf("adapter %d has wrong vault id %d", a.ID, a.MorphoVaultID)
		}
		if a.AdapterType != entity.MorphoAdapterTypeMarketV1 {
			t.Errorf("adapter %d has type %d, want the type its latest observation carried", a.ID, a.AdapterType)
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

// TestMorphoAdapterCurrentView_MatchesTheRepositoryRead pins the SQL surface the Python
// readers use (VEC-219) against the Go one, so the two cannot drift.
func TestMorphoAdapterCurrentView_MatchesTheRepositoryRead(t *testing.T) {
	fixture := setupMorphoTest(t)
	ctx := context.Background()
	vaultID := fixture.createTestVault(t, ctx, adapterAddr(0x47))

	fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x48), 1000)
	kept := fixture.createTestAdapterOfType(t, ctx, vaultID, adapterAddr(0x49), 1100, entity.MorphoAdapterTypeVaultV1)
	dropped := fixture.createTestAdapter(t, ctx, vaultID, adapterAddr(0x4a), 1200)
	fixture.observe(t, ctx, vaultID, adapterAddr(0x4a), removedAt(1300, 0, 1))

	rows, err := fixture.pool.Query(ctx,
		`SELECT id, adapter_type FROM morpho_adapter_current WHERE morpho_vault_id = $1 ORDER BY id`, vaultID)
	if err != nil {
		t.Fatalf("querying morpho_adapter_current: %v", err)
	}
	defer rows.Close()

	viewed := map[int64]int16{}
	for rows.Next() {
		var id int64
		var adapterType int16
		if err := rows.Scan(&id, &adapterType); err != nil {
			t.Fatalf("scan: %v", err)
		}
		viewed[id] = adapterType
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if len(viewed) != 2 {
		t.Fatalf("view returned %d adapters, want 2", len(viewed))
	}
	if _, ok := viewed[dropped]; ok {
		t.Errorf("the de-registered adapter %d is still in morpho_adapter_current", dropped)
	}
	if viewed[kept] != int16(entity.MorphoAdapterTypeVaultV1) {
		t.Errorf("view adapter_type = %d, want %d", viewed[kept], entity.MorphoAdapterTypeVaultV1)
	}

	active, err := fixture.repo.GetActiveAdaptersByVault(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetActiveAdaptersByVault: %v", err)
	}
	if len(active) != len(viewed) {
		t.Errorf("the view and the repository disagree: %d vs %d adapters", len(viewed), len(active))
	}
	for _, a := range active {
		if _, ok := viewed[a.ID]; !ok {
			t.Errorf("adapter %d is active for the repository but absent from the view", a.ID)
		}
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
