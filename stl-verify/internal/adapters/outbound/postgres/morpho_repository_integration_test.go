//go:build integration

package postgres

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"

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
	tables := []string{
		`morpho_market_state`,
		`morpho_market_position`,
		`morpho_vault_state`,
		`morpho_vault_position`,
		`morpho_market`,
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
		VaultVersion:   entity.MorphoVaultV2,
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
