//go:build integration

package postgres

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// positionTestFixture holds test dependencies for position repository tests.
type positionTestFixture struct {
	repo    *PositionRepository
	pool    *pgxpool.Pool
	cleanup func()
	// Pre-created IDs for foreign key references
	userID     int64
	protocolID int64
	tokenID    int64
}

// setupPositionTest creates a TimescaleDB container and returns a connected PositionRepository.
func setupPositionTest(t *testing.T) *positionTestFixture {
	t.Helper()
	ctx := context.Background()

	pool, _, cleanup := testutil.SetupTimescaleDB(t)

	repo, err := NewPositionRepository(pool, nil, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	fixture := &positionTestFixture{
		repo:    repo,
		pool:    pool,
		cleanup: cleanup,
	}

	// Create required foreign key references
	fixture.createTestFixtures(t, ctx)

	return fixture
}

// createTestFixtures creates the required chain, user, protocol, and token records.
func (f *positionTestFixture) createTestFixtures(t *testing.T, ctx context.Context) {
	t.Helper()

	// Chain is already seeded by migration, use chain_id=1

	// Create a test user
	err := f.pool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block) VALUES ($1, $2, $3) RETURNING id`,
		1, []byte{0x01, 0x02, 0x03}, 100,
	).Scan(&f.userID)
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	// Get the protocol ID (seeded by migration)
	err = f.pool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE name = 'SparkLend' LIMIT 1`,
	).Scan(&f.protocolID)
	if err != nil {
		t.Fatalf("failed to get protocol: %v", err)
	}

	// Create a test token
	err = f.pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4) RETURNING id`,
		1, []byte{0xaa, 0xbb, 0xcc}, "TEST", 18,
	).Scan(&f.tokenID)
	if err != nil {
		t.Fatalf("failed to create test token: %v", err)
	}
}

// queryCollaterals returns all collateral records for verification.
func (f *positionTestFixture) queryCollaterals(t *testing.T, ctx context.Context) []CollateralRecord {
	t.Helper()

	rows, err := f.pool.Query(ctx,
		`SELECT user_id, protocol_id, token_id, block_number, block_version, amount, event_type, tx_hash, collateral_enabled
		 FROM borrower_collateral ORDER BY block_number, block_version`)
	if err != nil {
		t.Fatalf("failed to query collaterals: %v", err)
	}
	defer rows.Close()

	var results []CollateralRecord
	for rows.Next() {
		var r CollateralRecord
		if err := rows.Scan(&r.UserID, &r.ProtocolID, &r.TokenID, &r.BlockNumber, &r.BlockVersion, &r.Amount, &r.EventType, &r.TxHash, &r.CollateralEnabled); err != nil {
			t.Fatalf("failed to scan collateral: %v", err)
		}
		results = append(results, r)
	}
	return results
}

// truncateCollaterals clears the borrower_collateral table between subtests.
func (f *positionTestFixture) truncateCollaterals(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := f.pool.Exec(ctx, `DELETE FROM borrower_collateral`)
	if err != nil {
		t.Fatalf("failed to truncate collaterals: %v", err)
	}
}

func TestSaveBorrowerCollaterals_EmptyRecords(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Empty slice should return nil without error
	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx, []CollateralRecord{})
	if err != nil {
		t.Errorf("expected nil error for empty records, got: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify no records were inserted
	records := fixture.queryCollaterals(t, ctx)
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
}

func TestSaveBorrowerCollaterals_SingleRecord(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	input := CollateralRecord{
		UserID:            fixture.userID,
		ProtocolID:        fixture.protocolID,
		TokenID:           fixture.tokenID,
		BlockNumber:       1000,
		BlockVersion:      0,
		Amount:            "123456789012345678901234567890",
		EventType:         "Supply",
		TxHash:            []byte{0xab, 0xc1, 0x23},
		CollateralEnabled: true,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx, []CollateralRecord{input})
	if err != nil {
		t.Fatalf("SaveBorrowerCollaterals failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify record was inserted correctly
	records := fixture.queryCollaterals(t, ctx)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	got := records[0]
	if got.UserID != input.UserID {
		t.Errorf("UserID mismatch: got %d, want %d", got.UserID, input.UserID)
	}
	if got.ProtocolID != input.ProtocolID {
		t.Errorf("ProtocolID mismatch: got %d, want %d", got.ProtocolID, input.ProtocolID)
	}
	if got.TokenID != input.TokenID {
		t.Errorf("TokenID mismatch: got %d, want %d", got.TokenID, input.TokenID)
	}
	if got.BlockNumber != input.BlockNumber {
		t.Errorf("BlockNumber mismatch: got %d, want %d", got.BlockNumber, input.BlockNumber)
	}
	if got.BlockVersion != input.BlockVersion {
		t.Errorf("BlockVersion mismatch: got %d, want %d", got.BlockVersion, input.BlockVersion)
	}
	if got.Amount != input.Amount {
		t.Errorf("Amount mismatch: got %s, want %s", got.Amount, input.Amount)
	}
	if got.EventType != input.EventType {
		t.Errorf("EventType mismatch: got %s, want %s", got.EventType, input.EventType)
	}
	if !bytes.Equal(got.TxHash, input.TxHash) {
		t.Errorf("TxHash mismatch: got %v, want %v", got.TxHash, input.TxHash)
	}
	if got.CollateralEnabled != input.CollateralEnabled {
		t.Errorf("CollateralEnabled mismatch: got %v, want %v", got.CollateralEnabled, input.CollateralEnabled)
	}
}

func TestSaveBorrowerCollaterals_TenRecords(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	// Create 10 unique records (different block_version for same block)
	inputs := make([]CollateralRecord, 10)
	for i := 0; i < 10; i++ {
		inputs[i] = CollateralRecord{
			UserID:            fixture.userID,
			ProtocolID:        fixture.protocolID,
			TokenID:           fixture.tokenID,
			BlockNumber:       2000,
			BlockVersion:      i,
			Amount:            fmt.Sprintf("%d000000000000000000", i+1),
			EventType:         fmt.Sprintf("Event%d", i),
			TxHash:            []byte{0x00, 0x00, byte(i)},
			CollateralEnabled: i%2 == 0,
		}
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx, inputs)
	if err != nil {
		t.Fatalf("SaveBorrowerCollaterals failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify all 10 records were inserted
	records := fixture.queryCollaterals(t, ctx)
	if len(records) != 10 {
		t.Fatalf("expected 10 records, got %d", len(records))
	}

	// Verify each record matches input
	for i, got := range records {
		want := inputs[i]
		if got.BlockVersion != want.BlockVersion {
			t.Errorf("record %d: BlockVersion mismatch: got %d, want %d", i, got.BlockVersion, want.BlockVersion)
		}
		if got.Amount != want.Amount {
			t.Errorf("record %d: Amount mismatch: got %s, want %s", i, got.Amount, want.Amount)
		}
		if got.EventType != want.EventType {
			t.Errorf("record %d: EventType mismatch: got %s, want %s", i, got.EventType, want.EventType)
		}
		if got.CollateralEnabled != want.CollateralEnabled {
			t.Errorf("record %d: CollateralEnabled mismatch: got %v, want %v", i, got.CollateralEnabled, want.CollateralEnabled)
		}
	}
}

func TestSaveBorrowerCollaterals_Rollback(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	inputs := make([]CollateralRecord, 10)
	for i := 0; i < 10; i++ {
		inputs[i] = CollateralRecord{
			UserID:            fixture.userID,
			ProtocolID:        fixture.protocolID,
			TokenID:           fixture.tokenID,
			BlockNumber:       3000,
			BlockVersion:      i,
			Amount:            "1000000000000000000",
			EventType:         "Supply",
			TxHash:            []byte{0x01, 0x00, byte(i)},
			CollateralEnabled: true,
		}
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx, inputs)
	if err != nil {
		t.Fatalf("SaveBorrowerCollaterals failed: %v", err)
	}

	// Rollback instead of commit
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify no records exist after rollback
	records := fixture.queryCollaterals(t, ctx)
	if len(records) != 0 {
		t.Errorf("expected 0 records after rollback, got %d", len(records))
	}
}

func TestSaveBorrowerCollaterals_DuplicateIgnored(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	// Create 10 records where record 5 is a duplicate of record 0
	inputs := make([]CollateralRecord, 10)
	for i := 0; i < 10; i++ {
		inputs[i] = CollateralRecord{
			UserID:            fixture.userID,
			ProtocolID:        fixture.protocolID,
			TokenID:           fixture.tokenID,
			BlockNumber:       4000,
			BlockVersion:      i,
			Amount:            fmt.Sprintf("%d000000000000000000", i+1),
			EventType:         fmt.Sprintf("Event%d", i),
			TxHash:            []byte{0x02, 0x00, byte(i)},
			CollateralEnabled: true,
		}
	}

	// First, insert the initial 10 records
	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx1, inputs)
	if err != nil {
		t.Fatalf("SaveBorrowerCollaterals failed: %v", err)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Now try to insert again with some records having same key but different data
	duplicateInputs := make([]CollateralRecord, 10)
	for i := 0; i < 10; i++ {
		duplicateInputs[i] = CollateralRecord{
			UserID:            fixture.userID,
			ProtocolID:        fixture.protocolID,
			TokenID:           fixture.tokenID,
			BlockNumber:       4000,
			BlockVersion:      i,                        // Same key as before
			Amount:            "9999999999999999999",    // Different amount - should be ignored
			EventType:         "Modified",               // Different event type - should be ignored
			TxHash:            []byte{0xff, 0xff, 0xff}, // Different tx hash - should be ignored
			CollateralEnabled: false,                    // Different enabled - should be ignored
		}
	}

	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// This should succeed with ON CONFLICT DO NOTHING
	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx2, duplicateInputs)
	if err != nil {
		t.Fatalf("SaveBorrowerCollaterals with duplicates failed: %v", err)
	}

	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify still only 10 records exist (no duplicates inserted)
	records := fixture.queryCollaterals(t, ctx)
	if len(records) != 10 {
		t.Fatalf("expected 10 records (duplicates ignored), got %d", len(records))
	}

	// Verify original data was preserved (not overwritten)
	for i, got := range records {
		want := inputs[i]
		if got.Amount != want.Amount {
			t.Errorf("record %d: Amount was modified: got %s, want %s (original should be preserved)", i, got.Amount, want.Amount)
		}
		if got.EventType != want.EventType {
			t.Errorf("record %d: EventType was modified: got %s, want %s (original should be preserved)", i, got.EventType, want.EventType)
		}
		if got.CollateralEnabled != want.CollateralEnabled {
			t.Errorf("record %d: CollateralEnabled was modified: got %v, want %v (original should be preserved)", i, got.CollateralEnabled, want.CollateralEnabled)
		}
	}
}

func TestSaveBorrowerCollaterals_PartialDuplicatesInSameBatch(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	// Insert initial record
	initial := CollateralRecord{
		UserID:            fixture.userID,
		ProtocolID:        fixture.protocolID,
		TokenID:           fixture.tokenID,
		BlockNumber:       5000,
		BlockVersion:      0,
		Amount:            "1000000000000000000",
		EventType:         "Original",
		TxHash:            []byte{0x03, 0x00, 0x00},
		CollateralEnabled: true,
	}

	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx1, []CollateralRecord{initial})
	if err != nil {
		t.Fatalf("initial insert failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit initial: %v", err)
	}

	// Now insert a batch where some are new and some are duplicates
	mixedBatch := []CollateralRecord{
		{
			UserID:            fixture.userID,
			ProtocolID:        fixture.protocolID,
			TokenID:           fixture.tokenID,
			BlockNumber:       5000,
			BlockVersion:      0, // Duplicate - should be ignored
			Amount:            "9999999999999999999",
			EventType:         "Duplicate",
			TxHash:            []byte{0x04, 0x00, 0x00},
			CollateralEnabled: false,
		},
		{
			UserID:            fixture.userID,
			ProtocolID:        fixture.protocolID,
			TokenID:           fixture.tokenID,
			BlockNumber:       5000,
			BlockVersion:      1, // New
			Amount:            "2000000000000000000",
			EventType:         "New1",
			TxHash:            []byte{0x04, 0x00, 0x01},
			CollateralEnabled: true,
		},
		{
			UserID:            fixture.userID,
			ProtocolID:        fixture.protocolID,
			TokenID:           fixture.tokenID,
			BlockNumber:       5000,
			BlockVersion:      2, // New
			Amount:            "3000000000000000000",
			EventType:         "New2",
			TxHash:            []byte{0x04, 0x00, 0x02},
			CollateralEnabled: false,
		},
	}

	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx2.Rollback(ctx)

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx2, mixedBatch)
	if err != nil {
		t.Fatalf("mixed batch insert failed: %v", err)
	}

	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify 3 total records (1 original + 2 new, duplicate ignored)
	records := fixture.queryCollaterals(t, ctx)
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// Verify version 0 has original data (not overwritten)
	if records[0].Amount != initial.Amount {
		t.Errorf("version 0 was modified: got %s, want %s", records[0].Amount, initial.Amount)
	}
	if records[0].EventType != initial.EventType {
		t.Errorf("version 0 EventType was modified: got %s, want %s", records[0].EventType, initial.EventType)
	}

	// Verify new records exist
	if records[1].BlockVersion != 1 || records[1].Amount != "2000000000000000000" {
		t.Errorf("version 1 mismatch: got version=%d amount=%s", records[1].BlockVersion, records[1].Amount)
	}
	if records[2].BlockVersion != 2 || records[2].Amount != "3000000000000000000" {
		t.Errorf("version 2 mismatch: got version=%d amount=%s", records[2].BlockVersion, records[2].Amount)
	}
}

func TestSaveBorrowerCollaterals_ForeignKeyViolation(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	// Try to insert with non-existent user_id
	invalidRecord := CollateralRecord{
		UserID:            999999, // Non-existent
		ProtocolID:        fixture.protocolID,
		TokenID:           fixture.tokenID,
		BlockNumber:       6000,
		BlockVersion:      0,
		Amount:            "1000000000000000000",
		EventType:         "Supply",
		TxHash:            []byte{0x05, 0x00, 0x00},
		CollateralEnabled: true,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx, []CollateralRecord{invalidRecord})
	if err == nil {
		t.Error("expected foreign key violation error, got nil")
	}
}

func TestSaveBorrowerCollaterals_LargeAmountPrecision(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	// Test with very large numbers (typical for wei values)
	largeAmount := "115792089237316195423570985008687907853269984665640564039457584007913129639935" // max uint256

	input := CollateralRecord{
		UserID:            fixture.userID,
		ProtocolID:        fixture.protocolID,
		TokenID:           fixture.tokenID,
		BlockNumber:       7000,
		BlockVersion:      0,
		Amount:            largeAmount,
		EventType:         "Supply",
		TxHash:            []byte{0x06, 0x00, 0x00},
		CollateralEnabled: true,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx, []CollateralRecord{input})
	if err != nil {
		t.Fatalf("SaveBorrowerCollaterals with large amount failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify precision was preserved
	records := fixture.queryCollaterals(t, ctx)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	if records[0].Amount != largeAmount {
		t.Errorf("large amount precision lost: got %s, want %s", records[0].Amount, largeAmount)
	}
}

func TestSaveBorrowerCollaterals_ConcurrentTransactions(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	// Create two separate users for concurrent inserts
	var userID2 int64
	err := fixture.pool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block) VALUES ($1, $2, $3) RETURNING id`,
		1, []byte{0x04, 0x05, 0x06}, 100,
	).Scan(&userID2)
	if err != nil {
		t.Fatalf("failed to create second user: %v", err)
	}

	// Start two transactions concurrently
	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)

	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)

	// Insert different records in each transaction
	records1 := []CollateralRecord{{
		UserID:            fixture.userID,
		ProtocolID:        fixture.protocolID,
		TokenID:           fixture.tokenID,
		BlockNumber:       8000,
		BlockVersion:      0,
		Amount:            "1000000000000000000",
		EventType:         "Tx1",
		TxHash:            []byte{0x07, 0x00, 0x01},
		CollateralEnabled: true,
	}}

	records2 := []CollateralRecord{{
		UserID:            userID2,
		ProtocolID:        fixture.protocolID,
		TokenID:           fixture.tokenID,
		BlockNumber:       8000,
		BlockVersion:      0,
		Amount:            "2000000000000000000",
		EventType:         "Tx2",
		TxHash:            []byte{0x07, 0x00, 0x02},
		CollateralEnabled: false,
	}}

	// Execute saves
	if err := fixture.repo.SaveBorrowerCollaterals(ctx, tx1, records1); err != nil {
		t.Fatalf("tx1 save failed: %v", err)
	}
	if err := fixture.repo.SaveBorrowerCollaterals(ctx, tx2, records2); err != nil {
		t.Fatalf("tx2 save failed: %v", err)
	}

	// Commit both
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 commit failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("tx2 commit failed: %v", err)
	}

	// Verify both records exist
	allRecords := fixture.queryCollaterals(t, ctx)
	if len(allRecords) != 2 {
		t.Errorf("expected 2 records from concurrent transactions, got %d", len(allRecords))
	}
}

func TestSaveBorrowerCollaterals_TransactionIsolation(t *testing.T) {
	fixture := setupPositionTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	record := CollateralRecord{
		UserID:            fixture.userID,
		ProtocolID:        fixture.protocolID,
		TokenID:           fixture.tokenID,
		BlockNumber:       9000,
		BlockVersion:      0,
		Amount:            "1000000000000000000",
		EventType:         "Isolated",
		TxHash:            []byte{0x08, 0x00, 0x00},
		CollateralEnabled: true,
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveBorrowerCollaterals(ctx, tx, []CollateralRecord{record})
	if err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Query outside transaction - should not see uncommitted data
	recordsOutside := fixture.queryCollaterals(t, ctx)
	if len(recordsOutside) != 0 {
		t.Errorf("uncommitted data visible outside transaction: got %d records", len(recordsOutside))
	}

	// Query inside transaction - should see data
	var count int
	err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM borrower_collateral WHERE block_number = 9000`).Scan(&count)
	if err != nil {
		t.Fatalf("query inside tx failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 record inside transaction, got %d", count)
	}

	// Now commit
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Now should be visible outside
	recordsAfterCommit := fixture.queryCollaterals(t, ctx)
	if len(recordsAfterCommit) != 1 {
		t.Errorf("expected 1 record after commit, got %d", len(recordsAfterCommit))
	}
}

// Compile-time check that pgx.Tx satisfies what we need
var _ pgx.Tx = (pgx.Tx)(nil)
