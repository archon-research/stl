//go:build integration

package backfill_gaps

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// setupPostgres creates a PostgreSQL container and returns a connected repository.
func setupPostgres(t *testing.T) (*postgres.BlockStateRepository, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:18",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	repo := postgres.NewBlockStateRepository(db, nil)
	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	cleanup := func() {
		db.Close()
		container.Terminate(ctx)
	}

	return repo, cleanup
}

// saveBlock is a helper to save a block with minimal boilerplate.
func saveBlock(t *testing.T, ctx context.Context, repo *postgres.BlockStateRepository, number int64) {
	t.Helper()
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:     number,
		Hash:       fmt.Sprintf("0x%064x", number),
		ParentHash: fmt.Sprintf("0x%064x", number-1),
		ReceivedAt: time.Now().Unix(),
		IsOrphaned: false,
	})
	if err != nil {
		t.Fatalf("failed to save block %d: %v", number, err)
	}
}

func TestFindGaps_NoGaps(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save consecutive blocks 1-10
	for i := int64(1); i <= 10; i++ {
		saveBlock(t, ctx, repo, i)
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 0 {
		t.Errorf("expected no gaps, got %v", gaps)
	}
}

func TestFindGaps_SingleGapInMiddle(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1, 2, 3, 7, 8, 9, 10 (gap at 4-6)
	for _, num := range []int64{1, 2, 3, 7, 8, 9, 10} {
		saveBlock(t, ctx, repo, num)
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 4 || gaps[0].To != 6 {
		t.Errorf("expected gap {4, 6}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_MultipleGaps(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1, 5, 10, 15 (gaps at 2-4, 6-9, 11-14)
	for _, num := range []int64{1, 5, 10, 15} {
		saveBlock(t, ctx, repo, num)
	}

	gaps, err := repo.FindGaps(ctx, 1, 15)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 3 {
		t.Fatalf("expected 3 gaps, got %d: %v", len(gaps), gaps)
	}

	expected := []outbound.BlockRange{
		{From: 2, To: 4},
		{From: 6, To: 9},
		{From: 11, To: 14},
	}

	for i, want := range expected {
		if gaps[i].From != want.From || gaps[i].To != want.To {
			t.Errorf("gap %d: expected {%d, %d}, got {%d, %d}",
				i, want.From, want.To, gaps[i].From, gaps[i].To)
		}
	}
}

func TestFindGaps_GapAtBeginning(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 5, 6, 7, 8, 9, 10 (gap at 1-4)
	for i := int64(5); i <= 10; i++ {
		saveBlock(t, ctx, repo, i)
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 1 || gaps[0].To != 4 {
		t.Errorf("expected gap {1, 4}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_AlternatingMissing(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 2, 4, 6 (blocks 1, 3, 5 are missing)
	for _, num := range []int64{2, 4, 6} {
		saveBlock(t, ctx, repo, num)
	}

	gaps, err := repo.FindGaps(ctx, 1, 6)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 3 {
		t.Fatalf("expected 3 gaps, got %d: %v", len(gaps), gaps)
	}

	expected := []outbound.BlockRange{
		{From: 1, To: 1},
		{From: 3, To: 3},
		{From: 5, To: 5},
	}

	for i, want := range expected {
		if gaps[i].From != want.From || gaps[i].To != want.To {
			t.Errorf("gap %d: expected {%d, %d}, got {%d, %d}",
				i, want.From, want.To, gaps[i].From, gaps[i].To)
		}
	}
}

func TestFindGaps_OnlyOneBlock(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save only block 50
	saveBlock(t, ctx, repo, 50)

	gaps, err := repo.FindGaps(ctx, 1, 100)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	// Should detect gap at beginning (1-49)
	// Note: FindGaps doesn't detect gap at the end (by design)
	if len(gaps) < 1 {
		t.Fatalf("expected at least 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 1 || gaps[0].To != 49 {
		t.Errorf("expected first gap {1, 49}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_EmptyTable(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Don't save any blocks
	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	// With empty table and minBlock=1, maxBlock=10, the beginning gap check
	// should detect the entire range is missing
	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap for empty table, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 1 || gaps[0].To != 10 {
		t.Errorf("expected gap {1, 10}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_IgnoresOrphanedBlocks(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1, 2, 3, 4, 5
	for i := int64(1); i <= 5; i++ {
		saveBlock(t, ctx, repo, i)
	}

	// Mark block 3 as orphaned
	err := repo.MarkBlockOrphaned(ctx, fmt.Sprintf("0x%064x", 3))
	if err != nil {
		t.Fatalf("failed to mark block orphaned: %v", err)
	}

	gaps, err := repo.FindGaps(ctx, 1, 5)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	// Block 3 is orphaned, so it should appear as a gap
	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap (orphaned block), got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 3 || gaps[0].To != 3 {
		t.Errorf("expected gap {3, 3}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_LargeGap(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1 and 1000 (large gap in between)
	saveBlock(t, ctx, repo, 1)
	saveBlock(t, ctx, repo, 1000)

	gaps, err := repo.FindGaps(ctx, 1, 1000)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 2 || gaps[0].To != 999 {
		t.Errorf("expected gap {2, 999}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestGetMinMaxBlockNumber(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Test empty table
	min, err := repo.GetMinBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMinBlockNumber failed: %v", err)
	}
	if min != 0 {
		t.Errorf("expected min=0 for empty table, got %d", min)
	}

	max, err := repo.GetMaxBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMaxBlockNumber failed: %v", err)
	}
	if max != 0 {
		t.Errorf("expected max=0 for empty table, got %d", max)
	}

	// Add some blocks
	for _, num := range []int64{10, 50, 100} {
		saveBlock(t, ctx, repo, num)
	}

	min, err = repo.GetMinBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMinBlockNumber failed: %v", err)
	}
	if min != 10 {
		t.Errorf("expected min=10, got %d", min)
	}

	max, err = repo.GetMaxBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMaxBlockNumber failed: %v", err)
	}
	if max != 100 {
		t.Errorf("expected max=100, got %d", max)
	}
}

func TestGetMinMaxBlockNumber_IgnoresOrphaned(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1, 50, 100
	for _, num := range []int64{1, 50, 100} {
		saveBlock(t, ctx, repo, num)
	}

	// Mark block 1 and 100 as orphaned
	repo.MarkBlockOrphaned(ctx, fmt.Sprintf("0x%064x", 1))
	repo.MarkBlockOrphaned(ctx, fmt.Sprintf("0x%064x", 100))

	min, err := repo.GetMinBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMinBlockNumber failed: %v", err)
	}
	if min != 50 {
		t.Errorf("expected min=50 (ignoring orphaned), got %d", min)
	}

	max, err := repo.GetMaxBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMaxBlockNumber failed: %v", err)
	}
	if max != 50 {
		t.Errorf("expected max=50 (ignoring orphaned), got %d", max)
	}
}

func TestSaveBlock_ConcurrentVersionRaceCondition(t *testing.T) {
	// This test demonstrates the TOCTOU (Time-of-Check-Time-of-Use) race condition
	// when two goroutines try to save blocks at the same height concurrently.
	//
	// Bug scenario:
	// 1. Goroutine A calls GetBlockVersionCount(100) → gets 0
	// 2. Goroutine B calls GetBlockVersionCount(100) → gets 0 (before A saves)
	// 3. Goroutine A saves block with Version: 0
	// 4. Goroutine B saves block with Version: 0
	// 5. Now there are TWO blocks at height 100 with Version: 0!
	//
	// This breaks the version uniqueness assumption and causes cache key collisions.

	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	const blockNum int64 = 100
	const numGoroutines = 10

	// Use a channel to synchronize the start of all goroutines
	startCh := make(chan struct{})
	doneCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			// Wait for the start signal
			<-startCh

			// Simulate the pattern used in live_data_service and backfill_gaps_service:
			// 1. Get version count
			// 2. Save block - version is now assigned atomically by SaveBlock
			_, err := repo.SaveBlock(ctx, outbound.BlockState{
				Number:     blockNum,
				Hash:       fmt.Sprintf("0x%064x_%d", blockNum, id),
				ParentHash: fmt.Sprintf("0x%064x", blockNum-1),
				ReceivedAt: time.Now().Unix(),
				IsOrphaned: false,
			})
			if err != nil {
				doneCh <- fmt.Errorf("goroutine %d: SaveBlock failed: %w", id, err)
				return
			}
			doneCh <- nil
		}(i)
	}

	// Start all goroutines at once to maximize race condition likelihood
	close(startCh)

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		if err := <-doneCh; err != nil {
			t.Logf("Error: %v", err)
		}
	}

	// Query all blocks at this height to check for duplicate versions
	finalCount, err := repo.GetBlockVersionCount(ctx, blockNum)
	if err != nil {
		t.Fatalf("failed to get final version count: %v", err)
	}

	// If there's no race condition, we should have exactly numGoroutines blocks
	// with versions 0, 1, 2, ..., numGoroutines-1
	if finalCount != numGoroutines {
		t.Errorf("expected %d blocks saved, but GetBlockVersionCount returns %d (some blocks may have duplicate versions)", numGoroutines, finalCount)
	}

	// The real test: check if all versions are unique
	// We need to query the database directly
	type blockVersion struct {
		hash    string
		version int
	}
	var blocks []blockVersion

	// Use a raw query to get all blocks at this height
	query := `SELECT hash, version FROM block_states WHERE number = $1 ORDER BY version`
	rowsResult, err := repo.DB().QueryContext(ctx, query, blockNum)
	if err != nil {
		t.Fatalf("failed to query blocks: %v", err)
	}
	defer rowsResult.Close()

	for rowsResult.Next() {
		var b blockVersion
		if err := rowsResult.Scan(&b.hash, &b.version); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		blocks = append(blocks, b)
	}

	t.Logf("Saved %d blocks at height %d", len(blocks), blockNum)

	// Check for duplicate versions
	versionCounts := make(map[int]int)
	for _, b := range blocks {
		versionCounts[b.version]++
		t.Logf("  Block %s: version=%d", b.hash[:20], b.version)
	}

	for version, count := range versionCounts {
		if count > 1 {
			t.Errorf("RACE CONDITION DETECTED: version %d appears %d times (should be unique)", version, count)
		}
	}

	// Also verify versions are sequential from 0 to len(blocks)-1
	for i := 0; i < len(blocks); i++ {
		if versionCounts[i] != 1 {
			t.Errorf("expected exactly one block with version %d, got %d", i, versionCounts[i])
		}
	}
}

func TestVerifyChainIntegrity_ValidChain(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	// Save a contiguous chain with correct parent_hash links
	for i := int64(1); i <= 10; i++ {
		saveBlock(t, ctx, repo, i)
	}

	// Verify chain integrity - should pass
	err := repo.VerifyChainIntegrity(ctx, 1, 10)
	if err != nil {
		t.Errorf("expected valid chain, got error: %v", err)
	}
}

func TestVerifyChainIntegrity_BrokenChain(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	// Save blocks 1-5 with correct links
	for i := int64(1); i <= 5; i++ {
		saveBlock(t, ctx, repo, i)
	}

	// Save block 6 with WRONG parent_hash (points to block 3 instead of 5)
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:     6,
		Hash:       fmt.Sprintf("0x%064x", 6),
		ParentHash: fmt.Sprintf("0x%064x", 3), // Wrong! Should be 5
		ReceivedAt: time.Now().Unix(),
		IsOrphaned: false,
	})
	if err != nil {
		t.Fatalf("failed to save block 6: %v", err)
	}

	// Verify chain integrity - should fail at block 6
	err = repo.VerifyChainIntegrity(ctx, 1, 6)
	if err == nil {
		t.Error("expected chain integrity error, got nil")
	} else {
		t.Logf("correctly detected chain integrity violation: %v", err)
		// Verify the error mentions block 6
		if !strings.Contains(err.Error(), "block 6") {
			t.Errorf("error should mention block 6, got: %v", err)
		}
	}
}

func TestVerifyChainIntegrity_EmptyRange(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	// Empty range should return nil (nothing to verify)
	err := repo.VerifyChainIntegrity(ctx, 10, 5)
	if err != nil {
		t.Errorf("expected nil for empty range, got: %v", err)
	}

	// Same block should return nil
	err = repo.VerifyChainIntegrity(ctx, 5, 5)
	if err != nil {
		t.Errorf("expected nil for same block range, got: %v", err)
	}
}

func TestVerifyChainIntegrity_WithGaps(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	// Save blocks with a gap (1, 2, 3, 5, 6) - missing block 4
	for i := int64(1); i <= 3; i++ {
		saveBlock(t, ctx, repo, i)
	}
	// Block 5's parent_hash points to 4 (which doesn't exist in DB, but that's okay)
	saveBlock(t, ctx, repo, 5)
	saveBlock(t, ctx, repo, 6)

	// This should pass because we only check consecutive blocks that exist
	// Block 5 and 6 are consecutive and properly linked
	err := repo.VerifyChainIntegrity(ctx, 1, 6)
	if err != nil {
		t.Errorf("expected valid chain with gaps (only consecutive blocks checked), got: %v", err)
	}
}

// RaceConditionRepo wraps a real repository to inject side effects for testing race conditions.
type RaceConditionRepo struct {
	outbound.BlockStateRepository
	onGetBlockByNumber func(int64)
}

func (r *RaceConditionRepo) GetBlockByNumber(ctx context.Context, number int64) (*outbound.BlockState, error) {
	// Call the underlying repo first
	block, err := r.BlockStateRepository.GetBlockByNumber(ctx, number)

	// Trigger the hook if defined
	if r.onGetBlockByNumber != nil {
		r.onGetBlockByNumber(number)
	}

	return block, err
}

// Mock EventSink for integration tests that don't need a real one
type mockEventSink struct{}

func (m *mockEventSink) Publish(ctx context.Context, event outbound.Event) error { return nil }
func (m *mockEventSink) Close() error                                            { return nil }

func TestIntegration_ProcessBlockData_LinkageRaceCondition(t *testing.T) {
	// 1. Setup
	// Use the real Postgres repository to confirm the race condition affects the production implementation.
	pgRepo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	// Wrap the real Postgres repo to inject the race condition
	raceRepo := &RaceConditionRepo{BlockStateRepository: pgRepo}

	// We keep mocks for Client/Cache/Sink as they are external dependencies
	// not involved in the DB consistency logic being tested.
	mockClient := newMockClient()
	mockCache := memory.NewBlockCache()
	mockSink := &mockEventSink{}

	// Create service
	svc, err := NewBackfillService(BackfillConfigDefaults(), mockClient, raceRepo, mockCache, mockSink)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	ctx := context.Background()

	// 2. Initial State: Block 99 exists with Hash A
	block99 := outbound.BlockState{
		Number:  99,
		Hash:    "0xAAAAAAAAAAAAAAAA",
		Version: 0,
	}
	if _, err := pgRepo.SaveBlock(ctx, block99); err != nil {
		t.Fatalf("failed to save initial block: %v", err)
	}

	// 3. Prepare Block 100 on Client (Parent = Hash A)
	block100Data := outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64","hash":"0xBBBBBBBBBBBBBBBB","parentHash":"0xAAAAAAAAAAAAAAAA","timestamp":"0x123456"}`),
	}

	// 4. Inject Race Condition
	// When validation checks Block 99, it sees Hash A.
	// Immediately after check, we simulate a reorg of Block 99 to Hash C.
	raceRepo.onGetBlockByNumber = func(num int64) {
		if num == 99 {
			// Simulate concurrent reorg: Replace Block 99 with Hash C
			// This mimics another service (LiveData) modifying the DB between our read and our write.

			// Force update Block 99 to "0xCCCCCCCCCCCCCCCC" (Hash C)
			pgRepo.MarkBlockOrphaned(ctx, "0xAAAAAAAAAAAAAAAA") // Orphan A
			pgRepo.SaveBlock(ctx, outbound.BlockState{          // Save C
				Number:     99,
				Hash:       "0xCCCCCCCCCCCCCCCC",
				ParentHash: "0xOLD_PARENT",
			})
		}
	}

	// 5. Execute processBlockData for Block 100
	err = svc.processBlockData(ctx, block100Data)

	// 6. Assertions

	// With the FIX, this SHOULD return an error (post-save validation failure)
	if err == nil {
		t.Errorf("ProcessBlockData should have returned a post-save validation error")
	} else {
		t.Logf("ProcessBlockData correctly returned error: %v", err)
	}

	// Verify DB state
	// saved100 should be NIL (canonical query) because it was orphaned by the fix
	saved100, _ := pgRepo.GetBlockByNumber(ctx, 100)
	current99, _ := pgRepo.GetBlockByNumber(ctx, 99)

	if saved100 != nil {
		t.Errorf("Block 100 should NOT be canonical (should be orphaned), but got: %+v", saved100)
	} else {
		t.Logf("Block 100 was correctly orphaned/removed from canonical chain")
	}

	if current99.Hash != "0xCCCCCCCCCCCCCCCC" {
		t.Fatalf("Block 99 should have been reorged to Hash C")
	}

	// Double check it exists as an orphan
	orphaned100Val, _ := pgRepo.GetBlockByHash(ctx, "0xBBBBBBBBBBBBBBBB")
	if orphaned100Val != nil && !orphaned100Val.IsOrphaned {
		t.Errorf("Block 100 should reside in DB as orphaned")
	}
}
