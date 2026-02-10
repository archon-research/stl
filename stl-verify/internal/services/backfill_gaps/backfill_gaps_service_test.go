package backfill_gaps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func newMockClient() *testutil.MockBlockchainClient {
	client := testutil.NewMockBlockchainClient()
	client.ReturnNilForUnknownHash = true
	return client
}

func TestBackfillService_FillsGaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate client with blocks 1-100
	for i := int64(1); i <= 100; i++ {
		client.AddBlock(i, "")
	}

	// Seed the state repo with blocks 1, 50, and 100 (leaving gaps 2-49 and 51-99)
	for _, num := range []int64{1, 50, 100} {
		if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     num,
			Hash:       client.GetHeader(num).Hash,
			ParentHash: client.GetHeader(num).ParentHash,
			ReceivedAt: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("failed to save block %d: %v", num, err)
		}
	}

	config := BackfillConfig{
		ChainID:   1,
		BatchSize: 10,
		Logger:    slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Run a single backfill pass
	if err := service.RunOnce(ctx); err != nil {
		t.Fatalf("backfill failed: %v", err)
	}

	// Verify all blocks are now in the state repo
	t.Run("all gaps filled", func(t *testing.T) {
		for i := int64(1); i <= 100; i++ {
			block, err := stateRepo.GetBlockByNumber(ctx, i)
			if err != nil {
				t.Errorf("error getting block %d: %v", i, err)
				continue
			}
			if block == nil {
				t.Errorf("block %d missing after backfill", i)
			}
		}
	})

	t.Run("events published for backfilled blocks", func(t *testing.T) {
		blockEvents := eventSink.GetEventsByType(outbound.EventTypeBlock)
		// Should have 100 events:
		// - 97 from gap fill (blocks 2-49 = 48, blocks 51-99 = 49)
		// - 3 from retry mechanism (seed blocks 1, 50, 100 that were saved but not published)
		expectedEvents := 100
		if len(blockEvents) != expectedEvents {
			t.Errorf("expected %d block events, got %d", expectedEvents, len(blockEvents))
		}

		// All should be marked as backfill
		for _, e := range blockEvents {
			be := e.(outbound.BlockEvent)
			if !be.IsBackfill {
				t.Errorf("block %d event not marked as backfill", be.BlockNumber)
			}
		}
	})
}

func TestBackfillService_VersionIsSavedToDatabase(t *testing.T) {
	// This test verifies that when backfill saves a block, the version field is
	// correctly persisted to the database.
	//
	// Bug scenario:
	// 1. Backfill processes block 5 and saves it to DB
	// 2. The Version field should be set based on GetBlockVersionCount BEFORE SaveBlock
	// 3. If Version is not set, it defaults to 0
	// 4. The published event should have the same version as what's in the DB

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate client with blocks 1-10
	for i := int64(1); i <= 10; i++ {
		client.AddBlock(i, "")
	}

	// Seed blocks 1 and 10 to create a gap at 2-9
	for _, num := range []int64{1, 10} {
		if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     num,
			Hash:       client.GetHeader(num).Hash,
			ParentHash: client.GetHeader(num).ParentHash,
			ReceivedAt: time.Now().Unix(),
			Version:    0,
		}); err != nil {
			t.Fatalf("failed to save block %d: %v", num, err)
		}
	}

	config := BackfillConfig{
		ChainID:   1,
		BatchSize: 10,
		Logger:    slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Run a single backfill pass
	if err := service.RunOnce(ctx); err != nil {
		t.Fatalf("backfill failed: %v", err)
	}

	// Check that backfilled block 5 was saved with version 0 (first block at that height)
	backfilledBlock, err := stateRepo.GetBlockByHash(ctx, client.GetHeader(5).Hash)
	if err != nil {
		t.Fatalf("failed to get backfilled block: %v", err)
	}
	if backfilledBlock == nil {
		t.Fatalf("backfilled block at height 5 not found")
	}
	if backfilledBlock.Version != 0 {
		t.Errorf("expected backfilled block to have version 0, got %d", backfilledBlock.Version)
	}

	// Check that the published event has version 0 (matching the DB)
	blockEvents := eventSink.GetBlockEvents()
	var block5Event *outbound.BlockEvent
	for _, e := range blockEvents {
		if e.BlockNumber == 5 {
			block5Event = &e
			break
		}
	}
	if block5Event == nil {
		t.Fatalf("no event found for block 5")
	}

	// BUG: The event version should match the saved block version
	// If GetBlockVersionCount is called AFTER SaveBlock, the event will have version 1
	// but the DB will have version 0 (because Version wasn't set in BlockState)
	if block5Event.Version != backfilledBlock.Version {
		t.Errorf("event version (%d) doesn't match saved block version (%d) - bug: version mismatch between DB and published event",
			block5Event.Version, backfilledBlock.Version)
	}
}

func TestVerifyChainIntegrity_MemoryAdapter_ValidChain(t *testing.T) {
	ctx := context.Background()
	stateRepo := memory.NewBlockStateRepository()

	// Save a contiguous chain with correct parent_hash links
	for i := int64(1); i <= 10; i++ {
		_, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Verify chain integrity - should pass
	err := stateRepo.VerifyChainIntegrity(ctx, 1, 10)
	if err != nil {
		t.Errorf("expected valid chain, got error: %v", err)
	}
}

func TestVerifyChainIntegrity_MemoryAdapter_BrokenChain(t *testing.T) {
	ctx := context.Background()
	stateRepo := memory.NewBlockStateRepository()

	// Save blocks 1-5 with correct links
	for i := int64(1); i <= 5; i++ {
		_, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Save block 6 with WRONG parent_hash (points to block 3 instead of 5)
	_, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     6,
		Hash:       fmt.Sprintf("0x%064x", 6),
		ParentHash: fmt.Sprintf("0x%064x", 3), // Wrong! Should be 5
	})
	if err != nil {
		t.Fatalf("failed to save block 6: %v", err)
	}

	// Verify chain integrity - should fail at block 6
	err = stateRepo.VerifyChainIntegrity(ctx, 1, 6)
	if err == nil {
		t.Error("expected chain integrity error, got nil")
	} else {
		t.Logf("correctly detected chain integrity violation: %v", err)
	}
}

func TestAdvanceWatermark_RefusesOnBrokenChain(t *testing.T) {
	ctx := context.Background()

	// Setup
	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add blocks 1-10 to the mock client with correct links
	for i := int64(1); i <= 10; i++ {
		client.AddBlock(i, "")
	}

	// Manually save blocks with a broken chain:
	// Blocks 1-5 correct, block 6 has wrong parent
	for i := int64(1); i <= 5; i++ {
		_, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Block 6 with wrong parent_hash
	_, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     6,
		Hash:       fmt.Sprintf("0x%064x", 6),
		ParentHash: fmt.Sprintf("0x%064x", 3), // Wrong! Points to block 3
	})
	if err != nil {
		t.Fatalf("failed to save block 6: %v", err)
	}

	// Blocks 7-10 continue (even though 6 is broken)
	for i := int64(7); i <= 10; i++ {
		_, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	config := BackfillConfig{
		ChainID:   1,
		BatchSize: 10,
		Logger:    slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Try to advance watermark - should fail due to broken chain
	err = service.advanceWatermark(ctx)
	if err == nil {
		t.Error("expected advanceWatermark to fail due to broken chain, got nil")
	} else {
		t.Logf("correctly refused to advance watermark: %v", err)
	}

	// Verify watermark was NOT advanced
	watermark, err := stateRepo.GetBackfillWatermark(ctx)
	if err != nil {
		t.Fatalf("failed to get watermark: %v", err)
	}
	if watermark != 0 {
		t.Errorf("expected watermark to remain at 0, got %d", watermark)
	}
}

// TestBackfillService_ReorgDuringDowntime_CannotMakeProgress verifies that when
// the DB boundary is on a stale chain and boundary checking is disabled, the
// service cannot fill a gap that crosses that boundary.
// We should not disable boundary checking in production, but this test ensures that
// the service behaves predictably
func TestBackfillService_ReorgDuringDowntime_CannotMakeProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	client := newMockClient()

	// Arrange: DB contains an "old" chain up to 110, but has a gap at 106-107.
	// RPC returns a "new" chain for the missing blocks, which does not link to the
	// DB boundary.
	for i := int64(100); i <= 110; i++ {
		if i == 106 || i == 107 {
			continue
		}
		parentHash := hashWithSuffix(i-1, "_old")
		saveBlockState(t, ctx, stateRepo, i, hashWithSuffix(i, "_old"), parentHash)
	}

	// RPC returns the missing blocks on a different chain segment (i.e., the DB boundary at 105 is stale).
	// We only stub 106-107 because those are the missing heights; the key is that 106's parent does not match
	// the hash of DB block 105.
	client.SetBlockHeader(106, hashWithSuffix(106, "_new"), hashWithSuffix(105, "_new")) // Note: 106_new parent is not 105_old
	client.SetBlockHeader(107, hashWithSuffix(107, "_new"), hashWithSuffix(106, "_new"))

	config := BackfillConfig{
		ChainID:            1,
		BatchSize:          10,
		BoundaryCheckDepth: -1, // Disabled: the service will not orphan stale boundary blocks.
		Logger:             slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Act: attempt to backfill the gap.
	if err := service.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	// Assert: the gap remains and no block events were published for those heights.
	assertBlocksMissing(t, ctx, stateRepo, 106, 107)

	blockEvents := eventSink.GetEventsByType(outbound.EventTypeBlock)
	for _, e := range blockEvents {
		be := e.(outbound.BlockEvent)
		if be.BlockNumber == 106 || be.BlockNumber == 107 {
			t.Fatalf("unexpected block event for %d", be.BlockNumber)
		}
	}

	// A second run should remain unable to make progress.
	if err := service.RunOnce(ctx); err != nil {
		t.Fatalf("second RunOnce failed: %v", err)
	}
	assertBlocksMissing(t, ctx, stateRepo, 106, 107)
}

// TestBackfillService_ReorgDuringDowntime_RecoveryWithBoundaryCheck verifies that
// boundary verification detects stale blocks and triggers reorg recovery.
func TestBackfillService_ReorgDuringDowntime_RecoveryWithBoundaryCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	client := newMockClient()

	// Arrange: DB has 100-105 on an old chain.
	for i := int64(100); i <= 105; i++ {
		parentHash := hashWithSuffix(i-1, "_old")
		saveBlockState(t, ctx, stateRepo, i, hashWithSuffix(i, "_old"), parentHash)
	}

	// RPC: 100-102 unchanged
	for i := int64(100); i <= 102; i++ {
		parentHash := hashWithSuffix(i-1, "_old")
		client.SetBlockHeader(i, hashWithSuffix(i, "_old"), parentHash)
	}

	// RPC: 103-105 are on a new chain (different hashes and parent links)
	for i := int64(103); i <= 105; i++ {
		newParentHash := hashWithSuffix(i-1, "_new")

		// Special case: 103's parent should point to 102_old to link to the existing chain
		if i == 103 {
			newParentHash = hashWithSuffix(102, "_old")
		}

		client.SetBlockHeader(i, hashWithSuffix(i, "_new"), newParentHash)
	}

	config := BackfillConfig{
		ChainID:            1,
		BatchSize:          10,
		BoundaryCheckDepth: 10,
		Logger:             slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Act
	err = service.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	// Assert: blocks 103-105 have the new hashes.
	for i := int64(103); i <= 105; i++ {
		expectedHash := hashWithSuffix(i, "_new")
		block, err := stateRepo.GetBlockByNumber(ctx, i)
		if err != nil {
			t.Fatalf("failed to get block %d: %v", i, err)
		}
		if block == nil {
			t.Errorf("block %d is missing after recovery", i)
		} else if block.Hash != expectedHash {
			t.Errorf("block %d has wrong hash after recovery: got %s, want %s", i, truncateHashForTest(block.Hash), truncateHashForTest(expectedHash))
		}
	}

	// Old hashes are marked orphaned.
	for i := int64(103); i <= 105; i++ {
		oldHash := hashWithSuffix(i, "_old")
		orphanedBlock, err := stateRepo.GetBlockByHash(ctx, oldHash)
		if err != nil {
			t.Fatalf("failed to look up old block %d: %v", i, err)
		}
		if orphanedBlock != nil && !orphanedBlock.IsOrphaned {
			t.Errorf("Old block %d should be marked as orphaned but isn't", i)
		}
	}
}

// truncateHashForTest is a helper for test output
func truncateHashForTest(hash string) string {
	if len(hash) > 30 {
		return hash[:15] + "..." + hash[len(hash)-10:]
	}
	return hash
}

// TestBackfillService_RejectsNonCanonicalBlock verifies that backfill rejects
// blocks that don't match the expected parent hash chain.
//
// Scenario:
//  1. Live service has already processed block 10 with hash A10 (parent_hash = A9)
//  2. Backfill tries to fill block 9 (the gap)
//  3. But due to a reorg in the RPC node, the RPC returns block 9 with hash B9
//     (a different chain where B9 -> B10 instead of A9 -> A10)
//  4. Backfill should detect this mismatch and NOT save/cache/publish the wrong block
//
// Why this matters:
// - If we save B9 and publish it, consumers will have inconsistent data
// - Block 10 references A9 as parent, but we cached data for B9
// - The chain integrity check happens AFTER caching, so damage is done
func TestBackfillService_RejectsNonCanonicalBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Create a client that returns a "reorged" version of block 9
	client := newMockClient()

	// Add blocks 1-8 with normal chain (these match what we'll put in DB)
	for i := int64(1); i <= 8; i++ {
		client.AddBlock(i, "")
	}

	// Add block 10 - this is what the live service already processed
	// Its parent_hash points to the "original" block 9's hash
	originalBlock9Hash := fmt.Sprintf("0x%064x", 9) // The hash that block 10 expects as parent

	// But the client returns a DIFFERENT block 9 (simulating reorg in RPC)
	// This block 9 has a different hash than what block 10 expects as parent
	reorgedBlock9Hash := "0xREORGED_BLOCK_9_HASH_DOES_NOT_MATCH_BLOCK_10_PARENT"
	client.SetBlockHeader(9, reorgedBlock9Hash, fmt.Sprintf("0x%064x", 8))

	// Add block 10 to client for completeness
	client.AddBlock(10, originalBlock9Hash)

	// Pre-populate state repo with blocks 1-8 and 10 (leaving gap at 9)
	for i := int64(1); i <= 8; i++ {
		if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       client.GetHeader(i).Hash,
			ParentHash: client.GetHeader(i).ParentHash,
			ReceivedAt: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Save block 10 with parent_hash pointing to ORIGINAL block 9 hash
	if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     10,
		Hash:       client.GetHeader(10).Hash,
		ParentHash: originalBlock9Hash, // This is A9, but client returns B9
		ReceivedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("failed to save block 10: %v", err)
	}

	config := BackfillConfig{
		ChainID:   1,
		BatchSize: 10,
		Logger:    slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Run backfill - it should try to fill block 9
	err = service.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	// The backfill should either:
	// 1. Reject block 9 because its hash doesn't match block 10's parent_hash, OR
	// 2. Log a warning and skip the block

	// Check that block 9 was NOT saved with the wrong hash
	block9, err := stateRepo.GetBlockByNumber(ctx, 9)
	if err != nil {
		t.Fatalf("failed to get block 9: %v", err)
	}
	if block9 != nil && block9.Hash == reorgedBlock9Hash {
		t.Error("BUG: Backfill saved block 9 with wrong hash that doesn't link to block 10")
		t.Errorf("Block 9 hash: %s", block9.Hash)
		t.Errorf("Block 10 parent_hash: %s", originalBlock9Hash)
		t.Error("This means we cached and published data for the wrong block!")
	}

	// Check that no events were published for block 9
	blockEvents := eventSink.GetEventsByType(outbound.EventTypeBlock)
	for _, e := range blockEvents {
		be := e.(outbound.BlockEvent)
		if be.BlockNumber == 9 {
			t.Error("BUG: Published event for block 9 with wrong hash")
		}
	}
}

// ---
// Utilities for tests
// ---

func hashWithSuffix(num int64, suffix string) string {
	return fmt.Sprintf("0x%064d%s", num, suffix)
}

func saveBlockState(t *testing.T, ctx context.Context, repo outbound.BlockStateRepository, num int64, hash, parentHash string) {
	t.Helper()

	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:     num,
		Hash:       hash,
		ParentHash: parentHash,
		ReceivedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("failed to save block %d: %v", num, err)
	}
}

// TestBackfillService_HighestVersionIsCanonicalAfterRecovery verifies that after
// recovering from stale blocks, the highest version at each block height is
// always the canonical (non-orphaned) block.
//
// This is critical for consumers who rely on: highest version = correct block.
func TestBackfillService_HighestVersionIsCanonicalAfterRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	client := newMockClient()

	// Arrange: DB has blocks 100-105 on an old chain (version 0 for each)
	for i := int64(100); i <= 105; i++ {
		parentHash := hashWithSuffix(i-1, "_old")
		saveBlockState(t, ctx, stateRepo, i, hashWithSuffix(i, "_old"), parentHash)
	}

	// RPC: 100-102 unchanged (same as DB)
	for i := int64(100); i <= 102; i++ {
		parentHash := hashWithSuffix(i-1, "_old")
		client.SetBlockHeader(i, hashWithSuffix(i, "_old"), parentHash)
	}

	// RPC: 103-105 are on a new chain (different hashes - simulating reorg during downtime)
	for i := int64(103); i <= 105; i++ {
		newParentHash := hashWithSuffix(i-1, "_new")
		if i == 103 {
			newParentHash = hashWithSuffix(102, "_old") // Links to existing chain
		}
		client.SetBlockHeader(i, hashWithSuffix(i, "_new"), newParentHash)
	}

	config := BackfillConfig{
		ChainID:            1,
		BatchSize:          10,
		BoundaryCheckDepth: 10,
		Logger:             slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Act
	err = service.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	// Assert: For blocks 103-105, verify highest version is the canonical block
	for i := int64(103); i <= 105; i++ {
		// Get all versions at this block number
		maxVersion, err := stateRepo.GetBlockVersionCount(ctx, i)
		if err != nil {
			t.Fatalf("failed to get version count for block %d: %v", i, err)
		}

		// Should have 2 versions: 0 (orphaned) and 1 (canonical)
		if maxVersion != 2 {
			t.Errorf("block %d: expected 2 versions, got %d", i, maxVersion)
		}

		// Get the canonical block (non-orphaned)
		canonicalBlock, err := stateRepo.GetBlockByNumber(ctx, i)
		if err != nil {
			t.Fatalf("failed to get canonical block %d: %v", i, err)
		}
		if canonicalBlock == nil {
			t.Fatalf("block %d: no canonical block found", i)
		}

		// Verify canonical block has the highest version
		if canonicalBlock.Version != maxVersion-1 {
			t.Errorf("block %d: canonical block has version %d, but highest version is %d; highest version should be canonical",
				i, canonicalBlock.Version, maxVersion-1)
		}

		// Verify canonical block has the new hash
		expectedHash := hashWithSuffix(i, "_new")
		if canonicalBlock.Hash != expectedHash {
			t.Errorf("block %d: canonical block has wrong hash: got %s, want %s",
				i, truncateHashForTest(canonicalBlock.Hash), truncateHashForTest(expectedHash))
		}

		// Verify old block (version 0) is orphaned
		oldHash := hashWithSuffix(i, "_old")
		oldBlock, err := stateRepo.GetBlockByHash(ctx, oldHash)
		if err != nil {
			t.Fatalf("failed to get old block %d: %v", i, err)
		}
		if oldBlock == nil {
			t.Errorf("block %d: old block not found", i)
		} else {
			if !oldBlock.IsOrphaned {
				t.Errorf("block %d: old block (version %d) should be orphaned", i, oldBlock.Version)
			}
			if oldBlock.Version != 0 {
				t.Errorf("block %d: old block should have version 0, got %d", i, oldBlock.Version)
			}
		}
	}
}

// TestBackfillService_SkipsStaleBlockWhenLiveAlreadyProcessed verifies that backfill
// won't save a stale block if live service has already saved a different (canonical)
// block at the same height.
//
// Race condition scenario:
// 1. Backfill fetches block 100 (hash A) at time T1
// 2. Reorg happens - block 100 is now hash B
// 3. Live service processes block 100 (hash B) → saved with version 0
// 4. Backfill tries to save block 100 (hash A)
// 5. Without the fix, backfill would save hash A with version 1, breaking the invariant
// 6. With the fix, backfill detects a different block exists and skips
func TestBackfillService_SkipsStaleBlockWhenLiveAlreadyProcessed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	client := newMockClient()

	// Setup: blocks 1-99 exist in DB and client
	for i := int64(1); i <= 99; i++ {
		client.AddBlock(i, "")
		saveBlockState(t, ctx, stateRepo, i, fmt.Sprintf("0x%064x", i), fmt.Sprintf("0x%064x", i-1))
	}

	// Block 100 - Live service has already processed the CANONICAL block (hash B)
	canonicalHash := "0xCANONICAL_HASH_B_SAVED_BY_LIVE_SERVICE"
	saveBlockState(t, ctx, stateRepo, 100, canonicalHash, fmt.Sprintf("0x%064x", 99))

	// But the RPC returns a STALE block (hash A) - simulating a slow/stale RPC node
	staleHash := "0xSTALE_HASH_A_FROM_SLOW_RPC_NODE"
	client.SetBlockHeader(100, staleHash, fmt.Sprintf("0x%064x", 99))

	config := BackfillConfig{
		ChainID:            1,
		BatchSize:          10,
		BoundaryCheckDepth: 0, // Disable boundary check for this test
		Logger:             slog.Default(),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Simulate backfill trying to process the stale block
	// This would happen if backfill fetched block 100 before live service saved it,
	// but processes it after live service has already saved the canonical block.
	staleBlockData := outbound.BlockData{
		BlockNumber: 100,
		Block:       []byte(fmt.Sprintf(`{"number":"0x64","hash":"%s","parentHash":"0x%064x","timestamp":"0x0"}`, staleHash, 99)),
	}

	// This should skip the block, not save it
	err = service.processBlockData(ctx, staleBlockData)
	if err != nil {
		t.Fatalf("processBlockData should not error when skipping stale block: %v", err)
	}

	// Verify only ONE block exists at height 100 (the canonical one)
	versionCount, err := stateRepo.GetBlockVersionCount(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get version count: %v", err)
	}
	if versionCount != 1 {
		t.Errorf("expected 1 version at block 100, got %d (stale block was incorrectly saved)", versionCount)
	}

	// Verify the block at height 100 is still the canonical one
	block, err := stateRepo.GetBlockByNumber(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get block: %v", err)
	}
	if block.Hash != canonicalHash {
		t.Errorf("block 100 has wrong hash: got %s, want %s", block.Hash, canonicalHash)
	}

	// Verify the stale block was NOT saved
	staleBlock, err := stateRepo.GetBlockByHash(ctx, staleHash)
	if err != nil {
		t.Fatalf("failed to check for stale block: %v", err)
	}
	if staleBlock != nil {
		t.Errorf("stale block should NOT have been saved, but it was (version=%d)", staleBlock.Version)
	}
}

func assertBlocksMissing(t *testing.T, ctx context.Context, repo outbound.BlockStateRepository, from, to int64) {
	t.Helper()

	for i := from; i <= to; i++ {
		block, err := repo.GetBlockByNumber(ctx, i)
		if err != nil {
			t.Fatalf("failed to get block %d: %v", i, err)
		}
		if block != nil {
			t.Fatalf("expected block %d to be missing, got hash=%s", i, truncateHashForTest(block.Hash))
		}
	}
}

// =============================================================================
// Failing Adapters for Testing Error Handling
// =============================================================================

// failingCache wraps a real cache but fails on demand.
type failingCache struct {
	*memory.BlockCache
	mu               sync.Mutex
	failOnBlock      int64  // Block number to fail on (0 = don't fail)
	failOnOperation  string // "block", "receipts", "traces", "blobs", or "any"
	failCount        int    // How many times to fail (0 = forever)
	currentFailCount int
}

func newFailingCache() *failingCache {
	return &failingCache{
		BlockCache: memory.NewBlockCache(),
	}
}

func (c *failingCache) shouldFail(blockNum int64, operation string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failOnBlock == 0 || c.failOnBlock != blockNum {
		return false
	}
	if c.failOnOperation != "any" && c.failOnOperation != operation {
		return false
	}
	if c.failCount > 0 && c.currentFailCount >= c.failCount {
		return false
	}
	c.currentFailCount++
	return true
}

func (c *failingCache) SetBlock(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	if c.shouldFail(blockNumber, "block") {
		return errors.New("simulated cache failure on SetBlock")
	}
	return c.BlockCache.SetBlock(ctx, chainID, blockNumber, version, data)
}

func (c *failingCache) SetReceipts(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	if c.shouldFail(blockNumber, "receipts") {
		return errors.New("simulated cache failure on SetReceipts")
	}
	return c.BlockCache.SetReceipts(ctx, chainID, blockNumber, version, data)
}

func (c *failingCache) SetTraces(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	if c.shouldFail(blockNumber, "traces") {
		return errors.New("simulated cache failure on SetTraces")
	}
	return c.BlockCache.SetTraces(ctx, chainID, blockNumber, version, data)
}

func (c *failingCache) SetBlobs(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	if c.shouldFail(blockNumber, "blobs") {
		return errors.New("simulated cache failure on SetBlobs")
	}
	return c.BlockCache.SetBlobs(ctx, chainID, blockNumber, version, data)
}

func (c *failingCache) SetBlockData(ctx context.Context, chainID int64, blockNumber int64, version int, data outbound.BlockDataInput) error {
	if c.shouldFail(blockNumber, "any") || c.shouldFail(blockNumber, "block") {
		return errors.New("simulated cache failure on SetBlockData")
	}
	return c.BlockCache.SetBlockData(ctx, chainID, blockNumber, version, data)
}

// failingEventSink wraps a real event sink but fails on demand.
// When no failures are configured (failOnBlock=0), it acts as a no-op sink.
type failingEventSink struct {
	*memory.EventSink
	mu               sync.Mutex
	failOnBlock      int64
	failCount        int
	currentFailCount int
}

func newFailingEventSink() *failingEventSink {
	return &failingEventSink{
		EventSink: memory.NewEventSink(),
	}
}

// newNoOpEventSink creates an event sink that silently accepts all events.
func newNoOpEventSink() *failingEventSink {
	return &failingEventSink{
		EventSink: memory.NewEventSink(),
	}
}

func (s *failingEventSink) Publish(ctx context.Context, event outbound.Event) error {
	s.mu.Lock()
	shouldFail := s.failOnBlock != 0 && event.GetBlockNumber() == s.failOnBlock
	if shouldFail && s.failCount > 0 && s.currentFailCount >= s.failCount {
		shouldFail = false
	}
	if shouldFail {
		s.currentFailCount++
	}
	s.mu.Unlock()

	if shouldFail {
		return errors.New("simulated publish failure")
	}
	return s.EventSink.Publish(ctx, event)
}

func TestVerifyBoundaryBlocks_DetectsUncleReorg(t *testing.T) {
	// 1. Setup
	repo := memory.NewBlockStateRepository()
	client := newMockClient()
	cache := memory.NewBlockCache()
	sink := newNoOpEventSink()

	cfg := BackfillConfigDefaults()
	cfg.ChainID = 1
	cfg.BoundaryCheckDepth = 5

	svc, err := NewBackfillService(cfg, client, repo, cache, sink)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	ctx := context.Background()

	// 2. Initial State: Repo has Block 100 (Hash A)
	block100 := outbound.BlockState{
		Number:     100,
		Hash:       "0xHASH_A",
		ParentHash: "0xHASH_99",
		Version:    0,
	}
	if _, err := repo.SaveBlock(ctx, block100); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	// 3. Client State:
	// Canonical Block 100 is Hash B
	client.AddBlock(100, "0xHASH_99") // This sets Hash to auto-generated "0x...064"
	// Let's force it to Hash B for clarity, but addBlock auto-generates hash.
	// client.blocks[100].header.Hash = "0xHASH_B"
	// But  generates "0x000...64" for 100.
	// Let's rely on addBlock behavior.
	// addBlock(100) -> Hash "0x00...0064" (Canonical)

	// But we need the Repo to hold a DIFFERENT hash.
	// "0xHASH_A" != "0x00...0064". So we are good there.

	// CRITICAL: We also add "0xHASH_A" as an UNCLE to the client.
	// This simulates the node still having the old block data available by hash
	// even though it's not in the canonical chain.
	client.AddUncle(100, "0xHASH_A", "0xHASH_99")

	// 4. Verify Boundary Blocks
	// Current Implementation: Calls GetBlockByHash("0xHASH_A").
	// Mock returns the uncle block (NOT nil).
	// Verify says "Exists -> Valid".
	// Test should FAIL if we expect reorg detection.

	staleBlocks, err := svc.verifyBoundaryBlocks(ctx)
	if err != nil {
		t.Fatalf("verifyBoundaryBlocks failed: %v", err)
	}

	// 5. Assertions
	// We expect Hash A to be detected as stale.
	if len(staleBlocks) == 0 {
		t.Errorf("FAIL: Failed to detect stale block 100 (Hash A) because it exists as an uncle")
	} else {
		t.Logf("PASS: Detected %d stale blocks", len(staleBlocks))
		if staleBlocks[0].Hash != "0xHASH_A" {
			t.Errorf("Expected stale block hash 0xHASH_A, got %s", staleBlocks[0].Hash)
		}
	}
}

func TestVerifyBoundaryBlocks_RecoverFromDeepReorgCorrected(t *testing.T) {
	ctx := context.Background()
	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	service, err := NewBackfillService(BackfillConfig{
		BatchSize:          10,
		BoundaryCheckDepth: 10,
	}, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Setup Initial State: Chain A (100A -> 101A)
	// Canonical: 100A, 101A
	block100A := outbound.BlockState{
		Number: 100, Hash: "0xHASH_100A", ParentHash: "0xHASH_99",
		IsOrphaned: false, ReceivedAt: time.Now().Unix(),
	}
	block101A := outbound.BlockState{
		Number: 101, Hash: "0xHASH_101A", ParentHash: "0xHASH_100A",
		IsOrphaned: false, ReceivedAt: time.Now().Unix(),
	}

	if _, err := stateRepo.SaveBlock(ctx, block100A); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}
	if _, err := stateRepo.SaveBlock(ctx, block101A); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	// Setup RPC State: Chain B (100B -> 101B)
	// We simulate a reorg where 100A and 101A are replaced by 100B and 101B
	// 99 is common ancestor
	client.SetBlockHeader(100, "0xHASH_100B", "0xHASH_99")
	client.SetBlockHeader(101, "0xHASH_101B", "0xHASH_100B")

	// Pre-populate ancestor 99 in DB so 100B binds correctly
	if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     99,
		Hash:       "0xHASH_99",
		ParentHash: "0xHASH_98",
		IsOrphaned: false,
	}); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	// Run verification
	// We need to export verifyBoundaryBlocks or call it via reflection if it is private.
	// But since this is package backfill_gaps, we have access to unexported methods!
	staleBlocks, err := service.verifyBoundaryBlocks(ctx)
	if err != nil {
		t.Fatalf("verifyBoundaryBlocks failed: %v", err)
	}

	if len(staleBlocks) != 2 {
		t.Fatalf("Expected 2 stale blocks, got %d", len(staleBlocks))
	}

	// Recover
	err = service.recoverFromStaleChain(ctx, staleBlocks)
	if err != nil {
		t.Logf("recoverFromStaleChain returned error (possibly expected in failing test): %v", err)
		// We don't fail immediately because we want to see if the state is broken or if it failed
		// due to the bug we are testing for.
		// The bug hypothesis: recoverFromStaleChain will fail validation and orphan correct blocks
		// OR it will succeed but leave orphans.
	}

	// 1. Old blocks should be orphaned
	old100, _ := stateRepo.GetBlockByHash(ctx, "0xHASH_100A")
	if !old100.IsOrphaned {
		t.Errorf("Block 100A should be orphaned")
	}
	old101, _ := stateRepo.GetBlockByHash(ctx, "0xHASH_101A")
	if !old101.IsOrphaned {
		t.Errorf("Block 101A should be orphaned")
	}

	// 2. New blocks should be PRESENT and NOT ORPHANED
	new100, err := stateRepo.GetBlockByNumber(ctx, 100)
	if err != nil {
		t.Fatalf("Failed to retrieve new canonical block 100: %v", err)
	}
	if new100.Hash != "0xHASH_100B" {
		t.Errorf("Canonical block 100 should be 100B, got %s", new100.Hash)
	}
	if new100.IsOrphaned {
		t.Errorf("New block 100B should NOT be orphaned")
	}

	new101, err := stateRepo.GetBlockByNumber(ctx, 101)
	if err != nil {
		t.Fatalf("Failed to retrieve new canonical block 101: %v", err)
	}
	if new101.Hash != "0xHASH_101B" {
		t.Errorf("Canonical block 101 should be 101B, got %s", new101.Hash)
	}
	if new101.IsOrphaned {
		t.Errorf("New block 101B should NOT be orphaned")
	}
}

// =============================================================================
// Silent Failure Bug Tests
// =============================================================================
//
// These tests verify that BackfillService correctly handles and retries
// cache/publish failures instead of silently swallowing errors.

// TestRetry_CacheFailureIsRetried verifies that when caching fails,
// the block is retried via GetBlocksWithIncompletePublish on the next pass.
func TestRetry_CacheFailureIsRetried(t *testing.T) {
	ctx := context.Background()

	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := newFailingCache()
	eventSink := newFailingEventSink()

	// Add blocks 1-5 to the client
	for i := int64(1); i <= 5; i++ {
		client.AddBlock(i, "")
	}

	// Configure cache to fail on block 3, but only once (will succeed on retry)
	cache.failOnBlock = 3
	cache.failOnOperation = "receipts"
	cache.failCount = 1

	service, err := NewBackfillService(
		BackfillConfig{
			ChainID:            1,
			BatchSize:          10,
			PollInterval:       time.Hour,
			BoundaryCheckDepth: -1,
			Logger:             slog.Default(),
		},
		client,
		stateRepo,
		cache,
		eventSink,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Save blocks 1 and 5 to create a gap (blocks 2-4 missing)
	for _, num := range []int64{1, 5} {
		block := outbound.BlockState{
			Number:     num,
			Hash:       client.GetHeader(num).Hash,
			ParentHash: client.GetHeader(num).ParentHash,
			ReceivedAt: time.Now().Unix(),
		}
		_, _ = stateRepo.SaveBlock(ctx, block)
		_ = stateRepo.MarkPublishComplete(ctx, block.Hash)
	}

	// Run backfill - block 3 will fail on first attempt but succeed on retry
	if err := service.RunOnce(ctx); err != nil {
		t.Logf("RunOnce returned error (may be expected): %v", err)
	}

	// Check: Block 3 should be saved and published after retry
	block3, err := stateRepo.GetBlockByNumber(ctx, 3)
	if err != nil {
		t.Fatalf("failed to get block 3: %v", err)
	}
	if block3 == nil {
		t.Fatal("block 3 was not saved to DB")
	}

	events := eventSink.GetBlockEvents()
	block3Published := false
	for _, e := range events {
		if e.BlockNumber == 3 {
			block3Published = true
			break
		}
	}

	if !block3Published {
		t.Error("Block 3 was saved to DB but never published after retry")
	}
}

// TestRetry_PublishFailureIsRetried verifies that when publish fails,
// the block is retried via GetBlocksWithIncompletePublish.
func TestRetry_PublishFailureIsRetried(t *testing.T) {
	ctx := context.Background()

	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := newFailingEventSink()

	for i := int64(1); i <= 5; i++ {
		client.AddBlock(i, "")
	}

	// Configure event sink to fail on block 3, but only once
	eventSink.failOnBlock = 3
	eventSink.failCount = 1

	service, err := NewBackfillService(
		BackfillConfig{
			ChainID:            1,
			BatchSize:          10,
			PollInterval:       time.Hour,
			BoundaryCheckDepth: -1,
			Logger:             slog.Default(),
		},
		client,
		stateRepo,
		cache,
		eventSink,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Create gap: save blocks 1 and 5
	for _, num := range []int64{1, 5} {
		block := outbound.BlockState{
			Number:     num,
			Hash:       client.GetHeader(num).Hash,
			ParentHash: client.GetHeader(num).ParentHash,
			ReceivedAt: time.Now().Unix(),
		}
		_, _ = stateRepo.SaveBlock(ctx, block)
		_ = stateRepo.MarkPublishComplete(ctx, block.Hash)
	}

	// Run backfill - block 3 publish will fail then be retried
	_ = service.RunOnce(ctx)

	// Verify block 3 was published after retry
	events := eventSink.GetBlockEvents()
	block3Published := false
	for _, e := range events {
		if e.BlockNumber == 3 {
			block3Published = true
			break
		}
	}

	if !block3Published {
		t.Error("Block 3 publish failed but was never retried")
	}
}

// TestProcessBlockData_ReturnsErrorOnCacheFailure verifies that processBlockData
// returns an error when cacheAndPublishBlockData fails.
func TestProcessBlockData_ReturnsErrorOnCacheFailure(t *testing.T) {
	ctx := context.Background()

	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := newFailingCache()
	eventSink := memory.NewEventSink()

	client.AddBlock(1, "")
	cache.failOnBlock = 1
	cache.failOnOperation = "any"

	service, _ := NewBackfillService(
		BackfillConfig{
			ChainID:            1,
			BatchSize:          10,
			BoundaryCheckDepth: -1,
			Logger:             slog.Default(),
		},
		client,
		stateRepo,
		cache,
		eventSink,
	)

	blockData, _ := client.GetBlocksBatch(ctx, []int64{1}, true)
	err := service.processBlockData(ctx, blockData[0])

	if err == nil {
		t.Error("processBlockData should return error when cache fails")
	} else {
		t.Logf("processBlockData correctly returned error: %v", err)
	}
}

// TestPublishNeverHappensWithoutCache verifies that an event is never published
// if any required data (block, receipts, traces) is missing from the BlockData.
// This protects against the scenario where RPC returns null without an error.
func TestPublishNeverHappensWithoutCache(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		blockData outbound.BlockData
		wantErr   string
	}{
		{
			name: "nil receipts without error should fail",
			blockData: outbound.BlockData{
				BlockNumber: 1,
				Block:       json.RawMessage(`{"number":"0x1","hash":"0x0000000000000000000000000000000000000000000000000000000000000001","parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000","timestamp":"0x0"}`),
				Receipts:    nil, // nil without error - edge case
				Traces:      json.RawMessage(`[]`),
			},
			wantErr: "missing receipts",
		},
		{
			name: "nil traces without error should fail",
			blockData: outbound.BlockData{
				BlockNumber: 1,
				Block:       json.RawMessage(`{"number":"0x1","hash":"0x0000000000000000000000000000000000000000000000000000000000000001","parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000","timestamp":"0x0"}`),
				Receipts:    json.RawMessage(`[]`),
				Traces:      nil, // nil without error - edge case
			},
			wantErr: "missing traces",
		},
		{
			name: "nil block should fail",
			blockData: outbound.BlockData{
				BlockNumber: 1,
				Block:       nil,
				Receipts:    json.RawMessage(`[]`),
				Traces:      json.RawMessage(`[]`),
			},
			wantErr: "missing block",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newMockClient()
			stateRepo := memory.NewBlockStateRepository()
			cache := memory.NewBlockCache()
			eventSink := memory.NewEventSink()

			service, _ := NewBackfillService(
				BackfillConfig{
					ChainID:            1,
					BatchSize:          10,
					BoundaryCheckDepth: -1,
					EnableTraces:       true,
					Logger:             slog.Default(),
				},
				client,
				stateRepo,
				cache,
				eventSink,
			)

			err := service.processBlockData(ctx, tt.blockData)

			// Should error - never publish without all data
			if err == nil {
				t.Errorf("expected error containing %q, got nil", tt.wantErr)

				// Check if event was incorrectly published
				events := eventSink.GetBlockEvents()
				if len(events) > 0 {
					t.Error("BUG: Event was published without all data being cached!")
				}
			} else {
				t.Logf("correctly rejected: %v", err)
			}
		})
	}
}
