package backfill_gaps

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockBlockchainClient provides predictable block data for testing.
type mockBlockchainClient struct {
	mu     sync.RWMutex
	blocks map[int64]blockTestData
	delay  time.Duration
}

type blockTestData struct {
	header   outbound.BlockHeader
	receipts json.RawMessage
	traces   json.RawMessage
	blobs    json.RawMessage
}

func newMockClient() *mockBlockchainClient {
	return &mockBlockchainClient{
		blocks: make(map[int64]blockTestData),
	}
}

func (m *mockBlockchainClient) addBlock(num int64, parentHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hash := fmt.Sprintf("0x%064x", num)
	if parentHash == "" && num > 0 {
		parentHash = fmt.Sprintf("0x%064x", num-1)
	}

	header := outbound.BlockHeader{
		Number:     fmt.Sprintf("0x%x", num),
		Hash:       hash,
		ParentHash: parentHash,
		Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
	}

	m.blocks[num] = blockTestData{
		header:   header,
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}
}

func (m *mockBlockchainClient) getHeader(num int64) outbound.BlockHeader {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.blocks[num].header
}

func (m *mockBlockchainClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.delay > 0 {
		time.Sleep(m.delay)
	}

	if bd, ok := m.blocks[blockNum]; ok {
		data, _ := json.Marshal(bd.header)
		return data, nil
	}
	return nil, fmt.Errorf("block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			h := bd.header
			return &h, nil
		}
	}
	// Return nil, nil when block not found (not an error - just doesn't exist)
	// This matches real RPC behavior where eth_getBlockByHash returns null for unknown hashes
	return nil, nil
}

func (m *mockBlockchainClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			data, _ := json.Marshal(bd.header)
			return data, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *mockBlockchainClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.receipts, nil
	}
	return nil, fmt.Errorf("receipts for block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			return bd.receipts, nil
		}
	}
	return nil, fmt.Errorf("receipts for block %s not found", hash)
}

func (m *mockBlockchainClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.traces, nil
	}
	return nil, fmt.Errorf("traces for block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			return bd.traces, nil
		}
	}
	return nil, fmt.Errorf("traces for block %s not found", hash)
}

func (m *mockBlockchainClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.blobs, nil
	}
	return nil, fmt.Errorf("blobs for block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			return bd.blobs, nil
		}
	}
	return nil, fmt.Errorf("blobs for block %s not found", hash)
}

func (m *mockBlockchainClient) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var max int64
	for num := range m.blocks {
		if num > max {
			max = num
		}
	}
	return max, nil
}

func (m *mockBlockchainClient) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.delay > 0 {
		time.Sleep(m.delay)
	}

	result := make([]outbound.BlockData, len(blockNums))
	for i, num := range blockNums {
		result[i] = outbound.BlockData{BlockNumber: num}
		if bd, ok := m.blocks[num]; ok {
			blockJSON, _ := json.Marshal(bd.header)
			result[i].Block = blockJSON
			result[i].Receipts = bd.receipts
			result[i].Traces = bd.traces
			result[i].Blobs = bd.blobs
		}
	}
	return result, nil
}

// setBlockHeader allows setting or overriding a block's header data.
func (m *mockBlockchainClient) setBlockHeader(num int64, hash, parentHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.blocks[num] = blockTestData{
		header: outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", num),
			Hash:       hash,
			ParentHash: parentHash,
			Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
		},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}
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
		client.addBlock(i, "")
	}

	// Seed the state repo with blocks 1, 50, and 100 (leaving gaps 2-49 and 51-99)
	for _, num := range []int64{1, 50, 100} {
		if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     num,
			Hash:       client.getHeader(num).Hash,
			ParentHash: client.getHeader(num).ParentHash,
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
		// Should have 97 events (blocks 2-49 = 48, blocks 51-99 = 49)
		expectedEvents := 48 + 49
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
		client.addBlock(i, "")
	}

	// Seed blocks 1 and 10 to create a gap at 2-9
	for _, num := range []int64{1, 10} {
		if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     num,
			Hash:       client.getHeader(num).Hash,
			ParentHash: client.getHeader(num).ParentHash,
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
	backfilledBlock, err := stateRepo.GetBlockByHash(ctx, client.getHeader(5).Hash)
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

	// The cache key should match the saved version
	expectedCacheKey := fmt.Sprintf("stl:1:5:%d:block", backfilledBlock.Version)
	if block5Event.CacheKey != expectedCacheKey {
		t.Errorf("expected cache key %q, got %q", expectedCacheKey, block5Event.CacheKey)
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
		client.addBlock(i, "")
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
	client.setBlockHeader(106, hashWithSuffix(106, "_new"), hashWithSuffix(105, "_new")) // Note: 106_new parent is not 105_old
	client.setBlockHeader(107, hashWithSuffix(107, "_new"), hashWithSuffix(106, "_new"))

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
		client.setBlockHeader(i, hashWithSuffix(i, "_old"), parentHash)
	}

	// RPC: 103-105 are on a new chain (different hashes and parent links)
	for i := int64(103); i <= 105; i++ {
		newParentHash := hashWithSuffix(i-1, "_new")

		// Special case: 103's parent should point to 102_old to link to the existing chain
		if i == 103 {
			newParentHash = hashWithSuffix(102, "_old")
		}

		client.setBlockHeader(i, hashWithSuffix(i, "_new"), newParentHash)
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
		client.addBlock(i, "")
	}

	// Add block 10 - this is what the live service already processed
	// Its parent_hash points to the "original" block 9's hash
	originalBlock9Hash := fmt.Sprintf("0x%064x", 9) // The hash that block 10 expects as parent

	// But the client returns a DIFFERENT block 9 (simulating reorg in RPC)
	// This block 9 has a different hash than what block 10 expects as parent
	reorgedBlock9Hash := "0xREORGED_BLOCK_9_HASH_DOES_NOT_MATCH_BLOCK_10_PARENT"
	client.setBlockHeader(9, reorgedBlock9Hash, fmt.Sprintf("0x%064x", 8))

	// Add block 10 to client for completeness
	client.addBlock(10, originalBlock9Hash)

	// Pre-populate state repo with blocks 1-8 and 10 (leaving gap at 9)
	for i := int64(1); i <= 8; i++ {
		if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       client.getHeader(i).Hash,
			ParentHash: client.getHeader(i).ParentHash,
			ReceivedAt: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Save block 10 with parent_hash pointing to ORIGINAL block 9 hash
	if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     10,
		Hash:       client.getHeader(10).Hash,
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
		client.setBlockHeader(i, hashWithSuffix(i, "_old"), parentHash)
	}

	// RPC: 103-105 are on a new chain (different hashes - simulating reorg during downtime)
	for i := int64(103); i <= 105; i++ {
		newParentHash := hashWithSuffix(i-1, "_new")
		if i == 103 {
			newParentHash = hashWithSuffix(102, "_old") // Links to existing chain
		}
		client.setBlockHeader(i, hashWithSuffix(i, "_new"), newParentHash)
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
		client.addBlock(i, "")
		saveBlockState(t, ctx, stateRepo, i, fmt.Sprintf("0x%064x", i), fmt.Sprintf("0x%064x", i-1))
	}

	// Block 100 - Live service has already processed the CANONICAL block (hash B)
	canonicalHash := "0xCANONICAL_HASH_B_SAVED_BY_LIVE_SERVICE"
	saveBlockState(t, ctx, stateRepo, 100, canonicalHash, fmt.Sprintf("0x%064x", 99))

	// But the RPC returns a STALE block (hash A) - simulating a slow/stale RPC node
	staleHash := "0xSTALE_HASH_A_FROM_SLOW_RPC_NODE"
	client.setBlockHeader(100, staleHash, fmt.Sprintf("0x%064x", 99))

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
