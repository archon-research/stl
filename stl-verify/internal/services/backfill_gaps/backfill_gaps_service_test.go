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
	return nil, fmt.Errorf("block %s not found", hash)
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

func (m *mockBlockchainClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.traces, nil
	}
	return nil, fmt.Errorf("traces for block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.blobs, nil
	}
	return nil, fmt.Errorf("blobs for block %d not found", blockNum)
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
	client.blocks[9] = blockTestData{
		header: outbound.BlockHeader{
			Number:     "0x9",
			Hash:       reorgedBlock9Hash,         // Different hash!
			ParentHash: fmt.Sprintf("0x%064x", 8), // Links to block 8
			Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
		},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}

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
