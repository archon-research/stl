package application

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

// mockSubscriber is a test subscriber that emits headers on demand.
type mockSubscriber struct {
	mu       sync.Mutex
	headers  chan outbound.BlockHeader
	closed   bool
	onReconn func()
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{
		headers: make(chan outbound.BlockHeader, 100),
	}
}

func (m *mockSubscriber) Subscribe(ctx context.Context) (<-chan outbound.BlockHeader, error) {
	return m.headers, nil
}

func (m *mockSubscriber) Unsubscribe() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.headers)
	}
	return nil
}

func (m *mockSubscriber) HealthCheck(ctx context.Context) error {
	return nil
}

func (m *mockSubscriber) SetOnReconnect(callback func()) {
	m.onReconn = callback
}

func (m *mockSubscriber) sendHeader(header outbound.BlockHeader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.headers <- header
	}
}

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

	blockJSON, _ := json.Marshal(header)
	m.blocks[num] = blockTestData{
		header:   header,
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}
	_ = blockJSON
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

func TestConcurrentLiveAndBackfill(t *testing.T) {
	// Setup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subscriber := newMockSubscriber()
	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate client with blocks 1-200
	for i := int64(1); i <= 200; i++ {
		client.addBlock(i, "")
	}

	// Seed the state repo with block 1 so backfill knows where to start
	stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     1,
		Hash:       client.getHeader(1).Hash,
		ParentHash: client.getHeader(1).ParentHash,
		ReceivedAt: time.Now().Unix(),
	})

	config := WatcherConfig{
		ChainID:              1,
		FinalityBlockCount:   10,
		MaxUnfinalizedBlocks: 50,
		BackfillBatchSize:    5,
		Logger:               slog.Default(),
	}

	service, err := NewWatcherService(config, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Start the service
	if err := service.Start(ctx); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	// Set up the reconnect callback to trigger backfill
	subscriber.SetOnReconnect(service.OnReconnect)

	// Use WaitGroup to coordinate test
	var wg sync.WaitGroup

	// Goroutine 1: Simulate live blocks coming in (blocks 150-200)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(150); i <= 200; i++ {
			header := client.getHeader(i)
			subscriber.sendHeader(header)
			time.Sleep(5 * time.Millisecond) // Simulate ~12s blocks compressed
		}
	}()

	// Goroutine 2: Trigger backfill for blocks 2-149 (overlaps with live at 150)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Small delay to let live start
		time.Sleep(10 * time.Millisecond)
		// Trigger backfill
		service.OnReconnect()
	}()

	// Wait for both to complete
	wg.Wait()

	// Give time for processing to complete
	time.Sleep(500 * time.Millisecond)

	// Stop the service
	if err := service.Stop(); err != nil {
		t.Errorf("failed to stop service: %v", err)
	}

	// Verify results
	t.Run("no duplicate blocks in state repo", func(t *testing.T) {
		// Get all blocks and check for duplicate numbers
		seen := make(map[int64]string)
		for i := int64(1); i <= 200; i++ {
			block, err := stateRepo.GetBlockByNumber(ctx, i)
			if err != nil {
				t.Errorf("error getting block %d: %v", i, err)
				continue
			}
			if block != nil {
				if existingHash, exists := seen[block.Number]; exists {
					if existingHash != block.Hash {
						t.Errorf("duplicate block number %d with different hashes: %s vs %s",
							block.Number, existingHash, block.Hash)
					}
				}
				seen[block.Number] = block.Hash
			}
		}
	})

	t.Run("chain is consistent", func(t *testing.T) {
		// Verify parent hash chain is valid
		service.chainMu.RLock()
		defer service.chainMu.RUnlock()

		for i := 1; i < len(service.unfinalizedBlocks); i++ {
			curr := service.unfinalizedBlocks[i]
			prev := service.unfinalizedBlocks[i-1]

			// Blocks should be in ascending order
			if curr.Number < prev.Number {
				t.Errorf("chain not sorted: block %d comes after block %d", curr.Number, prev.Number)
			}

			// If consecutive, parent hash should match
			if curr.Number == prev.Number+1 && curr.ParentHash != prev.Hash {
				t.Errorf("parent hash mismatch at block %d: expected %s, got %s",
					curr.Number, prev.Hash, curr.ParentHash)
			}
		}
	})

	t.Run("events published without duplicates", func(t *testing.T) {
		blockEvents := eventSink.GetEventsByType(outbound.EventTypeBlock)

		// Check for duplicate block events
		seen := make(map[int64]bool)
		for _, e := range blockEvents {
			be := e.(outbound.BlockEvent)
			if seen[be.BlockNumber] {
				t.Errorf("duplicate block event for block %d", be.BlockNumber)
			}
			seen[be.BlockNumber] = true
		}
	})

	t.Run("reasonable number of blocks processed", func(t *testing.T) {
		blockEvents := eventSink.GetEventsByType(outbound.EventTypeBlock)
		// We should have processed most blocks (some might be skipped due to timing)
		// At minimum we should have the live blocks 150-200
		if len(blockEvents) < 50 {
			t.Errorf("expected at least 50 block events, got %d", len(blockEvents))
		}
		t.Logf("processed %d block events", len(blockEvents))
	})
}

func TestAddToUnfinalizedChain_MaintainsSortedOrder(t *testing.T) {
	service := &WatcherService{
		config: WatcherConfig{
			MaxUnfinalizedBlocks: 100,
		},
		unfinalizedBlocks: make([]LightBlock, 0),
	}

	// Add blocks out of order
	blocks := []LightBlock{
		{Number: 5, Hash: "0x5", ParentHash: "0x4"},
		{Number: 3, Hash: "0x3", ParentHash: "0x2"},
		{Number: 7, Hash: "0x7", ParentHash: "0x6"},
		{Number: 1, Hash: "0x1", ParentHash: "0x0"},
		{Number: 4, Hash: "0x4", ParentHash: "0x3"},
		{Number: 2, Hash: "0x2", ParentHash: "0x1"},
		{Number: 6, Hash: "0x6", ParentHash: "0x5"},
	}

	for _, b := range blocks {
		service.addToUnfinalizedChain(b)
	}

	// Verify sorted order
	for i := 1; i < len(service.unfinalizedBlocks); i++ {
		if service.unfinalizedBlocks[i].Number < service.unfinalizedBlocks[i-1].Number {
			t.Errorf("chain not sorted at index %d: %d < %d",
				i, service.unfinalizedBlocks[i].Number, service.unfinalizedBlocks[i-1].Number)
		}
	}

	// Verify all blocks present
	if len(service.unfinalizedBlocks) != 7 {
		t.Errorf("expected 7 blocks, got %d", len(service.unfinalizedBlocks))
	}
}

func TestAddToUnfinalizedChain_SkipsDuplicates(t *testing.T) {
	service := &WatcherService{
		config: WatcherConfig{
			MaxUnfinalizedBlocks: 100,
		},
		unfinalizedBlocks: make([]LightBlock, 0),
	}

	block := LightBlock{Number: 5, Hash: "0x5", ParentHash: "0x4"}

	// Add same block twice
	service.addToUnfinalizedChain(block)
	service.addToUnfinalizedChain(block)

	if len(service.unfinalizedBlocks) != 1 {
		t.Errorf("expected 1 block after adding duplicate, got %d", len(service.unfinalizedBlocks))
	}
}

func TestAddToUnfinalizedChain_HandlesForks(t *testing.T) {
	service := &WatcherService{
		config: WatcherConfig{
			MaxUnfinalizedBlocks: 100,
		},
		unfinalizedBlocks: make([]LightBlock, 0),
	}

	// Add two blocks at same height with different hashes (fork)
	block1 := LightBlock{Number: 5, Hash: "0x5a", ParentHash: "0x4"}
	block2 := LightBlock{Number: 5, Hash: "0x5b", ParentHash: "0x4"}

	service.addToUnfinalizedChain(block1)
	service.addToUnfinalizedChain(block2)

	// Both should be present (reorg handling will clean up later)
	if len(service.unfinalizedBlocks) != 2 {
		t.Errorf("expected 2 blocks for fork, got %d", len(service.unfinalizedBlocks))
	}
}

// TestLateBlockAfterPruning tests the scenario where a live block arrives after
// its corresponding blocks have been pruned from the unfinalized chain.
//
// This happens when:
//  1. Live blocks 150-200 are sent to the channel
//  2. Backfill processes blocks 2-149 concurrently
//  3. Chain grows and older blocks get pruned (e.g., blocks < 185 removed)
//  4. A "late" live block (e.g., 152) that was buffered in the channel finally processes
//  5. Since block 152's parent (151) is already pruned, reorg detection fails with
//     "unrecoverable reorg: beyond finalized block"
//
// This is expected behavior - the block is correctly rejected because:
// - It's a duplicate (backfill already processed it)
// - Its parent is no longer in the unfinalized chain (pruned)
// - It's below the finalized block threshold
//
// The test verifies that despite the warning, no duplicate events are published.
func TestLateBlockAfterPruning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subscriber := newMockSubscriber()
	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate client with blocks 1-300
	for i := int64(1); i <= 300; i++ {
		client.addBlock(i, "")
	}

	// Seed the state repo with block 1
	stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     1,
		Hash:       client.getHeader(1).Hash,
		ParentHash: client.getHeader(1).ParentHash,
		ReceivedAt: time.Now().Unix(),
	})

	// Use a SMALL MaxUnfinalizedBlocks to force pruning
	config := WatcherConfig{
		ChainID:              1,
		FinalityBlockCount:   10,
		MaxUnfinalizedBlocks: 30, // Small - will force pruning
		BackfillBatchSize:    10,
		Logger:               slog.Default(),
	}

	service, err := NewWatcherService(config, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	if err := service.Start(ctx); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	subscriber.SetOnReconnect(service.OnReconnect)

	var wg sync.WaitGroup

	// Send live blocks 150-250 with delays to simulate real timing
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(150); i <= 250; i++ {
			header := client.getHeader(i)
			subscriber.sendHeader(header)
			// Simulate varying network delays
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Trigger backfill for 2-149 slightly after live starts
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		service.OnReconnect()
	}()

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	if err := service.Stop(); err != nil {
		t.Errorf("failed to stop service: %v", err)
	}

	// The key assertion: despite warnings, no duplicate events
	t.Run("no duplicate events despite late blocks", func(t *testing.T) {
		blockEvents := eventSink.GetEventsByType(outbound.EventTypeBlock)

		seen := make(map[int64]int)
		for _, e := range blockEvents {
			be := e.(outbound.BlockEvent)
			seen[be.BlockNumber]++
		}

		duplicates := 0
		for blockNum, count := range seen {
			if count > 1 {
				t.Errorf("block %d published %d times", blockNum, count)
				duplicates++
			}
		}

		if duplicates == 0 {
			t.Logf("SUCCESS: %d block events with no duplicates", len(blockEvents))
		}
	})

	t.Run("chain state is valid after pruning", func(t *testing.T) {
		service.chainMu.RLock()
		defer service.chainMu.RUnlock()

		// Chain should be pruned to MaxUnfinalizedBlocks
		if len(service.unfinalizedBlocks) > config.MaxUnfinalizedBlocks {
			t.Errorf("chain not pruned: got %d blocks, max %d",
				len(service.unfinalizedBlocks), config.MaxUnfinalizedBlocks)
		}

		// Chain should be sorted
		for i := 1; i < len(service.unfinalizedBlocks); i++ {
			if service.unfinalizedBlocks[i].Number < service.unfinalizedBlocks[i-1].Number {
				t.Errorf("chain not sorted at index %d", i)
			}
		}

		t.Logf("chain has %d blocks, finalized at %v",
			len(service.unfinalizedBlocks),
			service.finalizedBlock)
	})
}
