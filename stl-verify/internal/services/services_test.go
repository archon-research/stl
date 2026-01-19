package services

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
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
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

func (m *mockBlockchainClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.delay > 0 {
		time.Sleep(m.delay)
	}

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			blockJSON, _ := json.Marshal(bd.header)
			return outbound.BlockData{
				BlockNumber: blockNum,
				Block:       blockJSON,
				Receipts:    bd.receipts,
				Traces:      bd.traces,
				Blobs:       bd.blobs,
			}, nil
		}
	}
	return outbound.BlockData{}, fmt.Errorf("block %s not found", hash)
}

// TestConcurrentLiveAndBackfill is an integration test that verifies
// LiveService and BackfillService work correctly when running together.
func TestConcurrentLiveAndBackfill(t *testing.T) {
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
	if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     1,
		Hash:       client.getHeader(1).Hash,
		ParentHash: client.getHeader(1).ParentHash,
		ReceivedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	liveConfig := live_data.LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   10,
		MaxUnfinalizedBlocks: 50,
		Logger:               slog.Default(),
	}

	backfillConfig := backfill_gaps.BackfillConfig{
		ChainID:      1,
		BatchSize:    5,
		PollInterval: 100 * time.Millisecond,
		Logger:       slog.Default(),
	}

	liveService, err := live_data.NewLiveService(liveConfig, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create live service: %v", err)
	}

	backfillService, err := backfill_gaps.NewBackfillService(backfillConfig, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create backfill service: %v", err)
	}

	// Start both services
	if err := liveService.Start(ctx); err != nil {
		t.Fatalf("failed to start live service: %v", err)
	}

	if err := backfillService.Start(ctx); err != nil {
		t.Fatalf("failed to start backfill service: %v", err)
	}

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

	// Wait for live to complete
	wg.Wait()

	// Give time for backfill to fill gaps
	time.Sleep(500 * time.Millisecond)

	// Stop both services
	if err := backfillService.Stop(); err != nil {
		t.Errorf("failed to stop backfill service: %v", err)
	}
	if err := liveService.Stop(); err != nil {
		t.Errorf("failed to stop live service: %v", err)
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
