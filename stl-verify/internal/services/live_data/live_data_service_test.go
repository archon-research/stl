package live_data

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

// mockStateRepo is a test state repository that can simulate errors.
// It embeds the memory implementation and overrides specific methods for testing.
type mockStateRepo struct {
	*memory.BlockStateRepository
	mu              sync.RWMutex
	getByHashErr    error
	getRecentErr    error
	getRecentBlocks []outbound.BlockState // Override response
	saveBlockErr    error
}

func newMockStateRepo() *mockStateRepo {
	return &mockStateRepo{
		BlockStateRepository: memory.NewBlockStateRepository(),
	}
}

func (m *mockStateRepo) SaveBlock(ctx context.Context, state outbound.BlockState) error {
	m.mu.RLock()
	err := m.saveBlockErr
	m.mu.RUnlock()

	if err != nil {
		return err
	}
	return m.BlockStateRepository.SaveBlock(ctx, state)
}

func (m *mockStateRepo) GetBlockByHash(ctx context.Context, hash string) (*outbound.BlockState, error) {
	m.mu.RLock()
	err := m.getByHashErr
	m.mu.RUnlock()

	// If error is set, return it
	if err != nil {
		return nil, err
	}
	// Delegate to embedded implementation
	return m.BlockStateRepository.GetBlockByHash(ctx, hash)
}

func (m *mockStateRepo) GetRecentBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	m.mu.RLock()
	err := m.getRecentErr
	blocks := m.getRecentBlocks
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}
	if blocks != nil {
		return blocks, nil
	}
	return m.BlockStateRepository.GetRecentBlocks(ctx, limit)
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

func TestLiveService_AddToUnfinalizedChain_MaintainsSortedOrder(t *testing.T) {
	service := &LiveService{
		config: LiveConfig{
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
		service.addBlock(b)
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

func TestLiveService_AddBlock_HandlesForks(t *testing.T) {
	service := &LiveService{
		config: LiveConfig{
			MaxUnfinalizedBlocks: 100,
		},
		unfinalizedBlocks: make([]LightBlock, 0),
	}

	// Add two blocks at same height with different hashes (fork)
	block1 := LightBlock{Number: 5, Hash: "0x5a", ParentHash: "0x4"}
	block2 := LightBlock{Number: 5, Hash: "0x5b", ParentHash: "0x4"}

	service.addBlock(block1)
	service.addBlock(block2)

	// Both should be present (reorg handling will clean up later)
	if len(service.unfinalizedBlocks) != 2 {
		t.Errorf("expected 2 blocks for fork, got %d", len(service.unfinalizedBlocks))
	}
}

// TestLateBlockAfterPruning tests the scenario where a live block arrives after
// its corresponding blocks have been pruned from the unfinalized chain.
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
	if err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     1,
		Hash:       client.getHeader(1).Hash,
		ParentHash: client.getHeader(1).ParentHash,
		ReceivedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	// Use a SMALL MaxUnfinalizedBlocks to force pruning
	liveConfig := LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   10,
		MaxUnfinalizedBlocks: 30, // Small - will force pruning
		Logger:               slog.Default(),
	}

	liveService, err := NewLiveService(liveConfig, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create live service: %v", err)
	}

	if err := liveService.Start(ctx); err != nil {
		t.Fatalf("failed to start live service: %v", err)
	}

	var wg sync.WaitGroup

	// Send live blocks 150-250 with delays to simulate real timing
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(150); i <= 250; i++ {
			header := client.getHeader(i)
			subscriber.sendHeader(header)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	if err := liveService.Stop(); err != nil {
		t.Errorf("failed to stop live service: %v", err)
	}

	// The key assertion: despite warnings, no duplicate events
	t.Run("no duplicate events despite late blocks", func(t *testing.T) {
		blockEvents := eventSink.GetEventsByType(outbound.EventTypeBlock)

		seen := make(map[int64]int)
		for _, e := range blockEvents {
			be := e.(outbound.BlockEvent)
			seen[be.BlockNumber]++
		}

		for blockNum, count := range seen {
			if count > 1 {
				t.Errorf("block %d published %d times", blockNum, count)
			}
		}
	})
}

// =============================================================================
// NewLiveService validation tests
// =============================================================================

func TestNewLiveService_NilDependencies(t *testing.T) {
	tests := []struct {
		name        string
		nilField    string
		expectedErr string
	}{
		{"nil subscriber", "subscriber", "subscriber is required"},
		{"nil client", "client", "client is required"},
		{"nil stateRepo", "stateRepo", "stateRepo is required"},
		{"nil cache", "cache", "cache is required"},
		{"nil eventSink", "eventSink", "eventSink is required"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create all dependencies
			subscriber := newMockSubscriber()
			client := newMockClient()
			stateRepo := memory.NewBlockStateRepository()
			cache := memory.NewBlockCache()
			eventSink := memory.NewEventSink()

			// Nil out the specific field being tested
			var sub outbound.BlockSubscriber = subscriber
			var cli outbound.BlockchainClient = client
			var repo outbound.BlockStateRepository = stateRepo
			var c outbound.BlockCache = cache
			var sink outbound.EventSink = eventSink

			switch tc.nilField {
			case "subscriber":
				sub = nil
			case "client":
				cli = nil
			case "stateRepo":
				repo = nil
			case "cache":
				c = nil
			case "eventSink":
				sink = nil
			}

			_, err := NewLiveService(LiveConfig{}, sub, cli, repo, c, sink)
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if err.Error() != tc.expectedErr {
				t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
			}
		})
	}
}

func TestNewLiveService_AppliesDefaults(t *testing.T) {
	subscriber := newMockSubscriber()
	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pass zero-value config
	svc, err := NewLiveService(LiveConfig{}, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defaults := LiveConfigDefaults()
	if svc.config.ChainID != defaults.ChainID {
		t.Errorf("expected ChainID %d, got %d", defaults.ChainID, svc.config.ChainID)
	}
	if svc.config.FinalityBlockCount != defaults.FinalityBlockCount {
		t.Errorf("expected FinalityBlockCount %d, got %d", defaults.FinalityBlockCount, svc.config.FinalityBlockCount)
	}
	if svc.config.MaxUnfinalizedBlocks != defaults.MaxUnfinalizedBlocks {
		t.Errorf("expected MaxUnfinalizedBlocks %d, got %d", defaults.MaxUnfinalizedBlocks, svc.config.MaxUnfinalizedBlocks)
	}
	if svc.config.Logger == nil {
		t.Error("expected Logger to be set to default")
	}
}

func TestNewLiveService_UsesProvidedConfig(t *testing.T) {
	subscriber := newMockSubscriber()
	client := newMockClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	customLogger := slog.Default().With("custom", true)

	config := LiveConfig{
		ChainID:              42,
		FinalityBlockCount:   128,
		MaxUnfinalizedBlocks: 500,
		DisableBlobs:         true,
		Logger:               customLogger,
	}

	svc, err := NewLiveService(config, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if svc.config.ChainID != 42 {
		t.Errorf("expected ChainID 42, got %d", svc.config.ChainID)
	}
	if svc.config.FinalityBlockCount != 128 {
		t.Errorf("expected FinalityBlockCount 128, got %d", svc.config.FinalityBlockCount)
	}
	if svc.config.MaxUnfinalizedBlocks != 500 {
		t.Errorf("expected MaxUnfinalizedBlocks 500, got %d", svc.config.MaxUnfinalizedBlocks)
	}
	if !svc.config.DisableBlobs {
		t.Error("expected DisableBlobs to be true")
	}
}

// ============================================================================
// isDuplicateBlock edge case tests
// ============================================================================

func TestIsDuplicateBlock_FoundInMemory(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Add a block to the in-memory chain
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 100, Hash: "0xabcd1234", ParentHash: "0x0000"},
	}

	// Check for duplicate - should find it in memory
	isDup := svc.isDuplicateBlock(ctx, "0xabcd1234", 100)
	if !isDup {
		t.Error("expected isDuplicateBlock to return true for block in memory")
	}
}

func TestIsDuplicateBlock_FoundInDB(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate the state repo with a block
	_ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 50,
		Hash:   "0xdb_block_hash",
	})

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Memory is empty
	svc.unfinalizedBlocks = []LightBlock{}

	// Check for duplicate - should find it in DB
	isDup := svc.isDuplicateBlock(ctx, "0xdb_block_hash", 50)
	if !isDup {
		t.Error("expected isDuplicateBlock to return true for block in DB")
	}
}

func TestIsDuplicateBlock_NotFound(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Memory is empty, DB is empty
	svc.unfinalizedBlocks = []LightBlock{}

	// Check for duplicate - should not find it anywhere
	isDup := svc.isDuplicateBlock(ctx, "0xnonexistent", 999)
	if isDup {
		t.Error("expected isDuplicateBlock to return false for non-existent block")
	}
}

func TestIsDuplicateBlock_DBErrorContinues(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	stateRepo.getByHashErr = fmt.Errorf("database connection failed")

	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Memory is empty
	svc.unfinalizedBlocks = []LightBlock{}

	// Check for duplicate - DB error should log warning and return false
	isDup := svc.isDuplicateBlock(ctx, "0xsome_hash", 100)
	if isDup {
		t.Error("expected isDuplicateBlock to return false when DB errors")
	}
}

func TestIsDuplicateBlock_ChecksMemoryBeforeDB(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	// Set error that would fail if DB is called
	stateRepo.getByHashErr = fmt.Errorf("should not be called")

	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Add block to memory
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 100, Hash: "0xmemory_hash", ParentHash: "0x0000"},
	}

	// Should find in memory and return true without hitting DB
	isDup := svc.isDuplicateBlock(ctx, "0xmemory_hash", 100)
	if !isDup {
		t.Error("expected isDuplicateBlock to return true when found in memory")
	}
}

// ============================================================================
// detectReorg scenario tests
// ============================================================================

func TestDetectReorg_EmptyChain_NoReorg(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Empty chain
	svc.unfinalizedBlocks = []LightBlock{}

	header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0xnew_hash",
		ParentHash: "0xparent",
	}

	isReorg, depth, ancestor, err := svc.detectReorg(header, 100, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isReorg {
		t.Error("expected no reorg for empty chain")
	}
	if depth != 0 || ancestor != 0 {
		t.Errorf("expected depth=0, ancestor=0, got depth=%d, ancestor=%d", depth, ancestor)
	}
}

func TestDetectReorg_NextBlock_ParentMatches_NoReorg(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Add blocks for lookups
	for i := int64(1); i <= 100; i++ {
		client.addBlock(i, "")
	}

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Simulate chain at block 100
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Next block 101 with correct parent
	header := outbound.BlockHeader{
		Number:     "0x65", // 101
		Hash:       "0xblock101",
		ParentHash: "0xblock100", // Matches latest
	}

	isReorg, depth, ancestor, err := svc.detectReorg(header, 101, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isReorg {
		t.Error("expected no reorg when parent hash matches")
	}
	if depth != 0 || ancestor != 0 {
		t.Errorf("expected depth=0, ancestor=0, got depth=%d, ancestor=%d", depth, ancestor)
	}
}

func TestDetectReorg_NextBlock_ParentMismatch_Reorg(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Add alternate chain blocks for ancestry walk
	// Block 100_alt's parent is block 99 (which exists in our chain)
	client.blocks[100] = blockTestData{
		header: outbound.BlockHeader{
			Number:     "0x64",
			Hash:       "0xblock100_alt",
			ParentHash: "0xblock99", // Points to our block 99
		},
	}

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 64,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Simulate chain at block 100
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Block 101 with WRONG parent (orphan 100, replace with different chain)
	header := outbound.BlockHeader{
		Number:     "0x65", // 101
		Hash:       "0xblock101_alt",
		ParentHash: "0xblock100_alt", // Different parent - triggers reorg
	}

	isReorg, depth, ancestor, err := svc.detectReorg(header, 101, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReorg {
		t.Error("expected reorg when parent hash mismatches")
	}
	// Walk finds block100_alt's parent (0xblock99) in our chain
	// Common ancestor is 99. Depth is 0 because no blocks >= 101 exist in our chain.
	// Block 100 gets pruned during chain reconstruction, but isn't counted in depth
	// (depth counts blocks at or above the incoming block number)
	if depth != 0 {
		t.Errorf("expected depth 0 (no blocks >= 101 in chain), got %d", depth)
	}
	if ancestor != 99 {
		t.Errorf("expected common ancestor 99, got %d", ancestor)
	}
}

func TestDetectReorg_LowerBlockNumber_Reorg(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Add blocks for ancestry walk
	client.blocks[98] = blockTestData{
		header: outbound.BlockHeader{
			Number:     "0x62",
			Hash:       "0xblock98",
			ParentHash: "0xblock97",
		},
	}

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Chain at block 100
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97"},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Receive block 99 again with different hash - definite reorg
	header := outbound.BlockHeader{
		Number:     "0x63", // 99
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98", // Same ancestor
	}

	isReorg, depth, ancestor, err := svc.detectReorg(header, 99, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReorg {
		t.Error("expected reorg when block number <= latest")
	}
	// Common ancestor should be block 98, orphaning blocks 99 and 100 (depth=2)
	if ancestor != 98 {
		t.Errorf("expected common ancestor 98, got %d", ancestor)
	}
	if depth != 2 {
		t.Errorf("expected depth 2 (blocks 99, 100 orphaned), got %d", depth)
	}
}

func TestDetectReorg_Gap_NoReorg(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Chain at block 100
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Receive block 105 (gap of 4 blocks)
	header := outbound.BlockHeader{
		Number:     "0x69", // 105
		Hash:       "0xblock105",
		ParentHash: "0xblock104",
	}

	isReorg, depth, ancestor, err := svc.detectReorg(header, 105, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isReorg {
		t.Error("expected no reorg for gap - should be handled by backfill")
	}
	if depth != 0 || ancestor != 0 {
		t.Errorf("expected depth=0, ancestor=0 for gap, got depth=%d, ancestor=%d", depth, ancestor)
	}
}

// ============================================================================
// handleReorg common ancestor walk tests
// ============================================================================

func TestHandleReorg_FindsCommonAncestorInMemory(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Set up a chain with blocks 95-100
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 95, Hash: "0xblock95", ParentHash: "0xblock94"},
		{Number: 96, Hash: "0xblock96", ParentHash: "0xblock95"},
		{Number: 97, Hash: "0xblock97", ParentHash: "0xblock96"},
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97"},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Incoming block 99 with different hash but parent is block 98
	header := outbound.BlockHeader{
		Number:     "0x63",
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98", // Matches our block 98
	}

	isReorg, depth, ancestor, err := svc.handleReorg(header, 99, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReorg {
		t.Error("expected isReorg to be true")
	}
	if ancestor != 98 {
		t.Errorf("expected common ancestor 98, got %d", ancestor)
	}
	// Depth = blocks 99, 100 were orphaned
	if depth != 2 {
		t.Errorf("expected depth 2 (blocks 99, 100), got %d", depth)
	}
	// After reorg, chain should be pruned to blocks <= 98
	if len(svc.unfinalizedBlocks) != 4 {
		t.Errorf("expected 4 blocks remaining, got %d", len(svc.unfinalizedBlocks))
	}
}

func TestHandleReorg_WalksBackViaNetwork(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Set up remote chain that forks at block 97
	// Remote: 97 -> 98_alt -> 99_alt -> 100_alt
	client.blocks[97] = blockTestData{
		header: outbound.BlockHeader{
			Number:     "0x61",
			Hash:       "0xblock97", // Common ancestor
			ParentHash: "0xblock96",
		},
	}
	client.blocks[98] = blockTestData{
		header: outbound.BlockHeader{
			Number:     "0x62",
			Hash:       "0xblock98_alt",
			ParentHash: "0xblock97",
		},
	}

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Our chain: 95-100
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 95, Hash: "0xblock95", ParentHash: "0xblock94"},
		{Number: 96, Hash: "0xblock96", ParentHash: "0xblock95"},
		{Number: 97, Hash: "0xblock97", ParentHash: "0xblock96"},
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97"},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Incoming block 99 from alternate chain
	// Its parent is 98_alt, we need to walk back to find common ancestor
	header := outbound.BlockHeader{
		Number:     "0x63",
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98_alt", // Not in our chain
	}

	isReorg, depth, ancestor, err := svc.handleReorg(header, 99, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReorg {
		t.Error("expected isReorg to be true")
	}
	// Common ancestor should be 97 (found after walking 98_alt's parent)
	// Depth is 2: blocks 99 and 100 are orphaned (blocks at or above incoming block 99)
	if ancestor != 97 {
		t.Errorf("expected common ancestor 97, got %d", ancestor)
	}
	if depth != 2 {
		t.Errorf("expected depth 2 (blocks 99, 100 orphaned), got %d", depth)
	}
}

func TestHandleReorg_Errors(t *testing.T) {
	tests := []struct {
		name        string
		config      LiveConfig
		setupClient func(*mockBlockchainClient)
		setupChain  func(*LiveService)
		header      outbound.BlockHeader
		blockNum    int64
		wantErr     bool
		errContains string
	}{
		{
			name: "below_finalized_block",
			config: LiveConfig{
				FinalityBlockCount: 64,
			},
			setupChain: func(svc *LiveService) {
				svc.finalizedBlock = &LightBlock{Number: 50, Hash: "0xfinalized"}
				svc.unfinalizedBlocks = []LightBlock{
					{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
				}
			},
			header: outbound.BlockHeader{
				Number:     "0x32", // 50
				Hash:       "0xlate_block",
				ParentHash: "0xunknown",
			},
			blockNum:    50,
			wantErr:     true,
			errContains: "at or below finalized block",
		},
		{
			name: "no_common_ancestor",
			config: LiveConfig{
				FinalityBlockCount: 3, // Walk will exhaust after 3 iterations
			},
			setupClient: func(client *mockBlockchainClient) {
				// Set up a chain of unknown blocks that the walk will traverse
				client.blocks[99] = blockTestData{
					header: outbound.BlockHeader{
						Number:     "0x63",
						Hash:       "0xunknown_parent",
						ParentHash: "0xunknown_grandparent",
					},
				}
				client.blocks[98] = blockTestData{
					header: outbound.BlockHeader{
						Number:     "0x62",
						Hash:       "0xunknown_grandparent",
						ParentHash: "0xunknown_great_grandparent",
					},
				}
				client.blocks[97] = blockTestData{
					header: outbound.BlockHeader{
						Number:     "0x61",
						Hash:       "0xunknown_great_grandparent",
						ParentHash: "0xunknown_great_great_grandparent",
					},
				}
			},
			setupChain: func(svc *LiveService) {
				// Our chain - completely different from the unknown chain
				svc.unfinalizedBlocks = []LightBlock{
					{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
				}
			},
			header: outbound.BlockHeader{
				Number:     "0x64", // 100
				Hash:       "0xblock100_alt",
				ParentHash: "0xunknown_parent",
			},
			blockNum:    100,
			wantErr:     true,
			errContains: "no common ancestor found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateRepo := newMockStateRepo()
			cache := memory.NewBlockCache()
			eventSink := memory.NewEventSink()
			client := newMockClient()

			if tt.setupClient != nil {
				tt.setupClient(client)
			}

			svc, err := NewLiveService(tt.config, newMockSubscriber(), client, stateRepo, cache, eventSink)
			if err != nil {
				t.Fatalf("failed to create service: %v", err)
			}

			if tt.setupChain != nil {
				tt.setupChain(svc)
			}

			_, _, _, err = svc.handleReorg(tt.header, tt.blockNum, time.Now())

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestHandleReorg_RecordsReorgEvent(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Pre-save blocks to state repo
	_ = stateRepo.SaveBlock(ctx, outbound.BlockState{Number: 99, Hash: "0xblock99"})
	_ = stateRepo.SaveBlock(ctx, outbound.BlockState{Number: 100, Hash: "0xblock100"})

	// Our chain
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97"},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Reorg at block 99
	header := outbound.BlockHeader{
		Number:     "0x63",
		Hash:       "0xblock99_new",
		ParentHash: "0xblock98",
	}

	_, _, _, err = svc.handleReorg(header, 99, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify reorg event was recorded (memory impl stores in slice)
	events, err := stateRepo.GetReorgEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to get reorg events: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 reorg event, got %d", len(events))
	}
}

// Helper function for string contains check
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============================================================================
// updateFinalizedBlock pruning tests
// ============================================================================

func TestUpdateFinalizedBlock_NoOpForLowBlockNumber(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 64,
	}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Set up chain with blocks 1-10
	for i := int64(1); i <= 10; i++ {
		svc.unfinalizedBlocks = append(svc.unfinalizedBlocks, LightBlock{
			Number: i, Hash: fmt.Sprintf("0x%d", i), ParentHash: fmt.Sprintf("0x%d", i-1),
		})
	}

	// Block 10 - 64 = -54, which is <= 0
	svc.updateFinalizedBlock(10)

	// No finalized block should be set
	if svc.finalizedBlock != nil {
		t.Error("expected no finalized block for early blocks")
	}
	// Chain should be unchanged
	if len(svc.unfinalizedBlocks) != 10 {
		t.Errorf("expected 10 blocks, got %d", len(svc.unfinalizedBlocks))
	}
}

func TestUpdateFinalizedBlock_SetsFinalizedPointer(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 10,
	}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Set up chain with blocks 1-100
	for i := int64(1); i <= 100; i++ {
		svc.unfinalizedBlocks = append(svc.unfinalizedBlocks, LightBlock{
			Number: i, Hash: fmt.Sprintf("0x%d", i), ParentHash: fmt.Sprintf("0x%d", i-1),
		})
	}

	// Process block 100 → finalized = 100 - 10 = 90
	svc.updateFinalizedBlock(100)

	if svc.finalizedBlock == nil {
		t.Fatal("expected finalized block to be set")
	}
	if svc.finalizedBlock.Number != 90 {
		t.Errorf("expected finalized block 90, got %d", svc.finalizedBlock.Number)
	}
}

func TestUpdateFinalizedBlock_PrunesOldBlocks(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 10,
	}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Set up chain with blocks 1-100
	for i := int64(1); i <= 100; i++ {
		svc.unfinalizedBlocks = append(svc.unfinalizedBlocks, LightBlock{
			Number: i, Hash: fmt.Sprintf("0x%d", i), ParentHash: fmt.Sprintf("0x%d", i-1),
		})
	}

	svc.updateFinalizedBlock(100)

	// Cutoff = 90 - 5 = 85, so blocks < 85 should be pruned
	// Blocks 85-100 should remain (16 blocks)
	if len(svc.unfinalizedBlocks) != 16 {
		t.Errorf("expected 16 blocks remaining (85-100), got %d", len(svc.unfinalizedBlocks))
	}

	// First remaining block should be 85
	if svc.unfinalizedBlocks[0].Number != 85 {
		t.Errorf("expected first block to be 85, got %d", svc.unfinalizedBlocks[0].Number)
	}
}

func TestUpdateFinalizedBlock_FinalizedBlockNotInChain(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 10,
	}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Chain missing block 90
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 89, Hash: "0x89", ParentHash: "0x88"},
		{Number: 91, Hash: "0x91", ParentHash: "0x90"},
		{Number: 100, Hash: "0x100", ParentHash: "0x99"},
	}

	svc.updateFinalizedBlock(100)

	// Finalized should remain nil since block 90 not in chain
	if svc.finalizedBlock != nil {
		t.Errorf("expected nil finalized (90 not in chain), got block %d", svc.finalizedBlock.Number)
	}
}

// ============================================================================
// restoreInMemoryChain edge case tests
// ============================================================================

func TestRestoreInMemoryChain_EmptyDB(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Start with non-empty chain to verify it gets replaced
	svc.unfinalizedBlocks = []LightBlock{{Number: 999}}

	svc.restoreInMemoryChain()

	// DB is empty, chain should remain unchanged
	if len(svc.unfinalizedBlocks) != 1 {
		t.Errorf("expected chain unchanged for empty DB, got %d blocks", len(svc.unfinalizedBlocks))
	}
}

func TestRestoreInMemoryChain_RestoresBlocks(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Seed the DB with blocks
	for i := int64(1); i <= 50; i++ {
		_ = stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%d", i),
			ParentHash: fmt.Sprintf("0x%d", i-1),
		})
	}

	svc, err := NewLiveService(LiveConfig{
		MaxUnfinalizedBlocks: 100,
		FinalityBlockCount:   10,
	}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svc.restoreInMemoryChain()

	// Should restore all 50 blocks from DB
	if len(svc.unfinalizedBlocks) != 50 {
		t.Errorf("expected 50 blocks to be restored from DB, got %d", len(svc.unfinalizedBlocks))
	}

	// Verify blocks are sorted by number ascending (1 to 50)
	for i := 0; i < len(svc.unfinalizedBlocks); i++ {
		expected := int64(i + 1)
		if svc.unfinalizedBlocks[i].Number != expected {
			t.Errorf("expected block %d at index %d, got %d", expected, i, svc.unfinalizedBlocks[i].Number)
		}
	}
}

func TestRestoreInMemoryChain_SetsFinalizedBlock(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Seed with blocks 1-100
	for i := int64(1); i <= 100; i++ {
		_ = stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%d", i),
			ParentHash: fmt.Sprintf("0x%d", i-1),
		})
	}

	svc, err := NewLiveService(LiveConfig{
		MaxUnfinalizedBlocks: 200,
		FinalityBlockCount:   10,
	}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svc.restoreInMemoryChain()

	// Finalized block should be set
	// Tip = 100, finalized = 100 - 10 = 90
	if svc.finalizedBlock == nil {
		t.Error("expected finalized block to be set")
	}
	if svc.finalizedBlock != nil && svc.finalizedBlock.Number > 90 {
		t.Errorf("expected finalized block <= 90, got %d", svc.finalizedBlock.Number)
	}
}

func TestRestoreInMemoryChain_DBError_LogsWarning(t *testing.T) {
	stateRepo := newMockStateRepo()
	stateRepo.getRecentErr = fmt.Errorf("database unavailable")

	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Add some blocks to verify they aren't cleared on error
	svc.unfinalizedBlocks = []LightBlock{{Number: 1}, {Number: 2}}

	// Should not panic, just log warning
	svc.restoreInMemoryChain()

	// Chain should remain unchanged on error
	if len(svc.unfinalizedBlocks) != 2 {
		t.Errorf("expected chain unchanged on DB error, got %d blocks", len(svc.unfinalizedBlocks))
	}
}

func TestRestoreInMemoryChain_RespectsMaxUnfinalizedBlocks(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Override GetRecentBlocks to return blocks in ascending order (matching real impl)
	stateRepo.getRecentBlocks = []outbound.BlockState{
		{Number: 98, Hash: "0x98"},
		{Number: 99, Hash: "0x99"},
		{Number: 100, Hash: "0x100"},
	}

	svc, err := NewLiveService(LiveConfig{
		MaxUnfinalizedBlocks: 50, // Limit
	}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svc.restoreInMemoryChain()

	// Should restore all 3 blocks in ascending order
	if len(svc.unfinalizedBlocks) != 3 {
		t.Errorf("expected 3 blocks, got %d", len(svc.unfinalizedBlocks))
	}

	// First should be lowest number
	if svc.unfinalizedBlocks[0].Number != 98 {
		t.Errorf("expected first block 98, got %d", svc.unfinalizedBlocks[0].Number)
	}
	// Last should be highest number
	if svc.unfinalizedBlocks[2].Number != 100 {
		t.Errorf("expected last block 100, got %d", svc.unfinalizedBlocks[2].Number)
	}
}

// ============================================================================
// processBlock error handling tests
// ============================================================================

func TestProcessBlock_Errors(t *testing.T) {
	tests := []struct {
		name        string
		header      outbound.BlockHeader
		setupMocks  func(*mockStateRepo, *mockBlockchainClient)
		setupChain  func(*LiveService)
		wantErr     bool
		errContains string
	}{
		{
			name: "invalid_block_number",
			header: outbound.BlockHeader{
				Number:     "not_a_hex",
				Hash:       "0xhash",
				ParentHash: "0xparent",
			},
			wantErr:     true,
			errContains: "failed to parse block number",
		},
		{
			name: "save_block_error",
			header: outbound.BlockHeader{
				Number:     "0x64",
				Hash:       "0xnew_hash",
				ParentHash: "0x63",
			},
			setupMocks: func(repo *mockStateRepo, client *mockBlockchainClient) {
				repo.saveBlockErr = fmt.Errorf("database write failed")
				client.addBlock(100, "")
			},
			wantErr:     true,
			errContains: "failed to save block state",
		},
		{
			name: "reorg_detection_error",
			header: outbound.BlockHeader{
				Number:     "0x64", // 100 - at finalized height
				Hash:       "0xlate",
				ParentHash: "0xunknown",
			},
			setupChain: func(svc *LiveService) {
				svc.finalizedBlock = &LightBlock{Number: 100, Hash: "0xfinalized"}
				svc.unfinalizedBlocks = []LightBlock{
					{Number: 105, Hash: "0x105", ParentHash: "0x104"},
				}
			},
			wantErr:     true,
			errContains: "reorg detection failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateRepo := newMockStateRepo()
			cache := memory.NewBlockCache()
			eventSink := memory.NewEventSink()
			client := newMockClient()

			if tt.setupMocks != nil {
				tt.setupMocks(stateRepo, client)
			}

			config := LiveConfig{}
			if tt.name == "reorg_detection_error" {
				config.FinalityBlockCount = 10
			}

			svc, err := NewLiveService(config, newMockSubscriber(), client, stateRepo, cache, eventSink)
			if err != nil {
				t.Fatalf("failed to create service: %v", err)
			}

			svc.ctx, svc.cancel = context.WithCancel(context.Background())
			defer svc.cancel()

			if tt.setupChain != nil {
				tt.setupChain(svc)
			}

			err = svc.processBlock(tt.header, time.Now())

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestProcessBlock_SkipsDuplicate(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	client.addBlock(100, "")

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	// Set context for processBlock
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Add block to memory - will be detected as duplicate
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 100, Hash: "0x64", ParentHash: "0x63"},
	}

	header := outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0x64", // Same hash as in memory
		ParentHash: "0x63",
	}

	// Should succeed but not add duplicate
	err = svc.processBlock(header, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still be 1 block (not 2)
	if len(svc.unfinalizedBlocks) != 1 {
		t.Errorf("expected 1 block (duplicate skipped), got %d", len(svc.unfinalizedBlocks))
	}
}

func TestProcessBlock_AddsBlockToChain(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	client.addBlock(100, "")
	client.addBlock(101, "")

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	// Set context for processBlock
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Start with block 100
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 100, Hash: "0x100", ParentHash: "0x99"},
	}

	// Process block 101
	header := outbound.BlockHeader{
		Number:     "0x65",
		Hash:       "0x101",
		ParentHash: "0x100",
	}

	err = svc.processBlock(header, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should now have 2 blocks
	if len(svc.unfinalizedBlocks) != 2 {
		t.Errorf("expected 2 blocks, got %d", len(svc.unfinalizedBlocks))
	}
}

// ============================================================================
// fetchAndPublishBlockData failure tests
// ============================================================================

// mockFailingClient simulates fetch failures
type mockFailingClient struct {
	*mockBlockchainClient
	failGetBlock    bool
	failGetReceipts bool
	failGetTraces   bool
	failGetBlobs    bool
}

func newMockFailingClient() *mockFailingClient {
	return &mockFailingClient{
		mockBlockchainClient: newMockClient(),
	}
}

func (m *mockFailingClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	if m.failGetBlock {
		return nil, fmt.Errorf("simulated block fetch failure")
	}
	return m.mockBlockchainClient.GetBlockByNumber(ctx, blockNum, fullTx)
}

func (m *mockFailingClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetReceipts {
		return nil, fmt.Errorf("simulated receipts fetch failure")
	}
	return m.mockBlockchainClient.GetBlockReceipts(ctx, blockNum)
}

func (m *mockFailingClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetTraces {
		return nil, fmt.Errorf("simulated traces fetch failure")
	}
	return m.mockBlockchainClient.GetBlockTraces(ctx, blockNum)
}

func (m *mockFailingClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetBlobs {
		return nil, fmt.Errorf("simulated blobs fetch failure")
	}
	return m.mockBlockchainClient.GetBlobSidecars(ctx, blockNum)
}

// mockFailingCache simulates cache failures
type mockFailingCache struct {
	*memory.BlockCache
	failSetBlock    bool
	failSetReceipts bool
	failSetTraces   bool
	failSetBlobs    bool
}

func newMockFailingCache() *mockFailingCache {
	return &mockFailingCache{
		BlockCache: memory.NewBlockCache(),
	}
}

func (c *mockFailingCache) SetBlock(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	if c.failSetBlock {
		return fmt.Errorf("simulated cache block failure")
	}
	return c.BlockCache.SetBlock(ctx, chainID, blockNum, version, data)
}

func (c *mockFailingCache) SetReceipts(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	if c.failSetReceipts {
		return fmt.Errorf("simulated cache receipts failure")
	}
	return c.BlockCache.SetReceipts(ctx, chainID, blockNum, version, data)
}

func (c *mockFailingCache) SetTraces(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	if c.failSetTraces {
		return fmt.Errorf("simulated cache traces failure")
	}
	return c.BlockCache.SetTraces(ctx, chainID, blockNum, version, data)
}

func (c *mockFailingCache) SetBlobs(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	if c.failSetBlobs {
		return fmt.Errorf("simulated cache blobs failure")
	}
	return c.BlockCache.SetBlobs(ctx, chainID, blockNum, version, data)
}

// mockFailingEventSink simulates publish failures
type mockFailingEventSink struct {
	*memory.EventSink
	failPublish bool
}

func newMockFailingEventSink() *mockFailingEventSink {
	return &mockFailingEventSink{
		EventSink: memory.NewEventSink(),
	}
}

func (s *mockFailingEventSink) Publish(ctx context.Context, event outbound.Event) error {
	if s.failPublish {
		return fmt.Errorf("simulated publish failure")
	}
	return s.EventSink.Publish(ctx, event)
}

func TestFetchAndPublishBlockData_ErrorHandling(t *testing.T) {
	tests := []struct {
		name              string
		failGetBlock      bool
		failGetReceipts   bool
		failGetTraces     bool
		failGetBlobs      bool
		failCacheBlock    bool
		failCacheReceipts bool
		failCacheTraces   bool
		failCacheBlobs    bool
		failPublish       bool
		disableBlobs      bool
		wantErr           bool
		errContains       string
	}{
		{
			name:    "all_succeed",
			wantErr: false,
		},
		{
			name:         "block_fetch_fails",
			failGetBlock: true,
			wantErr:      true,
			errContains:  "failed to fetch block",
		},
		{
			name:            "receipts_fetch_fails",
			failGetReceipts: true,
			wantErr:         true,
			errContains:     "failed to fetch receipts",
		},
		{
			name:          "traces_fetch_fails",
			failGetTraces: true,
			wantErr:       true,
			errContains:   "failed to fetch traces",
		},
		{
			name:         "blobs_fetch_fails",
			failGetBlobs: true,
			wantErr:      true,
			errContains:  "failed to fetch blobs",
		},
		{
			name:           "block_cache_fails",
			failCacheBlock: true,
			wantErr:        true,
			errContains:    "failed to cache block",
		},
		{
			name:              "receipts_cache_fails",
			failCacheReceipts: true,
			wantErr:           true,
			errContains:       "failed to cache receipts",
		},
		{
			name:            "traces_cache_fails",
			failCacheTraces: true,
			wantErr:         true,
			errContains:     "failed to cache traces",
		},
		{
			name:           "blobs_cache_fails",
			failCacheBlobs: true,
			wantErr:        true,
			errContains:    "failed to cache blobs",
		},
		{
			name:        "publish_fails",
			failPublish: true,
			wantErr:     true,
			errContains: "failed to publish",
		},
		{
			name:         "blobs_disabled_blobs_error_ignored",
			failGetBlobs: true,
			disableBlobs: true,
			wantErr:      false,
		},
		{
			name:            "multiple_failures",
			failGetBlock:    true,
			failGetReceipts: true,
			failGetTraces:   true,
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			stateRepo := newMockStateRepo()

			client := newMockFailingClient()
			client.failGetBlock = tt.failGetBlock
			client.failGetReceipts = tt.failGetReceipts
			client.failGetTraces = tt.failGetTraces
			client.failGetBlobs = tt.failGetBlobs
			client.addBlock(100, "")

			cache := newMockFailingCache()
			cache.failSetBlock = tt.failCacheBlock
			cache.failSetReceipts = tt.failCacheReceipts
			cache.failSetTraces = tt.failCacheTraces
			cache.failSetBlobs = tt.failCacheBlobs

			eventSink := newMockFailingEventSink()
			eventSink.failPublish = tt.failPublish

			svc, err := NewLiveService(LiveConfig{
				DisableBlobs: tt.disableBlobs,
			}, newMockSubscriber(), client, stateRepo, cache, eventSink)
			if err != nil {
				t.Fatalf("failed to create service: %v", err)
			}

			header := outbound.BlockHeader{
				Number:     "0x64",
				Hash:       "0xhash",
				ParentHash: "0xparent",
				Timestamp:  "0x0",
			}

			err = svc.fetchAndPublishBlockData(ctx, header, 100, time.Now(), false)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestFetchAndPublishBlockData_WithBlobs_PublishesBlobEvent(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockFailingClient()
	client.addBlock(100, "")

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: false, // Enable blobs
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	header := outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0xhash",
		ParentHash: "0xparent",
		Timestamp:  "0x0",
	}

	err = svc.fetchAndPublishBlockData(ctx, header, 100, time.Now(), false)
	if err != nil {
		t.Fatalf("fetchAndPublishBlockData failed: %v", err)
	}

	events := eventSink.GetEvents()
	hasBlobs := false
	for _, e := range events {
		if _, ok := e.(outbound.BlobsEvent); ok {
			hasBlobs = true
		}
	}

	if !hasBlobs {
		t.Error("expected BlobsEvent when blobs enabled")
	}
}

func TestFetchAndPublishBlockData_ReorgFlag_SetsIsReorg(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockFailingClient()
	client.addBlock(100, "")

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: true,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	header := outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0xhash",
		ParentHash: "0xparent",
		Timestamp:  "0x0",
	}

	err = svc.fetchAndPublishBlockData(ctx, header, 100, time.Now(), true) // isReorg = true
	if err != nil {
		t.Fatalf("fetchAndPublishBlockData failed: %v", err)
	}

	events := eventSink.GetEvents()
	for _, e := range events {
		if be, ok := e.(outbound.BlockEvent); ok {
			if !be.IsReorg {
				t.Error("expected BlockEvent.IsReorg to be true")
			}
		}
	}
}

// ============================================================================
// parseBlockNumber edge case tests
// ============================================================================

func TestParseBlockNumber_ValidHex(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0x0", 0},
		{"0x1", 1},
		{"0xa", 10},
		{"0xf", 15},
		{"0x10", 16},
		{"0x64", 100},
		{"0x3e8", 1000},
		{"0x186a0", 100000},
		{"0xffffffffff", 1099511627775}, // Large number
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result, err := parseBlockNumber(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}

func TestParseBlockNumber_WithoutPrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0", 0},
		{"1", 1},
		{"a", 10},
		{"64", 100},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result, err := parseBlockNumber(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}

func TestParseBlockNumber_InvalidHex(t *testing.T) {
	tests := []string{
		"not_hex",
		"0xGHI",
		"xyz",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := parseBlockNumber(input)
			if err == nil {
				t.Errorf("expected error for invalid input: %s", input)
			}
		})
	}
}

func TestParseBlockNumber_EmptyString(t *testing.T) {
	_, err := parseBlockNumber("")
	if err == nil {
		t.Error("expected error for empty string")
	}
}

func TestParseBlockNumber_UppercaseHex(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0xA", 10},
		{"0xF", 15},
		{"0xFF", 255},
		{"0xABCD", 43981},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result, err := parseBlockNumber(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}

// ============================================================================
// Start/Stop lifecycle tests
// ============================================================================

func TestStart_SubscribeError_ReturnsError(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	subscriber := &failingSubscriber{err: fmt.Errorf("subscribe failed")}

	svc, err := NewLiveService(LiveConfig{}, subscriber, newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		t.Error("expected error when subscribe fails")
	}
	if err != nil && !contains(err.Error(), "failed to subscribe") {
		t.Errorf("unexpected error: %v", err)
	}
}

type failingSubscriber struct {
	err error
}

func (f *failingSubscriber) Subscribe(ctx context.Context) (<-chan outbound.BlockHeader, error) {
	return nil, f.err
}

func (f *failingSubscriber) Unsubscribe() error {
	return nil
}

func (f *failingSubscriber) HealthCheck(ctx context.Context) error {
	return nil
}

func (f *failingSubscriber) SetOnReconnect(callback func()) {}

func TestStart_Success_SetsContext(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// ctx should be nil before start
	if svc.ctx != nil {
		t.Error("expected ctx to be nil before Start")
	}

	err = svc.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	// ctx should be set after start
	if svc.ctx == nil {
		t.Error("expected ctx to be set after Start")
	}
	if svc.cancel == nil {
		t.Error("expected cancel to be set after Start")
	}
}

func TestStop_CancelsContext(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := svc.ctx

	err = svc.Stop()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Context should be cancelled
	select {
	case <-ctx.Done():
		// Expected
	default:
		t.Error("expected context to be cancelled after Stop")
	}
}

func TestStop_BeforeStart_NoError(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Stop before start should not panic
	err = svc.Stop()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStart_RestoresChainFromDB(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate DB with blocks
	for i := int64(1); i <= 10; i++ {
		_ = stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number: i,
			Hash:   fmt.Sprintf("0x%d", i),
		})
	}

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Chain should be empty before start
	if len(svc.unfinalizedBlocks) != 0 {
		t.Errorf("expected empty chain before start, got %d blocks", len(svc.unfinalizedBlocks))
	}

	err = svc.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	// Chain should be restored from DB with all 10 blocks
	if len(svc.unfinalizedBlocks) != 10 {
		t.Errorf("expected 10 blocks to be restored from DB, got %d", len(svc.unfinalizedBlocks))
	}
}
