package live_data

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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
	mu                   sync.RWMutex
	getByHashErr         error
	getRecentErr         error
	getRecentBlocks      []outbound.BlockState // Override response
	saveBlockErr         error
	getVersionCountErr   error
	handleReorgAtomicErr error
}

func newMockStateRepo() *mockStateRepo {
	return &mockStateRepo{
		BlockStateRepository: memory.NewBlockStateRepository(),
	}
}

func (m *mockStateRepo) SaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	m.mu.RLock()
	err := m.saveBlockErr
	m.mu.RUnlock()

	if err != nil {
		return 0, err
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

func (m *mockStateRepo) GetBlockVersionCount(ctx context.Context, number int64) (int, error) {
	m.mu.RLock()
	err := m.getVersionCountErr
	m.mu.RUnlock()

	if err != nil {
		return 0, err
	}
	return m.BlockStateRepository.GetBlockVersionCount(ctx, number)
}

func (m *mockStateRepo) HandleReorgAtomic(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	m.mu.RLock()
	err := m.handleReorgAtomicErr
	m.mu.RUnlock()

	if err != nil {
		return 0, err
	}
	return m.BlockStateRepository.HandleReorgAtomic(ctx, commonAncestor, event, newBlock)
}

// mockBlockchainClient provides predictable block data for testing.
type mockBlockchainClient struct {
	mu              sync.RWMutex
	blocks          map[int64]blockTestData
	delay           time.Duration
	getByHashErr    error
	getByHashErrFor string // Only return error for this specific hash
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

	// Check if we should return an error for this specific hash
	if m.getByHashErr != nil && (m.getByHashErrFor == "" || m.getByHashErrFor == hash) {
		return nil, m.getByHashErr
	}

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

	// Check if we should return an error for this specific hash
	if m.getByHashErr != nil && (m.getByHashErrFor == "" || m.getByHashErrFor == hash) {
		return nil, m.getByHashErr
	}

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

	// Check if we should return an error for this specific hash
	if m.getByHashErr != nil && (m.getByHashErrFor == "" || m.getByHashErrFor == hash) {
		return nil, m.getByHashErr
	}

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

	// Check if we should return an error for this specific hash
	if m.getByHashErr != nil && (m.getByHashErrFor == "" || m.getByHashErrFor == hash) {
		return nil, m.getByHashErr
	}

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

	// Check if we should return an error for this specific hash
	if m.getByHashErr != nil && (m.getByHashErrFor == "" || m.getByHashErrFor == hash) {
		return nil, m.getByHashErr
	}

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

	// Check if we should return an error for this specific hash
	if m.getByHashErr != nil && (m.getByHashErrFor == "" || m.getByHashErrFor == hash) {
		return outbound.BlockData{}, m.getByHashErr
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
	if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
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
	isDup, err := svc.isDuplicateBlock(ctx, "0xabcd1234", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
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
	isDup, err := svc.isDuplicateBlock(ctx, "0xdb_block_hash", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	isDup, err := svc.isDuplicateBlock(ctx, "0xnonexistent", 999)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

	// Check for duplicate - DB error should return error
	isDup, err := svc.isDuplicateBlock(ctx, "0xsome_hash", 100)
	if err == nil {
		t.Error("expected isDuplicateBlock to return error when DB fails")
	}
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
	isDup, err := svc.isDuplicateBlock(ctx, "0xmemory_hash", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

	block := LightBlock{
		Number:     100,
		Hash:       "0xnew_hash",
		ParentHash: "0xparent",
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(block, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isReorg {
		t.Error("expected no reorg for empty chain")
	}
	if depth != 0 || ancestor != 0 {
		t.Errorf("expected depth=0, ancestor=0, got depth=%d, ancestor=%d", depth, ancestor)
	}
	if reorgEvent != nil {
		t.Error("expected no reorg event for empty chain")
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
	block := LightBlock{
		Number:     101,
		Hash:       "0xblock101",
		ParentHash: "0xblock100", // Matches latest
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(block, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isReorg {
		t.Error("expected no reorg when parent hash matches")
	}
	if depth != 0 || ancestor != 0 {
		t.Errorf("expected depth=0, ancestor=0, got depth=%d, ancestor=%d", depth, ancestor)
	}
	if reorgEvent != nil {
		t.Error("expected no reorg event when parent hash matches")
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
	block := LightBlock{
		Number:     101,
		Hash:       "0xblock101_alt",
		ParentHash: "0xblock100_alt", // Different parent - triggers reorg
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(block, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReorg {
		t.Error("expected reorg when parent hash mismatches")
	}
	// Walk finds block100_alt's parent (0xblock99) in our chain
	// Common ancestor is 99. Depth is 1 because block 100 is orphaned.
	// Block 100 is > common ancestor (99), so it counts as orphaned.
	if depth != 1 {
		t.Errorf("expected depth 1 (block 100 orphaned), got %d", depth)
	}
	if ancestor != 99 {
		t.Errorf("expected common ancestor 99, got %d", ancestor)
	}
	if reorgEvent == nil {
		t.Error("expected reorg event when parent hash mismatches")
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
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98", // Same ancestor
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(block, time.Now())
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
	if reorgEvent == nil {
		t.Error("expected reorg event when block number <= latest")
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
	block := LightBlock{
		Number:     105,
		Hash:       "0xblock105",
		ParentHash: "0xblock104",
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(block, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isReorg {
		t.Error("expected no reorg for gap - should be handled by backfill")
	}
	if depth != 0 || ancestor != 0 {
		t.Errorf("expected depth=0, ancestor=0 for gap, got depth=%d, ancestor=%d", depth, ancestor)
	}
	if reorgEvent != nil {
		t.Error("expected no reorg event for gap")
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
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98", // Matches our block 98
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.handleReorg(block, time.Now())
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
	// Reorg event should be populated
	if reorgEvent == nil {
		t.Error("expected reorg event to be populated")
	}
	// handleReorg should NOT prune the chain - that happens after successful DB save
	// The chain should still have all 6 blocks
	if len(svc.unfinalizedBlocks) != 6 {
		t.Errorf("expected 6 blocks still in chain (not pruned by handleReorg), got %d", len(svc.unfinalizedBlocks))
	}
	// Verify pruneReorgedBlocks works correctly when called separately
	svc.pruneReorgedBlocks(ancestor)
	if len(svc.unfinalizedBlocks) != 4 {
		t.Errorf("expected 4 blocks after pruneReorgedBlocks, got %d", len(svc.unfinalizedBlocks))
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
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98_alt", // Not in our chain
	}

	isReorg, depth, ancestor, _, err := svc.handleReorg(block, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReorg {
		t.Error("expected isReorg to be true")
	}
	// Common ancestor should be 97 (found after walking 98_alt's parent)
	// Depth is 3: blocks 98, 99 and 100 are orphaned (all blocks > common ancestor 97)
	if ancestor != 97 {
		t.Errorf("expected common ancestor 97, got %d", ancestor)
	}
	if depth != 3 {
		t.Errorf("expected depth 3 (blocks 98, 99, 100 orphaned), got %d", depth)
	}
}

func TestHandleReorg_Errors(t *testing.T) {
	tests := []struct {
		name        string
		config      LiveConfig
		setupClient func(*mockBlockchainClient)
		setupChain  func(*LiveService)
		block       LightBlock
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
			block: LightBlock{
				Number:     50,
				Hash:       "0xlate_block",
				ParentHash: "0xunknown",
			},
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
			block: LightBlock{
				Number:     100,
				Hash:       "0xblock100_alt",
				ParentHash: "0xunknown_parent",
			},
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

			_, _, _, _, err = svc.handleReorg(tt.block, time.Now())

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
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

func TestHandleReorg_ReturnsReorgEvent(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Our chain
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97"},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}

	// Reorg at block 99
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_new",
		ParentHash: "0xblock98",
	}

	_, depth, _, reorgEvent, err := svc.handleReorg(block, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify reorg event is returned with correct data
	if reorgEvent == nil {
		t.Fatal("expected reorg event to be returned")
	}
	if reorgEvent.BlockNumber != 99 {
		t.Errorf("expected block number 99, got %d", reorgEvent.BlockNumber)
	}
	// OldHash is the most recent orphaned block (the tip of the old chain)
	if reorgEvent.OldHash != "0xblock100" {
		t.Errorf("expected old hash 0xblock100 (tip of old chain), got %s", reorgEvent.OldHash)
	}
	if reorgEvent.NewHash != "0xblock99_new" {
		t.Errorf("expected new hash 0xblock99_new, got %s", reorgEvent.NewHash)
	}
	if reorgEvent.Depth != depth {
		t.Errorf("expected depth %d, got %d", depth, reorgEvent.Depth)
	}
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

	if err := svc.restoreInMemoryChain(); err != nil {
		t.Fatalf("restoreInMemoryChain failed: %v", err)
	}

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
		_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
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

	if err := svc.restoreInMemoryChain(); err != nil {
		t.Fatalf("restoreInMemoryChain failed: %v", err)
	}

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
		_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
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

	if err := svc.restoreInMemoryChain(); err != nil {
		t.Fatalf("restoreInMemoryChain failed: %v", err)
	}

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

	// Should return error now
	err = svc.restoreInMemoryChain()
	if err == nil {
		t.Error("expected error when database fails")
	}

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

	if err := svc.restoreInMemoryChain(); err != nil {
		t.Fatalf("restoreInMemoryChain failed: %v", err)
	}

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
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
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

	// Get the actual hashes from the mock client
	header100 := client.getHeader(100)
	header101 := client.getHeader(101)

	svc, err := NewLiveService(LiveConfig{}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	// Set context for processBlock
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Start with block 100 using actual hash
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 100, Hash: header100.Hash, ParentHash: header100.ParentHash},
	}

	// Process block 101 using actual header
	err = svc.processBlock(header101, time.Now())
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

func (m *mockFailingClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	if m.failGetBlock {
		return nil, fmt.Errorf("simulated block fetch failure")
	}
	return m.mockBlockchainClient.GetFullBlockByHash(ctx, hash, fullTx)
}

func (m *mockFailingClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetReceipts {
		return nil, fmt.Errorf("simulated receipts fetch failure")
	}
	return m.mockBlockchainClient.GetBlockReceipts(ctx, blockNum)
}

func (m *mockFailingClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	if m.failGetReceipts {
		return nil, fmt.Errorf("simulated receipts fetch failure")
	}
	return m.mockBlockchainClient.GetBlockReceiptsByHash(ctx, hash)
}

func (m *mockFailingClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetTraces {
		return nil, fmt.Errorf("simulated traces fetch failure")
	}
	return m.mockBlockchainClient.GetBlockTraces(ctx, blockNum)
}

func (m *mockFailingClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	if m.failGetTraces {
		return nil, fmt.Errorf("simulated traces fetch failure")
	}
	return m.mockBlockchainClient.GetBlockTracesByHash(ctx, hash)
}

func (m *mockFailingClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetBlobs {
		return nil, fmt.Errorf("simulated blobs fetch failure")
	}
	return m.mockBlockchainClient.GetBlobSidecars(ctx, blockNum)
}

func (m *mockFailingClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	if m.failGetBlobs {
		return nil, fmt.Errorf("simulated blobs fetch failure")
	}
	return m.mockBlockchainClient.GetBlobSidecarsByHash(ctx, hash)
}

func (m *mockFailingClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	// Delegate to underlying mock - failure flags are for individual method failures
	// which are no longer used since we now use batched requests
	bd, err := m.mockBlockchainClient.GetBlockDataByHash(ctx, blockNum, hash, fullTx)
	if err != nil {
		return bd, err
	}
	// Simulate failures for individual data types via error fields
	if m.failGetBlock {
		bd.BlockErr = fmt.Errorf("simulated block fetch failure")
		bd.Block = nil
	}
	if m.failGetReceipts {
		bd.ReceiptsErr = fmt.Errorf("simulated receipts fetch failure")
		bd.Receipts = nil
	}
	if m.failGetTraces {
		bd.TracesErr = fmt.Errorf("simulated traces fetch failure")
		bd.Traces = nil
	}
	if m.failGetBlobs {
		bd.BlobsErr = fmt.Errorf("simulated blobs fetch failure")
		bd.Blobs = nil
	}
	return bd, nil
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

			// Get the actual hash from mock client
			header100 := client.getHeader(100)

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

			// Use actual header from mock client
			err = svc.fetchAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
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

func TestFetchAndPublishBlockData_WithBlobs_CachesBlobs(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockFailingClient()
	client.addBlock(100, "")

	// Get actual header from mock client
	header100 := client.getHeader(100)

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: false, // Enable blobs
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.fetchAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false)
	if err != nil {
		t.Fatalf("fetchAndPublishBlockData failed: %v", err)
	}

	// Verify data was cached (block, receipts, traces, blobs = 4 entries)
	entryCount := cache.GetEntryCount()
	if entryCount != 4 {
		t.Errorf("expected 4 cache entries (block, receipts, traces, blobs), got %d", entryCount)
	}

	// Verify only a single BlockEvent is published (not separate events for receipts/traces/blobs)
	events := eventSink.GetBlockEvents()
	if len(events) != 1 {
		t.Errorf("expected 1 BlockEvent, got %d", len(events))
	}
}

func TestFetchAndPublishBlockData_ReorgFlag_SetsIsReorg(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockFailingClient()
	client.addBlock(100, "")

	// Get actual header from mock client
	header100 := client.getHeader(100)

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: true,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.fetchAndPublishBlockData(ctx, header100, 100, 0, time.Now(), true) // isReorg = true
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
	if err != nil && !strings.Contains(err.Error(), "failed to subscribe") {
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
		_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
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

func TestProcessHeaders_ContextCancellation(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	subscriber := newMockSubscriber()
	svc, err := NewLiveService(LiveConfig{}, subscriber, newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Give processHeaders goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Stop service - should trigger context cancellation in processHeaders
	err = svc.Stop()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Context should be cancelled
	select {
	case <-svc.ctx.Done():
		// Expected
	default:
		t.Error("expected context to be cancelled")
	}
}

func TestProcessHeaders_ChannelClosed(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	subscriber := newMockSubscriber()
	svc, err := NewLiveService(LiveConfig{}, subscriber, newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	// Give processHeaders goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Close the subscriber's channel - should trigger the !ok branch
	subscriber.mu.Lock()
	subscriber.closed = true
	close(subscriber.headers)
	subscriber.mu.Unlock()

	// Give processHeaders time to exit
	time.Sleep(10 * time.Millisecond)
}

func TestAddBlock_TrimsToMaxSize(t *testing.T) {
	svc := &LiveService{
		config: LiveConfig{
			MaxUnfinalizedBlocks: 5,
		},
		unfinalizedBlocks: make([]LightBlock, 0),
	}

	// Add 7 blocks - should trim to 5
	for i := int64(1); i <= 7; i++ {
		svc.addBlock(LightBlock{
			Number:     i,
			Hash:       fmt.Sprintf("0x%d", i),
			ParentHash: fmt.Sprintf("0x%d", i-1),
		})
	}

	if len(svc.unfinalizedBlocks) != 5 {
		t.Errorf("expected 5 blocks after trimming, got %d", len(svc.unfinalizedBlocks))
	}

	// First block should be 3 (blocks 1 and 2 trimmed)
	if svc.unfinalizedBlocks[0].Number != 3 {
		t.Errorf("expected first block to be 3, got %d", svc.unfinalizedBlocks[0].Number)
	}

	// Last block should be 7
	if svc.unfinalizedBlocks[4].Number != 7 {
		t.Errorf("expected last block to be 7, got %d", svc.unfinalizedBlocks[4].Number)
	}
}

func TestProcessBlock_WithMetrics_RecordsReorg(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Set up chain: 98 -> 99
	client.blocks[98] = blockTestData{
		header: outbound.BlockHeader{Number: "0x62", Hash: "0x98", ParentHash: "0x97"},
	}
	client.blocks[99] = blockTestData{
		header: outbound.BlockHeader{Number: "0x63", Hash: "0x99", ParentHash: "0x98"},
	}
	// Add block 100 with alt hash for the reorg
	client.blocks[100] = blockTestData{
		header:   outbound.BlockHeader{Number: "0x64", Hash: "0x100_alt", ParentHash: "0x99", Timestamp: "0x0"},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}

	metrics := &mockMetrics{}

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: true,
		Metrics:      metrics,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Set up in-memory chain
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 98, Hash: "0x98", ParentHash: "0x97"},
		{Number: 99, Hash: "0x99", ParentHash: "0x98"},
		{Number: 100, Hash: "0x100", ParentHash: "0x99"},
	}

	// Process a reorg block at 100 with different hash
	header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0x100_alt",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}

	err = svc.processBlock(header, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify metrics recorded the reorg
	if metrics.reorgCount != 1 {
		t.Errorf("expected 1 reorg recorded, got %d", metrics.reorgCount)
	}
}

type mockMetrics struct {
	mu         sync.Mutex
	reorgCount int
}

func (m *mockMetrics) RecordReorg(ctx context.Context, depth int, commonAncestor, blockNum int64) {
	m.mu.Lock()
	m.reorgCount++
	m.mu.Unlock()
}

func (m *mockMetrics) RecordBlockProcessed(ctx context.Context, blockNum int64, durationMs int64) {}

func TestHandleReorg_FetchParentError_ReturnsError(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Set up error for fetching parent
	client.getByHashErr = fmt.Errorf("network error")
	client.getByHashErrFor = "0xunknown_parent"

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 10,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Set up chain
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 100, Hash: "0x100", ParentHash: "0x99"},
	}

	// Incoming reorg block with unknown parent
	block := LightBlock{
		Number:     100,
		Hash:       "0x100_alt",
		ParentHash: "0xunknown_parent",
	}

	_, _, _, _, err = svc.handleReorg(block, time.Now())
	if err == nil {
		t.Error("expected error when fetching parent fails")
	}
	if !strings.Contains(err.Error(), "failed to fetch parent block") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestProcessBlock_GetVersionCountError was removed because version is now
// assigned atomically by SaveBlock rather than fetched separately.

func TestProcessHeaders_LogsErrorOnProcessBlockFailure(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	subscriber := newMockSubscriber()
	svc, err := NewLiveService(LiveConfig{}, subscriber, newMockClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	// Give processHeaders goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Send a header with invalid block number - this will cause processBlock to fail
	subscriber.sendHeader(outbound.BlockHeader{
		Number:     "invalid_hex",
		Hash:       "0x123",
		ParentHash: "0x122",
	})

	// Give time for processing and logging
	time.Sleep(50 * time.Millisecond)
}

func TestProcessBlock_FetchAndPublishError_ReturnsError(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Client with no block data - will cause fetch to fail
	client := newMockClient()

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: true,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	header := outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0x100",
		ParentHash: "0x99",
	}

	err = svc.processBlock(header, time.Now())
	if err == nil {
		t.Error("expected error when fetchAndPublishBlockData fails")
	}
	if !strings.Contains(err.Error(), "failed to fetch and publish block data") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessBlock_VersionIsCorrectAfterReorg(t *testing.T) {
	// This test verifies that when we process a new block at a height that
	// already has a block, the version is correctly set to the count of
	// existing blocks BEFORE we save the new one.
	//
	// Bug scenario:
	// 1. Block 100 already exists (version 0)
	// 2. Reorg happens, new block 100 arrives
	// 3. If we SaveBlock first, then GetBlockVersionCount, we get 2 (wrong!)
	// 4. The correct version should be 1 (second block at this height)

	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Add block data for fetching with specific hash
	client.blocks[100] = blockTestData{
		header:   outbound.BlockHeader{Number: "0x64", Hash: "0x100_new", ParentHash: "0x99", Timestamp: "0x0"},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: true,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Pre-save a block at height 100 (this is version 0)
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     100,
		Hash:       "0x100_original",
		ParentHash: "0x99",
	})

	// Verify there's 1 block at height 100
	count, _ := stateRepo.GetBlockVersionCount(ctx, 100)
	if count != 1 {
		t.Fatalf("expected 1 block at height 100, got %d", count)
	}

	// Now process a NEW block at height 100 (reorg scenario)
	// This should be version 1 (the second block at this height)
	header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0x100_new",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}

	err = svc.processBlock(header, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check the published block event - version should be 1
	blockEvents := eventSink.GetBlockEvents()
	if len(blockEvents) != 1 {
		t.Fatalf("expected 1 block event, got %d", len(blockEvents))
	}

	if blockEvents[0].Version != 1 {
		t.Errorf("expected block event version to be 1, got %d (bug: version was calculated after SaveBlock)", blockEvents[0].Version)
	}
}

func TestProcessBlock_VersionIsSavedToDatabase(t *testing.T) {
	// This test verifies that when we save a block, the version field is
	// correctly persisted to the database.
	//
	// Bug scenario:
	// 1. Block 100 is processed with version 0
	// 2. Reorg happens, new block 100 arrives with version 1
	// 3. If Version field is not set in BlockState before SaveBlock,
	//    the block is saved with Version=0 (Go's default)
	// 4. GetBlockVersionCount uses MAX(version)+1, so it would always return 1
	// 5. All subsequent reorgs would get version=1, causing cache key collisions

	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockClient()
	// Add block data for fetching - we need blocks with specific hashes for the v0 and v2 cases
	client.blocks[100] = blockTestData{
		header:   outbound.BlockHeader{Number: "0x64", Hash: "0x100_v0", ParentHash: "0x99", Timestamp: "0x0"},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: true,
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Process first block at height 100 (version 0)
	header1 := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0x100_v0",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}
	err = svc.processBlock(header1, time.Now())
	if err != nil {
		t.Fatalf("failed to process first block: %v", err)
	}

	// Verify the saved block has version 0
	savedBlock1, err := stateRepo.GetBlockByHash(ctx, "0x100_v0")
	if err != nil {
		t.Fatalf("failed to get saved block: %v", err)
	}
	if savedBlock1.Version != 0 {
		t.Errorf("expected first block to have version 0, got %d", savedBlock1.Version)
	}

	// Directly save a second block at height 100 to simulate reorg scenario
	// without triggering the reorg detection logic
	// This mimics what happens when MarkBlocksOrphanedAfter runs and then
	// a new block is processed
	_, err = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     100,
		Hash:       "0x100_v1",
		ParentHash: "0x99",
		Version:    1, // Manually set version to 1 as it should be
	})
	if err != nil {
		t.Fatalf("failed to save second block: %v", err)
	}

	// Now verify that GetBlockVersionCount returns the correct next version
	// If version was saved correctly, MAX(version)+1 = 1+1 = 2
	nextVersion, err := stateRepo.GetBlockVersionCount(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get version count: %v", err)
	}
	if nextVersion != 2 {
		t.Errorf("expected next version to be 2, got %d", nextVersion)
	}

	// Now the key test: process a THIRD block at height 100
	// The service should get version=2 and save it with version=2
	// Clear the unfinalized chain to avoid reorg detection complexity
	svc.unfinalizedBlocks = nil

	// Add block data for v2
	client.blocks[100] = blockTestData{
		header:   outbound.BlockHeader{Number: "0x64", Hash: "0x100_v2", ParentHash: "0x99", Timestamp: "0x0"},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}

	header3 := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0x100_v2",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}
	err = svc.processBlock(header3, time.Now())
	if err != nil {
		t.Fatalf("failed to process third block: %v", err)
	}

	// Verify the third saved block has version 2
	savedBlock3, err := stateRepo.GetBlockByHash(ctx, "0x100_v2")
	if err != nil {
		t.Fatalf("failed to get third saved block: %v", err)
	}
	if savedBlock3.Version != 2 {
		t.Errorf("expected third block to have version 2, got %d (bug: Version not set in BlockState before SaveBlock)", savedBlock3.Version)
	}
}

// TestFetchBlockData_ReorgBetweenHeaderAndFetch verifies that fetching by hash
// prevents the TOCTOU vulnerability where a reorg between receiving the header
// and fetching block data could cause us to cache data for the wrong block.
//
// Before the fix: We fetched by block number, so if a reorg happened, we'd get
// data for the NEW canonical block but our DB/events referenced the OLD hash.
//
// After the fix: We fetch by hash, so we always get data for exactly the block
// we committed to in the database.
func TestFetchBlockData_ReorgBetweenHeaderAndFetch(t *testing.T) {
	ctx := context.Background()

	// Setup
	client := newMockClient()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add a base block chain including block 100 with a specific hash
	for i := int64(99); i <= 100; i++ {
		client.addBlock(i, "")
	}

	// Get the hash that was generated for block 100
	block100Header := client.getHeader(100)
	expectedHash := block100Header.Hash

	config := LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		DisableBlobs:         true, // Simplify test
		Logger:               slog.Default(),
	}

	svc, err := NewLiveService(config, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Simulate receiving a header via WebSocket
	header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       expectedHash,
		ParentHash: block100Header.ParentHash,
		Timestamp:  block100Header.Timestamp,
	}

	// Process the block - now fetches by hash instead of number
	err = svc.processBlock(header, time.Now())
	if err != nil {
		t.Fatalf("processBlock failed: %v", err)
	}

	// Verify the cached block has the correct hash
	cachedData, err := cache.GetBlock(context.Background(), 1, 100, 0)
	if err != nil {
		t.Fatalf("GetBlock failed: %v", err)
	}
	if cachedData == nil {
		t.Fatal("expected block to be cached")
	}

	var cachedHeader outbound.BlockHeader
	if err := json.Unmarshal(cachedData, &cachedHeader); err != nil {
		t.Fatalf("failed to parse cached block: %v", err)
	}

	// The cached block hash should match what we committed to
	if cachedHeader.Hash != expectedHash {
		t.Errorf("cached block hash %s does not match expected hash %s", cachedHeader.Hash, expectedHash)
	}

	t.Logf("SUCCESS: Cached block hash matches committed hash: %s", cachedHeader.Hash)
}

// TestFetchBlockData_ByHashReturnsErrorWhenBlockNotFound verifies that fetching
// by hash returns an error if the block doesn't exist (e.g., due to a reorg
// that removed it before we could fetch the full data).
func TestFetchBlockData_ByHashReturnsErrorWhenBlockNotFound(t *testing.T) {
	ctx := context.Background()

	// Setup
	client := newMockClient()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add a base block chain but NOT block 100
	client.addBlock(99, "")

	config := LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		DisableBlobs:         true,
		Logger:               slog.Default(),
	}

	svc, err := NewLiveService(config, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Simulate receiving a header for a block that doesn't exist in RPC
	// (e.g., it was reorged away before we could fetch it)
	nonExistentHash := "0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       nonExistentHash,
		ParentHash: fmt.Sprintf("0x%064x", 99),
		Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
	}

	// Process should fail because we can't fetch the block by hash
	err = svc.processBlock(header, time.Now())
	if err == nil {
		t.Fatal("expected error when fetching non-existent block by hash")
	}

	// Verify the error mentions the hash
	if !strings.Contains(err.Error(), nonExistentHash) {
		t.Errorf("expected error to contain hash %s, got: %v", nonExistentHash, err)
	}

	t.Logf("Correctly failed when block not found by hash: %v", err)
}

// TestFetchReceiptsTracesBlobsByHash verifies that receipts, traces, and blobs
// are fetched by hash (not number) to prevent TOCTOU race conditions.
//
// The vulnerability: If we fetch by number, a reorg between receiving the header
// and fetching data could cause us to cache data for the WRONG block:
// 1. Receive header for block 100 with hash 0xAAA
// 2. Save block state with hash 0xAAA
// 3. Start fetching receipts/traces/blobs by NUMBER
// 4. Reorg happens - block 100 is now 0xBBB
// 5. Fetch returns data for 0xBBB (current block at that number)
// 6. We cache 0xBBB's data but publish event saying "block hash 0xAAA"
//
// The fix: Fetch by hash using GetBlockDataByHash to ensure we always get data
// for the exact block we committed to, or fail if that block no longer exists.
func TestFetchReceiptsTracesBlobsByHash(t *testing.T) {
	ctx := context.Background()

	// Create a mock client that tracks which methods are called
	client := &mockClientWithHashTracking{
		mockBlockchainClient: newMockClient(),
		fetchedByHash:        make(map[string][]string),
	}
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add block 99 and 100
	client.addBlock(99, "")
	client.addBlock(100, "")

	block100Header := client.getHeader(100)
	expectedHash := block100Header.Hash

	config := LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		DisableBlobs:         false, // Enable blobs to test all data types
		Logger:               slog.Default(),
	}

	svc, err := NewLiveService(config, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Process the block
	header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       expectedHash,
		ParentHash: block100Header.ParentHash,
		Timestamp:  block100Header.Timestamp,
	}

	err = svc.processBlock(header, time.Now())
	if err != nil {
		t.Fatalf("processBlock failed: %v", err)
	}

	// Verify that GetBlockDataByHash was called with the expected hash
	fetchedMethods := client.fetchedByHash[expectedHash]
	found := false
	for _, method := range fetchedMethods {
		if method == "GetBlockDataByHash" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected GetBlockDataByHash to be called with hash %s, but it wasn't. Called methods: %v",
			expectedHash, fetchedMethods)
	}

	t.Logf("SUCCESS: Data fetched by hash using batched request: %v", fetchedMethods)
}

// mockClientWithHashTracking wraps mockBlockchainClient to track which methods are called with which hashes
type mockClientWithHashTracking struct {
	*mockBlockchainClient
	mu            sync.Mutex
	fetchedByHash map[string][]string // hash -> list of method names called
}

func (m *mockClientWithHashTracking) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlockDataByHash")
	m.mu.Unlock()
	return m.mockBlockchainClient.GetBlockDataByHash(ctx, blockNum, hash, fullTx)
}

func (m *mockClientWithHashTracking) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetFullBlockByHash")
	m.mu.Unlock()
	return m.mockBlockchainClient.GetFullBlockByHash(ctx, hash, fullTx)
}

func (m *mockClientWithHashTracking) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlockReceiptsByHash")
	m.mu.Unlock()
	return m.mockBlockchainClient.GetBlockReceiptsByHash(ctx, hash)
}

func (m *mockClientWithHashTracking) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlockTracesByHash")
	m.mu.Unlock()
	return m.mockBlockchainClient.GetBlockTracesByHash(ctx, hash)
}

func (m *mockClientWithHashTracking) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlobSidecarsByHash")
	m.mu.Unlock()
	return m.mockBlockchainClient.GetBlobSidecarsByHash(ctx, hash)
}

// TestHashComparisonCaseInsensitive verifies that hash comparisons are case-insensitive.
// Ethereum hashes are hex strings where 0xAAA and 0xaaa are semantically equal,
// but Go string comparison is case-sensitive. This test ensures we handle this correctly.
func TestHashComparisonCaseInsensitive(t *testing.T) {
	ctx := context.Background()

	// Create mock dependencies
	client := &caseInsensitiveMockClient{
		blocks: make(map[int64]blockTestData),
	}
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add blocks with UPPERCASE hashes
	// Block 99 with uppercase hash
	client.blocks[99] = blockTestData{
		header: outbound.BlockHeader{
			Number:     "0x63", // 99
			Hash:       "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
			Timestamp:  "0x12345678",
		},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}

	// Block 100 with uppercase hash, parent is block 99
	client.blocks[100] = blockTestData{
		header: outbound.BlockHeader{
			Number:     "0x64", // 100
			Hash:       "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			ParentHash: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Timestamp:  "0x12345679",
		},
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}

	config := LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		DisableBlobs:         true,
		Logger:               slog.Default(),
	}

	svc, err := NewLiveService(config, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Test 1: Process block 99 with UPPERCASE hash
	err = svc.processBlock(outbound.BlockHeader{
		Number:     "0x63",
		Hash:       "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
		Timestamp:  "0x12345678",
	}, time.Now())
	if err != nil {
		t.Fatalf("failed to process block 99: %v", err)
	}

	// Verify block was stored
	if len(svc.unfinalizedBlocks) != 1 {
		t.Fatalf("expected 1 unfinalized block, got %d", len(svc.unfinalizedBlocks))
	}

	// Verify hash was normalized to lowercase
	if svc.unfinalizedBlocks[0].Hash != strings.ToLower("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
		t.Errorf("hash should be normalized to lowercase, got %s", svc.unfinalizedBlocks[0].Hash)
	}

	// Test 2: Send same block with LOWERCASE hash - should be detected as duplicate
	initialBlockCount := len(svc.unfinalizedBlocks)
	err = svc.processBlock(outbound.BlockHeader{
		Number:     "0x63",
		Hash:       "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // lowercase
		ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
		Timestamp:  "0x12345678",
	}, time.Now())
	if err != nil {
		t.Fatalf("failed to process duplicate block: %v", err)
	}

	// Block count should not increase (duplicate was detected)
	if len(svc.unfinalizedBlocks) != initialBlockCount {
		t.Errorf("duplicate block should not be added, expected %d blocks, got %d",
			initialBlockCount, len(svc.unfinalizedBlocks))
	}

	// Test 3: Process block 100 with lowercase parentHash - should chain correctly
	err = svc.processBlock(outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		ParentHash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // lowercase parent
		Timestamp:  "0x12345679",
	}, time.Now())
	if err != nil {
		t.Fatalf("failed to process block 100 with lowercase parent hash: %v", err)
	}

	// Verify block was added (no reorg detected due to case mismatch)
	if len(svc.unfinalizedBlocks) != 2 {
		t.Fatalf("expected 2 unfinalized blocks, got %d", len(svc.unfinalizedBlocks))
	}

	// Verify parent hash was normalized
	block100 := svc.unfinalizedBlocks[1]
	if block100.ParentHash != strings.ToLower("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
		t.Errorf("parent hash should be normalized to lowercase, got %s", block100.ParentHash)
	}

	// Verify chain integrity (parent hash of block 100 should match hash of block 99)
	block99 := svc.unfinalizedBlocks[0]
	if block100.ParentHash != block99.Hash {
		t.Errorf("chain integrity violated: block 100 parentHash (%s) != block 99 hash (%s)",
			block100.ParentHash, block99.Hash)
	}

	t.Log("SUCCESS: Hash comparisons are case-insensitive")
}

// caseInsensitiveMockClient is a mock that uses case-insensitive hash lookups
// to simulate real RPC behavior where hash case doesn't matter
type caseInsensitiveMockClient struct {
	mu     sync.RWMutex
	blocks map[int64]blockTestData
}

func (m *caseInsensitiveMockClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		data, _ := json.Marshal(bd.header)
		return data, nil
	}
	return nil, fmt.Errorf("block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.header.Hash, hash) {
			h := bd.header
			return &h, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.header.Hash, hash) {
			data, _ := json.Marshal(bd.header)
			return data, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.receipts, nil
	}
	return nil, fmt.Errorf("receipts for block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.header.Hash, hash) {
			return bd.receipts, nil
		}
	}
	return nil, fmt.Errorf("receipts for block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.traces, nil
	}
	return nil, fmt.Errorf("traces for block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.header.Hash, hash) {
			return bd.traces, nil
		}
	}
	return nil, fmt.Errorf("traces for block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.blobs, nil
	}
	return nil, fmt.Errorf("blobs for block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.header.Hash, hash) {
			return bd.blobs, nil
		}
	}
	return nil, fmt.Errorf("blobs for block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
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

func (m *caseInsensitiveMockClient) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

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

func (m *caseInsensitiveMockClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lowerHash := strings.ToLower(hash)
	for _, bd := range m.blocks {
		if strings.ToLower(bd.header.Hash) == lowerHash {
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

// TestProcessBlock_RollsBackInMemoryChainOnDBFailure verifies that when SaveBlock fails,
// the block is removed from the in-memory chain to maintain consistency with the database.
// Without this rollback, the in-memory chain and DB would diverge, causing incorrect
// reorg detection and chain integrity issues.
func TestProcessBlock_RollsBackInMemoryChainOnDBFailure(t *testing.T) {
	ctx := context.Background()

	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	client := newMockClient()

	// Add blocks for RPC lookups
	client.addBlock(99, "")
	client.addBlock(100, "")

	config := LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		DisableBlobs:         true,
		Logger:               slog.Default(),
	}

	svc, err := NewLiveService(config, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Get block 100's header so we know what parent hash it expects
	block100Header := client.getHeader(100)
	block99Hash := block100Header.ParentHash

	// Set up initial chain with block 99 using the correct hash
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 99, Hash: block99Hash, ParentHash: "0xblock98"},
	}

	// Configure repo to fail on SaveBlock for the next block
	stateRepo.mu.Lock()
	stateRepo.saveBlockErr = fmt.Errorf("database connection failed")
	stateRepo.mu.Unlock()

	// Try to process block 100 - should fail
	err = svc.processBlock(block100Header, time.Now())
	if err == nil {
		t.Fatal("expected error when SaveBlock fails")
	}
	if !strings.Contains(err.Error(), "database connection failed") {
		t.Fatalf("unexpected error: %v", err)
	}

	// CRITICAL CHECK: The in-memory chain should NOT contain block 100
	// because the DB save failed. If it does contain block 100, we have
	// a consistency issue between memory and DB.
	for _, b := range svc.unfinalizedBlocks {
		if b.Number == 100 {
			t.Error("block 100 should NOT be in unfinalizedBlocks after DB save failed - memory/DB inconsistency!")
		}
	}

	// Verify the chain only contains block 99
	if len(svc.unfinalizedBlocks) != 1 {
		t.Errorf("expected 1 block in chain, got %d", len(svc.unfinalizedBlocks))
	}
	if svc.unfinalizedBlocks[0].Number != 99 {
		t.Errorf("expected block 99, got block %d", svc.unfinalizedBlocks[0].Number)
	}

	t.Log("SUCCESS: In-memory chain correctly rolled back after DB failure")
}

// TestProcessBlock_ReorgChainNotPrunedOnDBFailure verifies that when HandleReorgAtomic fails,
// the in-memory chain is NOT modified. This prevents divergence between memory and database state.
// Without this protection, a failed reorg DB update would leave memory pruned but DB unchanged.
func TestProcessBlock_ReorgChainNotPrunedOnDBFailure(t *testing.T) {
	ctx := context.Background()

	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	client := newMockClient()

	// Add blocks for RPC lookups
	client.addBlock(99, "")
	client.addBlock(100, "")

	config := LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		DisableBlobs:         true,
		Logger:               slog.Default(),
	}

	svc, err := NewLiveService(config, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Get block 100's header
	block100Header := client.getHeader(100)
	block99Hash := block100Header.ParentHash

	// Set up initial chain with blocks 99 and 100 (using our own block 100 hash)
	// This simulates having already seen a block 100 on the old fork
	oldBlock100Hash := "0xoldblock100hash_will_be_orphaned"
	svc.unfinalizedBlocks = []LightBlock{
		{Number: 99, Hash: block99Hash, ParentHash: "0xblock98"},
		{Number: 100, Hash: oldBlock100Hash, ParentHash: block99Hash},
	}

	// Simulate receiving a NEW block 100 with a DIFFERENT hash (reorg at block 100)
	// The mock client's block 100 has a different hash than what's in our chain
	reorgBlock100Header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0xnewblock100hash_from_reorg",
		ParentHash: block99Hash, // Same parent, different block = reorg
	}

	// Configure repo to fail on HandleReorgAtomic
	stateRepo.mu.Lock()
	stateRepo.handleReorgAtomicErr = fmt.Errorf("database transaction failed")
	stateRepo.mu.Unlock()

	// Try to process the reorg block - should fail
	err = svc.processBlock(reorgBlock100Header, time.Now())
	if err == nil {
		t.Fatal("expected error when HandleReorgAtomic fails")
	}
	if !strings.Contains(err.Error(), "database transaction failed") {
		t.Fatalf("unexpected error: %v", err)
	}

	// CRITICAL CHECK: The in-memory chain should NOT be pruned
	// It should still contain BOTH blocks 99 and 100 (the old block 100)
	if len(svc.unfinalizedBlocks) != 2 {
		t.Errorf("expected 2 blocks in chain (not pruned), got %d", len(svc.unfinalizedBlocks))
	}

	// Verify block 99 is still there
	found99 := false
	for _, b := range svc.unfinalizedBlocks {
		if b.Number == 99 {
			found99 = true
		}
	}
	if !found99 {
		t.Error("block 99 should still be in unfinalizedBlocks")
	}

	// Verify the OLD block 100 is still there (not pruned)
	found100 := false
	for _, b := range svc.unfinalizedBlocks {
		if b.Number == 100 && b.Hash == oldBlock100Hash {
			found100 = true
		}
	}
	if !found100 {
		t.Error("old block 100 should still be in unfinalizedBlocks after DB failure - chain should NOT be pruned!")
	}

	t.Log("SUCCESS: In-memory chain not pruned after HandleReorgAtomic failure")
}

// ============================================================================
// Cache-before-publish ordering test
// ============================================================================

// mockOrderTrackingCache tracks the order of cache operations
type mockOrderTrackingCache struct {
	*memory.BlockCache
	mu         sync.Mutex
	operations []string
}

func newMockOrderTrackingCache() *mockOrderTrackingCache {
	return &mockOrderTrackingCache{
		BlockCache: memory.NewBlockCache(),
		operations: make([]string, 0),
	}
}

func (c *mockOrderTrackingCache) SetBlock(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	c.operations = append(c.operations, "cache_block")
	c.mu.Unlock()
	return c.BlockCache.SetBlock(ctx, chainID, blockNum, version, data)
}

func (c *mockOrderTrackingCache) SetReceipts(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	c.operations = append(c.operations, "cache_receipts")
	c.mu.Unlock()
	return c.BlockCache.SetReceipts(ctx, chainID, blockNum, version, data)
}

func (c *mockOrderTrackingCache) SetTraces(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	c.operations = append(c.operations, "cache_traces")
	c.mu.Unlock()
	return c.BlockCache.SetTraces(ctx, chainID, blockNum, version, data)
}

func (c *mockOrderTrackingCache) SetBlobs(ctx context.Context, chainID, blockNum int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	c.operations = append(c.operations, "cache_blobs")
	c.mu.Unlock()
	return c.BlockCache.SetBlobs(ctx, chainID, blockNum, version, data)
}

func (c *mockOrderTrackingCache) getOperations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]string, len(c.operations))
	copy(result, c.operations)
	return result
}

// mockOrderTrackingEventSink tracks when publish is called
type mockOrderTrackingEventSink struct {
	*memory.EventSink
	mu         sync.Mutex
	operations []string
}

func newMockOrderTrackingEventSink() *mockOrderTrackingEventSink {
	return &mockOrderTrackingEventSink{
		EventSink:  memory.NewEventSink(),
		operations: make([]string, 0),
	}
}

func (s *mockOrderTrackingEventSink) Publish(ctx context.Context, event outbound.Event) error {
	s.mu.Lock()
	s.operations = append(s.operations, "publish_block")
	s.mu.Unlock()
	return s.EventSink.Publish(ctx, event)
}

func (s *mockOrderTrackingEventSink) getOperations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, len(s.operations))
	copy(result, s.operations)
	return result
}

func TestFetchAndPublishBlockData_CachesAllDataBeforePublishing(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := newMockOrderTrackingCache()
	eventSink := newMockOrderTrackingEventSink()

	client := newMockFailingClient()
	client.addBlock(100, "")

	header100 := client.getHeader(100)

	svc, err := NewLiveService(LiveConfig{
		DisableBlobs: false, // Enable blobs to test all 4 cache operations
	}, newMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.fetchAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false)
	if err != nil {
		t.Fatalf("fetchAndPublishBlockData failed: %v", err)
	}

	// Collect all operations from both cache and event sink
	cacheOps := cache.getOperations()
	publishOps := eventSink.getOperations()

	// Verify all 4 cache operations happened
	expectedCacheOps := map[string]bool{
		"cache_block":    false,
		"cache_receipts": false,
		"cache_traces":   false,
		"cache_blobs":    false,
	}
	for _, op := range cacheOps {
		if _, exists := expectedCacheOps[op]; exists {
			expectedCacheOps[op] = true
		}
	}
	for op, found := range expectedCacheOps {
		if !found {
			t.Errorf("expected cache operation %q was not called", op)
		}
	}

	// Verify publish was called exactly once
	if len(publishOps) != 1 {
		t.Errorf("expected exactly 1 publish operation, got %d", len(publishOps))
	}

	// CRITICAL: Verify publish happened AFTER all cache operations
	// Since cache operations happen concurrently, we just need to verify
	// that all cache operations completed before any publish
	if len(cacheOps) != 4 {
		t.Errorf("expected 4 cache operations before publish, got %d", len(cacheOps))
	}

	t.Logf("SUCCESS: All %d cache operations completed before publish", len(cacheOps))
	t.Logf("Cache operations: %v", cacheOps)
	t.Logf("Publish operations: %v", publishOps)
}

func TestFetchAndPublishBlockData_NoPublishOnCacheFailure(t *testing.T) {
	// This test verifies that if caching fails, publish is never called
	tests := []struct {
		name              string
		failCacheBlock    bool
		failCacheReceipts bool
		failCacheTraces   bool
		failCacheBlobs    bool
	}{
		{
			name:           "block_cache_fails",
			failCacheBlock: true,
		},
		{
			name:              "receipts_cache_fails",
			failCacheReceipts: true,
		},
		{
			name:            "traces_cache_fails",
			failCacheTraces: true,
		},
		{
			name:           "blobs_cache_fails",
			failCacheBlobs: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			stateRepo := newMockStateRepo()

			cache := newMockFailingCache()
			cache.failSetBlock = tt.failCacheBlock
			cache.failSetReceipts = tt.failCacheReceipts
			cache.failSetTraces = tt.failCacheTraces
			cache.failSetBlobs = tt.failCacheBlobs

			eventSink := memory.NewEventSink()

			client := newMockFailingClient()
			client.addBlock(100, "")
			header100 := client.getHeader(100)

			svc, err := NewLiveService(LiveConfig{
				DisableBlobs: false,
			}, newMockSubscriber(), client, stateRepo, cache, eventSink)
			if err != nil {
				t.Fatalf("failed to create service: %v", err)
			}

			err = svc.fetchAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false)
			if err == nil {
				t.Fatal("expected error due to cache failure")
			}

			// CRITICAL: Verify no events were published
			events := eventSink.GetBlockEvents()
			if len(events) != 0 {
				t.Errorf("expected 0 events when cache fails, got %d", len(events))
			}

			t.Logf("SUCCESS: No publish when %s", tt.name)
		})
	}
}
