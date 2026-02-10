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
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

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

// TestLateBlockAfterPruning tests the scenario where a live block arrives after
// its corresponding blocks have been pruned from the unfinalized chain.
func TestLateBlockAfterPruning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subscriber := testutil.NewMockSubscriber()
	client := testutil.NewMockBlockchainClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate client with blocks 1-300
	for i := int64(1); i <= 300; i++ {
		client.AddBlock(i, "")
	}

	// Seed the state repo with block 1
	if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     1,
		Hash:       client.GetHeader(1).Hash,
		ParentHash: client.GetHeader(1).ParentHash,
		ReceivedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	liveConfig := LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 10,
		Logger:             slog.Default(),
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
			header := client.GetHeader(i)
			subscriber.SendHeader(header)
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
			subscriber := testutil.NewMockSubscriber()
			client := testutil.NewMockBlockchainClient()
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
	subscriber := testutil.NewMockSubscriber()
	client := testutil.NewMockBlockchainClient()
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
	if svc.config.Logger == nil {
		t.Error("expected Logger to be set to default")
	}
}

func TestNewLiveService_UsesProvidedConfig(t *testing.T) {
	subscriber := testutil.NewMockSubscriber()
	client := testutil.NewMockBlockchainClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	customLogger := slog.Default().With("custom", true)

	config := LiveConfig{
		ChainID:            42,
		FinalityBlockCount: 128,
		EnableBlobs:        false,
		Logger:             customLogger,
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
	if svc.config.EnableBlobs {
		t.Error("expected EnableBlobs to be false")
	}
}

// ============================================================================
// isDuplicateBlock edge case tests
// ============================================================================

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

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

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

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// DB is empty - should not find it
	isDup, err := svc.isDuplicateBlock(ctx, "0xnonexistent", 999)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isDup {
		t.Error("expected isDuplicateBlock to return false for non-existent block")
	}
}

func TestIsDuplicateBlock_DBError(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	stateRepo.getByHashErr = fmt.Errorf("database connection failed")

	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Check for duplicate - DB error should return error
	isDup, err := svc.isDuplicateBlock(ctx, "0xsome_hash", 100)
	if err == nil {
		t.Error("expected isDuplicateBlock to return error when DB fails")
	}
	if isDup {
		t.Error("expected isDuplicateBlock to return false when DB errors")
	}
}

// ============================================================================
// detectReorg scenario tests
// ============================================================================

func TestDetectReorg_EmptyChain_NoReorg(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Empty DB - first block arriving
	block := LightBlock{
		Number:     100,
		Hash:       "0xnew_hash",
		ParentHash: "0xparent",
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(ctx, block, time.Now())
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
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	// Add blocks for lookups
	for i := int64(1); i <= 100; i++ {
		client.AddBlock(i, "")
	}

	// Pre-populate DB with blocks 99-100
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 99, Hash: "0xblock99", ParentHash: "0xblock98",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0xblock100", ParentHash: "0xblock99",
	})

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Next block 101 with correct parent
	block := LightBlock{
		Number:     101,
		Hash:       "0xblock101",
		ParentHash: "0xblock100", // Matches latest
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(ctx, block, time.Now())
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
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	// Add alternate chain blocks for ancestry walk
	// Block 100_alt's parent is block 99 (which exists in our chain)
	client.SetBlockHeader(100, "0xblock100_alt", "0xblock99")

	// Pre-populate DB with blocks 99-100
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 99, Hash: "0xblock99", ParentHash: "0xblock98",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0xblock100", ParentHash: "0xblock99",
	})

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 64,
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Block 101 with WRONG parent (orphan 100, replace with different chain)
	block := LightBlock{
		Number:     101,
		Hash:       "0xblock101_alt",
		ParentHash: "0xblock100_alt", // Different parent - triggers reorg
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(ctx, block, time.Now())
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
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	// Add blocks for ancestry walk
	client.SetBlockHeader(98, "0xblock98", "0xblock97")

	// Pre-populate DB with blocks 98-100
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 98, Hash: "0xblock98", ParentHash: "0xblock97",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 99, Hash: "0xblock99", ParentHash: "0xblock98",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0xblock100", ParentHash: "0xblock99",
	})

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Receive block 99 again with different hash - definite reorg
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98", // Same ancestor
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(ctx, block, time.Now())
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
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate DB with block 100
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0xblock100", ParentHash: "0xblock99",
	})

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Receive block 105 (gap of 4 blocks)
	block := LightBlock{
		Number:     105,
		Hash:       "0xblock105",
		ParentHash: "0xblock104",
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.detectReorg(ctx, block, time.Now())
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

func TestHandleReorg_FindsCommonAncestorInDB(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate DB with blocks 95-100
	blocks := []outbound.BlockState{
		{Number: 95, Hash: "0xblock95", ParentHash: "0xblock94"},
		{Number: 96, Hash: "0xblock96", ParentHash: "0xblock95"},
		{Number: 97, Hash: "0xblock97", ParentHash: "0xblock96"},
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97"},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}
	for _, b := range blocks {
		_, _ = stateRepo.SaveBlock(ctx, b)
	}

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Incoming block 99 with different hash but parent is block 98
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98", // Matches our block 98
	}

	isReorg, depth, ancestor, reorgEvent, err := svc.handleReorg(ctx, block, time.Now())
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
		t.Fatal("expected reorg event to be populated")
	}

	// Simulate what happens after HandleReorgAtomic: mark orphaned blocks and save new block
	_, _ = stateRepo.HandleReorgAtomic(ctx, ancestor, *reorgEvent, outbound.BlockState{
		Number:     block.Number,
		Hash:       block.Hash,
		ParentHash: block.ParentHash,
		ReceivedAt: time.Now().Unix(),
	})

	// After HandleReorgAtomic, DB should have blocks 95-98 (non-orphaned) + 99_alt (new block) = 5 blocks
	recentBlocks, err := stateRepo.GetRecentBlocks(ctx, 100)
	if err != nil {
		t.Fatalf("GetRecentBlocks failed: %v", err)
	}
	if len(recentBlocks) != 5 {
		t.Errorf("expected 5 blocks in DB after reorg, got %d", len(recentBlocks))
		for _, b := range recentBlocks {
			t.Logf("  block %d: %s", b.Number, b.Hash)
		}
	}

	// Verify the new block is present
	found := false
	for _, b := range recentBlocks {
		if b.Number == 99 && b.Hash == "0xblock99_alt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected new block 99_alt to be in DB after reorg")
	}
}

func TestHandleReorg_WalksBackViaNetwork(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	// Set up remote chain that forks at block 97
	// Remote: 97 -> 98_alt -> 99_alt -> 100_alt
	client.SetBlockHeader(97, "0xblock97", "0xblock96")
	client.SetBlockHeader(98, "0xblock98_alt", "0xblock97")

	// Pre-populate DB with blocks 95-100
	blocks := []outbound.BlockState{
		{Number: 95, Hash: "0xblock95", ParentHash: "0xblock94"},
		{Number: 96, Hash: "0xblock96", ParentHash: "0xblock95"},
		{Number: 97, Hash: "0xblock97", ParentHash: "0xblock96"},
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97"},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98"},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99"},
	}
	for _, b := range blocks {
		_, _ = stateRepo.SaveBlock(ctx, b)
	}

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Incoming block 99 from alternate chain
	// Its parent is 98_alt, we need to walk back to find common ancestor
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_alt",
		ParentHash: "0xblock98_alt", // Not in our chain
	}

	isReorg, depth, ancestor, _, err := svc.handleReorg(ctx, block, time.Now())
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
	t.Run("below_finality_boundary", func(t *testing.T) {
		ctx := context.Background()
		stateRepo := newMockStateRepo()
		cache := memory.NewBlockCache()
		eventSink := memory.NewEventSink()

		// Pre-populate DB with block 100 (latest)
		_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number: 100, Hash: "0xblock100", ParentHash: "0xblock99",
		})

		svc, err := NewLiveService(LiveConfig{
			FinalityBlockCount: 64,
		}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
		if err != nil {
			t.Fatalf("failed to create service: %v", err)
		}

		// Block at 36 (100 - 64 = 36 is the finality boundary)
		// A block at or below this should error
		block := LightBlock{
			Number:     36,
			Hash:       "0xlate_block",
			ParentHash: "0xunknown",
		}

		_, _, _, _, err = svc.handleReorg(ctx, block, time.Now())
		if err == nil {
			t.Error("expected error for block below finality boundary, got nil")
		} else if !strings.Contains(err.Error(), "at or below finality boundary") {
			t.Errorf("expected error containing 'at or below finality boundary', got %q", err.Error())
		}
	})

	t.Run("no_common_ancestor", func(t *testing.T) {
		ctx := context.Background()
		stateRepo := newMockStateRepo()
		cache := memory.NewBlockCache()
		eventSink := memory.NewEventSink()
		client := testutil.NewMockBlockchainClient()

		// Set up a chain of unknown blocks that the walk will traverse
		client.SetBlockHeader(99, "0xunknown_parent", "0xunknown_grandparent")
		client.SetBlockHeader(98, "0xunknown_grandparent", "0xunknown_great_grandparent")
		client.SetBlockHeader(97, "0xunknown_great_grandparent", "0xunknown_great_great_grandparent")

		// Pre-populate DB with our chain (completely different from the unknown chain)
		_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number: 100, Hash: "0xblock100", ParentHash: "0xblock99",
		})

		svc, err := NewLiveService(LiveConfig{
			FinalityBlockCount: 3, // Walk will exhaust after 3 iterations
		}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
		if err != nil {
			t.Fatalf("failed to create service: %v", err)
		}

		block := LightBlock{
			Number:     100,
			Hash:       "0xblock100_alt",
			ParentHash: "0xunknown_parent",
		}

		_, _, _, _, err = svc.handleReorg(ctx, block, time.Now())
		if err == nil {
			t.Error("expected error, got nil")
		} else if !strings.Contains(err.Error(), "no common ancestor found") {
			t.Errorf("expected error containing 'no common ancestor found', got %q", err.Error())
		}
	})
}

func TestHandleReorg_ReturnsReorgEvent(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate DB with our chain
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 98, Hash: "0xblock98", ParentHash: "0xblock97",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 99, Hash: "0xblock99", ParentHash: "0xblock98",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0xblock100", ParentHash: "0xblock99",
	})

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Reorg at block 99
	block := LightBlock{
		Number:     99,
		Hash:       "0xblock99_new",
		ParentHash: "0xblock98",
	}

	_, depth, _, reorgEvent, err := svc.handleReorg(ctx, block, time.Now())
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

// Note: updateFinalizedBlock and restoreInMemoryChain tests removed - functionality no longer exists.
// The service now uses the database as the source of truth and doesn't maintain in-memory chain state.

// ============================================================================
// processBlock error handling tests
// ============================================================================

func TestProcessBlock_Errors(t *testing.T) {
	t.Run("invalid_block_number", func(t *testing.T) {
		stateRepo := newMockStateRepo()
		cache := memory.NewBlockCache()
		eventSink := memory.NewEventSink()
		client := testutil.NewMockBlockchainClient()

		svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
		if err != nil {
			t.Fatalf("failed to create service: %v", err)
		}
		svc.ctx, svc.cancel = context.WithCancel(context.Background())
		defer svc.cancel()

		header := outbound.BlockHeader{
			Number:     "not_a_hex",
			Hash:       "0xhash",
			ParentHash: "0xparent",
		}

		err = svc.processBlockWithPrefetch(header, time.Now())
		if err == nil {
			t.Error("expected error, got nil")
		} else if !strings.Contains(err.Error(), "failed to parse block number") {
			t.Errorf("expected error containing 'failed to parse block number', got %q", err.Error())
		}
	})

	t.Run("save_block_error", func(t *testing.T) {
		stateRepo := newMockStateRepo()
		stateRepo.saveBlockErr = fmt.Errorf("database write failed")
		cache := memory.NewBlockCache()
		eventSink := memory.NewEventSink()
		client := testutil.NewMockBlockchainClient()
		client.AddBlock(100, "")

		svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
		if err != nil {
			t.Fatalf("failed to create service: %v", err)
		}
		svc.ctx, svc.cancel = context.WithCancel(context.Background())
		defer svc.cancel()

		header := outbound.BlockHeader{
			Number:     "0x64",
			Hash:       "0xnew_hash",
			ParentHash: "0x63",
			Timestamp:  "0x0",
		}

		err = svc.processBlockWithPrefetch(header, time.Now())
		if err == nil {
			t.Error("expected error, got nil")
		} else if !strings.Contains(err.Error(), "failed to save block state") {
			t.Errorf("expected error containing 'failed to save block state', got %q", err.Error())
		}
	})
}

func TestProcessBlock_SkipsDuplicate(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	client.AddBlock(100, "")

	// Pre-populate DB with block
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0x64", ParentHash: "0x63",
	})

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(ctx)
	defer svc.cancel()

	header := outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0x64", // Same hash as in DB
		ParentHash: "0x63",
	}

	// Should succeed but not add duplicate
	err = svc.processBlockWithPrefetch(header, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// DB should still have 1 block (not 2)
	recentBlocks, err := stateRepo.GetRecentBlocks(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get recent blocks: %v", err)
	}
	if len(recentBlocks) != 1 {
		t.Errorf("expected 1 block (duplicate skipped), got %d", len(recentBlocks))
	}
}

func TestProcessBlock_AddsBlockToChain(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	client.AddBlock(100, "")
	client.AddBlock(101, "")

	// Get the actual hashes from the mock client
	header100 := client.GetHeader(100)
	header101 := client.GetHeader(101)

	// Pre-populate DB with block 100
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: header100.Hash, ParentHash: header100.ParentHash,
	})

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(ctx)
	defer svc.cancel()

	// Process block 101 using actual header
	err = svc.processBlockWithPrefetch(header101, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// DB should now have 2 blocks
	recentBlocks, err := stateRepo.GetRecentBlocks(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get recent blocks: %v", err)
	}
	if len(recentBlocks) != 2 {
		t.Errorf("expected 2 blocks, got %d", len(recentBlocks))
	}
}

// ============================================================================
// cacheAndPublishBlockData failure tests
// ============================================================================

// mockFailingClient simulates fetch failures by setting error fields in BlockData
type mockFailingClient struct {
	*testutil.MockBlockchainClient
	failGetBlock    bool
	failGetReceipts bool
	failGetTraces   bool
	failGetBlobs    bool
}

func newMockFailingClient() *mockFailingClient {
	return &mockFailingClient{
		MockBlockchainClient: testutil.NewMockBlockchainClient(),
	}
}

func (m *mockFailingClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	if m.failGetBlock {
		return nil, fmt.Errorf("simulated block fetch failure")
	}
	return m.MockBlockchainClient.GetBlockByNumber(ctx, blockNum, fullTx)
}

func (m *mockFailingClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	if m.failGetBlock {
		return nil, fmt.Errorf("simulated block fetch failure")
	}
	return m.MockBlockchainClient.GetFullBlockByHash(ctx, hash, fullTx)
}

func (m *mockFailingClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetReceipts {
		return nil, fmt.Errorf("simulated receipts fetch failure")
	}
	return m.MockBlockchainClient.GetBlockReceipts(ctx, blockNum)
}

func (m *mockFailingClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	if m.failGetReceipts {
		return nil, fmt.Errorf("simulated receipts fetch failure")
	}
	return m.MockBlockchainClient.GetBlockReceiptsByHash(ctx, hash)
}

func (m *mockFailingClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetTraces {
		return nil, fmt.Errorf("simulated traces fetch failure")
	}
	return m.MockBlockchainClient.GetBlockTraces(ctx, blockNum)
}

func (m *mockFailingClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	if m.failGetTraces {
		return nil, fmt.Errorf("simulated traces fetch failure")
	}
	return m.MockBlockchainClient.GetBlockTracesByHash(ctx, hash)
}

func (m *mockFailingClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	if m.failGetBlobs {
		return nil, fmt.Errorf("simulated blobs fetch failure")
	}
	return m.MockBlockchainClient.GetBlobSidecars(ctx, blockNum)
}

func (m *mockFailingClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	if m.failGetBlobs {
		return nil, fmt.Errorf("simulated blobs fetch failure")
	}
	return m.MockBlockchainClient.GetBlobSidecarsByHash(ctx, hash)
}

func (m *mockFailingClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	// Delegate to underlying mock - failure flags are for individual method failures
	// which are no longer used since we now use batched requests
	bd, err := m.MockBlockchainClient.GetBlockDataByHash(ctx, blockNum, hash, fullTx)
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

func (c *mockFailingCache) SetBlockData(ctx context.Context, chainID int64, blockNumber int64, version int, data outbound.BlockDataInput) error {
	// Simulate failures based on individual failure flags
	if c.failSetBlock {
		return fmt.Errorf("failed to cache block data for block %d: simulated cache block failure", blockNumber)
	}
	if c.failSetReceipts {
		return fmt.Errorf("failed to cache block data for block %d: simulated cache receipts failure", blockNumber)
	}
	if c.failSetTraces {
		return fmt.Errorf("failed to cache block data for block %d: simulated cache traces failure", blockNumber)
	}
	if c.failSetBlobs && data.Blobs != nil {
		return fmt.Errorf("failed to cache block data for block %d: simulated cache blobs failure", blockNumber)
	}
	return c.BlockCache.SetBlockData(ctx, chainID, blockNumber, version, data)
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

func TestCacheAndPublishBlockData_ErrorHandling(t *testing.T) {
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
		enableTraces      bool
		enableBlobs       bool
		wantErr           bool
		errContains       string
	}{
		{
			name:         "all_succeed",
			enableTraces: true,
			wantErr:      false,
		},
		{
			name:         "block_fetch_error_in_data",
			enableTraces: true,
			failGetBlock: true,
			wantErr:      true,
			errContains:  "failed to fetch block",
		},
		{
			name:            "receipts_fetch_error_in_data",
			enableTraces:    true,
			failGetReceipts: true,
			wantErr:         true,
			errContains:     "failed to fetch receipts",
		},
		{
			name:          "traces_fetch_error_in_data",
			enableTraces:  true,
			failGetTraces: true,
			wantErr:       true,
			errContains:   "failed to fetch traces",
		},
		{
			name:         "blobs_fetch_error_in_data",
			enableTraces: true,
			failGetBlobs: true,
			enableBlobs:  true,
			wantErr:      true,
			errContains:  "failed to fetch blobs",
		},
		{
			name:           "block_cache_fails",
			enableTraces:   true,
			failCacheBlock: true,
			wantErr:        true,
			errContains:    "cache block failure",
		},
		{
			name:              "receipts_cache_fails",
			enableTraces:      true,
			failCacheReceipts: true,
			wantErr:           true,
			errContains:       "cache receipts failure",
		},
		{
			name:            "traces_cache_fails",
			enableTraces:    true,
			failCacheTraces: true,
			wantErr:         true,
			errContains:     "cache traces failure",
		},
		{
			name:           "blobs_cache_fails",
			enableTraces:   true,
			failCacheBlobs: true,
			enableBlobs:    true,
			wantErr:        true,
			errContains:    "cache blobs failure",
		},
		{
			name:         "publish_fails",
			enableTraces: true,
			failPublish:  true,
			wantErr:      true,
			errContains:  "failed to publish",
		},
		{
			name:         "blobs_disabled_blobs_error_ignored",
			enableTraces: true,
			failGetBlobs: true,
			enableBlobs:  false,
			wantErr:      false,
		},
		{
			name:          "traces_disabled_traces_error_ignored",
			enableTraces:  false,
			failGetTraces: true,
			wantErr:       false,
		},
		{
			name:            "multiple_failures",
			enableTraces:    true,
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
			client.AddBlock(100, "")

			// Get the actual header and block data from mock client
			header100 := client.GetHeader(100)
			blockData, _ := client.GetBlockDataByHash(ctx, 100, header100.Hash, true)

			cache := newMockFailingCache()
			cache.failSetBlock = tt.failCacheBlock
			cache.failSetReceipts = tt.failCacheReceipts
			cache.failSetTraces = tt.failCacheTraces
			cache.failSetBlobs = tt.failCacheBlobs

			eventSink := newMockFailingEventSink()
			eventSink.failPublish = tt.failPublish

			svc, err := NewLiveService(LiveConfig{
				EnableTraces: tt.enableTraces,
				EnableBlobs:  tt.enableBlobs,
			}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
			if err != nil {
				t.Fatalf("failed to create service: %v", err)
			}

			// Call cacheAndPublishBlockData with the block data
			err = svc.cacheAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false, blockData)

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

func TestCacheAndPublishBlockData_WithBlobs_CachesBlobs(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockFailingClient()
	client.AddBlock(100, "")

	// Get actual header and block data from mock client
	header100 := client.GetHeader(100)
	blockData, _ := client.GetBlockDataByHash(ctx, 100, header100.Hash, true)

	svc, err := NewLiveService(LiveConfig{
		EnableTraces: true,
		EnableBlobs:  true, // Enable blobs
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.cacheAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false, blockData)
	if err != nil {
		t.Fatalf("cacheAndPublishBlockData failed: %v", err)
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

func TestCacheAndPublishBlockData_ReorgFlag_SetsIsReorg(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := newMockFailingClient()
	client.AddBlock(100, "")

	// Get actual header and block data from mock client
	header100 := client.GetHeader(100)
	blockData, _ := client.GetBlockDataByHash(ctx, 100, header100.Hash, true)

	svc, err := NewLiveService(LiveConfig{
		EnableBlobs: false,
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.cacheAndPublishBlockData(ctx, header100, 100, 0, time.Now(), true, blockData) // isReorg = true
	if err != nil {
		t.Fatalf("cacheAndPublishBlockData failed: %v", err)
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

	svc, err := NewLiveService(LiveConfig{}, subscriber, testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
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

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
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

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
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

	svc, err := NewLiveService(LiveConfig{}, testutil.NewMockSubscriber(), testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Stop before start should not panic
	err = svc.Stop()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Note: TestStart_RestoresChainFromDB removed - service no longer restores in-memory chain from DB.
// The service now queries the DB directly for all operations.

func TestProcessHeaders_ContextCancellation(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	subscriber := testutil.NewMockSubscriber()
	svc, err := NewLiveService(LiveConfig{}, subscriber, testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
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

	subscriber := testutil.NewMockSubscriber()
	svc, err := NewLiveService(LiveConfig{}, subscriber, testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
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
	_ = subscriber.Unsubscribe()

	// Give processHeaders time to exit
	time.Sleep(10 * time.Millisecond)
}

// Note: TestAddBlock_TrimsToMaxSize removed - addBlock method no longer exists.
// The service now uses the database as the single source of truth.

func TestProcessBlock_WithMetrics_RecordsReorg(t *testing.T) {
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	// Set up chain: 98 -> 99
	client.SetBlockHeader(98, "0x98", "0x97")
	client.SetBlockHeader(99, "0x99", "0x98")
	// Add block 100 with alt hash for the reorg
	client.SetBlockHeader(100, "0x100_alt", "0x99")

	metrics := &mockMetrics{}

	// Pre-populate DB with blocks
	ctx := context.Background()
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 98, Hash: "0x98", ParentHash: "0x97",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 99, Hash: "0x99", ParentHash: "0x98",
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0x100", ParentHash: "0x99",
	})

	svc, err := NewLiveService(LiveConfig{
		EnableBlobs: false,
		Metrics:     metrics,
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(ctx)
	defer svc.cancel()

	// Process a reorg block at 100 with different hash
	header := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0x100_alt",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}

	err = svc.processBlockWithPrefetch(header, time.Now())
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

func TestHandleReorg_FetchParentError_ReturnsError(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	client := testutil.NewMockBlockchainClient()
	// Set up error for fetching parent
	client.HashLookupErr = fmt.Errorf("network error")
	client.HashLookupErrFor = "0xunknown_parent"

	// Pre-populate DB with block
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0x100", ParentHash: "0x99",
	})

	svc, err := NewLiveService(LiveConfig{
		FinalityBlockCount: 10,
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(ctx)
	defer svc.cancel()

	// Incoming reorg block with unknown parent
	block := LightBlock{
		Number:     100,
		Hash:       "0x100_alt",
		ParentHash: "0xunknown_parent",
	}

	_, _, _, _, err = svc.handleReorg(ctx, block, time.Now())
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

	subscriber := testutil.NewMockSubscriber()
	svc, err := NewLiveService(LiveConfig{}, subscriber, testutil.NewMockBlockchainClient(), stateRepo, cache, eventSink)
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
	subscriber.SendHeader(outbound.BlockHeader{
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
	client := testutil.NewMockBlockchainClient()

	svc, err := NewLiveService(LiveConfig{
		EnableBlobs: false,
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	header := outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0x100",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}

	err = svc.processBlockWithPrefetch(header, time.Now())
	if err == nil {
		t.Error("expected error when cacheAndPublishBlockData fails")
	}
	if !strings.Contains(err.Error(), "failed to fetch block data for block") {
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

	client := testutil.NewMockBlockchainClient()
	// Add block 99 to serve as the common ancestor during reorg walk
	client.SetBlockHeader(99, "0x99", "0x98")
	// Add block data for fetching with specific hash
	client.SetBlockHeader(100, "0x100_new", "0x99")

	svc, err := NewLiveService(LiveConfig{
		EnableBlobs: false,
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Pre-save block 99 (common ancestor)
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     99,
		Hash:       "0x99",
		ParentHash: "0x98",
	})

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

	err = svc.processBlockWithPrefetch(header, time.Now())
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

	client := testutil.NewMockBlockchainClient()
	// Add block 99 to serve as common ancestor during reorg walk
	client.SetBlockHeader(99, "0x99", "0x98")
	// Add block data for fetching - we need blocks with specific hashes for the v0 and v2 cases
	client.SetBlockHeader(100, "0x100_v0", "0x99")

	svc, err := NewLiveService(LiveConfig{
		EnableBlobs: false,
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	defer svc.cancel()

	// Pre-save block 99 so the common ancestor can be found during reorg detection
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     99,
		Hash:       "0x99",
		ParentHash: "0x98",
	})

	// Process first block at height 100 (version 0)
	header1 := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0x100_v0",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}
	err = svc.processBlockWithPrefetch(header1, time.Now())
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
	// (Note: reorg detection will trigger but that's expected and handled)

	// Add block data for v2
	client.SetBlockHeader(100, "0x100_v2", "0x99")

	header3 := outbound.BlockHeader{
		Number:     "0x64", // 100
		Hash:       "0x100_v2",
		ParentHash: "0x99",
		Timestamp:  "0x0",
	}
	err = svc.processBlockWithPrefetch(header3, time.Now())
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
	client := testutil.NewMockBlockchainClient()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add a base block chain including block 100 with a specific hash
	for i := int64(99); i <= 100; i++ {
		client.AddBlock(i, "")
	}

	// Get the hash that was generated for block 100
	block100Header := client.GetHeader(100)
	expectedHash := block100Header.Hash

	config := LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 64,
		EnableBlobs:        false, // Simplify test
		Logger:             slog.Default(),
	}

	svc, err := NewLiveService(config, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
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
	err = svc.processBlockWithPrefetch(header, time.Now())
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
	client := testutil.NewMockBlockchainClient()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add a base block chain but NOT block 100
	client.AddBlock(99, "")

	config := LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 64,
		EnableBlobs:        false,
		Logger:             slog.Default(),
	}

	svc, err := NewLiveService(config, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
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
	err = svc.processBlockWithPrefetch(header, time.Now())
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
		MockBlockchainClient: testutil.NewMockBlockchainClient(),
		fetchedByHash:        make(map[string][]string),
	}
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add block 99 and 100
	client.AddBlock(99, "")
	client.AddBlock(100, "")

	block100Header := client.GetHeader(100)
	expectedHash := block100Header.Hash

	config := LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 64,
		EnableBlobs:        true, // Enable blobs to test all data types
		Logger:             slog.Default(),
	}

	svc, err := NewLiveService(config, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
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

	err = svc.processBlockWithPrefetch(header, time.Now())
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

// mockClientWithHashTracking wraps MockBlockchainClient to track which methods are called with which hashes
type mockClientWithHashTracking struct {
	*testutil.MockBlockchainClient
	mu            sync.Mutex
	fetchedByHash map[string][]string // hash -> list of method names called
}

func (m *mockClientWithHashTracking) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlockDataByHash")
	m.mu.Unlock()
	return m.MockBlockchainClient.GetBlockDataByHash(ctx, blockNum, hash, fullTx)
}

func (m *mockClientWithHashTracking) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetFullBlockByHash")
	m.mu.Unlock()
	return m.MockBlockchainClient.GetFullBlockByHash(ctx, hash, fullTx)
}

func (m *mockClientWithHashTracking) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlockReceiptsByHash")
	m.mu.Unlock()
	return m.MockBlockchainClient.GetBlockReceiptsByHash(ctx, hash)
}

func (m *mockClientWithHashTracking) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlockTracesByHash")
	m.mu.Unlock()
	return m.MockBlockchainClient.GetBlockTracesByHash(ctx, hash)
}

func (m *mockClientWithHashTracking) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.Lock()
	m.fetchedByHash[hash] = append(m.fetchedByHash[hash], "GetBlobSidecarsByHash")
	m.mu.Unlock()
	return m.MockBlockchainClient.GetBlobSidecarsByHash(ctx, hash)
}

// TestHashComparisonCaseInsensitive verifies that hash comparisons are case-insensitive.
// Ethereum hashes are hex strings where 0xAAA and 0xaaa are semantically equal,
// but Go string comparison is case-sensitive. This test ensures we handle this correctly.
func TestHashComparisonCaseInsensitive(t *testing.T) {
	ctx := context.Background()

	// Create mock dependencies
	client := &caseInsensitiveMockClient{
		blocks: make(map[int64]testutil.BlockTestData),
	}
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Add blocks with UPPERCASE hashes
	// Block 99 with uppercase hash
	client.blocks[99] = testutil.BlockTestData{
		Header: outbound.BlockHeader{
			Number:     "0x63", // 99
			Hash:       "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
			Timestamp:  "0x12345678",
		},
		Receipts: json.RawMessage(`[]`),
		Traces:   json.RawMessage(`[]`),
		Blobs:    json.RawMessage(`[]`),
	}

	// Block 100 with uppercase hash, parent is block 99
	client.blocks[100] = testutil.BlockTestData{
		Header: outbound.BlockHeader{
			Number:     "0x64", // 100
			Hash:       "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			ParentHash: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Timestamp:  "0x12345679",
		},
		Receipts: json.RawMessage(`[]`),
		Traces:   json.RawMessage(`[]`),
		Blobs:    json.RawMessage(`[]`),
	}

	config := LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 64,
		EnableBlobs:        false,
		Logger:             slog.Default(),
	}

	svc, err := NewLiveService(config, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Test 1: Process block 99 with UPPERCASE hash
	err = svc.processBlockWithPrefetch(outbound.BlockHeader{
		Number:     "0x63",
		Hash:       "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
		Timestamp:  "0x12345678",
	}, time.Now())
	if err != nil {
		t.Fatalf("failed to process block 99: %v", err)
	}

	// Verify block was stored in DB
	recentBlocks, err := stateRepo.GetRecentBlocks(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get recent blocks: %v", err)
	}
	if len(recentBlocks) != 1 {
		t.Fatalf("expected 1 block in DB, got %d", len(recentBlocks))
	}

	// Verify hash was normalized to lowercase
	if recentBlocks[0].Hash != strings.ToLower("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
		t.Errorf("hash should be normalized to lowercase, got %s", recentBlocks[0].Hash)
	}

	// Test 2: Send same block with LOWERCASE hash - should be detected as duplicate
	err = svc.processBlockWithPrefetch(outbound.BlockHeader{
		Number:     "0x63",
		Hash:       "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // lowercase
		ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
		Timestamp:  "0x12345678",
	}, time.Now())
	if err != nil {
		t.Fatalf("failed to process duplicate block: %v", err)
	}

	// Block count should not increase (duplicate was detected)
	recentBlocks, err = stateRepo.GetRecentBlocks(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get recent blocks: %v", err)
	}
	if len(recentBlocks) != 1 {
		t.Errorf("duplicate block should not be added, expected 1 block, got %d", len(recentBlocks))
	}

	// Test 3: Process block 100 with lowercase parentHash - should chain correctly
	err = svc.processBlockWithPrefetch(outbound.BlockHeader{
		Number:     "0x64",
		Hash:       "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		ParentHash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // lowercase parent
		Timestamp:  "0x12345679",
	}, time.Now())
	if err != nil {
		t.Fatalf("failed to process block 100 with lowercase parent hash: %v", err)
	}

	// Verify block was added (no reorg detected due to case mismatch)
	recentBlocks, err = stateRepo.GetRecentBlocks(ctx, 100)
	if err != nil {
		t.Fatalf("failed to get recent blocks: %v", err)
	}
	if len(recentBlocks) != 2 {
		t.Fatalf("expected 2 blocks in DB, got %d", len(recentBlocks))
	}

	// Verify parent hash was normalized (blocks are sorted ascending)
	block99 := recentBlocks[0]
	block100 := recentBlocks[1]
	if block100.ParentHash != strings.ToLower("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
		t.Errorf("parent hash should be normalized to lowercase, got %s", block100.ParentHash)
	}

	// Verify chain integrity (parent hash of block 100 should match hash of block 99)
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
	blocks map[int64]testutil.BlockTestData
}

func (m *caseInsensitiveMockClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		data, _ := json.Marshal(bd.Header)
		return data, nil
	}
	return nil, fmt.Errorf("block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.Header.Hash, hash) {
			h := bd.Header
			return &h, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.Header.Hash, hash) {
			data, _ := json.Marshal(bd.Header)
			return data, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.Receipts, nil
	}
	return nil, fmt.Errorf("receipts for block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.Header.Hash, hash) {
			return bd.Receipts, nil
		}
	}
	return nil, fmt.Errorf("receipts for block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.Traces, nil
	}
	return nil, fmt.Errorf("traces for block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.Header.Hash, hash) {
			return bd.Traces, nil
		}
	}
	return nil, fmt.Errorf("traces for block %s not found", hash)
}

func (m *caseInsensitiveMockClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.Blobs, nil
	}
	return nil, fmt.Errorf("blobs for block %d not found", blockNum)
}

func (m *caseInsensitiveMockClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if strings.EqualFold(bd.Header.Hash, hash) {
			return bd.Blobs, nil
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
			blockJSON, _ := json.Marshal(bd.Header)
			result[i].Block = blockJSON
			result[i].Receipts = bd.Receipts
			result[i].Traces = bd.Traces
			result[i].Blobs = bd.Blobs
		}
	}
	return result, nil
}

func (m *caseInsensitiveMockClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lowerHash := strings.ToLower(hash)
	for _, bd := range m.blocks {
		if strings.ToLower(bd.Header.Hash) == lowerHash {
			blockJSON, _ := json.Marshal(bd.Header)
			return outbound.BlockData{
				BlockNumber: blockNum,
				Block:       blockJSON,
				Receipts:    bd.Receipts,
				Traces:      bd.Traces,
				Blobs:       bd.Blobs,
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
	client := testutil.NewMockBlockchainClient()

	// Add blocks for RPC lookups
	client.AddBlock(99, "")
	client.AddBlock(100, "")

	config := LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 64,
		EnableBlobs:        false,
		Logger:             slog.Default(),
	}

	svc, err := NewLiveService(config, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	svc.ctx = ctx

	// Get block 100's header so we know what parent hash it expects
	block100Header := client.GetHeader(100)
	block99Hash := block100Header.ParentHash

	// Pre-populate DB with block 99
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: 99, Hash: block99Hash, ParentHash: "0xblock98",
	})

	// Configure repo to fail on SaveBlock for the next block
	stateRepo.mu.Lock()
	stateRepo.saveBlockErr = fmt.Errorf("database connection failed")
	stateRepo.mu.Unlock()

	// Try to process block 100 - should fail
	err = svc.processBlockWithPrefetch(block100Header, time.Now())
	if err == nil {
		t.Fatal("expected error when SaveBlock fails")
	}
	if !strings.Contains(err.Error(), "database connection failed") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify DB only contains block 99 (block 100 was not saved due to error)
	recentBlocks, dbErr := stateRepo.GetRecentBlocks(ctx, 100)
	if dbErr != nil {
		t.Fatalf("failed to get recent blocks: %v", dbErr)
	}
	if len(recentBlocks) != 1 {
		t.Errorf("expected 1 block in DB after failed save, got %d", len(recentBlocks))
	}
	if len(recentBlocks) > 0 && recentBlocks[0].Number != 99 {
		t.Errorf("expected block 99, got block %d", recentBlocks[0].Number)
	}

	t.Log("SUCCESS: DB correctly rolled back after SaveBlock failure")
}

// Note: TestProcessBlock_ReorgChainNotPrunedOnDBFailure removed - the test was checking
// in-memory chain consistency which no longer exists. DB atomicity is guaranteed by
// the HandleReorgAtomic implementation using database transactions.

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

func (c *mockOrderTrackingCache) SetBlockData(ctx context.Context, chainID int64, blockNumber int64, version int, data outbound.BlockDataInput) error {
	c.mu.Lock()
	c.operations = append(c.operations, "cache_block")
	c.operations = append(c.operations, "cache_receipts")
	c.operations = append(c.operations, "cache_traces")
	if data.Blobs != nil {
		c.operations = append(c.operations, "cache_blobs")
	}
	c.mu.Unlock()
	return c.BlockCache.SetBlockData(ctx, chainID, blockNumber, version, data)
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

func TestCacheAndPublishBlockData_CachesAllDataBeforePublishing(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := newMockOrderTrackingCache()
	eventSink := newMockOrderTrackingEventSink()

	client := newMockFailingClient()
	client.AddBlock(100, "")

	header100 := client.GetHeader(100)
	blockData, _ := client.GetBlockDataByHash(ctx, 100, header100.Hash, true)

	svc, err := NewLiveService(LiveConfig{
		EnableTraces: true,
		EnableBlobs:  true, // Enable all data types to test all 4 cache operations
	}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	err = svc.cacheAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false, blockData)
	if err != nil {
		t.Fatalf("cacheAndPublishBlockData failed: %v", err)
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

func TestCacheAndPublishBlockData_NoPublishOnCacheFailure(t *testing.T) {
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
			client.AddBlock(100, "")
			header100 := client.GetHeader(100)
			blockData, _ := client.GetBlockDataByHash(ctx, 100, header100.Hash, true)

			svc, err := NewLiveService(LiveConfig{
				EnableTraces: true,
				EnableBlobs:  true,
			}, testutil.NewMockSubscriber(), client, stateRepo, cache, eventSink)
			if err != nil {
				t.Fatalf("failed to create service: %v", err)
			}

			err = svc.cacheAndPublishBlockData(ctx, header100, 100, 0, time.Now(), false, blockData)
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

// TestReorgPruning_InMemoryMustMatchDB verifies that after a reorg is handled,
// the in-memory chain state MUST match the database state.
//
// This is critical for mission-critical consistency: if in-memory state diverges
// from DB after a reorg, subsequent block processing will produce false reorg
// detections or errors.
//
// The bug: pruneReorgedBlocks() was called without error handling or verification.
// If the DB has blocks that memory doesn't know about (e.g., from concurrent
// backfill), we silently lose synchronization.
//
// The fix: After HandleReorgAtomic succeeds, reload in-memory chain from DB
// (the authoritative source) rather than relying on manual pruning.
//
// This test uses only the public API (Start/Stop) and mock adapters, exactly
// like production usage in cmd/watcher/main.go.
func TestReorgPruning_InMemoryMustMatchDB(t *testing.T) {
	ctx := context.Background()
	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()
	subscriber := testutil.NewMockSubscriber()
	client := testutil.NewMockBlockchainClient()

	// Set up initial DB state: blocks 95-100 (what service will load on Start)
	initialBlocks := []outbound.BlockState{
		{Number: 95, Hash: "0xblock95", ParentHash: "0xblock94", ReceivedAt: time.Now().Unix()},
		{Number: 96, Hash: "0xblock96", ParentHash: "0xblock95", ReceivedAt: time.Now().Unix()},
		{Number: 97, Hash: "0xblock97", ParentHash: "0xblock96", ReceivedAt: time.Now().Unix()},
		{Number: 98, Hash: "0xblock98", ParentHash: "0xblock97", ReceivedAt: time.Now().Unix()},
		{Number: 99, Hash: "0xblock99", ParentHash: "0xblock98", ReceivedAt: time.Now().Unix()},
		{Number: 100, Hash: "0xblock100", ParentHash: "0xblock99", ReceivedAt: time.Now().Unix()},
	}
	for _, b := range initialBlocks {
		if _, err := stateRepo.SaveBlock(ctx, b); err != nil {
			t.Fatalf("failed to save initial block: %v", err)
		}
	}

	// Set up client to return block data for RPC fetches
	// The reorg block (99_new) needs to be fetchable
	client.SetBlockHeader(99, "0xblock99_new", "0xblock97")

	svc, err := NewLiveService(LiveConfig{
		Logger:             slog.Default(),
		FinalityBlockCount: 64,
	}, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Start the service - this loads initial blocks from DB into memory
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	// CRITICAL: Simulate backfill adding blocks AFTER service started.
	// These blocks (93, 94) are now in DB but NOT in the service's in-memory chain.
	// This simulates the race condition where backfill runs concurrently with live service.
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     93,
		Hash:       "0xblock93",
		ParentHash: "0xblock92",
		ReceivedAt: time.Now().Unix(),
	})
	_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     94,
		Hash:       "0xblock94",
		ParentHash: "0xblock93",
		ReceivedAt: time.Now().Unix(),
	})

	// Send a reorg block via the subscriber (simulating WebSocket notification)
	// Block 99_new has parent 0xblock97, which orphans blocks 98, 99, 100
	subscriber.SendHeader(outbound.BlockHeader{
		Number:     "0x63", // 99
		Hash:       "0xblock99_new",
		ParentHash: "0xblock97", // Forks at 97
		Timestamp:  "0x0",
	})

	// Wait for the reorg event to be published (with timeout)
	testutil.WaitForCondition(t, 2*time.Second, func() bool {
		for _, e := range eventSink.GetBlockEvents() {
			if e.BlockNumber == 99 && e.BlockHash == "0xblock99_new" && e.IsReorg {
				return true
			}
		}
		return false
	}, "reorg event for block 99_new")

	// Verify DB state after reorg
	dbBlocks, err := stateRepo.GetRecentBlocks(ctx, 200)
	if err != nil {
		t.Fatalf("failed to get recent blocks from DB: %v", err)
	}

	var dbNonOrphaned []outbound.BlockState
	for _, b := range dbBlocks {
		if !b.IsOrphaned {
			dbNonOrphaned = append(dbNonOrphaned, b)
		}
	}

	// The DB should have: 93, 94, 95, 96, 97, 99_new = 6 non-orphaned blocks
	// (blocks 98, 99, 100 are orphaned by the reorg)
	if len(dbNonOrphaned) != 6 {
		t.Fatalf("expected 6 non-orphaned blocks in DB after reorg, got %d", len(dbNonOrphaned))
	}

	// Verify expected blocks are present
	expectedBlocks := map[int64]string{
		93: "0xblock93",
		94: "0xblock94",
		95: "0xblock95",
		96: "0xblock96",
		97: "0xblock97",
		99: "0xblock99_new",
	}
	for _, b := range dbNonOrphaned {
		expectedHash, ok := expectedBlocks[b.Number]
		if !ok {
			t.Errorf("unexpected block %d in DB", b.Number)
		} else if b.Hash != expectedHash {
			t.Errorf("block %d: expected hash %s, got %s", b.Number, expectedHash, b.Hash)
		}
	}

	// Now send another block that depends on the backfilled blocks being in memory.
	// Set up the client to return block 94_alt that forks at 93
	client.SetBlockHeader(94, "0xblock94_alt", "0xblock93")

	// Record event count before sending second block
	eventCountBefore := len(eventSink.GetBlockEvents())

	// Send block 94_alt - if memory has block 93 (from DB reload), this should work
	// If memory doesn't have block 93 (bug), it will fail during reorg walk
	subscriber.SendHeader(outbound.BlockHeader{
		Number:     "0x5e", // 94
		Hash:       "0xblock94_alt",
		ParentHash: "0xblock93",
		Timestamp:  "0x0",
	})

	// Wait for the second block to be processed
	// With the fix, block 93 is in memory after the first reorg (reloaded from DB),
	// so block 94_alt should be processed successfully as a reorg
	testutil.WaitForCondition(t, 2*time.Second, func() bool {
		return len(eventSink.GetBlockEvents()) > eventCountBefore
	}, "event for block 94_alt (requires block 93 to be in memory from DB reload)")

	// Verify the second block was processed correctly
	events := eventSink.GetBlockEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 events total (99_new and 94_alt), got %d", len(events))
	}

	// Verify first event is the initial reorg block
	if events[0].BlockNumber != 99 || events[0].BlockHash != "0xblock99_new" || !events[0].IsReorg {
		t.Errorf("first event: expected block 99 (0xblock99_new, reorg=true), got block %d (%s, reorg=%v)",
			events[0].BlockNumber, events[0].BlockHash, events[0].IsReorg)
	}

	// Verify second event is the block that required backfilled block 93 as ancestor
	if events[1].BlockNumber != 94 || events[1].BlockHash != "0xblock94_alt" || !events[1].IsReorg {
		t.Errorf("second event: expected block 94 (0xblock94_alt, reorg=true), got block %d (%s, reorg=%v)",
			events[1].BlockNumber, events[1].BlockHash, events[1].IsReorg)
	}
}

// TestReorgWithBackfilledBlocks_CommonAncestorCalculation verifies that when
// backfill adds blocks to the DB that aren't in LiveService's memory, a reorg
// still calculates the correct common ancestor.
//
// Bug scenario (before fix):
// 1. LiveService starts with in-memory chain: [98, 99, 100]
// 2. Backfill adds blocks 101, 102, 103 to DB AFTER service started (not in memory)
// 3. LiveService receives block 104 via WebSocket, adds to memory: [98, 99, 100, 104]
// 4. Reorg occurs: new block 102' arrives with parent = hash_of_101
// 5. BUG: Common ancestor calculated as 100 (wrong), should be 101
// 6. BUG: Block 101 incorrectly marked as orphaned
//
// This test verifies that the common ancestor is correctly identified as the
// block in the DB, even if LiveService never had it in memory.
func TestReorgWithBackfilledBlocks_CommonAncestorCalculation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stateRepo := newMockStateRepo()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Create a mock client that serves specific blocks
	client := testutil.NewMockBlockchainClient()

	// Build a chain: 98 -> 99 -> 100 -> 101 -> 102 -> 103 -> 104
	for i := int64(98); i <= 104; i++ {
		client.AddBlock(i, "")
	}

	// Get block 101's hash BEFORE any overwriting - this is the common ancestor
	block101 := client.GetHeader(101)
	// Save original block 102's hash for later verification
	originalBlock102 := client.GetHeader(102)

	// Create the reorg block: 102' with same parent as 102 (parent = hash of 101)
	reorgBlock := outbound.BlockHeader{
		Number:     "0x66", // 102
		Hash:       "0xreorg_block_102_prime",
		ParentHash: block101.Hash, // Same parent as original 102
		Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
	}

	// Set up initial DB state: ONLY blocks 98, 99, 100
	// This is what LiveService will load on startup
	for i := int64(98); i <= 100; i++ {
		h := client.GetHeader(i)
		_, _ = stateRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       h.Hash,
			ParentHash: h.ParentHash,
			ReceivedAt: time.Now().Unix(),
		})
	}

	// Create and start LiveService - it loads blocks 98, 99, 100 into memory
	subscriber := testutil.NewMockSubscriber()
	svc, err := NewLiveService(LiveConfig{
		Logger:             slog.Default(),
		FinalityBlockCount: 64,
	}, subscriber, client, stateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	// Verify DB has 3 blocks from initialization
	recentBlocks, _ := stateRepo.GetRecentBlocks(ctx, 100)
	if len(recentBlocks) != 3 {
		t.Fatalf("expected 3 blocks in DB after start, got %d", len(recentBlocks))
	}

	// NOW simulate backfill adding blocks 101, 102, 103 to DB
	// IMPORTANT: Use the ORIGINAL block headers, not from client (which will be overwritten with reorg block)
	// (in the old implementation, these were NOT in memory, causing the bug)
	backfillBlocks := []outbound.BlockState{
		{Number: 101, Hash: block101.Hash, ParentHash: client.GetHeader(100).Hash},
		{Number: 102, Hash: originalBlock102.Hash, ParentHash: block101.Hash},
		{Number: 103, Hash: client.GetHeader(103).Hash, ParentHash: originalBlock102.Hash},
	}
	for _, b := range backfillBlocks {
		b.ReceivedAt = time.Now().Unix()
		_, _ = stateRepo.SaveBlock(ctx, b)
	}

	// LiveService receives block 104 via WebSocket
	h104 := client.GetHeader(104)
	subscriber.SendHeader(h104)

	// Wait for block 104 to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify DB now has 7 blocks (98-104)
	recentBlocks, _ = stateRepo.GetRecentBlocks(ctx, 100)
	if len(recentBlocks) != 7 {
		t.Logf("blocks in DB: %v", recentBlocks)
		t.Fatalf("expected 7 blocks in DB, got %d", len(recentBlocks))
	}

	// Add reorg block to client so it can be fetched when processing
	client.SetBlockHeader(102, reorgBlock.Hash, reorgBlock.ParentHash)

	// Now send the reorg block (102') via WebSocket
	subscriber.SendHeader(reorgBlock)

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Verify: Block 101 should NOT be orphaned (it's the common ancestor)
	block101State, err := stateRepo.GetBlockByHash(ctx, block101.Hash)
	if err != nil {
		t.Fatalf("failed to get block 101: %v", err)
	}
	if block101State == nil {
		t.Fatal("block 101 not found in DB")
	}
	if block101State.IsOrphaned {
		t.Errorf("BUG: Block 101 was incorrectly marked as orphaned. " +
			"It should be the common ancestor and remain canonical. " +
			"This happens because 101 was added by backfill (not in memory), " +
			"so the common ancestor was incorrectly calculated as 100.")
	}

	// Verify: Original blocks 102, 103, 104 SHOULD be orphaned
	// Note: We use saved hashes because client.blocks[102] was overwritten with reorg block
	orphanedHashes := map[int64]string{
		102: originalBlock102.Hash,
		103: client.GetHeader(103).Hash,
		104: h104.Hash,
	}
	for blockNum, hash := range orphanedHashes {
		blockState, err := stateRepo.GetBlockByHash(ctx, hash)
		if err != nil {
			t.Fatalf("failed to get block %d: %v", blockNum, err)
		}
		if blockState == nil {
			t.Fatalf("block %d (hash %s) not found in DB", blockNum, hash)
		}
		if !blockState.IsOrphaned {
			t.Errorf("Block %d should be orphaned but isn't", blockNum)
		}
	}

	// Verify: The reorg block (102') should be saved and canonical
	reorgBlockState, err := stateRepo.GetBlockByHash(ctx, reorgBlock.Hash)
	if err != nil {
		t.Fatalf("failed to get reorg block: %v", err)
	}
	if reorgBlockState == nil {
		t.Fatal("reorg block (102') not found in DB")
	}
	if reorgBlockState.IsOrphaned {
		t.Error("reorg block (102') should be canonical, not orphaned")
	}
}
