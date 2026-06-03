//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	dsn, dbCleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	redisAddr, redisCleanup := testutil.StartRedisForMain()
	sharedRedisAddr = redisAddr

	lsCfg, lsCleanup := testutil.StartLocalStackForMain("sns,sqs")
	sharedLocalStackCfg = lsCfg

	code := m.Run()

	lsCleanup()
	redisCleanup()
	dbCleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

// newTestBlockchainClient returns a MockBlockchainClient configured for
// watcher e2e tests. It sets UseBlockErrForMissing so that GetBlocksBatch
// returns BlockErr on blocks not previously added.
func newTestBlockchainClient() *testutil.MockBlockchainClient {
	c := testutil.NewMockBlockchainClient()
	c.UseBlockErrForMissing = true
	return c
}

// =============================================================================
// Test Cases
// =============================================================================

// TestLiveService_ProcessesNewBlock tests the full flow of processing a new block.
func TestLiveService_ProcessesNewBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Set up infrastructure
	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	// Create mock subscriber that will emit blocks
	mockSub := testutil.NewMockSubscriber()

	// Create mock blockchain client
	mockClient := newTestBlockchainClient()

	// Create live service
	config := live_data.LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 2, // Use small finality for faster tests
		EnableBlobs:        false,
		Logger:             slog.Default(),
	}

	liveService, err := live_data.NewLiveService(
		config,
		mockSub,
		mockClient,
		infra.BlockStateRepo,
		infra.Cache,
		infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create live service: %v", err)
	}

	// Start the service
	if err := liveService.Start(ctx); err != nil {
		t.Fatalf("failed to start live service: %v", err)
	}
	defer liveService.Stop()

	// Set up block data that mock client will return
	mockClient.AddBlock(100, "0x"+fmt.Sprintf("%064x", 99))

	// Emit block notification via header
	header := mockClient.GetHeader(100)
	mockSub.SendHeader(header)

	// Wait for messages on all queues
	const messageTimeout = 10 * time.Second

	t.Run("block_published", func(t *testing.T) {
		msg := infra.WaitForMessage(t, infra.BlocksQueueURL, messageTimeout)
		if msg == "" {
			t.Fatal("expected block message, got none")
		}
		t.Logf("Received block message: %s", truncate(msg, 200))

		// Verify the message contains expected data
		var blockMsg map[string]interface{}
		if err := json.Unmarshal([]byte(msg), &blockMsg); err != nil {
			t.Fatalf("failed to parse block message: %v", err)
		}
	})

	// Note: With the new unified topic architecture, we only publish a single block event
	// that includes references to block/receipts/traces/blobs data in cache.
	// The separate receipts/traces topics are no longer used.

	t.Run("block_state_saved", func(t *testing.T) {
		var state *outbound.BlockState
		if !testutil.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
			var err error
			state, err = infra.BlockStateRepo.GetBlockByNumber(ctx, 100)
			if err != nil {
				t.Fatalf("failed to get block state: %v", err)
			}
			return state != nil
		}) {
			t.Fatal("expected block state to be saved")
		}
		expectedHash := fmt.Sprintf("0x%064x", 100)
		if state.Hash != expectedHash {
			t.Errorf("expected hash %s, got %s", expectedHash, state.Hash)
		}
	})
}

// TestMultipleBlocksInSequence tests processing multiple blocks in sequence.
func TestMultipleBlocksInSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	mockSub := testutil.NewMockSubscriber()
	mockClient := newTestBlockchainClient()

	config := live_data.LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 3,
		EnableBlobs:        false,
		Logger:             slog.Default(),
	}

	liveService, err := live_data.NewLiveService(
		config,
		mockSub,
		mockClient,
		infra.BlockStateRepo,
		infra.Cache,
		infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create live service: %v", err)
	}

	if err := liveService.Start(ctx); err != nil {
		t.Fatalf("failed to start live service: %v", err)
	}
	defer liveService.Stop()

	// Process 10 blocks, waiting for each to be saved before sending the next
	const numBlocks = 10
	for i := int64(100); i < 100+numBlocks; i++ {
		parentHash := fmt.Sprintf("0x%064x", i-1)
		mockClient.AddBlock(i, parentHash)
		header := mockClient.GetHeader(i)
		mockSub.SendHeader(header)
		time.Sleep(50 * time.Millisecond)
	}

	// Wait until all blocks are saved
	if !testutil.WaitFor(t, 10*time.Second, 100*time.Millisecond, func() bool {
		for i := int64(100); i < 100+numBlocks; i++ {
			state, _ := infra.BlockStateRepo.GetBlockByNumber(ctx, i)
			if state == nil {
				return false
			}
		}
		return true
	}) {
		t.Fatal("timed out waiting for all blocks to be saved")
	}

	// Verify all blocks are saved
	for i := int64(100); i < 100+numBlocks; i++ {
		state, err := infra.BlockStateRepo.GetBlockByNumber(ctx, i)
		if err != nil {
			t.Errorf("failed to get block %d: %v", i, err)
			continue
		}
		if state == nil {
			t.Errorf("block %d not found in database", i)
			continue
		}
		expectedHash := fmt.Sprintf("0x%064x", i)
		if state.Hash != expectedHash {
			t.Errorf("block %d: expected hash %s, got %s", i, expectedHash, state.Hash)
		}
	}

	// Verify last block
	lastBlock, err := infra.BlockStateRepo.GetLastBlock(ctx)
	if err != nil {
		t.Fatalf("failed to get last block: %v", err)
	}
	if lastBlock == nil {
		t.Fatal("expected last block to exist")
	}
	if lastBlock.Number != 109 {
		t.Errorf("expected last block number 109, got %d", lastBlock.Number)
	}
}

// =============================================================================
// Multi-Chain Tests
// =============================================================================

// TestMultiChain_Isolation verifies that two chains sharing the same
// database have proper data isolation: blocks, last block, and queries are scoped
// by chain_id.
func TestMultiChain_Isolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create per-chain repos sharing the same DB pool
	ethRepo := postgres.NewBlockStateRepository(infra.Pool, 1, logger)
	avaxRepo := postgres.NewBlockStateRepository(infra.Pool, 43114, logger)

	// Create per-chain mock infrastructure
	ethSub := testutil.NewMockSubscriber()
	avaxSub := testutil.NewMockSubscriber()
	ethClient := testutil.NewMockBlockchainClient()
	avaxClient := testutil.NewMockBlockchainClient()

	// Create per-chain live services
	ethLive, err := live_data.NewLiveService(
		live_data.LiveConfig{ChainID: 1, FinalityBlockCount: 2, Logger: logger},
		ethSub, ethClient, ethRepo, infra.Cache, infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create eth live service: %v", err)
	}

	avaxLive, err := live_data.NewLiveService(
		live_data.LiveConfig{ChainID: 43114, FinalityBlockCount: 2, Logger: logger},
		avaxSub, avaxClient, avaxRepo, infra.Cache, infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create avalanche live service: %v", err)
	}

	// Start both
	if err := ethLive.Start(ctx); err != nil {
		t.Fatalf("failed to start eth service: %v", err)
	}
	defer ethLive.Stop()

	if err := avaxLive.Start(ctx); err != nil {
		t.Fatalf("failed to start avalanche service: %v", err)
	}
	defer avaxLive.Stop()

	// Add overlapping block numbers: both chains have blocks 100-102
	// Ethereum hashes start with 0x1...
	for i := int64(100); i <= 102; i++ {
		parentHash := fmt.Sprintf("0x1%063x", i-1)
		ethClient.SetBlockHeader(i, fmt.Sprintf("0x1%063x", i), parentHash)
	}
	// Avalanche hashes start with 0x2...
	for i := int64(100); i <= 102; i++ {
		parentHash := fmt.Sprintf("0x2%063x", i-1)
		avaxClient.SetBlockHeader(i, fmt.Sprintf("0x2%063x", i), parentHash)
	}

	// Send block headers to both chains
	for i := int64(100); i <= 102; i++ {
		ethSub.SendHeader(ethClient.GetHeader(i))
		avaxSub.SendHeader(avaxClient.GetHeader(i))
	}

	// Wait for blocks to be processed
	if !testutil.WaitFor(t, 10*time.Second, 100*time.Millisecond, func() bool {
		ethLast, _ := ethRepo.GetLastBlock(ctx)
		avaxLast, _ := avaxRepo.GetLastBlock(ctx)
		return ethLast != nil && ethLast.Number == 102 && avaxLast != nil && avaxLast.Number == 102
	}) {
		t.Fatal("timed out waiting for both chains to process blocks")
	}

	t.Run("ethereum_blocks_isolated", func(t *testing.T) {
		block, err := ethRepo.GetBlockByNumber(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected eth block 100")
		}
		if block.Hash != fmt.Sprintf("0x1%063x", 100) {
			t.Errorf("expected eth hash, got %s", block.Hash)
		}
	})

	t.Run("avalanche_blocks_isolated", func(t *testing.T) {
		block, err := avaxRepo.GetBlockByNumber(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected avalanche block 100")
		}
		if block.Hash != fmt.Sprintf("0x2%063x", 100) {
			t.Errorf("expected avalanche hash, got %s", block.Hash)
		}
	})

	t.Run("last_block_per_chain", func(t *testing.T) {
		ethLast, err := ethRepo.GetLastBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ethLast == nil || ethLast.Number != 102 {
			t.Errorf("expected eth last block 102, got %v", ethLast)
		}

		avaxLast, err := avaxRepo.GetLastBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if avaxLast == nil || avaxLast.Number != 102 {
			t.Errorf("expected avalanche last block 102, got %v", avaxLast)
		}
	})
}

// TestMultiChain_BackfillWithGaps verifies that backfill watermarks
// are chain-scoped: each chain's watermark advances independently.
func TestMultiChain_BackfillWithGaps(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ethRepo := postgres.NewBlockStateRepository(infra.Pool, 1, logger)
	avaxRepo := postgres.NewBlockStateRepository(infra.Pool, 43114, logger)

	t.Run("watermarks_are_independent", func(t *testing.T) {
		// Set different watermarks per chain
		if err := ethRepo.SetBackfillWatermark(ctx, 500); err != nil {
			t.Fatalf("failed to set eth watermark: %v", err)
		}
		if err := avaxRepo.SetBackfillWatermark(ctx, 200); err != nil {
			t.Fatalf("failed to set avalanche watermark: %v", err)
		}

		ethWM, err := ethRepo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("failed to get eth watermark: %v", err)
		}
		if ethWM != 500 {
			t.Errorf("expected eth watermark 500, got %d", ethWM)
		}

		avaxWM, err := avaxRepo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("failed to get avalanche watermark: %v", err)
		}
		if avaxWM != 200 {
			t.Errorf("expected avalanche watermark 200, got %d", avaxWM)
		}
	})

	t.Run("gaps_are_chain_scoped", func(t *testing.T) {
		// Reset watermarks for this subtest
		if err := ethRepo.SetBackfillWatermark(ctx, 0); err != nil {
			t.Fatalf("failed to reset eth watermark: %v", err)
		}
		if err := avaxRepo.SetBackfillWatermark(ctx, 0); err != nil {
			t.Fatalf("failed to reset avalanche watermark: %v", err)
		}

		// Save eth blocks 1, 2, 5 (gap at 3-4)
		now := time.Now().Unix()
		for _, num := range []int64{1, 2, 5} {
			_, err := ethRepo.SaveBlock(ctx, outbound.BlockState{
				Number:         num,
				Hash:           fmt.Sprintf("0x1_gap_%d", num),
				ParentHash:     fmt.Sprintf("0x1_gap_%d", num-1),
				ReceivedAt:     now,
				BlockTimestamp: now,
			})
			if err != nil {
				t.Fatalf("failed to save eth block %d: %v", num, err)
			}
		}

		// Save avalanche blocks 1-5 (no gap)
		for i := int64(1); i <= 5; i++ {
			_, err := avaxRepo.SaveBlock(ctx, outbound.BlockState{
				Number:         i,
				Hash:           fmt.Sprintf("0x2_gap_%d", i),
				ParentHash:     fmt.Sprintf("0x2_gap_%d", i-1),
				ReceivedAt:     now,
				BlockTimestamp: now,
			})
			if err != nil {
				t.Fatalf("failed to save avalanche block %d: %v", i, err)
			}
		}

		// Eth should have a gap
		ethGaps, err := ethRepo.FindGaps(ctx, 1, 5)
		if err != nil {
			t.Fatalf("failed to find eth gaps: %v", err)
		}
		if len(ethGaps) != 1 {
			t.Fatalf("expected 1 eth gap, got %d: %v", len(ethGaps), ethGaps)
		}
		if ethGaps[0].From != 3 || ethGaps[0].To != 4 {
			t.Errorf("expected eth gap [3,4], got [%d,%d]", ethGaps[0].From, ethGaps[0].To)
		}

		// Avalanche should have no gaps
		avaxGaps, err := avaxRepo.FindGaps(ctx, 1, 5)
		if err != nil {
			t.Fatalf("failed to find avalanche gaps: %v", err)
		}
		if len(avaxGaps) != 0 {
			t.Errorf("expected no avalanche gaps, got %d: %v", len(avaxGaps), avaxGaps)
		}
	})
}

// TestWatcherBackfill_SelfHealsWronglyOrphanedGap is the from-the-outside
// reproduction of the VEC-277 arbitrum incident. It wires a BackfillService
// the way the watcher main does — real Postgres, real Redis cache, real SNS
// event sink, mock blockchain client — and drives the REAL gap-fill loop via
// Start() (not the RunOnce test helper).
//
// It seeds the production-shaped stuck state: a canonical block wrongly
// orphaned with no replacement (what the blanket-orphan did to a late-arriving
// block's canonical successor), with the watermark pinned below it. It then
// asserts the running service drains the gap and advances the watermark on its
// own, within a deadline. Pre-fix this never converges: the loop refetches the
// orphan-only row every poll as a no-op, FindGaps keeps reporting it, and the
// watermark stays pinned — so this test would time out, which is exactly the
// signal the incident lacked.
func TestWatcherBackfill_SelfHealsWronglyOrphanedGap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewBlockStateRepository(infra.Pool, 1, logger)

	// Mock the canonical chain 1..5 and seed the DB from the same headers so
	// parent_hash linkage is consistent (the heal's linkage re-check passes).
	client := newTestBlockchainClient()
	for i := int64(1); i <= 5; i++ {
		client.AddBlock(i, "")
	}
	const orphanedNum int64 = 3
	for i := int64(1); i <= 5; i++ {
		h := client.GetHeader(i)
		if _, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           h.Hash,
			ParentHash:     h.ParentHash,
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("seed block %d: %v", i, err)
		}
		// All published before the incident; otherwise watermark advancement
		// would be capped by GetMinUnpublishedBlock rather than by the gap.
		if err := repo.MarkPublishComplete(ctx, h.Hash); err != nil {
			t.Fatalf("mark published %d: %v", i, err)
		}
	}
	orphanHash := client.GetHeader(orphanedNum).Hash
	if err := repo.MarkBlockOrphaned(ctx, orphanHash); err != nil {
		t.Fatalf("orphan block %d: %v", orphanedNum, err)
	}
	if err := repo.SetBackfillWatermark(ctx, orphanedNum-1); err != nil {
		t.Fatalf("pin watermark: %v", err)
	}

	// Pre-condition: the gap exists and the watermark is pinned below it.
	gaps, err := repo.FindGaps(ctx, 1, 5)
	if err != nil {
		t.Fatalf("FindGaps pre: %v", err)
	}
	if len(gaps) != 1 || gaps[0].From != orphanedNum || gaps[0].To != orphanedNum {
		t.Fatalf("expected single pre-fix gap [%d,%d], got %v", orphanedNum, orphanedNum, gaps)
	}

	// Start the REAL backfill loop (BoundaryCheckDepth -1 isolates the gap-fill
	// → heal → watermark path from the RPC boundary check).
	svc, err := backfill_gaps.NewBackfillService(backfill_gaps.BackfillConfig{
		ChainID:            1,
		BatchSize:          10,
		PollInterval:       200 * time.Millisecond,
		BoundaryCheckDepth: -1,
		Logger:             logger,
	}, client, repo, infra.Cache, infra.EventSink)
	if err != nil {
		t.Fatalf("NewBackfillService: %v", err)
	}
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// The running loop must drain the gap and advance the watermark to head on
	// its own. Pre-fix this never happens and we hit the deadline.
	deadline := time.Now().Add(30 * time.Second)
	for {
		wm, err := repo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("GetBackfillWatermark: %v", err)
		}
		gapsNow, err := repo.FindGaps(ctx, 1, 5)
		if err != nil {
			t.Fatalf("FindGaps poll: %v", err)
		}
		if wm == 5 && len(gapsNow) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backfill loop did not self-heal within deadline: watermark=%d gaps=%v (pre-fix it never converges)", wm, gapsNow)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The orphan must be resurrected as canonical with its original hash, not
	// re-inserted as a new row.
	got, err := repo.GetBlockByNumber(ctx, orphanedNum)
	if err != nil {
		t.Fatalf("GetBlockByNumber: %v", err)
	}
	if got == nil {
		t.Fatalf("expected canonical block at %d, got nil", orphanedNum)
	}
	if got.IsOrphaned || got.Hash != orphanHash {
		t.Fatalf("expected canonical block %d with original hash %s, got %+v", orphanedNum, orphanHash, got)
	}
}

// TestWatcherLive_OutOfOrderArrival_DoesNotOrphanCanonicalSuccessor reproduces
// the VEC-277 TRIGGER through the live service's real path: headers arriving
// out of order on a fast chain. It drives the public entrypoint
// (subscriber → processHeaders → detectReorg → handleReorg → HandleReorgAtomic)
// against real Postgres, with no pre-seeded orphan — the orphaning, if the bug
// were present, would be produced by the code itself.
//
// Sequence: block 100 arrives, then 102 (a forward gap; saved canonical), then
// 101 arrives LATE. Pre-fix, 101 is misclassified as a reorg and the blanket
// orphan UPDATE (number > commonAncestor) wrongly orphans the still-canonical
// 102, creating the orphan-only row that pins the backfill loop. Post-fix, the
// forward-frontier preserveChain keeps 102 canonical. The test asserts all
// three heights end canonical and contiguous — i.e. out-of-order delivery is
// handled correctly end to end.
func TestWatcherLive_OutOfOrderArrival_DoesNotOrphanCanonicalSuccessor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	mockSub := testutil.NewMockSubscriber()
	mockClient := newTestBlockchainClient()
	// Canonical chain 100,101,102 with linked parent hashes (AddBlock links
	// hash(N)=0x..N to parent 0x..N-1).
	for _, n := range []int64{100, 101, 102} {
		mockClient.AddBlock(n, "")
	}

	liveService, err := live_data.NewLiveService(
		live_data.LiveConfig{
			ChainID:            1,
			FinalityBlockCount: 16,
			Logger:             slog.Default(),
		},
		mockSub,
		mockClient,
		infra.BlockStateRepo,
		infra.Cache,
		infra.EventSink,
	)
	if err != nil {
		t.Fatalf("NewLiveService: %v", err)
	}
	if err := liveService.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer liveService.Stop()

	canonicalAt := func(num int64) bool {
		st, err := infra.BlockStateRepo.GetBlockByNumber(ctx, num)
		if err != nil {
			t.Fatalf("GetBlockByNumber(%d): %v", num, err)
		}
		return st != nil && !st.IsOrphaned && st.Hash == fmt.Sprintf("0x%064x", num)
	}

	// 100 arrives and becomes head.
	mockSub.SendHeader(mockClient.GetHeader(100))
	if !testutil.WaitFor(t, 10*time.Second, 50*time.Millisecond, func() bool { return canonicalAt(100) }) {
		t.Fatal("block 100 never became canonical")
	}

	// 102 arrives before 101 — a forward gap. detectReorg saves it canonically
	// (number > head+1), so 102 is a canonical successor with parent_hash = 101.
	mockSub.SendHeader(mockClient.GetHeader(102))
	if !testutil.WaitFor(t, 10*time.Second, 50*time.Millisecond, func() bool { return canonicalAt(102) }) {
		t.Fatal("block 102 never became canonical")
	}

	// 101 arrives LATE. number <= head(102) → handleReorg path. This is the
	// exact step that wrongly orphaned 102 pre-fix.
	mockSub.SendHeader(mockClient.GetHeader(101))
	if !testutil.WaitFor(t, 10*time.Second, 50*time.Millisecond, func() bool { return canonicalAt(101) }) {
		t.Fatal("late block 101 never became canonical")
	}

	t.Run("canonical successor 102 not orphaned by the late arrival", func(t *testing.T) {
		// The defining regression: 102 must still be canonical after 101's
		// reorg-classified arrival.
		if !canonicalAt(102) {
			row, _ := infra.BlockStateRepo.GetBlockByHash(ctx, fmt.Sprintf("0x%064x", 102))
			t.Fatalf("block 102 was wrongly orphaned by out-of-order arrival of 101 (the VEC-277 bug); row=%+v", row)
		}
	})

	t.Run("chain is contiguous and canonical 100..102", func(t *testing.T) {
		for _, n := range []int64{100, 101, 102} {
			if !canonicalAt(n) {
				t.Errorf("expected block %d canonical with hash 0x%064x", n, n)
			}
		}
		gaps, err := infra.BlockStateRepo.FindGaps(ctx, 100, 102)
		if err != nil {
			t.Fatalf("FindGaps: %v", err)
		}
		if len(gaps) != 0 {
			t.Errorf("expected no gaps across 100..102 after out-of-order delivery, got %v", gaps)
		}
	})
}

// TestMultiChain_ReorgIsolation verifies that a reorg on one chain
// does not affect blocks on another chain.
func TestMultiChain_ReorgIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ethRepo := postgres.NewBlockStateRepository(infra.Pool, 1, logger)
	avaxRepo := postgres.NewBlockStateRepository(infra.Pool, 43114, logger)

	// Save blocks 100-102 on both chains
	for i := int64(100); i <= 102; i++ {
		_, err := ethRepo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0x1_reorg_%d", i),
			ParentHash:     fmt.Sprintf("0x1_reorg_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save eth block %d: %v", i, err)
		}

		_, err = avaxRepo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0x2_reorg_%d", i),
			ParentHash:     fmt.Sprintf("0x2_reorg_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save avalanche block %d: %v", i, err)
		}
	}

	// Trigger reorg on Ethereum only at block 101
	reorgEvent := outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: 101,
		OldHash:     "0x1_reorg_101",
		NewHash:     "0x1_reorg_101_new",
		Depth:       1,
	}
	newBlock := outbound.BlockState{
		Number:         101,
		Hash:           "0x1_reorg_101_new",
		ParentHash:     "0x1_reorg_100",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}

	_, err := ethRepo.HandleReorgAtomic(ctx, 100, reorgEvent, newBlock)
	if err != nil {
		t.Fatalf("HandleReorgAtomic failed: %v", err)
	}

	t.Run("ethereum_reorg_applied", func(t *testing.T) {
		// Old eth block 101 should be orphaned
		oldBlock, err := ethRepo.GetBlockByHash(ctx, "0x1_reorg_101")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !oldBlock.IsOrphaned {
			t.Error("expected old eth block 101 to be orphaned")
		}

		// New eth block 101 should be canonical
		canonical, err := ethRepo.GetBlockByNumber(ctx, 101)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if canonical == nil || canonical.Hash != "0x1_reorg_101_new" {
			t.Error("expected new canonical eth block at 101")
		}
	})

	t.Run("avalanche_unaffected", func(t *testing.T) {
		// Avalanche block 101 should still be canonical and not orphaned
		avaxBlock, err := avaxRepo.GetBlockByNumber(ctx, 101)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if avaxBlock == nil {
			t.Fatal("expected avalanche block 101 to still exist")
		}
		if avaxBlock.Hash != "0x2_reorg_101" {
			t.Errorf("expected avalanche block hash 0x2_reorg_101, got %s", avaxBlock.Hash)
		}
		if avaxBlock.IsOrphaned {
			t.Error("avalanche block 101 should NOT be orphaned")
		}

		// Avalanche block 102 should also be unaffected
		avax102, err := avaxRepo.GetBlockByNumber(ctx, 102)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if avax102 == nil || avax102.IsOrphaned {
			t.Error("avalanche block 102 should NOT be orphaned")
		}
	})

	t.Run("reorg_events_chain_scoped", func(t *testing.T) {
		ethEvents, err := ethRepo.GetReorgEvents(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ethEvents) != 1 {
			t.Errorf("expected 1 eth reorg event, got %d", len(ethEvents))
		}

		avaxEvents, err := avaxRepo.GetReorgEvents(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(avaxEvents) != 0 {
			t.Errorf("expected 0 avalanche reorg events, got %d", len(avaxEvents))
		}
	})
}

// =============================================================================
// Test Infrastructure
// =============================================================================

// TestInfrastructure holds all test dependencies.
type TestInfrastructure struct {
	Pool           *pgxpool.Pool
	BlockStateRepo *postgres.BlockStateRepository
	Cache          *rediscache.BlockCache
	EventSink      *snsadapter.EventSink
	SNSClient      *sns.Client
	SQSClient      *sqs.Client

	// Queue URLs for reading messages
	BlocksQueueURL   string
	ReceiptsQueueURL string
	TracesQueueURL   string
	BlobsQueueURL    string

	Cleanup func()
}

func setupTestInfrastructure(t *testing.T, ctx context.Context) *TestInfrastructure {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	infra := &TestInfrastructure{}
	var cleanupFuncs []func()

	// Use shared PostgreSQL container with per-test schema isolation
	pool, _, schemaCleanup := testutil.SetupTestSchema(t, sharedDSN)
	cleanupFuncs = append(cleanupFuncs, schemaCleanup)
	infra.Pool = pool

	blockStateRepo := postgres.NewBlockStateRepository(pool, 1, logger)
	infra.BlockStateRepo = blockStateRepo

	// Use shared Redis container
	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      sharedRedisAddr,
		Password:  "",
		DB:        0,
		TTL:       1 * time.Hour,
		KeyPrefix: testutil.SanitizeTestName(t.Name()),
	}, logger)
	if err != nil {
		t.Fatalf("failed to create redis cache: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := cache.Close(); err != nil {
			logger.Error("failed to close redis cache", "error", err)
		}
	})
	infra.Cache = cache

	// Use shared LocalStack container
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(sharedLocalStackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(sharedLocalStackCfg.Endpoint)
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(sharedLocalStackCfg.Endpoint)
	})
	infra.SNSClient = snsClient
	infra.SQSClient = sqsClient

	// Create SNS topics and SQS queues with test-unique names to avoid
	// cross-test interference on the shared LocalStack container.
	testPrefix := testutil.SanitizeTestName(t.Name())
	topics := createSNSTopics(t, ctx, snsClient, testPrefix)
	queues := createSQSQueues(t, ctx, sqsClient, testPrefix)
	subscribeQueuesToTopics(t, ctx, snsClient, sqsClient, topics, queues)

	infra.BlocksQueueURL = queues["blocks"]
	infra.ReceiptsQueueURL = queues["receipts"]
	infra.TracesQueueURL = queues["traces"]
	infra.BlobsQueueURL = queues["blobs"]

	// Create event sink
	eventSink, err := snsadapter.NewEventSink(snsClient, snsadapter.Config{
		TopicARN: topics["blocks"],
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("failed to create event sink: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := eventSink.Close(); err != nil {
			logger.Error("failed to close event sink", "error", err)
		}
	})
	infra.EventSink = eventSink

	infra.Cleanup = func() {
		for i := len(cleanupFuncs) - 1; i >= 0; i-- {
			cleanupFuncs[i]()
		}
	}

	return infra
}

// WaitForMessage waits for a message on the given SQS queue.
func (infra *TestInfrastructure) WaitForMessage(t *testing.T, queueURL string, timeout time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Try immediately, then on each tick
	for {
		result, err := infra.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Logf("error receiving message: %v", err)
		} else if len(result.Messages) > 0 {
			infra.SQSClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: result.Messages[0].ReceiptHandle,
			})
			return aws.ToString(result.Messages[0].Body)
		}

		select {
		case <-ctx.Done():
			return ""
		case <-ticker.C:
		}
	}
}

// =============================================================================
// SNS/SQS Setup
// =============================================================================

func createSNSTopics(t *testing.T, ctx context.Context, client *sns.Client, prefix string) map[string]string {
	t.Helper()
	topics := make(map[string]string)

	for _, name := range []string{"blocks", "receipts", "traces", "blobs"} {
		result, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
			Name: aws.String(fmt.Sprintf("%s-%s.fifo", prefix, name)),
			Attributes: map[string]string{
				"FifoTopic":                 "true",
				"ContentBasedDeduplication": "false",
			},
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
		topics[name] = *result.TopicArn
	}

	return topics
}

func createSQSQueues(t *testing.T, ctx context.Context, client *sqs.Client, prefix string) map[string]string {
	t.Helper()
	queues := make(map[string]string)

	for _, name := range []string{"blocks", "receipts", "traces", "blobs"} {
		// AWS SQS queue names are limited to 80 characters.
		const maxQueueNameLen = 80
		suffix := fmt.Sprintf("-%s-queue.fifo", name)
		safePrefix := prefix
		if len(safePrefix)+len(suffix) > maxQueueNameLen {
			safePrefix = safePrefix[:maxQueueNameLen-len(suffix)]
		}

		result, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(safePrefix + suffix),
			Attributes: map[string]string{
				string(sqstypes.QueueAttributeNameVisibilityTimeout): "30",
				string(sqstypes.QueueAttributeNameFifoQueue):         "true",
			},
		})
		if err != nil {
			t.Fatalf("failed to create queue %s: %v", name, err)
		}
		queues[name] = *result.QueueUrl
	}

	return queues
}

func subscribeQueuesToTopics(t *testing.T, ctx context.Context, snsClient *sns.Client, sqsClient *sqs.Client, topics, queues map[string]string) {
	t.Helper()

	for name, topicARN := range topics {
		queueURL := queues[name]

		// Resolve the actual queue ARN via the API instead of hardcoding region/account.
		attrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(queueURL),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
		})
		if err != nil {
			t.Fatalf("failed to get queue ARN for %s: %v", name, err)
		}
		queueARN := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

		_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn: aws.String(topicARN),
			Protocol: aws.String("sqs"),
			Endpoint: aws.String(queueARN),
			Attributes: map[string]string{
				"RawMessageDelivery": "true",
			},
		})
		if err != nil {
			t.Fatalf("failed to subscribe queue to topic %s: %v", name, err)
		}
		t.Logf("Subscribed queue %s to topic %s", queueURL, topicARN)
	}
}

// =============================================================================
// Helpers
// =============================================================================

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
