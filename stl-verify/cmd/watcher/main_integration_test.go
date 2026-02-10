//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

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
	gnoRepo := postgres.NewBlockStateRepository(infra.Pool, 100, logger)

	// Create per-chain mock infrastructure
	ethSub := testutil.NewMockSubscriber()
	gnoSub := testutil.NewMockSubscriber()
	ethClient := testutil.NewMockBlockchainClient()
	gnoClient := testutil.NewMockBlockchainClient()

	// Create per-chain live services
	ethLive, err := live_data.NewLiveService(
		live_data.LiveConfig{ChainID: 1, FinalityBlockCount: 2, Logger: logger},
		ethSub, ethClient, ethRepo, infra.Cache, infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create eth live service: %v", err)
	}

	gnoLive, err := live_data.NewLiveService(
		live_data.LiveConfig{ChainID: 100, FinalityBlockCount: 2, Logger: logger},
		gnoSub, gnoClient, gnoRepo, infra.Cache, infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create gnosis live service: %v", err)
	}

	// Start both
	if err := ethLive.Start(ctx); err != nil {
		t.Fatalf("failed to start eth service: %v", err)
	}
	defer ethLive.Stop()

	if err := gnoLive.Start(ctx); err != nil {
		t.Fatalf("failed to start gnosis service: %v", err)
	}
	defer gnoLive.Stop()

	// Add overlapping block numbers: both chains have blocks 100-102
	// Ethereum hashes start with 0x1...
	for i := int64(100); i <= 102; i++ {
		parentHash := fmt.Sprintf("0x1%063x", i-1)
		ethClient.SetBlockHeader(i, fmt.Sprintf("0x1%063x", i), parentHash)
	}
	// Gnosis hashes start with 0x2...
	for i := int64(100); i <= 102; i++ {
		parentHash := fmt.Sprintf("0x2%063x", i-1)
		gnoClient.SetBlockHeader(i, fmt.Sprintf("0x2%063x", i), parentHash)
	}

	// Send block headers to both chains
	for i := int64(100); i <= 102; i++ {
		ethSub.SendHeader(ethClient.GetHeader(i))
		gnoSub.SendHeader(gnoClient.GetHeader(i))
	}

	// Wait for blocks to be processed
	if !testutil.WaitFor(t, 10*time.Second, 100*time.Millisecond, func() bool {
		ethLast, _ := ethRepo.GetLastBlock(ctx)
		gnoLast, _ := gnoRepo.GetLastBlock(ctx)
		return ethLast != nil && ethLast.Number == 102 && gnoLast != nil && gnoLast.Number == 102
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

	t.Run("gnosis_blocks_isolated", func(t *testing.T) {
		block, err := gnoRepo.GetBlockByNumber(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected gnosis block 100")
		}
		if block.Hash != fmt.Sprintf("0x2%063x", 100) {
			t.Errorf("expected gnosis hash, got %s", block.Hash)
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

		gnoLast, err := gnoRepo.GetLastBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gnoLast == nil || gnoLast.Number != 102 {
			t.Errorf("expected gnosis last block 102, got %v", gnoLast)
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
	gnoRepo := postgres.NewBlockStateRepository(infra.Pool, 100, logger)

	t.Run("watermarks_are_independent", func(t *testing.T) {
		// Set different watermarks per chain
		if err := ethRepo.SetBackfillWatermark(ctx, 500); err != nil {
			t.Fatalf("failed to set eth watermark: %v", err)
		}
		if err := gnoRepo.SetBackfillWatermark(ctx, 200); err != nil {
			t.Fatalf("failed to set gnosis watermark: %v", err)
		}

		ethWM, err := ethRepo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("failed to get eth watermark: %v", err)
		}
		if ethWM != 500 {
			t.Errorf("expected eth watermark 500, got %d", ethWM)
		}

		gnoWM, err := gnoRepo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("failed to get gnosis watermark: %v", err)
		}
		if gnoWM != 200 {
			t.Errorf("expected gnosis watermark 200, got %d", gnoWM)
		}
	})

	t.Run("gaps_are_chain_scoped", func(t *testing.T) {
		// Reset watermarks for this subtest
		ethRepo.SetBackfillWatermark(ctx, 0)
		gnoRepo.SetBackfillWatermark(ctx, 0)

		// Save eth blocks 1, 2, 5 (gap at 3-4)
		for _, num := range []int64{1, 2, 5} {
			_, err := ethRepo.SaveBlock(ctx, outbound.BlockState{
				Number:     num,
				Hash:       fmt.Sprintf("0x1_gap_%d", num),
				ParentHash: fmt.Sprintf("0x1_gap_%d", num-1),
				ReceivedAt: time.Now().Unix(),
			})
			if err != nil {
				t.Fatalf("failed to save eth block %d: %v", num, err)
			}
		}

		// Save gnosis blocks 1-5 (no gap)
		for i := int64(1); i <= 5; i++ {
			_, err := gnoRepo.SaveBlock(ctx, outbound.BlockState{
				Number:     i,
				Hash:       fmt.Sprintf("0x2_gap_%d", i),
				ParentHash: fmt.Sprintf("0x2_gap_%d", i-1),
				ReceivedAt: time.Now().Unix(),
			})
			if err != nil {
				t.Fatalf("failed to save gnosis block %d: %v", i, err)
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

		// Gnosis should have no gaps
		gnoGaps, err := gnoRepo.FindGaps(ctx, 1, 5)
		if err != nil {
			t.Fatalf("failed to find gnosis gaps: %v", err)
		}
		if len(gnoGaps) != 0 {
			t.Errorf("expected no gnosis gaps, got %d: %v", len(gnoGaps), gnoGaps)
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
	gnoRepo := postgres.NewBlockStateRepository(infra.Pool, 100, logger)

	// Save blocks 100-102 on both chains
	for i := int64(100); i <= 102; i++ {
		_, err := ethRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x1_reorg_%d", i),
			ParentHash: fmt.Sprintf("0x1_reorg_%d", i-1),
			ReceivedAt: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save eth block %d: %v", i, err)
		}

		_, err = gnoRepo.SaveBlock(ctx, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x2_reorg_%d", i),
			ParentHash: fmt.Sprintf("0x2_reorg_%d", i-1),
			ReceivedAt: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save gnosis block %d: %v", i, err)
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
		Number:     101,
		Hash:       "0x1_reorg_101_new",
		ParentHash: "0x1_reorg_100",
		ReceivedAt: time.Now().Unix(),
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

	t.Run("gnosis_unaffected", func(t *testing.T) {
		// Gnosis block 101 should still be canonical and not orphaned
		gnoBlock, err := gnoRepo.GetBlockByNumber(ctx, 101)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gnoBlock == nil {
			t.Fatal("expected gnosis block 101 to still exist")
		}
		if gnoBlock.Hash != "0x2_reorg_101" {
			t.Errorf("expected gnosis block hash 0x2_reorg_101, got %s", gnoBlock.Hash)
		}
		if gnoBlock.IsOrphaned {
			t.Error("gnosis block 101 should NOT be orphaned")
		}

		// Gnosis block 102 should also be unaffected
		gno102, err := gnoRepo.GetBlockByNumber(ctx, 102)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gno102 == nil || gno102.IsOrphaned {
			t.Error("gnosis block 102 should NOT be orphaned")
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

		gnoEvents, err := gnoRepo.GetReorgEvents(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(gnoEvents) != 0 {
			t.Errorf("expected 0 gnosis reorg events, got %d", len(gnoEvents))
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

	containers []testcontainers.Container
	Cleanup    func()
}

func setupTestInfrastructure(t *testing.T, ctx context.Context) *TestInfrastructure {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	infra := &TestInfrastructure{}
	var cleanupFuncs []func()

	// Start PostgreSQL
	postgresContainer, postgresCfg := startPostgres(t, ctx)
	infra.containers = append(infra.containers, postgresContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			logger.Error("failed to terminate postgres container", "error", err)
		}
	})

	pool, err := pgxpool.New(ctx, postgresCfg.ConnectionString())
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		pool.Close()
	})
	infra.Pool = pool

	// Run migrations
	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(pool, 1, logger)
	infra.BlockStateRepo = blockStateRepo

	// Start Redis
	redisContainer, redisCfg := startRedis(t, ctx)
	infra.containers = append(infra.containers, redisContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			logger.Error("failed to terminate redis container", "error", err)
		}
	})

	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      redisCfg.Addr,
		Password:  redisCfg.Password,
		DB:        redisCfg.DB,
		TTL:       1 * time.Hour,
		KeyPrefix: "test",
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

	// Start LocalStack
	localstackContainer, localstackCfg := startLocalStack(t, ctx)
	infra.containers = append(infra.containers, localstackContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := localstackContainer.Terminate(ctx); err != nil {
			logger.Error("failed to terminate localstack container", "error", err)
		}
	})

	// Create AWS clients
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(localstackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	infra.SNSClient = snsClient
	infra.SQSClient = sqsClient

	// Create SNS topics and SQS queues
	topics := createSNSTopics(t, ctx, snsClient)
	queues := createSQSQueues(t, ctx, sqsClient)
	subscribeQueuesToTopics(t, ctx, snsClient, topics, queues)

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
// Container Setup
// =============================================================================

// PostgresTestConfig contains all configuration needed to connect to test Postgres.
type PostgresTestConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
}

// ConnectionString returns the full postgres connection URL.
func (c PostgresTestConfig) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
}

func startPostgres(t *testing.T, ctx context.Context) (testcontainers.Container, PostgresTestConfig) {
	t.Helper()

	config := PostgresTestConfig{
		User:     "test",
		Password: "test",
		Database: "testdb",
		SSLMode:  "disable",
	}

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     config.User,
			"POSTGRES_PASSWORD": config.Password,
			"POSTGRES_DB":       config.Database,
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
		t.Fatalf("failed to start postgres: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	config.Host = host
	config.Port = port.Port()

	return container, config
}

// RedisTestConfig contains all configuration needed to connect to test Redis.
type RedisTestConfig struct {
	Addr     string
	Password string
	DB       int
}

func startRedis(t *testing.T, ctx context.Context) (testcontainers.Container, RedisTestConfig) {
	t.Helper()

	config := RedisTestConfig{
		Password: "",
		DB:       0,
	}

	req := testcontainers.ContainerRequest{
		Image:        "redis:8.0-M04-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "6379")
	config.Addr = fmt.Sprintf("%s:%s", host, port.Port())

	return container, config
}

// LocalStackTestConfig contains all configuration needed to connect to test LocalStack.
type LocalStackTestConfig struct {
	Endpoint string
	Region   string
}

func startLocalStack(t *testing.T, ctx context.Context) (testcontainers.Container, LocalStackTestConfig) {
	t.Helper()

	config := LocalStackTestConfig{
		Region: "us-east-1",
	}

	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:latest",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES": "sns,sqs",
			"DEBUG":    "0",
		},
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start localstack: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4566")
	config.Endpoint = fmt.Sprintf("http://%s:%s", host, port.Port())

	return container, config
}

func createSNSTopics(t *testing.T, ctx context.Context, client *sns.Client) map[string]string {
	t.Helper()
	topics := make(map[string]string)

	for _, name := range []string{"blocks", "receipts", "traces", "blobs"} {
		result, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
			Name: aws.String(fmt.Sprintf("test-%s.fifo", name)),
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

func createSQSQueues(t *testing.T, ctx context.Context, client *sqs.Client) map[string]string {
	t.Helper()
	queues := make(map[string]string)

	for _, name := range []string{"blocks", "receipts", "traces", "blobs"} {
		result, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(fmt.Sprintf("test-%s-queue.fifo", name)),
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

func subscribeQueuesToTopics(t *testing.T, ctx context.Context, snsClient *sns.Client, topics, queues map[string]string) {
	t.Helper()

	for name, topicARN := range topics {
		queueURL := queues[name]
		// Get queue ARN from URL (LocalStack pattern for FIFO queues)
		queueARN := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:test-%s-queue.fifo", name)

		_, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
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
