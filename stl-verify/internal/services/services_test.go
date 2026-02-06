package services

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestConcurrentLiveAndBackfill is an integration test that verifies
// LiveService and BackfillService work correctly when running together.
func TestConcurrentLiveAndBackfill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subscriber := testutil.NewMockSubscriber()
	client := testutil.NewMockBlockchainClient()
	stateRepo := memory.NewBlockStateRepository()
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Pre-populate client with blocks 1-200
	for i := int64(1); i <= 200; i++ {
		client.AddBlock(i, "")
	}

	// Seed the state repo with block 1 so backfill knows where to start
	if _, err := stateRepo.SaveBlock(ctx, outbound.BlockState{
		Number:     1,
		Hash:       client.GetHeader(1).Hash,
		ParentHash: client.GetHeader(1).ParentHash,
		ReceivedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	liveConfig := live_data.LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 10,
		Logger:             slog.Default(),
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
			header := client.GetHeader(i)
			subscriber.SendHeader(header)
			time.Sleep(5 * time.Millisecond) // Simulate ~12s blocks compressed
		}
	}()

	// Wait for live to complete
	wg.Wait()

	// Poll until backfill fills the gaps (blocks 2-149) or timeout
	deadline := time.Now().Add(10 * time.Second)
	gapsFilled := false
	for time.Now().Before(deadline) {
		gapsFilled = true
		// Check a sample of gap blocks to see if backfill is done
		for _, blockNum := range []int64{2, 50, 100, 149} {
			block, _ := stateRepo.GetBlockByNumber(ctx, blockNum)
			if block == nil {
				gapsFilled = false
				break
			}
		}
		if gapsFilled {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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
