//go:build integration

package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const blockstateDBName = "test_blockstate"

var blockstatePool *pgxpool.Pool

func init() {
	useFileDatabase(blockstateDBName, &blockstatePool)
}

// truncateBlockState clears all block-related tables for test isolation within the schema.
func truncateBlockState(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := blockstatePool.Exec(ctx, `DELETE FROM block_states`)
	if err != nil {
		t.Fatalf("failed to truncate block_states: %v", err)
	}
	_, err = blockstatePool.Exec(ctx, `DELETE FROM reorg_events`)
	if err != nil {
		t.Fatalf("failed to truncate reorg_events: %v", err)
	}
	// Reset backfill_watermark to default value instead of deleting, and put
	// back a row a sibling test deleted, so no test inherits a missing cursor.
	_, err = blockstatePool.Exec(ctx, `UPDATE backfill_watermark SET watermark = 0, rewind_count = 0`)
	if err != nil {
		t.Fatalf("failed to reset backfill_watermark: %v", err)
	}
	_, err = blockstatePool.Exec(ctx,
		`INSERT INTO backfill_watermark (chain_id, watermark, rewind_count) VALUES (1, 0, 0)
		 ON CONFLICT (chain_id) DO NOTHING`)
	if err != nil {
		t.Fatalf("failed to restore backfill_watermark row: %v", err)
	}
}

// seedWatermark puts a chain's cursor at a known position. Seeding is SQL, not
// a port method: production only ever moves the cursor through
// AdvanceBackfillWatermark's compare-and-set or a reorg commit's rewind, and an
// unconditional writer on the port is what let a rewind be overwritten
// (ARCT-379).
func seedWatermark(t *testing.T, ctx context.Context, repo *BlockStateRepository, watermark, rewindCount int64) {
	t.Helper()
	if _, err := repo.Pool().Exec(ctx,
		`INSERT INTO backfill_watermark (chain_id, watermark, rewind_count) VALUES ($1, $2, $3)
		 ON CONFLICT (chain_id) DO UPDATE SET watermark = EXCLUDED.watermark, rewind_count = EXCLUDED.rewind_count`,
		repo.chainID, watermark, rewindCount); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}
}

// setupPostgres returns a connected repository using the schema-specific pool.
// It truncates tables to ensure test isolation within the schema.
func setupPostgres(t *testing.T) (*BlockStateRepository, func()) {
	t.Helper()
	return setupPostgresWithLogger(t, nil)
}

// setupPostgresWithLogger is setupPostgres for tests that assert on the
// repository's log output.
func setupPostgresWithLogger(t *testing.T, logger *slog.Logger) (*BlockStateRepository, func()) {
	t.Helper()
	truncateBlockState(t, context.Background())
	return NewBlockStateRepository(blockstatePool, 1, logger), func() {}
}

// TestSaveBlock_DuplicateHashIsIdempotent tests that saving the same block hash
// multiple times is idempotent - the second save returns the existing version
// without modifying any data. In blockchain, same hash = identical content
// (hash is derived from block header including parent_hash), so duplicates
// should be silently ignored rather than updating the row.
func TestSaveBlock_DuplicateHashIsIdempotent(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// First save: block data
	originalReceivedAt := time.Now().Unix()
	firstState := outbound.BlockState{
		Number:         100,
		Hash:           "0xabc123",
		ParentHash:     "0xparent1",
		ReceivedAt:     originalReceivedAt,
		BlockTimestamp: originalReceivedAt,
		IsOrphaned:     false,
	}

	version1, err := repo.SaveBlock(ctx, firstState)
	if err != nil {
		t.Fatalf("first save failed: %v", err)
	}

	// Second save: same hash (duplicate arrival, e.g., from reconnect or backfill)
	// Even though we're passing different values, a real duplicate would have
	// identical content. The test verifies we ignore the second save entirely.
	duplicateState := outbound.BlockState{
		Number:         100,
		Hash:           "0xabc123", // Same hash = same block
		ParentHash:     "0xparent1",
		ReceivedAt:     originalReceivedAt + 500, // Different received_at (we saw it again later)
		BlockTimestamp: originalReceivedAt,
		IsOrphaned:     false,
	}

	version2, err := repo.SaveBlock(ctx, duplicateState)
	if err != nil {
		t.Fatalf("second save failed: %v", err)
	}

	t.Run("returns same version", func(t *testing.T) {
		if version1 != version2 {
			t.Errorf("expected same version for duplicate hash, got v1=%d, v2=%d", version1, version2)
		}
	})

	// Verify the original data was preserved (not updated with second save's received_at)
	retrieved, err := repo.GetBlockByHash(ctx, "0xabc123")
	if err != nil {
		t.Fatalf("failed to retrieve block: %v", err)
	}

	t.Run("original received_at preserved", func(t *testing.T) {
		if retrieved.ReceivedAt != originalReceivedAt {
			t.Errorf("received_at was overwritten: got %d, want %d", retrieved.ReceivedAt, originalReceivedAt)
		}
	})
}

// TestHandleReorgAtomic_AllOrNothingSemantics tests that HandleReorgAtomic
// performs all operations atomically - either all succeed or none do.
// After calling HandleReorgAtomic, we should have:
// 1. A reorg event recorded
// 2. Old blocks marked as orphaned
// 3. New canonical block saved
// All in a single transaction.
func TestHandleReorgAtomic_AllOrNothingSemantics(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Setup: Create a chain of blocks 100, 101, 102
	for i := int64(100); i <= 102; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xoriginal_%d", i),
			ParentHash:     fmt.Sprintf("0xoriginal_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
			IsOrphaned:     false,
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Create reorg event and new block for atomic handling
	reorgEvent := outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: 101,
		OldHash:     "0xoriginal_101",
		NewHash:     "0xnew_101",
		Depth:       1, // commonAncestor = 101 - 1 = 100
	}

	newBlock := outbound.BlockState{
		Number:         101,
		Hash:           "0xnew_101",
		ParentHash:     "0xoriginal_100",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
		IsOrphaned:     false,
	}

	// Execute atomic reorg
	version, err := repo.HandleReorgAtomic(ctx, 100, reorgEvent, newBlock)
	if err != nil {
		t.Fatalf("HandleReorgAtomic failed: %v", err)
	}

	t.Run("version_is_assigned", func(t *testing.T) {
		// Original block 101 was version 0, new one should be version 1
		if version != 1 {
			t.Errorf("expected version 1, got %d", version)
		}
	})

	t.Run("new_block_is_canonical", func(t *testing.T) {
		block, err := repo.GetBlockByNumber(ctx, 101)
		if err != nil {
			t.Fatalf("failed to get block: %v", err)
		}
		if block == nil {
			t.Fatal("expected canonical block at 101, got nil")
		}
		if block.Hash != "0xnew_101" {
			t.Errorf("expected new block hash, got %q", block.Hash)
		}
	})

	t.Run("old_blocks_are_orphaned", func(t *testing.T) {
		// Block 101 original should be orphaned
		oldBlock, err := repo.GetBlockByHash(ctx, "0xoriginal_101")
		if err != nil {
			t.Fatalf("failed to get old block: %v", err)
		}
		if !oldBlock.IsOrphaned {
			t.Error("expected old block 101 to be orphaned")
		}

		// Block 102 should also be orphaned (it was after common ancestor 100)
		block102, err := repo.GetBlockByHash(ctx, "0xoriginal_102")
		if err != nil {
			t.Fatalf("failed to get block 102: %v", err)
		}
		if !block102.IsOrphaned {
			t.Error("expected block 102 to be orphaned")
		}
	})

	t.Run("reorg_event_is_recorded", func(t *testing.T) {
		// Query reorg events directly via raw SQL
		rows, err := repo.Pool().Query(ctx, `
			SELECT id, detected_at, block_number, old_hash, new_hash, depth
			FROM reorg_events
			ORDER BY detected_at DESC
			LIMIT 10
		`)
		if err != nil {
			t.Fatalf("failed to get reorg events: %v", err)
		}
		defer rows.Close()

		var events []outbound.ReorgEvent
		for rows.Next() {
			var e outbound.ReorgEvent
			if err := rows.Scan(&e.ID, &e.DetectedAt, &e.BlockNumber, &e.OldHash, &e.NewHash, &e.Depth); err != nil {
				t.Fatalf("failed to scan reorg event: %v", err)
			}
			events = append(events, e)
		}

		if len(events) != 1 {
			t.Fatalf("expected 1 reorg event, got %d", len(events))
		}
		if events[0].Depth != 1 {
			t.Errorf("expected depth 1, got %d", events[0].Depth)
		}
	})
}

// TestGetLastBlock tests retrieving the most recent canonical block.
func TestGetLastBlock(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("returns nil when no blocks exist", func(t *testing.T) {
		block, err := repo.GetLastBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block != nil {
			t.Errorf("expected nil, got block %d", block.Number)
		}
	})

	// Save some blocks
	for i := int64(100); i <= 105; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xblock_%d", i),
			ParentHash:     fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	t.Run("returns highest block number", func(t *testing.T) {
		block, err := repo.GetLastBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected block, got nil")
		}
		if block.Number != 105 {
			t.Errorf("expected block 105, got %d", block.Number)
		}
	})

	// Mark the last block as orphaned
	if err := repo.MarkBlockOrphaned(ctx, "0xblock_105"); err != nil {
		t.Fatalf("failed to mark block orphaned: %v", err)
	}

	t.Run("excludes orphaned blocks", func(t *testing.T) {
		block, err := repo.GetLastBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected block, got nil")
		}
		if block.Number != 104 {
			t.Errorf("expected block 104 (105 is orphaned), got %d", block.Number)
		}
	})
}

// TestGetBlockByNumber tests retrieving canonical blocks by number.
func TestGetBlockByNumber(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save a block
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         100,
		Hash:           "0xcanonical",
		ParentHash:     "0xparent",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	t.Run("returns canonical block", func(t *testing.T) {
		block, err := repo.GetBlockByNumber(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected block, got nil")
		}
		if block.Hash != "0xcanonical" {
			t.Errorf("expected hash 0xcanonical, got %s", block.Hash)
		}
	})

	t.Run("returns nil for non-existent block", func(t *testing.T) {
		block, err := repo.GetBlockByNumber(ctx, 999)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block != nil {
			t.Errorf("expected nil, got block %d", block.Number)
		}
	})

	// Mark block as orphaned and save a new one at same number
	if err := repo.MarkBlockOrphaned(ctx, "0xcanonical"); err != nil {
		t.Fatalf("failed to mark orphaned: %v", err)
	}
	_, err = repo.SaveBlock(ctx, outbound.BlockState{
		Number:         100,
		Hash:           "0xnew_canonical",
		ParentHash:     "0xparent",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save new block: %v", err)
	}

	t.Run("returns only canonical block when orphaned exists", func(t *testing.T) {
		block, err := repo.GetBlockByNumber(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected block, got nil")
		}
		if block.Hash != "0xnew_canonical" {
			t.Errorf("expected new canonical hash, got %s", block.Hash)
		}
	})
}

// TestGetBlockByHash tests retrieving blocks by hash (including orphaned).
func TestGetBlockByHash(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save and then orphan a block
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         100,
		Hash:           "0xorphaned_hash",
		ParentHash:     "0xparent",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save block: %v", err)
	}
	if err := repo.MarkBlockOrphaned(ctx, "0xorphaned_hash"); err != nil {
		t.Fatalf("failed to mark orphaned: %v", err)
	}

	t.Run("returns orphaned block by hash", func(t *testing.T) {
		block, err := repo.GetBlockByHash(ctx, "0xorphaned_hash")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block == nil {
			t.Fatal("expected block, got nil")
		}
		if !block.IsOrphaned {
			t.Error("expected block to be orphaned")
		}
	})

	t.Run("returns nil for non-existent hash", func(t *testing.T) {
		block, err := repo.GetBlockByHash(ctx, "0xnonexistent")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if block != nil {
			t.Errorf("expected nil, got block")
		}
	})
}

// TestGetBlockVersionCount tests version counting for reorg scenarios.
func TestGetBlockVersionCount(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("returns 0 when no blocks exist", func(t *testing.T) {
		count, err := repo.GetBlockVersionCount(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0, got %d", count)
		}
	})

	// Save first version
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0xv0", ParentHash: "0xparent", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	t.Run("returns 1 after first block", func(t *testing.T) {
		count, err := repo.GetBlockVersionCount(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1, got %d", count)
		}
	})

	// Mark as orphaned and save v1
	repo.MarkBlockOrphaned(ctx, "0xv0")
	_, err = repo.SaveBlock(ctx, outbound.BlockState{
		Number: 100, Hash: "0xv1", ParentHash: "0xparent", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	t.Run("returns 2 after second block at same height", func(t *testing.T) {
		count, err := repo.GetBlockVersionCount(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}
	})
}

// TestGetRecentBlocks tests retrieving recent canonical blocks.
func TestGetRecentBlocks(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save 10 blocks
	for i := int64(1); i <= 10; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xblock_%d", i),
			ParentHash:     fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Orphan block 5
	repo.MarkBlockOrphaned(ctx, "0xblock_5")

	t.Run("returns correct number of blocks", func(t *testing.T) {
		blocks, err := repo.GetRecentBlocks(ctx, 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(blocks) != 5 {
			t.Errorf("expected 5 blocks, got %d", len(blocks))
		}
	})

	t.Run("excludes orphaned blocks", func(t *testing.T) {
		blocks, err := repo.GetRecentBlocks(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, b := range blocks {
			if b.Number == 5 {
				t.Error("orphaned block 5 should not be included")
			}
		}
	})

	t.Run("returns blocks in descending order", func(t *testing.T) {
		blocks, err := repo.GetRecentBlocks(ctx, 3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Should be 10, 9, 8 (descending)
		if blocks[0].Number != 10 || blocks[1].Number != 9 || blocks[2].Number != 8 {
			t.Errorf("expected [10,9,8], got [%d,%d,%d]", blocks[0].Number, blocks[1].Number, blocks[2].Number)
		}
	})
}

// TestMinMaxBlockNumber tests GetMinBlockNumber and GetMaxBlockNumber.
func TestMinMaxBlockNumber(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("returns 0 when no blocks exist", func(t *testing.T) {
		min, err := repo.GetMinBlockNumber(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if min != 0 {
			t.Errorf("expected min 0, got %d", min)
		}

		max, err := repo.GetMaxBlockNumber(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if max != 0 {
			t.Errorf("expected max 0, got %d", max)
		}
	})

	// Save blocks 100-110
	for i := int64(100); i <= 110; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xblock_%d", i),
			ParentHash:     fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	t.Run("returns correct min and max", func(t *testing.T) {
		min, err := repo.GetMinBlockNumber(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if min != 100 {
			t.Errorf("expected min 100, got %d", min)
		}

		max, err := repo.GetMaxBlockNumber(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if max != 110 {
			t.Errorf("expected max 110, got %d", max)
		}
	})

	// Orphan min and max blocks
	repo.MarkBlockOrphaned(ctx, "0xblock_100")
	repo.MarkBlockOrphaned(ctx, "0xblock_110")

	t.Run("excludes orphaned blocks", func(t *testing.T) {
		min, err := repo.GetMinBlockNumber(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if min != 101 {
			t.Errorf("expected min 101, got %d", min)
		}

		max, err := repo.GetMaxBlockNumber(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if max != 109 {
			t.Errorf("expected max 109, got %d", max)
		}
	})
}

// TestMarkPublishComplete tests marking blocks as published.
func TestMarkPublishComplete(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save a block
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         100,
		Hash:           "0xtest_block",
		ParentHash:     "0xparent",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	t.Run("marks block published", func(t *testing.T) {
		if err := repo.MarkPublishComplete(ctx, "0xtest_block"); err != nil {
			t.Fatalf("failed to mark block published: %v", err)
		}
		block, _ := repo.GetBlockByHash(ctx, "0xtest_block")
		if !block.BlockPublished {
			t.Error("expected BlockPublished to be true")
		}
	})

	t.Run("returns error for non-existent block", func(t *testing.T) {
		err := repo.MarkPublishComplete(ctx, "0xnonexistent")
		if err == nil {
			t.Error("expected error for non-existent block")
		}
	})
}

// TestGetBlocksWithIncompletePublish tests finding blocks needing republish.
func TestGetBlocksWithIncompletePublish(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks with different publish states
	for i := int64(1); i <= 3; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xblock_%d", i),
			ParentHash:     fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Mark block 1 as published
	if err := repo.MarkPublishComplete(ctx, "0xblock_1"); err != nil {
		t.Fatalf("failed to mark publish: %v", err)
	}

	// Blocks 2 and 3 have not been published

	t.Run("returns incomplete blocks", func(t *testing.T) {
		blocks, err := repo.GetBlocksWithIncompletePublish(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Blocks 2 and 3 should be returned (block 1 is published)
		if len(blocks) != 2 {
			t.Fatalf("expected 2 blocks, got %d", len(blocks))
		}
		if blocks[0].Number != 2 || blocks[1].Number != 3 {
			t.Errorf("expected blocks [2,3], got [%d,%d]", blocks[0].Number, blocks[1].Number)
		}
	})

	t.Run("respects limit", func(t *testing.T) {
		blocks, err := repo.GetBlocksWithIncompletePublish(ctx, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(blocks) != 1 {
			t.Fatalf("expected 1 block, got %d", len(blocks))
		}
	})
}

// TestHandleReorgAtomic_Idempotency tests that HandleReorgAtomic is idempotent.
func TestHandleReorgAtomic_Idempotency(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save initial block
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         100,
		Hash:           "0xoriginal",
		ParentHash:     "0xparent",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	reorgEvent := outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: 100,
		OldHash:     "0xoriginal",
		NewHash:     "0xnew",
		Depth:       1,
	}

	newBlock := outbound.BlockState{
		Number:         100,
		Hash:           "0xnew",
		ParentHash:     "0xparent",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}

	// First call
	version1, err := repo.HandleReorgAtomic(ctx, 99, reorgEvent, newBlock)
	if err != nil {
		t.Fatalf("first HandleReorgAtomic failed: %v", err)
	}

	// Second call with same block hash should be idempotent
	version2, err := repo.HandleReorgAtomic(ctx, 99, reorgEvent, newBlock)
	if err != nil {
		t.Fatalf("second HandleReorgAtomic failed: %v", err)
	}

	if version1 != version2 {
		t.Errorf("expected same version on idempotent call, got v1=%d, v2=%d", version1, version2)
	}
}

// TestBackfillWatermark tests GetBackfillWatermark against a seeded row.
func TestBackfillWatermark(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("initial watermark is 0", func(t *testing.T) {
		watermark, err := repo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if watermark != 0 {
			t.Errorf("expected watermark 0, got %d", watermark)
		}
	})

	t.Run("reads the stored watermark", func(t *testing.T) {
		seedWatermark(t, ctx, repo, 100, 0)

		watermark, err := repo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if watermark != 100 {
			t.Errorf("expected watermark 100, got %d", watermark)
		}
	})

	t.Run("reads a moved watermark", func(t *testing.T) {
		seedWatermark(t, ctx, repo, 500, 0)

		watermark, err := repo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if watermark != 500 {
			t.Errorf("expected watermark 500, got %d", watermark)
		}
	})

	t.Run("returns 0 when no watermark row exists (new chain)", func(t *testing.T) {
		// Delete the watermark row entirely to simulate a brand new chain
		// that has never had a watermark set (no row in the table).
		_, err := blockstatePool.Exec(ctx, `DELETE FROM backfill_watermark WHERE chain_id = $1`, int64(1))
		if err != nil {
			t.Fatalf("failed to delete watermark row: %v", err)
		}

		watermark, err := repo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if watermark != 0 {
			t.Errorf("expected watermark 0 for missing row, got %d", watermark)
		}
	})

	t.Run("returns the zero cursor when no row exists", func(t *testing.T) {
		cursor, err := repo.GetBackfillCursor(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cursor != (outbound.BackfillCursor{}) {
			t.Errorf("cursor = %+v, want the zero cursor", cursor)
		}
	})
}

// TestFindGaps tests gap detection in block sequences.
func TestFindGaps(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("no gaps in contiguous chain", func(t *testing.T) {
		// Save blocks 1-10
		for i := int64(1); i <= 10; i++ {
			_, err := repo.SaveBlock(ctx, outbound.BlockState{
				Number:         i,
				Hash:           fmt.Sprintf("0xblock_%d", i),
				ParentHash:     fmt.Sprintf("0xblock_%d", i-1),
				ReceivedAt:     time.Now().Unix(),
				BlockTimestamp: time.Now().Unix(),
			})
			if err != nil {
				t.Fatalf("failed to save block %d: %v", i, err)
			}
		}

		gaps, err := repo.FindGaps(ctx, 1, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(gaps) != 0 {
			t.Errorf("expected no gaps, got %d: %v", len(gaps), gaps)
		}
	})
}

// TestFindGaps_WithGap tests gap detection with missing blocks.
func TestFindGaps_WithGap(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1, 2, 5, 6, 10 (missing 3-4 and 7-9)
	for _, num := range []int64{1, 2, 5, 6, 10} {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         num,
			Hash:           fmt.Sprintf("0xblock_%d", num),
			ParentHash:     fmt.Sprintf("0xblock_%d", num-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", num, err)
		}
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Expect gaps: [3,4] and [7,9]
	if len(gaps) != 2 {
		t.Fatalf("expected 2 gaps, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 3 || gaps[0].To != 4 {
		t.Errorf("expected first gap [3,4], got [%d,%d]", gaps[0].From, gaps[0].To)
	}
	if gaps[1].From != 7 || gaps[1].To != 9 {
		t.Errorf("expected second gap [7,9], got [%d,%d]", gaps[1].From, gaps[1].To)
	}
}

// TestFindGaps_WatermarkSkipsVerifiedBlocks tests that watermark optimizes gap detection.
func TestFindGaps_WatermarkSkipsVerifiedBlocks(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1-5 and 8-10 (missing 6-7)
	for _, num := range []int64{1, 2, 3, 4, 5, 8, 9, 10} {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         num,
			Hash:           fmt.Sprintf("0xblock_%d", num),
			ParentHash:     fmt.Sprintf("0xblock_%d", num-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", num, err)
		}
	}

	// Set watermark to 5 (blocks 1-5 are verified)
	seedWatermark(t, ctx, repo, 5, 0)

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should only find gap [6,7] since 1-5 are above watermark
	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}
	if gaps[0].From != 6 || gaps[0].To != 7 {
		t.Errorf("expected gap [6,7], got [%d,%d]", gaps[0].From, gaps[0].To)
	}
}

// TestFindGaps_InvalidRange tests FindGaps with invalid range.
func TestFindGaps_InvalidRange(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// minBlock > maxBlock should return nil
	gaps, err := repo.FindGaps(ctx, 100, 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gaps != nil {
		t.Errorf("expected nil, got %v", gaps)
	}
}

// TestFindGaps_WatermarkCoversRange tests when watermark covers entire range.
func TestFindGaps_WatermarkCoversRange(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Set watermark higher than the range we're checking
	seedWatermark(t, ctx, repo, 100, 0)

	gaps, err := repo.FindGaps(ctx, 1, 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should find no gaps since watermark is above the range
	if gaps != nil {
		t.Errorf("expected nil, got %v", gaps)
	}
}

// TestFindGaps_GapAtBeginning tests gap detection when first block is missing.
func TestFindGaps_GapAtBeginning(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 5-10 (missing 1-4)
	for i := int64(5); i <= 10; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xblock_%d", i),
			ParentHash:     fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}
	if gaps[0].From != 1 || gaps[0].To != 4 {
		t.Errorf("expected gap [1,4], got [%d,%d]", gaps[0].From, gaps[0].To)
	}
}

// TestFindGaps_IgnoresOrphanedBlocks tests that orphaned blocks are treated as gaps.
func TestFindGaps_IgnoresOrphanedBlocks(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks 1-5
	for i := int64(1); i <= 5; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xblock_%d", i),
			ParentHash:     fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Orphan block 3
	if err := repo.MarkBlockOrphaned(ctx, "0xblock_3"); err != nil {
		t.Fatalf("failed to mark orphaned: %v", err)
	}

	gaps, err := repo.FindGaps(ctx, 1, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Block 3 is orphaned, so there should be a gap at 3
	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap (orphaned block), got %d: %v", len(gaps), gaps)
	}
	if gaps[0].From != 3 || gaps[0].To != 3 {
		t.Errorf("expected gap [3,3], got [%d,%d]", gaps[0].From, gaps[0].To)
	}
}

// TestVerifyChainIntegrity tests chain integrity verification.
func TestVerifyChainIntegrity(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("valid chain passes", func(t *testing.T) {
		// Save a properly linked chain
		for i := int64(1); i <= 5; i++ {
			_, err := repo.SaveBlock(ctx, outbound.BlockState{
				Number:         i,
				Hash:           fmt.Sprintf("0x%064d", i),
				ParentHash:     fmt.Sprintf("0x%064d", i-1),
				ReceivedAt:     time.Now().Unix(),
				BlockTimestamp: time.Now().Unix(),
			})
			if err != nil {
				t.Fatalf("failed to save block %d: %v", i, err)
			}
		}

		err := repo.VerifyChainIntegrity(ctx, 1, 5)
		if err != nil {
			t.Errorf("expected valid chain, got error: %v", err)
		}
	})
}

// TestVerifyChainIntegrity_BrokenChain tests detection of broken chain links.
func TestVerifyChainIntegrity_BrokenChain(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save blocks with a broken link at block 3
	for i := int64(1); i <= 5; i++ {
		parentHash := fmt.Sprintf("0x%064d", i-1)
		if i == 3 {
			parentHash = "0xwrong_parent" // This doesn't match block 2's hash
		}
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0x%064d", i),
			ParentHash:     parentHash,
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	err := repo.VerifyChainIntegrity(ctx, 1, 5)
	if err == nil {
		t.Error("expected error for broken chain, got nil")
	}
	// Should indicate the break is at block 3
	if err != nil && !strings.Contains(err.Error(), "block 3") {
		t.Errorf("expected error to mention block 3, got: %v", err)
	}
}

// TestVerifyChainIntegrity_EmptyRange tests chain verification with fromBlock >= toBlock.
func TestVerifyChainIntegrity_EmptyRange(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("fromBlock equals toBlock", func(t *testing.T) {
		err := repo.VerifyChainIntegrity(ctx, 5, 5)
		if err != nil {
			t.Errorf("expected no error for empty range, got: %v", err)
		}
	})

	t.Run("fromBlock greater than toBlock", func(t *testing.T) {
		err := repo.VerifyChainIntegrity(ctx, 10, 5)
		if err != nil {
			t.Errorf("expected no error for empty range, got: %v", err)
		}
	})
}

// TestVerifyChainIntegrity_IgnoresOrphanedBlocks tests that orphaned blocks are excluded.
func TestVerifyChainIntegrity_IgnoresOrphanedBlocks(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save a valid chain
	for i := int64(1); i <= 5; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0x%064d", i),
			ParentHash:     fmt.Sprintf("0x%064d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Orphan block 2 and add a replacement with correct parent
	if err := repo.MarkBlockOrphaned(ctx, fmt.Sprintf("0x%064d", 2)); err != nil {
		t.Fatalf("failed to mark orphaned: %v", err)
	}

	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         2,
		Hash:           "0xnew_block_2",
		ParentHash:     fmt.Sprintf("0x%064d", 1), // Correct parent
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save replacement block: %v", err)
	}

	// Now block 3 has parent pointing to old block 2 hash, which is orphaned
	// The chain should be considered broken
	err = repo.VerifyChainIntegrity(ctx, 1, 5)
	if err == nil {
		t.Error("expected chain integrity error (block 3 points to orphaned parent)")
	}
}

// TestSaveBlock_ConcurrentRaceConditionWithRetry tests that SaveBlock handles concurrent saves
// with retry logic when unique constraint violations occur.
func TestSaveBlock_ConcurrentRaceConditionWithRetry(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	const blockNum int64 = 100
	const numGoroutines = 10

	// Use a channel to synchronize the start of all goroutines
	startCh := make(chan struct{})
	resultCh := make(chan struct {
		version int
		err     error
	}, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			// Wait for the start signal
			<-startCh

			// Each goroutine tries to save a block with a unique hash
			version, err := repo.SaveBlock(ctx, outbound.BlockState{
				Number:         blockNum,
				Hash:           fmt.Sprintf("0x%064d_%d", blockNum, id),
				ParentHash:     fmt.Sprintf("0x%064d", blockNum-1),
				ReceivedAt:     time.Now().Unix(),
				BlockTimestamp: time.Now().Unix(),
				IsOrphaned:     false,
			})
			resultCh <- struct {
				version int
				err     error
			}{version, err}
		}(i)
	}

	// Start all goroutines at once to maximize race condition likelihood
	close(startCh)

	// Wait for all goroutines to complete
	successCount := 0
	for i := 0; i < numGoroutines; i++ {
		result := <-resultCh
		if result.err == nil {
			successCount++
		} else {
			t.Logf("Goroutine failed: %v", result.err)
		}
	}

	// All goroutines should succeed thanks to retry logic
	if successCount != numGoroutines {
		t.Errorf("expected all %d saves to succeed, but only %d succeeded", numGoroutines, successCount)
	}

	// Verify all blocks were saved
	count, err := repo.GetBlockVersionCount(ctx, blockNum)
	if err != nil {
		t.Fatalf("failed to get version count: %v", err)
	}
	if count != numGoroutines {
		t.Errorf("expected %d blocks, got %d", numGoroutines, count)
	}
}

// TestGetRecentBlocks_EmptyDatabase tests GetRecentBlocks when no blocks exist.
func TestGetRecentBlocks_EmptyDatabase(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	blocks, err := repo.GetRecentBlocks(ctx, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Errorf("expected 0 blocks, got %d", len(blocks))
	}
}

// TestHandleReorgAtomic_MultipleBlocksOrphaned tests reorg handling with multiple blocks.
func TestHandleReorgAtomic_MultipleBlocksOrphaned(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Create a chain of 10 blocks (100-109)
	for i := int64(100); i <= 109; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xoriginal_%d", i),
			ParentHash:     fmt.Sprintf("0xoriginal_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Reorg at block 105 with depth 5 (common ancestor is 100)
	reorgEvent := outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: 105,
		OldHash:     "0xoriginal_105",
		NewHash:     "0xnew_105",
		Depth:       5,
	}

	newBlock := outbound.BlockState{
		Number:         105,
		Hash:           "0xnew_105",
		ParentHash:     "0xoriginal_100",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}

	_, err := repo.HandleReorgAtomic(ctx, 100, reorgEvent, newBlock)
	if err != nil {
		t.Fatalf("HandleReorgAtomic failed: %v", err)
	}

	// Verify blocks 101-109 are orphaned
	for i := int64(101); i <= 109; i++ {
		block, err := repo.GetBlockByHash(ctx, fmt.Sprintf("0xoriginal_%d", i))
		if err != nil {
			t.Fatalf("failed to get block %d: %v", i, err)
		}
		if !block.IsOrphaned {
			t.Errorf("expected block %d to be orphaned", i)
		}
	}

	// Verify new block is canonical
	canonical, err := repo.GetBlockByNumber(ctx, 105)
	if err != nil {
		t.Fatalf("failed to get canonical block: %v", err)
	}
	if canonical.Hash != "0xnew_105" {
		t.Errorf("expected new canonical block, got %s", canonical.Hash)
	}
}

// TestHandleReorgAtomic_ShortNewChainPreservesCommonAncestor prevents regression of a bug
// where a shorter new chain caused the common ancestor to be incorrectly orphaned.
func TestHandleReorgAtomic_ShortNewChainPreservesCommonAncestor(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// 1. Setup initial chain: 100 -> 101 -> 102
	blocks := []outbound.BlockState{
		{Number: 100, Hash: "0xhash100", ParentHash: "0xhash99", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix()},
		{Number: 101, Hash: "0xhash101", ParentHash: "0xhash100", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix()},
		{Number: 102, Hash: "0xhash102", ParentHash: "0xhash101", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix()},
	}

	for _, b := range blocks {
		if _, err := repo.SaveBlock(ctx, b); err != nil {
			t.Fatalf("failed to save setup block %d: %v", b.Number, err)
		}
	}

	// 2. Simulate Reorg
	// Old Chain: 100 -> 101 -> 102 (Tip 102). Depth=2 (orphaning 101, 102).
	// New Chain: 100 -> 101' (Tip 101').
	// Common Ancestor: 100.

	newBlock := outbound.BlockState{
		Number:         101,
		Hash:           "0xhash101_prime",
		ParentHash:     "0xhash100",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}

	event := outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: newBlock.Number, // 101
		OldHash:     "0xhash102",     // Tip of old chain
		NewHash:     newBlock.Hash,
		Depth:       2, // 101 and 102 are orphaned
	}

	// We pass commonAncestor=100 explicitly.
	// Previous buggy logic calculated: 101 - 2 = 99. Orphans > 99 (orphans 100).
	// Correct logic: Orphans > 100.
	_, err := repo.HandleReorgAtomic(ctx, 100, event, newBlock)
	if err != nil {
		t.Fatalf("HandleReorgAtomic failed: %v", err)
	}

	// 3. Verify: Check if 100 is still canonical
	state100, err := repo.GetBlockByHash(ctx, "0xhash100")
	if err != nil {
		t.Fatalf("failed to retrieve block 100: %v", err)
	}

	if state100.IsOrphaned {
		t.Errorf("REGRESSION: Common ancestor block 100 was incorrectly orphaned!")
	}
}

// TestHandleReorgAtomic_MultiBlockReorgGap demonstrates that HandleReorgAtomic
// creates a gap if the new chain has multiple blocks but valid intermediate blocks are not provided.
// This is not a bug in HandleReorgAtomic itself, but a system behavior validation.
func TestHandleReorgAtomic_MultiBlockReorgGap(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// 1. Setup initial chain: ... -> 100 -> 101 -> 102
	blocks := []outbound.BlockState{
		{Number: 100, Hash: "0xhash100", ParentHash: "0xhash99", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix()},
		{Number: 101, Hash: "0xhash101", ParentHash: "0xhash100", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix()},
		{Number: 102, Hash: "0xhash102", ParentHash: "0xhash101", ReceivedAt: time.Now().Unix(), BlockTimestamp: time.Now().Unix()},
	}

	for _, b := range blocks {
		if _, err := repo.SaveBlock(ctx, b); err != nil {
			t.Fatalf("failed to save setup block %d: %v", b.Number, err)
		}
	}

	// 2. Simulate Reorg
	// New Chain: 100 -> 101' -> 102'
	// Tip: 102'. Common Ancestor: 100.
	// We receive 102' as the new head. Intermediate 101' is implicitly part of the chain but not sent in the call.

	newBlock := outbound.BlockState{
		Number:         102,
		Hash:           "0xhash102_prime",
		ParentHash:     "0xhash101_prime", // Parent is MISSING from DB
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}

	event := outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: newBlock.Number, // 102
		OldHash:     "0xhash102",
		NewHash:     newBlock.Hash,
		Depth:       2, // 101, 102 replaced
	}

	// Executing atomic reorg for the tip 102'
	_, err := repo.HandleReorgAtomic(ctx, 100, event, newBlock)
	if err != nil {
		t.Fatalf("HandleReorgAtomic failed: %v", err)
	}

	// 3. Verify Gap
	// Block 102' should exist
	b102, err := repo.GetBlockByNumber(ctx, 102)
	if err != nil {
		t.Fatalf("failed to get block 102: %v", err)
	}
	if b102.Hash != "0xhash102_prime" {
		t.Errorf("block 102 mismatch")
	}

	// Block 101' should be missing (or we find the orphaned one, but GetBlockByNumber filters orphans)
	b101, err := repo.GetBlockByNumber(ctx, 101)
	if b101 != nil {
		t.Errorf("expected gap at 101, but found block: %s", b101.Hash)
	} else if err != nil {
		// pgx might return nil for NoRows depending on impl, but our repo returns (nil, nil) for NoRows usually
		// checking impl:
		// if errors.Is(err, sql.ErrNoRows) { return nil, nil }
		// so err should be nil if missing.
	}

	// Double check we have a gap
	if b101 == nil {
		t.Log("Confirmed: Gap exists at block 101")
	}
}

// TestHandleReorgAtomic_ConcurrentCallsWithSerializableMustAllSucceed verifies that
// HandleReorgAtomic handles concurrent operations correctly when using SERIALIZABLE isolation.
//
// This test calls HandleReorgAtomic concurrently with operations that create overlapping
// orphan ranges. With SERIALIZABLE isolation and retry logic, all operations should succeed.
func TestHandleReorgAtomic_ConcurrentCallsWithSerializableMustAllSucceed(t *testing.T) {
	// Set test timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	// Setup: Create a chain of blocks (100-115)
	for i := int64(100); i <= 115; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xoriginal_%d", i),
			ParentHash:     fmt.Sprintf("0xoriginal_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Run concurrent HandleReorgAtomic calls
	// All use the same common ancestor but different new block numbers/hashes
	// This creates serialization pressure that tests the retry logic
	const numGoroutines = 10
	startCh := make(chan struct{})
	resultCh := make(chan struct {
		id      int
		version int
		err     error
	}, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			<-startCh

			blockNum := int64(110 + id%3) // Overlapping block numbers
			event := outbound.ReorgEvent{
				DetectedAt:  time.Now(),
				BlockNumber: blockNum,
				OldHash:     fmt.Sprintf("0xoriginal_%d", blockNum),
				NewHash:     fmt.Sprintf("0xnew_%d", id),
				Depth:       int(blockNum - 105), // Common ancestor at 105
			}

			newBlock := outbound.BlockState{
				Number:         blockNum,
				Hash:           fmt.Sprintf("0xnew_%d", id),
				ParentHash:     "0xoriginal_105",
				ReceivedAt:     time.Now().Unix(),
				BlockTimestamp: time.Now().Unix(),
			}

			// Call the actual HandleReorgAtomic which now has SERIALIZABLE + retry logic
			version, err := repo.HandleReorgAtomic(ctx, 105, event, newBlock)
			resultCh <- struct {
				id      int
				version int
				err     error
			}{id, version, err}
		}(i)
	}

	close(startCh)

	// Collect results
	successCount := 0
	var failures []string
	for i := 0; i < numGoroutines; i++ {
		result := <-resultCh
		if result.err == nil {
			successCount++
		} else {
			failures = append(failures, fmt.Sprintf("goroutine %d: %v", result.id, result.err))
		}
	}

	t.Logf("Results: %d/%d succeeded", successCount, numGoroutines)

	// With SERIALIZABLE isolation and retry logic, ALL operations should succeed
	if successCount != numGoroutines {
		t.Errorf("Expected all %d HandleReorgAtomic calls to succeed, but only %d succeeded.\n"+
			"Failures:\n%s",
			numGoroutines, successCount, strings.Join(failures, "\n"))
	}
}

// TestFindGaps_DetectsReorgGap confirms that the gap created by HandleReorgAtomic
// (when it doesn't save intermediate blocks) is correctly detected by FindGaps.
// This confirms the BackfillService will eventually repair the state.
func TestFindGaps_DetectsReorgGap(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// 1. Create the scenario with a gap (same as TestHandleReorgAtomic_MultiBlockReorgGap)
	// Initial: 100, 101, 102
	for i := int64(100); i <= 102; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xhash%d", i),
			ParentHash:     fmt.Sprintf("0xhash%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save setup block %d: %v", i, err)
		}
	}

	// Reorg: 100 -> [GAP 101'] -> 102'
	// HandleReorgAtomic only saves 102', leaving 101' missing.
	newBlock := outbound.BlockState{
		Number:         102,
		Hash:           "0xhash102_prime",
		ParentHash:     "0xhash101_prime",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}
	event := outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: newBlock.Number,
		OldHash:     "0xhash102",
		NewHash:     newBlock.Hash,
		Depth:       2,
	}

	_, err := repo.HandleReorgAtomic(ctx, 100, event, newBlock)
	if err != nil {
		t.Fatalf("HandleReorgAtomic failed: %v", err)
	}

	// 2. Run FindGaps looking at range 100-102
	gaps, err := repo.FindGaps(ctx, 100, 102)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	// 3. Verify it found gap at 101
	if len(gaps) != 1 {
		t.Fatalf("Expected 1 gap, got %d", len(gaps))
	}

	gap := gaps[0]
	if gap.From != 101 || gap.To != 101 {
		t.Errorf("Expected gap at 101-101, got %d-%d", gap.From, gap.To)
	}
}

// TestGetMinUnpublishedBlock tests finding the minimum unpublished canonical block.
func TestGetMinUnpublishedBlock(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	t.Run("no unpublished blocks returns false", func(t *testing.T) {
		// No blocks at all
		blockNum, found, err := repo.GetMinUnpublishedBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if found {
			t.Errorf("expected found=false when no blocks exist, got blockNum=%d", blockNum)
		}
	})

	// Save blocks 1-5, mark all as published
	for i := int64(1); i <= 5; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xpub_%d", i),
			ParentHash:     fmt.Sprintf("0xpub_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
		if err := repo.MarkPublishComplete(ctx, fmt.Sprintf("0xpub_%d", i)); err != nil {
			t.Fatalf("failed to mark block %d published: %v", i, err)
		}
	}

	t.Run("all published returns false", func(t *testing.T) {
		blockNum, found, err := repo.GetMinUnpublishedBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if found {
			t.Errorf("expected found=false when all blocks published, got blockNum=%d", blockNum)
		}
	})

	// Save blocks 6-10, leave them unpublished
	for i := int64(6); i <= 10; i++ {
		_, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xpub_%d", i),
			ParentHash:     fmt.Sprintf("0xpub_%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	t.Run("mix of published and unpublished returns lowest unpublished", func(t *testing.T) {
		blockNum, found, err := repo.GetMinUnpublishedBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Fatal("expected found=true")
		}
		if blockNum != 6 {
			t.Errorf("expected blockNum=6, got %d", blockNum)
		}
	})

	// Mark block 6 published, so 7 becomes the lowest unpublished
	if err := repo.MarkPublishComplete(ctx, "0xpub_6"); err != nil {
		t.Fatalf("failed to mark block 6 published: %v", err)
	}

	t.Run("after publishing lowest, returns next lowest", func(t *testing.T) {
		blockNum, found, err := repo.GetMinUnpublishedBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Fatal("expected found=true")
		}
		if blockNum != 7 {
			t.Errorf("expected blockNum=7, got %d", blockNum)
		}
	})

	t.Run("orphaned unpublished blocks are ignored", func(t *testing.T) {
		// Orphan block 7 (unpublished) — should be excluded
		if err := repo.MarkBlockOrphaned(ctx, "0xpub_7"); err != nil {
			t.Fatalf("failed to orphan block 7: %v", err)
		}

		blockNum, found, err := repo.GetMinUnpublishedBlock(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Fatal("expected found=true")
		}
		// Block 7 is orphaned, so 8 should be the lowest unpublished
		if blockNum != 8 {
			t.Errorf("expected blockNum=8 (7 is orphaned), got %d", blockNum)
		}
	})
}

// seedBlockStatesAcrossDays bulk-inserts rows for chainID spread across
// multiple days of created_at, producing multiple TimescaleDB chunks. Uses
// pgx CopyFrom (binary protocol, minimal round-trips) inside an explicit
// transaction with SET LOCAL session_replication_role = 'replica' to bypass
// the assign_block_version trigger for the duration of the copy. SET LOCAL is
// auto-reset by Postgres on commit/rollback, so the pooled connection can
// never leak "triggers disabled" state to the next test that acquires it.
// We want rows to exist to exercise the planner, not to exercise the trigger
// — the trigger's slowness is exactly what the caller test guards against.
func seedBlockStatesAcrossDays(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, rows, days int) {
	t.Helper()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(ctx); err != nil {
				t.Logf("rollback seed tx: %v", err)
			}
		}
	}()

	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = 'replica'"); err != nil {
		t.Fatalf("disable triggers (session_replication_role): %v", err)
	}

	now := time.Now().UTC()
	batch := make([][]any, 0, rows)
	// Block numbers start at 1_000_000 — well below the test's probe number
	// (1<<40 ≈ 1.1e12), so the probe is guaranteed to miss every seeded row.
	for i := 0; i < rows; i++ {
		createdAt := now.Add(-time.Duration(i%days) * 24 * time.Hour)
		hash := fmt.Sprintf("0x%064x", i)
		parent := fmt.Sprintf("0x%064x", i-1)
		batch = append(batch, []any{chainID, int64(i + 1_000_000), hash, parent, int64(0), false, 0, createdAt})
	}

	copied, err := tx.CopyFrom(ctx,
		pgx.Identifier{"block_states"},
		[]string{"chain_id", "number", "hash", "parent_hash", "received_at", "is_orphaned", "version", "created_at"},
		pgx.CopyFromRows(batch),
	)
	if err != nil {
		t.Fatalf("copy seed rows: %v", err)
	}
	if copied != int64(rows) {
		t.Fatalf("copied %d rows, expected %d", copied, rows)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}
	committed = true

	if _, err := pool.Exec(ctx, `ANALYZE block_states`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
}

// parseTotalBuffersFromExplain sums every "shared hit=X" and "shared read=Y"
// occurrence in an EXPLAIN (ANALYZE, BUFFERS) TEXT-format plan. We don't walk
// nodes — the sum across all "Buffers:" lines is what we care about.
func parseTotalBuffersFromExplain(plan string) int {
	// Postgres emits a separate "Planning:" section at the end of EXPLAIN
	// output with its own buffer accounting. On TimescaleDB hypertables the
	// planning phase inspects chunk catalog pages (thousands of buffers for
	// ~100 chunks) and would dwarf the execution cost we actually care about.
	// Strip it before summing.
	if idx := strings.Index(plan, "\nPlanning:"); idx >= 0 {
		plan = plan[:idx]
	}
	re := regexp.MustCompile(`shared (?:hit|read)=(\d+)`)
	total := 0
	for _, m := range re.FindAllStringSubmatch(plan, -1) {
		// Regex already restricts m[1] to digits; strconv.Atoi cannot fail.
		n, _ := strconv.Atoi(m[1])
		total += n
	}
	return total
}

// TestSaveBlock_TriggerQueryPlanIsEfficient is a regression guard for VEC-144.
// The assign_block_version() BEFORE INSERT trigger runs
//
//	SELECT MAX(version) FROM block_states WHERE chain_id=$1 AND number=$2
//
// on every INSERT. If no index matches this predicate, the planner falls back
// to sequentially scanning every chunk, making a single INSERT take seconds on
// a multi-million-row hypertable (observed on arbitrum: ~1500 ms, 160k buffer
// hits). This test asserts the plan stays index-based and touches few pages.
func TestSaveBlock_TriggerQueryPlanIsEfficient(t *testing.T) {
	truncateBlockState(t, context.Background())
	ctx := context.Background()

	const chainID = int64(42161)

	// 50k rows spread across 10 days — enough to split into several chunks and
	// to tip the planner past the "everything is a seq scan on tiny tables"
	// regime. Small enough to seed in <5s.
	seedBlockStatesAcrossDays(t, ctx, blockstatePool, chainID, 50_000, 10)

	const probeNumber = int64(1) << 40 // guaranteed no rows — realistic trigger miss case

	explainRows, err := blockstatePool.Query(ctx, `
		EXPLAIN (ANALYZE, BUFFERS)
		SELECT COALESCE(MAX(version), -1) + 1
		FROM block_states
		WHERE chain_id = $1 AND number = $2
	`, chainID, probeNumber)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	defer explainRows.Close()
	var planLines []string
	for explainRows.Next() {
		var line string
		if err := explainRows.Scan(&line); err != nil {
			t.Fatalf("EXPLAIN scan: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := explainRows.Err(); err != nil {
		t.Fatalf("EXPLAIN rows: %v", err)
	}
	planText := strings.Join(planLines, "\n")

	if strings.Contains(planText, "Seq Scan") {
		t.Fatalf("trigger query fell back to a sequential scan — check indexes on block_states.\nplan:\n%s", planText)
	}

	buffers := parseTotalBuffersFromExplain(planText)
	const maxBuffers = 200
	if buffers > maxBuffers {
		t.Fatalf("trigger query touched %d buffers; expected ≤%d. A matching index on (chain_id, number, version DESC) is required.\nplan:\n%s", buffers, maxBuffers, planText)
	}
}

// anchorHash203 is the canonical row above the 200-202 orphaned segment the
// heal tests use; ClearBlocksOrphaned refuses a segment with no live anchor.
const anchorHash203 = "0xsegment_anchor_203"

// seedOrphanedSegment seeds a linked run of orphaned blocks over [from, to] and
// returns their hashes in ascending block order.
func seedOrphanedSegment(t *testing.T, ctx context.Context, repo *BlockStateRepository, from, to int64) []string {
	t.Helper()
	var hashes []string
	for number := from; number <= to; number++ {
		hash := fmt.Sprintf("0xsegment_%d", number)
		seedOrphanOnlyHeight(t, ctx, repo, number, hash)
		hashes = append(hashes, hash)
	}
	return hashes
}

// canonicalHashesAt returns the non-orphaned hashes stored over [from, to].
func canonicalHashesAt(t *testing.T, ctx context.Context, from, to int64) []string {
	t.Helper()
	rows, err := blockstatePool.Query(ctx,
		`SELECT hash FROM block_states
		 WHERE chain_id = 1 AND number >= $1 AND number <= $2 AND NOT is_orphaned
		 ORDER BY number`, from, to)
	if err != nil {
		t.Fatalf("query canonical hashes: %v", err)
	}
	defer rows.Close()
	var hashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			t.Fatalf("scan canonical hash: %v", err)
		}
		hashes = append(hashes, hash)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate canonical hashes: %v", err)
	}
	return hashes
}

// TestClearBlocksOrphaned verifies the self-heal port: a wrongly-orphaned
// segment is cleared as one unit, and clearing an already-canonical member
// again is a no-op.
func TestClearBlocksOrphaned(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	hashes := seedOrphanedSegment(t, ctx, repo, 200, 202)
	saveCanonicalBlockAs(t, ctx, repo, 203, anchorHash203, "0xsegment_202")

	if err := repo.ClearBlocksOrphaned(ctx, anchorHash203, hashes); err != nil {
		t.Fatalf("ClearBlocksOrphaned: %v", err)
	}
	if got := canonicalHashesAt(t, ctx, 200, 202); len(got) != len(hashes) {
		t.Fatalf("canonical hashes = %v, want all of %v", got, hashes)
	}

	if err := repo.ClearBlocksOrphaned(ctx, anchorHash203, hashes); err != nil {
		t.Fatalf("ClearBlocksOrphaned (idempotent call): %v", err)
	}
}

// TestClearBlocksOrphaned_ClearsNoneWhenAHashIsMissing is the ARCT-379 round-2
// atomicity regression: healing a segment one row at a time left the rows above
// a mid-loop failure canonical and the rest orphaned — a chain broken in a way
// neither the gap finder nor the un-orphan walk can repair.
func TestClearBlocksOrphaned_ClearsNoneWhenAHashIsMissing(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	hashes := seedOrphanedSegment(t, ctx, repo, 200, 202)
	saveCanonicalBlockAs(t, ctx, repo, 203, anchorHash203, "0xsegment_202")

	if err := repo.ClearBlocksOrphaned(ctx, anchorHash203, append(hashes, "0xdoes_not_exist")); err == nil {
		t.Fatal("expected an error for the unknown hash, got nil")
	}
	if got := canonicalHashesAt(t, ctx, 200, 202); len(got) != 0 {
		t.Errorf("canonical hashes = %v, want none (the failed heal must clear nothing)", got)
	}
}

// TestClearBlocksOrphaned_RefusesWhenTheAnchorIsNoLongerCanonical is the
// ARCT-379 round-3 race: the caller computes the segment on the pool, so a
// reorg can orphan the anchor between the walk and this call. A plain read
// cannot see that under READ COMMITTED once the tx is open — the anchor is
// taken FOR UPDATE, and the whole heal refused when it is no longer canonical.
func TestClearBlocksOrphaned_RefusesWhenTheAnchorIsNoLongerCanonical(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	hashes := seedOrphanedSegment(t, ctx, repo, 200, 202)
	saveCanonicalBlockAs(t, ctx, repo, 203, anchorHash203, "0xsegment_202")
	if err := repo.MarkBlockOrphaned(ctx, anchorHash203); err != nil {
		t.Fatalf("orphan the anchor: %v", err)
	}

	if err := repo.ClearBlocksOrphaned(ctx, anchorHash203, hashes); err == nil {
		t.Fatal("expected ClearBlocksOrphaned to refuse an orphaned anchor, got nil")
	}
	if got := canonicalHashesAt(t, ctx, 200, 202); len(got) != 0 {
		t.Errorf("canonical hashes = %v, want none (the segment must stay orphaned)", got)
	}
}

// TestClearBlocksOrphaned_RefusesWhenConflictingCanonicalExists covers PR #373
// review Finding 6: the heal must NOT produce a second canonical row at the
// same number. Setup an orphan-only row at height N, then insert a different
// canonical row at the same N (simulating a live reorg that won the race).
// Clearing the orphan flag on the original row must fail rather than break the
// "highest version = canonical" invariant.
func TestClearBlocksOrphaned_RefusesWhenConflictingCanonicalExists(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	const num int64 = 700
	const orphanHash = "0xclear_race_orphan_700"
	const canonicalHash = "0xclear_race_canonical_700"

	// Seed the orphan-only row.
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         num,
		Hash:           orphanHash,
		ParentHash:     "0xclear_race_699",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed orphan-only row: %v", err)
	}
	if err := repo.MarkBlockOrphaned(ctx, orphanHash); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}

	// Simulate a live reorg insert at the same number with a different hash.
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         num,
		Hash:           canonicalHash,
		ParentHash:     "0xclear_race_699",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed new canonical row: %v", err)
	}

	// Attempting to clear the orphan flag on the losing row must fail — the
	// guard inside ClearBlocksOrphaned should detect the conflicting
	// canonical row and bail.
	saveCanonicalBlockAs(t, ctx, repo, num+1, "0xclear_race_anchor_701", canonicalHash)

	if err := repo.ClearBlocksOrphaned(ctx, "0xclear_race_anchor_701", []string{orphanHash}); err == nil {
		t.Fatal("expected ClearBlocksOrphaned to refuse with conflicting canonical row, got nil")
	}

	// Invariant: exactly one non-orphaned row at this number.
	var canonicalCount int
	if err := blockstatePool.QueryRow(ctx,
		`SELECT COUNT(*) FROM block_states WHERE chain_id = $1 AND number = $2 AND NOT is_orphaned`,
		1, num).Scan(&canonicalCount); err != nil {
		t.Fatalf("count canonical: %v", err)
	}
	if canonicalCount != 1 {
		t.Fatalf("expected exactly 1 canonical row at number %d, got %d", num, canonicalCount)
	}
}

// commitReorgAbove105 commits a reorg that replaces height 106 and orphans
// everything above commonAncestor.
func commitReorgAbove105(t *testing.T, ctx context.Context, repo *BlockStateRepository, commonAncestor int64) {
	t.Helper()
	if _, err := repo.HandleReorgAtomic(ctx, commonAncestor,
		reorgAbove105Event(commonAncestor), reorgAbove105Block()); err != nil {
		t.Fatalf("HandleReorgAtomic: %v", err)
	}
}

// reorgAbove105Block is the winning block such a reorg inserts at height 106.
func reorgAbove105Block() outbound.BlockState {
	return outbound.BlockState{
		Number:         106,
		Hash:           "0xhash106_prime",
		ParentHash:     "0xhash105_prime",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}
}

func reorgAbove105Event(commonAncestor int64) outbound.ReorgEvent {
	return outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: 106,
		OldHash:     "0xhash105",
		NewHash:     "0xhash106_prime",
		Depth:       int(106 - commonAncestor),
	}
}

// TestHandleReorgAtomic_RewindsWatermark is the ARCT-379 regression: a reorg
// orphans the heights above the common ancestor without re-fetching them, so
// the watermark must drop back to the ancestor or the gap finder (which only
// scans above the watermark) never sees the resulting hole. A watermark already
// below the ancestor must be left alone, and only a real rewind is reported.
func TestHandleReorgAtomic_RewindsWatermark(t *testing.T) {
	tests := []struct {
		name          string
		watermark     int64
		wantCursor    outbound.BackfillCursor
		wantRewindLog int
	}{
		{name: "above the common ancestor rewinds", watermark: 105, wantCursor: outbound.BackfillCursor{Watermark: 103, RewindCount: 8}, wantRewindLog: 1},
		{name: "below the common ancestor keeps its value and still counts the reorg", watermark: 50, wantCursor: outbound.BackfillCursor{Watermark: 50, RewindCount: 8}, wantRewindLog: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := &testutil.SlogRecorder{}
			repo, cleanup := setupPostgresWithLogger(t, slog.New(logs))
			t.Cleanup(cleanup)

			ctx := context.Background()
			seedCanonicalChain(t, ctx, repo, 100, 105)
			seedWatermark(t, ctx, repo, tt.watermark, tt.wantCursor.RewindCount-1)

			commitReorgAbove105(t, ctx, repo, 103)

			cursor, err := repo.GetBackfillCursor(ctx)
			if err != nil {
				t.Fatalf("GetBackfillCursor: %v", err)
			}
			if cursor != tt.wantCursor {
				t.Errorf("cursor = %+v, want %+v", cursor, tt.wantCursor)
			}
			if got := logs.CountInfo("rewound backfill watermark"); got != tt.wantRewindLog {
				t.Errorf("rewind logs = %d, want %d", got, tt.wantRewindLog)
			}
		})
	}
}

// TestHandleReorgAtomic_ReportsTheWatermarkItReplaced pins the rewind's report
// to the row the statement actually overwrote. Under READ COMMITTED a CTE reads
// the statement snapshot while the UPDATE behind it re-reads the row a
// concurrent writer committed, so the two disagree exactly when a writer raced
// the reorg — and the log then names a value that was never replaced.
func TestHandleReorgAtomic_ReportsTheWatermarkItReplaced(t *testing.T) {
	logs := &testutil.SlogRecorder{}
	repo, cleanup := setupPostgresWithLogger(t, slog.New(logs))
	t.Cleanup(cleanup)

	ctx := context.Background()
	seedCanonicalChain(t, ctx, repo, 100, 105)
	seedWatermark(t, ctx, repo, 105, 0)

	// A concurrent writer holds the watermark row at 104. The reorg's rewind
	// blocks behind it and, once released, replaces 104 — not the 105 its own
	// snapshot still shows.
	blocker, err := blockstatePool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocking tx: %v", err)
	}
	if _, err := blocker.Exec(ctx,
		`UPDATE backfill_watermark SET watermark = 104 WHERE chain_id = 1`); err != nil {
		t.Fatalf("blocking update: %v", err)
	}

	reorgDone := make(chan error, 1)
	go func() {
		_, err := repo.HandleReorgAtomic(ctx, 103, reorgAbove105Event(103), reorgAbove105Block())
		reorgDone <- err
	}()

	waitForBlockedBackend(t, ctx)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("commit blocking tx: %v", err)
	}
	if err := <-reorgDone; err != nil {
		t.Fatalf("HandleReorgAtomic: %v", err)
	}

	if got := loggedAttr(t, logs, "rewound backfill watermark", "from"); got != "104" {
		t.Errorf("rewound from = %s, want 104 (the value the statement replaced)", got)
	}
}

// waitForBlockedBackend blocks until a backend on this database is waiting on a
// lock, so the test hands over only once the racing statement is queued.
func waitForBlockedBackend(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := blockstatePool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatalf("poll pg_stat_activity: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no backend ever blocked on the watermark row")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// loggedAttr returns the named attribute of the first record whose message
// contains msg.
func loggedAttr(t *testing.T, logs *testutil.SlogRecorder, msg, key string) string {
	t.Helper()
	for _, record := range logs.Records {
		if !strings.Contains(record.Message, msg) {
			continue
		}
		value := ""
		record.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				value = a.Value.String()
			}
			return value == ""
		})
		return value
	}
	t.Fatalf("no log record matching %q", msg)
	return ""
}

// TestHandleReorgAtomic_RewoundWatermarkExposesOrphanedHeightsToFindGaps is the
// point of the rewind: the heights the reorg orphaned without replacing are
// back inside the gap finder's scan range.
func TestHandleReorgAtomic_RewoundWatermarkExposesOrphanedHeightsToFindGaps(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	seedCanonicalChain(t, ctx, repo, 100, 105)
	seedWatermark(t, ctx, repo, 105, 0)

	commitReorgAbove105(t, ctx, repo, 103)

	gaps, err := repo.FindGaps(ctx, 100, 106)
	if err != nil {
		t.Fatalf("FindGaps: %v", err)
	}
	if len(gaps) != 1 || gaps[0].From != 104 || gaps[0].To != 105 {
		t.Errorf("gaps = %v, want [{104 105}]", gaps)
	}
}

// saveCanonicalBlock saves one canonical block with an explicit parent hash.
func saveCanonicalBlock(t *testing.T, ctx context.Context, repo *BlockStateRepository, number int64, parentHash string) {
	t.Helper()
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         number,
		Hash:           fmt.Sprintf("0xhash%d", number),
		ParentHash:     parentHash,
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("save block %d: %v", number, err)
	}
}

// saveCanonicalBlockAs saves one canonical block with an explicit hash, for the
// cases that need two rows at one height.
func saveCanonicalBlockAs(t *testing.T, ctx context.Context, repo *BlockStateRepository, number int64, hash, parentHash string) {
	t.Helper()
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         number,
		Hash:           hash,
		ParentHash:     parentHash,
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("save block %d: %v", number, err)
	}
}

// seedCanonicalChain saves a linked canonical chain over [from, to].
func seedCanonicalChain(t *testing.T, ctx context.Context, repo *BlockStateRepository, from, to int64) {
	t.Helper()
	for i := from; i <= to; i++ {
		saveCanonicalBlock(t, ctx, repo, i, fmt.Sprintf("0xhash%d", i-1))
	}
}

// seedOrphanOnlyHeight saves a block and orphans it, leaving the height with no
// canonical row — the state a reorg leaves behind when the canonical broadcast
// for that height was dropped (ARCT-379).
func seedOrphanOnlyHeight(t *testing.T, ctx context.Context, repo *BlockStateRepository, number int64, hash string) {
	t.Helper()
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         number,
		Hash:           hash,
		ParentHash:     fmt.Sprintf("0xparent%d", number),
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed block %d: %v", number, err)
	}
	if err := repo.MarkBlockOrphaned(ctx, hash); err != nil {
		t.Fatalf("orphan block %d: %v", number, err)
	}
}

// TestGetLowestCanonicalAbove backs the un-orphan walk's anchor lookup: one
// query instead of a probe per height, over a bounded range so an isolated
// orphan cannot make it scan the whole table.
func TestGetLowestCanonicalAbove(t *testing.T) {
	tests := []struct {
		name       string
		number     int64
		maxNumber  int64
		wantNumber int64
	}{
		{name: "returns the lowest canonical block above", number: 500, maxNumber: 510, wantNumber: 504},
		{name: "skips orphaned rows", number: 502, maxNumber: 510, wantNumber: 504},
		{name: "bound excludes the canonical block", number: 500, maxNumber: 503},
		{name: "no canonical block above", number: 505, maxNumber: 515},
	}

	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	seedOrphanOnlyHeight(t, ctx, repo, 501, "0xanchor_orphan_501")
	seedOrphanOnlyHeight(t, ctx, repo, 503, "0xanchor_orphan_503")
	saveCanonicalBlock(t, ctx, repo, 504, "0xhash503")
	saveCanonicalBlock(t, ctx, repo, 505, "0xhash504")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := repo.GetLowestCanonicalAbove(ctx, tt.number, tt.maxNumber)
			if err != nil {
				t.Fatalf("GetLowestCanonicalAbove: %v", err)
			}
			if tt.wantNumber == 0 {
				if got != nil {
					t.Fatalf("block = %+v, want none", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("block = nil, want the canonical block at %d", tt.wantNumber)
			}
			if got.Number != tt.wantNumber {
				t.Errorf("block number = %d, want %d", got.Number, tt.wantNumber)
			}
		})
	}
}

func TestFindOrphanOnlyHeights_ReportsHeightWithNoCanonicalRow(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	seedCanonicalChain(t, ctx, repo, 200, 203)
	if err := repo.MarkBlockOrphaned(ctx, "0xhash202"); err != nil {
		t.Fatalf("orphan block 202: %v", err)
	}

	heights, err := repo.FindOrphanOnlyHeights(ctx, 200, 203)
	if err != nil {
		t.Fatalf("FindOrphanOnlyHeights: %v", err)
	}
	if len(heights) != 1 || heights[0] != 202 {
		t.Errorf("heights = %v, want [202]", heights)
	}
}

func TestFindOrphanOnlyHeights_IgnoresHeightWithCanonicalSibling(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	seedOrphanOnlyHeight(t, ctx, repo, 300, "0xfork_a_300")
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         300,
		Hash:           "0xfork_b_300",
		ParentHash:     "0xparent300",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed canonical sibling: %v", err)
	}

	heights, err := repo.FindOrphanOnlyHeights(ctx, 300, 300)
	if err != nil {
		t.Fatalf("FindOrphanOnlyHeights: %v", err)
	}
	if len(heights) != 0 {
		t.Errorf("heights = %v, want none (the height has a canonical row)", heights)
	}
}

func TestFindOrphanOnlyHeights_RespectsRangeBounds(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	seedOrphanOnlyHeight(t, ctx, repo, 400, "0xorphan_400")
	seedOrphanOnlyHeight(t, ctx, repo, 410, "0xorphan_410")
	seedOrphanOnlyHeight(t, ctx, repo, 420, "0xorphan_420")

	heights, err := repo.FindOrphanOnlyHeights(ctx, 405, 415)
	if err != nil {
		t.Fatalf("FindOrphanOnlyHeights: %v", err)
	}
	if len(heights) != 1 || heights[0] != 410 {
		t.Errorf("heights = %v, want [410]", heights)
	}
}

// TestAdvanceBackfillWatermark is the compare-and-set the gap filler advances
// with: a cursor another writer moved (a reorg commit) must not be overwritten
// with a value computed from the cursor it replaced (ARCT-379).
func TestAdvanceBackfillWatermark(t *testing.T) {
	tests := []struct {
		name          string
		unseededChain bool
		expected      outbound.BackfillCursor
		wantAdvanced  bool
		wantWatermark int64
	}{
		{name: "matching expected advances", expected: outbound.BackfillCursor{Watermark: 100, RewindCount: 2}, wantAdvanced: true, wantWatermark: 150},
		{name: "stale watermark leaves the stored value", expected: outbound.BackfillCursor{Watermark: 90, RewindCount: 2}, wantAdvanced: false, wantWatermark: 100},
		{name: "a reorg since the scan leaves the stored value", expected: outbound.BackfillCursor{Watermark: 100, RewindCount: 1}, wantAdvanced: false, wantWatermark: 100},
		{name: "unset expected seeds a chain with no row", unseededChain: true, wantAdvanced: true, wantWatermark: 150},
		{name: "stale expected on a chain with no row refuses", unseededChain: true, expected: outbound.BackfillCursor{Watermark: 90}, wantAdvanced: false, wantWatermark: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, cleanup := setupPostgres(t)
			t.Cleanup(cleanup)

			ctx := context.Background()
			if tt.unseededChain {
				repo = newUnseededChainRepository(t, ctx)
			} else {
				seedWatermark(t, ctx, repo, 100, 2)
			}

			advanced, err := repo.AdvanceBackfillWatermark(ctx, tt.expected, 150)
			if err != nil {
				t.Fatalf("AdvanceBackfillWatermark: %v", err)
			}
			if advanced != tt.wantAdvanced {
				t.Errorf("advanced = %v, want %v", advanced, tt.wantAdvanced)
			}

			watermark, err := repo.GetBackfillWatermark(ctx)
			if err != nil {
				t.Fatalf("GetBackfillWatermark: %v", err)
			}
			if watermark != tt.wantWatermark {
				t.Errorf("watermark = %d, want %d", watermark, tt.wantWatermark)
			}
		})
	}
}

// newUnseededChainRepository returns a repository for a chain the migrations
// never seeded a backfill_watermark row for — every chain but Ethereum and
// Avalanche starts there.
func newUnseededChainRepository(t *testing.T, ctx context.Context) *BlockStateRepository {
	t.Helper()
	const chainID int64 = 8453
	if _, err := blockstatePool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES ($1, $2) ON CONFLICT (chain_id) DO NOTHING`,
		chainID, "ARCT-379 unseeded chain"); err != nil {
		t.Fatalf("insert chain: %v", err)
	}
	if _, err := blockstatePool.Exec(ctx,
		`DELETE FROM backfill_watermark WHERE chain_id = $1`, chainID); err != nil {
		t.Fatalf("clear watermark row: %v", err)
	}
	return NewBlockStateRepository(blockstatePool, chainID, nil)
}

// TestRewindBackfillWatermark pins the rewind against the SQL the reorg commit
// and the stale-chain recovery share: the watermark only ever moves down, and
// the rewind count is bumped whether or not it moved — a no-op rewind that left
// the count alone is what let a compare-and-set match across a reorg and retire
// the height that reorg had just emptied (ARCT-379).
func TestRewindBackfillWatermark(t *testing.T) {
	tests := []struct {
		name          string
		unseededChain bool
		to            int64
		wantPrevious  int64
		wantRewound   bool
		wantCursor    outbound.BackfillCursor
	}{
		{
			name:         "a target below the watermark lowers it",
			to:           90,
			wantPrevious: 100,
			wantRewound:  true,
			wantCursor:   outbound.BackfillCursor{Watermark: 90, RewindCount: 3},
		},
		{
			name:         "a target at the watermark counts without moving it",
			to:           100,
			wantPrevious: 100,
			wantCursor:   outbound.BackfillCursor{Watermark: 100, RewindCount: 3},
		},
		{
			name:         "a target above the watermark counts without raising it",
			to:           120,
			wantPrevious: 100,
			wantCursor:   outbound.BackfillCursor{Watermark: 100, RewindCount: 3},
		},
		{
			name:          "a chain with no row is seeded unset and counted",
			unseededChain: true,
			to:            90,
			wantCursor:    outbound.BackfillCursor{RewindCount: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, cleanup := setupPostgres(t)
			t.Cleanup(cleanup)

			ctx := context.Background()
			if tt.unseededChain {
				repo = newUnseededChainRepository(t, ctx)
			} else {
				seedWatermark(t, ctx, repo, 100, 2)
			}

			previous, rewound, err := repo.RewindBackfillWatermark(ctx, tt.to)
			if err != nil {
				t.Fatalf("RewindBackfillWatermark: %v", err)
			}
			if previous != tt.wantPrevious {
				t.Errorf("previous = %d, want %d", previous, tt.wantPrevious)
			}
			if rewound != tt.wantRewound {
				t.Errorf("rewound = %v, want %v", rewound, tt.wantRewound)
			}

			cursor, err := repo.GetBackfillCursor(ctx)
			if err != nil {
				t.Fatalf("GetBackfillCursor: %v", err)
			}
			if cursor != tt.wantCursor {
				t.Errorf("cursor = %+v, want %+v", cursor, tt.wantCursor)
			}
		})
	}
}

// TestVerifyChainIntegrity_ReportsFirstViolation covers the ARCT-379 hole: two
// canonical rows with a missing height between them used to read as a valid
// chain, because only pairs where prev = number - 1 were compared.
func TestVerifyChainIntegrity_ReportsFirstViolation(t *testing.T) {
	tests := []struct {
		name            string
		seed            func(t *testing.T, ctx context.Context, repo *BlockStateRepository)
		from, to        int64
		wantErrContains string
	}{
		{
			name: "missing height between two canonical blocks",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 102)
				seedCanonicalChain(t, ctx, repo, 104, 105)
			},
			from:            100,
			to:              105,
			wantErrContains: "canonical block(s) 103 to 103 missing between blocks 102 and 104",
		},
		{
			name: "parent hash break",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
				saveCanonicalBlock(t, ctx, repo, 104, "0xhash101")
				saveCanonicalBlock(t, ctx, repo, 105, "0xhash104")
			},
			from:            100,
			to:              105,
			wantErrContains: "chain integrity violation at block 104",
		},
		{
			name: "heights below the first canonical block",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 105)
			},
			from: 0,
			to:   105,
		},
		{
			name: "missing heights after the last canonical block",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
			},
			from:            100,
			to:              105,
			wantErrContains: "canonical block(s) 104 to 105 missing after block 103",
		},
		{
			name: "two canonical rows at one height",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 105)
				saveCanonicalBlockAs(t, ctx, repo, 103, "0xhash103_twin", "0xhash102")
			},
			from:            100,
			to:              105,
			wantErrContains: "duplicate canonical rows at height 103: 0xhash103_twin and 0xhash103",
		},
		{
			name: "parent break below a missing height is reported first",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 101)
				saveCanonicalBlock(t, ctx, repo, 102, "0xhash100")
				saveCanonicalBlock(t, ctx, repo, 103, "0xhash102")
				saveCanonicalBlock(t, ctx, repo, 105, "0xhash104")
			},
			from:            100,
			to:              105,
			wantErrContains: "chain integrity violation at block 102",
		},
		{
			name: "missing height below a parent break is reported first",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 101)
				saveCanonicalBlock(t, ctx, repo, 103, "0xhash102")
				saveCanonicalBlock(t, ctx, repo, 104, "0xhash101")
				saveCanonicalBlock(t, ctx, repo, 105, "0xhash104")
			},
			from:            100,
			to:              105,
			wantErrContains: "canonical block(s) 102 to 102 missing between blocks 101 and 103",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, cleanup := setupPostgres(t)
			t.Cleanup(cleanup)

			ctx := context.Background()
			tt.seed(t, ctx, repo)

			err := repo.VerifyChainIntegrity(ctx, tt.from, tt.to)
			if tt.wantErrContains == "" {
				if err != nil {
					t.Fatalf("VerifyChainIntegrity = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("VerifyChainIntegrity = nil, want error containing %q", tt.wantErrContains)
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Errorf("VerifyChainIntegrity = %v, want error containing %q", err, tt.wantErrContains)
			}
		})
	}
}

// TestFindOrphanOnlyHeights_ReportsMoreThanOneHundredHeights: the validator
// reports what this returns, so a cap would under-report a reorg storm.
func TestFindOrphanOnlyHeights_ReportsMoreThanOneHundredHeights(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	const first, count = int64(500), int64(150)
	for number := first; number < first+count; number++ {
		seedOrphanOnlyHeight(t, ctx, repo, number, fmt.Sprintf("0xorphan_%d", number))
	}

	heights, err := repo.FindOrphanOnlyHeights(ctx, first, first+count-1)
	if err != nil {
		t.Fatalf("FindOrphanOnlyHeights: %v", err)
	}
	if int64(len(heights)) != count {
		t.Fatalf("len(heights) = %d, want %d", len(heights), count)
	}
	if heights[0] != first || heights[count-1] != first+count-1 {
		t.Errorf("heights span [%d, %d], want [%d, %d]", heights[0], heights[count-1], first, first+count-1)
	}
}

// TestVerifyParentLinks covers the check the validator runs above the backfill
// watermark: a missing height up there is the gap filler's live work, but a
// broken link or a duplicated height never repairs itself and would otherwise
// stay invisible until the lag alert fires (ARCT-379).
func TestVerifyParentLinks(t *testing.T) {
	tests := []struct {
		name            string
		seed            func(t *testing.T, ctx context.Context, repo *BlockStateRepository)
		from, to        int64
		wantErrContains string
	}{
		{
			name: "parent hash break is reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
				saveCanonicalBlock(t, ctx, repo, 104, "0xhash101")
			},
			from:            100,
			to:              105,
			wantErrContains: "chain integrity violation at block 104",
		},
		{
			name: "missing height is not reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 102)
				seedCanonicalChain(t, ctx, repo, 104, 105)
			},
			from: 100,
			to:   105,
		},
		{
			name: "missing heights after the last canonical block are not reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
			},
			from: 100,
			to:   105,
		},
		{
			name: "two canonical rows at one height are reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 105)
				saveCanonicalBlockAs(t, ctx, repo, 103, "0xhash103_twin", "0xhash102")
			},
			from:            100,
			to:              105,
			wantErrContains: "duplicate canonical rows at height 103: 0xhash103_twin and 0xhash103",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, cleanup := setupPostgres(t)
			t.Cleanup(cleanup)

			ctx := context.Background()
			tt.seed(t, ctx, repo)

			err := repo.VerifyParentLinks(ctx, tt.from, tt.to)
			if tt.wantErrContains == "" {
				if err != nil {
					t.Fatalf("VerifyParentLinks = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("VerifyParentLinks = nil, want error containing %q", tt.wantErrContains)
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Errorf("VerifyParentLinks = %v, want error containing %q", err, tt.wantErrContains)
			}
		})
	}
}
