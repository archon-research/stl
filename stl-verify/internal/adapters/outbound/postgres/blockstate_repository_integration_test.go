//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// setupPostgres creates a TimescaleDB container and returns a connected repository.
func setupPostgres(t *testing.T) (*BlockStateRepository, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
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
		t.Fatalf("failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	repo := NewBlockStateRepository(db, nil)
	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	cleanup := func() {
		db.Close()
		container.Terminate(ctx)
	}

	return repo, cleanup
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
		Number:     100,
		Hash:       "0xabc123",
		ParentHash: "0xparent1",
		ReceivedAt: originalReceivedAt,
		IsOrphaned: false,
	}

	version1, err := repo.SaveBlock(ctx, firstState)
	if err != nil {
		t.Fatalf("first save failed: %v", err)
	}

	// Second save: same hash (duplicate arrival, e.g., from reconnect or backfill)
	// Even though we're passing different values, a real duplicate would have
	// identical content. The test verifies we ignore the second save entirely.
	duplicateState := outbound.BlockState{
		Number:     100,
		Hash:       "0xabc123", // Same hash = same block
		ParentHash: "0xparent1",
		ReceivedAt: originalReceivedAt + 500, // Different received_at (we saw it again later)
		IsOrphaned: false,
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
			Number:     i,
			Hash:       fmt.Sprintf("0xoriginal_%d", i),
			ParentHash: fmt.Sprintf("0xoriginal_%d", i-1),
			ReceivedAt: time.Now().Unix(),
			IsOrphaned: false,
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
		Number:     101,
		Hash:       "0xnew_101",
		ParentHash: "0xoriginal_100",
		ReceivedAt: time.Now().Unix(),
		IsOrphaned: false,
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
		rows, err := repo.DB().QueryContext(ctx, `
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
			Number:     i,
			Hash:       fmt.Sprintf("0xblock_%d", i),
			ParentHash: fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt: time.Now().Unix(),
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
		Number:     100,
		Hash:       "0xcanonical",
		ParentHash: "0xparent",
		ReceivedAt: time.Now().Unix(),
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
		Number:     100,
		Hash:       "0xnew_canonical",
		ParentHash: "0xparent",
		ReceivedAt: time.Now().Unix(),
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
		Number:     100,
		Hash:       "0xorphaned_hash",
		ParentHash: "0xparent",
		ReceivedAt: time.Now().Unix(),
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
		Number: 100, Hash: "0xv0", ParentHash: "0xparent", ReceivedAt: time.Now().Unix(),
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
		Number: 100, Hash: "0xv1", ParentHash: "0xparent", ReceivedAt: time.Now().Unix(),
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
			Number:     i,
			Hash:       fmt.Sprintf("0xblock_%d", i),
			ParentHash: fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt: time.Now().Unix(),
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
			Number:     i,
			Hash:       fmt.Sprintf("0xblock_%d", i),
			ParentHash: fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt: time.Now().Unix(),
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

// TestMarkPublishComplete tests marking publish types as complete.
func TestMarkPublishComplete(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save a block
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:     100,
		Hash:       "0xtest_block",
		ParentHash: "0xparent",
		ReceivedAt: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	t.Run("marks block published", func(t *testing.T) {
		if err := repo.MarkPublishComplete(ctx, "0xtest_block", outbound.PublishTypeBlock); err != nil {
			t.Fatalf("failed to mark block published: %v", err)
		}
		block, _ := repo.GetBlockByHash(ctx, "0xtest_block")
		if !block.BlockPublished {
			t.Error("expected BlockPublished to be true")
		}
	})

	t.Run("marks receipts published", func(t *testing.T) {
		if err := repo.MarkPublishComplete(ctx, "0xtest_block", outbound.PublishTypeReceipts); err != nil {
			t.Fatalf("failed to mark receipts published: %v", err)
		}
		block, _ := repo.GetBlockByHash(ctx, "0xtest_block")
		if !block.ReceiptsPublished {
			t.Error("expected ReceiptsPublished to be true")
		}
	})

	t.Run("marks traces published", func(t *testing.T) {
		if err := repo.MarkPublishComplete(ctx, "0xtest_block", outbound.PublishTypeTraces); err != nil {
			t.Fatalf("failed to mark traces published: %v", err)
		}
		block, _ := repo.GetBlockByHash(ctx, "0xtest_block")
		if !block.TracesPublished {
			t.Error("expected TracesPublished to be true")
		}
	})

	t.Run("marks blobs published", func(t *testing.T) {
		if err := repo.MarkPublishComplete(ctx, "0xtest_block", outbound.PublishTypeBlobs); err != nil {
			t.Fatalf("failed to mark blobs published: %v", err)
		}
		block, _ := repo.GetBlockByHash(ctx, "0xtest_block")
		if !block.BlobsPublished {
			t.Error("expected BlobsPublished to be true")
		}
	})

	t.Run("returns error for non-existent block", func(t *testing.T) {
		err := repo.MarkPublishComplete(ctx, "0xnonexistent", outbound.PublishTypeBlock)
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
			Number:     i,
			Hash:       fmt.Sprintf("0xblock_%d", i),
			ParentHash: fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", i, err)
		}
	}

	// Mark block 1 as fully published
	for _, pt := range []outbound.PublishType{
		outbound.PublishTypeBlock,
		outbound.PublishTypeReceipts,
		outbound.PublishTypeTraces,
		outbound.PublishTypeBlobs,
	} {
		if err := repo.MarkPublishComplete(ctx, "0xblock_1", pt); err != nil {
			t.Fatalf("failed to mark publish: %v", err)
		}
	}

	// Mark block 2 partially published (missing traces and blobs)
	repo.MarkPublishComplete(ctx, "0xblock_2", outbound.PublishTypeBlock)
	repo.MarkPublishComplete(ctx, "0xblock_2", outbound.PublishTypeReceipts)

	// Block 3 has nothing published

	t.Run("returns incomplete blocks", func(t *testing.T) {
		blocks, err := repo.GetBlocksWithIncompletePublish(ctx, 10, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Blocks 2 and 3 should be returned (block 1 is complete)
		if len(blocks) != 2 {
			t.Fatalf("expected 2 blocks, got %d", len(blocks))
		}
		if blocks[0].Number != 2 || blocks[1].Number != 3 {
			t.Errorf("expected blocks [2,3], got [%d,%d]", blocks[0].Number, blocks[1].Number)
		}
	})

	t.Run("respects limit", func(t *testing.T) {
		blocks, err := repo.GetBlocksWithIncompletePublish(ctx, 1, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(blocks) != 1 {
			t.Fatalf("expected 1 block, got %d", len(blocks))
		}
	})

	t.Run("enableBlobs=false ignores blob status", func(t *testing.T) {
		// Mark block 2's traces complete - now only blobs is missing
		repo.MarkPublishComplete(ctx, "0xblock_2", outbound.PublishTypeTraces)

		blocks, err := repo.GetBlocksWithIncompletePublish(ctx, 10, false)
		if err != nil {
			t.Fatalf("unexpected error: %v\"", err)
		}
		// Only block 3 should be returned (block 2's missing blobs is ignored)
		if len(blocks) != 1 {
			t.Fatalf("expected 1 block (blobs ignored), got %d", len(blocks))
		}
		if blocks[0].Number != 3 {
			t.Errorf("expected block 3, got %d", blocks[0].Number)
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
		Number:     100,
		Hash:       "0xoriginal",
		ParentHash: "0xparent",
		ReceivedAt: time.Now().Unix(),
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
		Number:     100,
		Hash:       "0xnew",
		ParentHash: "0xparent",
		ReceivedAt: time.Now().Unix(),
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

// TestBackfillWatermark tests GetBackfillWatermark and SetBackfillWatermark.
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

	t.Run("set and get watermark", func(t *testing.T) {
		if err := repo.SetBackfillWatermark(ctx, 100); err != nil {
			t.Fatalf("failed to set watermark: %v", err)
		}

		watermark, err := repo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if watermark != 100 {
			t.Errorf("expected watermark 100, got %d", watermark)
		}
	})

	t.Run("update watermark", func(t *testing.T) {
		if err := repo.SetBackfillWatermark(ctx, 500); err != nil {
			t.Fatalf("failed to set watermark: %v", err)
		}

		watermark, err := repo.GetBackfillWatermark(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if watermark != 500 {
			t.Errorf("expected watermark 500, got %d", watermark)
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
				Number:     i,
				Hash:       fmt.Sprintf("0xblock_%d", i),
				ParentHash: fmt.Sprintf("0xblock_%d", i-1),
				ReceivedAt: time.Now().Unix(),
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
			Number:     num,
			Hash:       fmt.Sprintf("0xblock_%d", num),
			ParentHash: fmt.Sprintf("0xblock_%d", num-1),
			ReceivedAt: time.Now().Unix(),
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
			Number:     num,
			Hash:       fmt.Sprintf("0xblock_%d", num),
			ParentHash: fmt.Sprintf("0xblock_%d", num-1),
			ReceivedAt: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save block %d: %v", num, err)
		}
	}

	// Set watermark to 5 (blocks 1-5 are verified)
	if err := repo.SetBackfillWatermark(ctx, 5); err != nil {
		t.Fatalf("failed to set watermark: %v", err)
	}

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
	if err := repo.SetBackfillWatermark(ctx, 100); err != nil {
		t.Fatalf("failed to set watermark: %v", err)
	}

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
			Number:     i,
			Hash:       fmt.Sprintf("0xblock_%d", i),
			ParentHash: fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt: time.Now().Unix(),
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
			Number:     i,
			Hash:       fmt.Sprintf("0xblock_%d", i),
			ParentHash: fmt.Sprintf("0xblock_%d", i-1),
			ReceivedAt: time.Now().Unix(),
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
				Number:     i,
				Hash:       fmt.Sprintf("0x%064d", i),
				ParentHash: fmt.Sprintf("0x%064d", i-1),
				ReceivedAt: time.Now().Unix(),
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
			Number:     i,
			Hash:       fmt.Sprintf("0x%064d", i),
			ParentHash: parentHash,
			ReceivedAt: time.Now().Unix(),
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
			Number:     i,
			Hash:       fmt.Sprintf("0x%064d", i),
			ParentHash: fmt.Sprintf("0x%064d", i-1),
			ReceivedAt: time.Now().Unix(),
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
		Number:     2,
		Hash:       "0xnew_block_2",
		ParentHash: fmt.Sprintf("0x%064d", 1), // Correct parent
		ReceivedAt: time.Now().Unix(),
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

// TestMarkPublishComplete_InvalidType tests MarkPublishComplete with invalid type.
func TestMarkPublishComplete_InvalidType(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Save a block
	_, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:     100,
		Hash:       "0xtest",
		ParentHash: "0xparent",
		ReceivedAt: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("failed to save block: %v", err)
	}

	// Try with invalid publish type
	err = repo.MarkPublishComplete(ctx, "0xtest", outbound.PublishType("invalid_type"))
	if err == nil {
		t.Error("expected error for invalid publish type")
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
				Number:     blockNum,
				Hash:       fmt.Sprintf("0x%064d_%d", blockNum, id),
				ParentHash: fmt.Sprintf("0x%064d", blockNum-1),
				ReceivedAt: time.Now().Unix(),
				IsOrphaned: false,
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
			Number:     i,
			Hash:       fmt.Sprintf("0xoriginal_%d", i),
			ParentHash: fmt.Sprintf("0xoriginal_%d", i-1),
			ReceivedAt: time.Now().Unix(),
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
		Number:     105,
		Hash:       "0xnew_105",
		ParentHash: "0xoriginal_100",
		ReceivedAt: time.Now().Unix(),
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
		{Number: 100, Hash: "0xhash100", ParentHash: "0xhash99", ReceivedAt: time.Now().Unix()},
		{Number: 101, Hash: "0xhash101", ParentHash: "0xhash100", ReceivedAt: time.Now().Unix()},
		{Number: 102, Hash: "0xhash102", ParentHash: "0xhash101", ReceivedAt: time.Now().Unix()},
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
		Number:     101,
		Hash:       "0xhash101_prime",
		ParentHash: "0xhash100",
		ReceivedAt: time.Now().Unix(),
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
		{Number: 100, Hash: "0xhash100", ParentHash: "0xhash99", ReceivedAt: time.Now().Unix()},
		{Number: 101, Hash: "0xhash101", ParentHash: "0xhash100", ReceivedAt: time.Now().Unix()},
		{Number: 102, Hash: "0xhash102", ParentHash: "0xhash101", ReceivedAt: time.Now().Unix()},
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
		Number:     102,
		Hash:       "0xhash102_prime",
		ParentHash: "0xhash101_prime", // Parent is MISSING from DB
		ReceivedAt: time.Now().Unix(),
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
			Number:     i,
			Hash:       fmt.Sprintf("0xhash%d", i),
			ParentHash: fmt.Sprintf("0xhash%d", i-1),
			ReceivedAt: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("failed to save setup block %d: %v", i, err)
		}
	}

	// Reorg: 100 -> [GAP 101'] -> 102'
	// HandleReorgAtomic only saves 102', leaving 101' missing.
	newBlock := outbound.BlockState{
		Number:     102,
		Hash:       "0xhash102_prime",
		ParentHash: "0xhash101_prime",
		ReceivedAt: time.Now().Unix(),
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
