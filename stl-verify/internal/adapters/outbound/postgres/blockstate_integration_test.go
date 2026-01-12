//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// setupPostgres creates a PostgreSQL container and returns a connected repository.
func setupPostgres(t *testing.T) (*BlockStateRepository, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
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
	defer cleanup()

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
	defer cleanup()

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
	version, err := repo.HandleReorgAtomic(ctx, reorgEvent, newBlock)
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
	defer cleanup()

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
	defer cleanup()

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
	defer cleanup()

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
	defer cleanup()

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
	defer cleanup()

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
	defer cleanup()

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
	defer cleanup()

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
	defer cleanup()

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

	t.Run("disableBlobs ignores blob status", func(t *testing.T) {
		// Mark block 2's traces complete - now only blobs is missing
		repo.MarkPublishComplete(ctx, "0xblock_2", outbound.PublishTypeTraces)

		blocks, err := repo.GetBlocksWithIncompletePublish(ctx, 10, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
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
	defer cleanup()

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
	version1, err := repo.HandleReorgAtomic(ctx, reorgEvent, newBlock)
	if err != nil {
		t.Fatalf("first HandleReorgAtomic failed: %v", err)
	}

	// Second call with same block hash should be idempotent
	version2, err := repo.HandleReorgAtomic(ctx, reorgEvent, newBlock)
	if err != nil {
		t.Fatalf("second HandleReorgAtomic failed: %v", err)
	}

	if version1 != version2 {
		t.Errorf("expected same version on idempotent call, got v1=%d, v2=%d", version1, version2)
	}
}
