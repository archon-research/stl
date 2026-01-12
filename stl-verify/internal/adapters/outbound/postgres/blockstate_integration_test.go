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
