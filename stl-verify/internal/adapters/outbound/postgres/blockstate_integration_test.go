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
