//go:build integration

package backfill_gaps

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// setupPostgres creates a PostgreSQL container and returns a connected repository.
func setupPostgres(t *testing.T) (*postgres.BlockStateRepository, func()) {
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

	repo := postgres.NewBlockStateRepository(db)
	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	cleanup := func() {
		db.Close()
		container.Terminate(ctx)
	}

	return repo, cleanup
}

// saveBlock is a helper to save a block with minimal boilerplate.
func saveBlock(t *testing.T, ctx context.Context, repo *postgres.BlockStateRepository, number int64) {
	t.Helper()
	err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:     number,
		Hash:       fmt.Sprintf("0x%064x", number),
		ParentHash: fmt.Sprintf("0x%064x", number-1),
		ReceivedAt: time.Now().Unix(),
		IsOrphaned: false,
	})
	if err != nil {
		t.Fatalf("failed to save block %d: %v", number, err)
	}
}

func TestFindGaps_NoGaps(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save consecutive blocks 1-10
	for i := int64(1); i <= 10; i++ {
		saveBlock(t, ctx, repo, i)
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 0 {
		t.Errorf("expected no gaps, got %v", gaps)
	}
}

func TestFindGaps_SingleGapInMiddle(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save blocks 1, 2, 3, 7, 8, 9, 10 (gap at 4-6)
	for _, num := range []int64{1, 2, 3, 7, 8, 9, 10} {
		saveBlock(t, ctx, repo, num)
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 4 || gaps[0].To != 6 {
		t.Errorf("expected gap {4, 6}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_MultipleGaps(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save blocks 1, 5, 10, 15 (gaps at 2-4, 6-9, 11-14)
	for _, num := range []int64{1, 5, 10, 15} {
		saveBlock(t, ctx, repo, num)
	}

	gaps, err := repo.FindGaps(ctx, 1, 15)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 3 {
		t.Fatalf("expected 3 gaps, got %d: %v", len(gaps), gaps)
	}

	expected := []outbound.BlockRange{
		{From: 2, To: 4},
		{From: 6, To: 9},
		{From: 11, To: 14},
	}

	for i, want := range expected {
		if gaps[i].From != want.From || gaps[i].To != want.To {
			t.Errorf("gap %d: expected {%d, %d}, got {%d, %d}",
				i, want.From, want.To, gaps[i].From, gaps[i].To)
		}
	}
}

func TestFindGaps_GapAtBeginning(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save blocks 5, 6, 7, 8, 9, 10 (gap at 1-4)
	for i := int64(5); i <= 10; i++ {
		saveBlock(t, ctx, repo, i)
	}

	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 1 || gaps[0].To != 4 {
		t.Errorf("expected gap {1, 4}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_AlternatingMissing(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save blocks 2, 4, 6 (blocks 1, 3, 5 are missing)
	for _, num := range []int64{2, 4, 6} {
		saveBlock(t, ctx, repo, num)
	}

	gaps, err := repo.FindGaps(ctx, 1, 6)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 3 {
		t.Fatalf("expected 3 gaps, got %d: %v", len(gaps), gaps)
	}

	expected := []outbound.BlockRange{
		{From: 1, To: 1},
		{From: 3, To: 3},
		{From: 5, To: 5},
	}

	for i, want := range expected {
		if gaps[i].From != want.From || gaps[i].To != want.To {
			t.Errorf("gap %d: expected {%d, %d}, got {%d, %d}",
				i, want.From, want.To, gaps[i].From, gaps[i].To)
		}
	}
}

func TestFindGaps_OnlyOneBlock(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save only block 50
	saveBlock(t, ctx, repo, 50)

	gaps, err := repo.FindGaps(ctx, 1, 100)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	// Should detect gap at beginning (1-49)
	// Note: FindGaps doesn't detect gap at the end (by design)
	if len(gaps) < 1 {
		t.Fatalf("expected at least 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 1 || gaps[0].To != 49 {
		t.Errorf("expected first gap {1, 49}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_EmptyTable(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Don't save any blocks
	gaps, err := repo.FindGaps(ctx, 1, 10)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	// With empty table and minBlock=1, maxBlock=10, the beginning gap check
	// should detect the entire range is missing
	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap for empty table, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 1 || gaps[0].To != 10 {
		t.Errorf("expected gap {1, 10}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_IgnoresOrphanedBlocks(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save blocks 1, 2, 3, 4, 5
	for i := int64(1); i <= 5; i++ {
		saveBlock(t, ctx, repo, i)
	}

	// Mark block 3 as orphaned
	err := repo.MarkBlockOrphaned(ctx, fmt.Sprintf("0x%064x", 3))
	if err != nil {
		t.Fatalf("failed to mark block orphaned: %v", err)
	}

	gaps, err := repo.FindGaps(ctx, 1, 5)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	// Block 3 is orphaned, so it should appear as a gap
	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap (orphaned block), got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 3 || gaps[0].To != 3 {
		t.Errorf("expected gap {3, 3}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestFindGaps_LargeGap(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save blocks 1 and 1000 (large gap in between)
	saveBlock(t, ctx, repo, 1)
	saveBlock(t, ctx, repo, 1000)

	gaps, err := repo.FindGaps(ctx, 1, 1000)
	if err != nil {
		t.Fatalf("FindGaps failed: %v", err)
	}

	if len(gaps) != 1 {
		t.Fatalf("expected 1 gap, got %d: %v", len(gaps), gaps)
	}

	if gaps[0].From != 2 || gaps[0].To != 999 {
		t.Errorf("expected gap {2, 999}, got {%d, %d}", gaps[0].From, gaps[0].To)
	}
}

func TestGetMinMaxBlockNumber(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Test empty table
	min, err := repo.GetMinBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMinBlockNumber failed: %v", err)
	}
	if min != 0 {
		t.Errorf("expected min=0 for empty table, got %d", min)
	}

	max, err := repo.GetMaxBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMaxBlockNumber failed: %v", err)
	}
	if max != 0 {
		t.Errorf("expected max=0 for empty table, got %d", max)
	}

	// Add some blocks
	for _, num := range []int64{10, 50, 100} {
		saveBlock(t, ctx, repo, num)
	}

	min, err = repo.GetMinBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMinBlockNumber failed: %v", err)
	}
	if min != 10 {
		t.Errorf("expected min=10, got %d", min)
	}

	max, err = repo.GetMaxBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMaxBlockNumber failed: %v", err)
	}
	if max != 100 {
		t.Errorf("expected max=100, got %d", max)
	}
}

func TestGetMinMaxBlockNumber_IgnoresOrphaned(t *testing.T) {
	repo, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Save blocks 1, 50, 100
	for _, num := range []int64{1, 50, 100} {
		saveBlock(t, ctx, repo, num)
	}

	// Mark block 1 and 100 as orphaned
	repo.MarkBlockOrphaned(ctx, fmt.Sprintf("0x%064x", 1))
	repo.MarkBlockOrphaned(ctx, fmt.Sprintf("0x%064x", 100))

	min, err := repo.GetMinBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMinBlockNumber failed: %v", err)
	}
	if min != 50 {
		t.Errorf("expected min=50 (ignoring orphaned), got %d", min)
	}

	max, err := repo.GetMaxBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetMaxBlockNumber failed: %v", err)
	}
	if max != 50 {
		t.Errorf("expected max=50 (ignoring orphaned), got %d", max)
	}
}
