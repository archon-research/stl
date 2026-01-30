//go:build integration

package backfill_gaps

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	// LargeBenchmarkRowCount is the number of rows to seed for large benchmarks.
	LargeBenchmarkRowCount = 10_000_000

	// LargeBenchmarkBatchInsertSize is how many rows to insert per batch.
	LargeBenchmarkBatchInsertSize = 10_000
)

// setupLargePostgres creates a PostgreSQL container optimized for large data benchmarks.
func setupLargePostgres(b *testing.B) (*pgxpool.Pool, *postgres.BlockStateRepository, func()) {
	b.Helper()
	ctx := context.Background()

	// Use a more performant PostgreSQL configuration for benchmarking
	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "bench",
			"POSTGRES_PASSWORD": "bench",
			"POSTGRES_DB":       "benchdb",
		},
		Cmd: []string{
			"postgres",
			"-c", "shared_buffers=512MB",
			"-c", "work_mem=128MB",
			"-c", "maintenance_work_mem=256MB",
			"-c", "effective_cache_size=1GB",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		b.Fatalf("failed to start postgres container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		b.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		b.Fatalf("failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://bench:bench@%s:%s/benchdb?sslmode=disable", host, port.Port())

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		b.Fatalf("failed to parse config: %v", err)
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		b.Fatalf("failed to connect to database: %v", err)
	}

	// Wait for connection
	for i := 0; i < 60; i++ {
		if err := pool.Ping(ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	repo := postgres.NewBlockStateRepository(pool, nil)

	// Run migrations
	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		b.Fatalf("failed to apply migrations: %v", err)
	}

	cleanup := func() {
		pool.Close()
		container.Terminate(ctx)
	}

	return pool, repo, cleanup
}

// seedLargeDataset inserts rowCount blocks into the database using multi-row batch inserts.
// It creates gaps at specified positions for testing gap detection.
func seedLargeDataset(b *testing.B, pool *pgxpool.Pool, rowCount int64, gapRanges []outbound.BlockRange) {
	b.Helper()
	ctx := context.Background()

	b.Logf("Seeding %d blocks (this may take a few minutes)...", rowCount)
	startTime := time.Now()

	// Build gap set for fast lookup
	gapSet := make(map[int64]bool)
	for _, gap := range gapRanges {
		for i := gap.From; i <= gap.To; i++ {
			gapSet[i] = true
		}
	}

	receivedAt := time.Now().Unix()
	insertedCount := int64(0)

	// Collect blocks to insert in batches
	batch := make([]outbound.BlockState, 0, LargeBenchmarkBatchInsertSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Build multi-row INSERT statement using placeholders
		query := `INSERT INTO block_states (number, hash, parent_hash, received_at, is_orphaned) VALUES %s`

		placeholders := make([]string, 0, len(batch))
		args := make([]interface{}, 0, len(batch)*5)

		for i, block := range batch {
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
				i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
			args = append(args, block.Number, block.Hash, block.ParentHash, block.ReceivedAt, block.IsOrphaned)
		}

		finalQuery := fmt.Sprintf(query, strings.Join(placeholders, ", "))

		_, err := pool.Exec(ctx, finalQuery, args...)
		if err != nil {
			return fmt.Errorf("batch insert failed: %w", err)
		}

		insertedCount += int64(len(batch))
		batch = batch[:0] // Reset batch

		// Log progress every million rows
		if insertedCount%(100_000) == 0 {
			elapsed := time.Since(startTime)
			rate := float64(insertedCount) / elapsed.Seconds()
			b.Logf("  Inserted %d/%d blocks (%.0f rows/sec)...", insertedCount, rowCount, rate)
		}

		return nil
	}

	for i := int64(1); i <= rowCount; i++ {
		// Skip gaps
		if gapSet[i] {
			continue
		}

		batch = append(batch, outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
			ReceivedAt: receivedAt,
			IsOrphaned: false,
		})

		// Flush when batch is full
		if len(batch) >= LargeBenchmarkBatchInsertSize {
			if err := flushBatch(); err != nil {
				b.Fatalf("failed to flush batch: %v", err)
			}
		}
	}

	// Flush remaining
	if err := flushBatch(); err != nil {
		b.Fatalf("failed to flush final batch: %v", err)
	}

	// Analyze table for better query planning
	_, err := pool.Exec(ctx, "ANALYZE block_states")
	if err != nil {
		b.Logf("Warning: failed to analyze table: %v", err)
	}

	elapsed := time.Since(startTime)
	b.Logf("Seeded %d blocks in %v (%.0f rows/sec)", insertedCount, elapsed, float64(insertedCount)/elapsed.Seconds())
}

// largeBenchmarkClient provides block data for the large benchmark.
type largeBenchmarkClient struct {
	maxBlock int64
}

func newLargeBenchmarkClient(maxBlock int64) *largeBenchmarkClient {
	return &largeBenchmarkClient{maxBlock: maxBlock}
}

func (m *largeBenchmarkClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	header := outbound.BlockHeader{
		Number:     fmt.Sprintf("0x%x", blockNum),
		Hash:       fmt.Sprintf("0x%064x", blockNum),
		ParentHash: fmt.Sprintf("0x%064x", blockNum-1),
		Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
	}
	data, _ := json.Marshal(header)
	return data, nil
}

func (m *largeBenchmarkClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *largeBenchmarkClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *largeBenchmarkClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	return json.RawMessage(`[]`), nil
}

func (m *largeBenchmarkClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	return json.RawMessage(`[]`), nil
}

func (m *largeBenchmarkClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	return json.RawMessage(`[]`), nil
}

func (m *largeBenchmarkClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	return json.RawMessage(`[]`), nil
}

func (m *largeBenchmarkClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	return json.RawMessage(`[]`), nil
}

func (m *largeBenchmarkClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	return json.RawMessage(`[]`), nil
}

func (m *largeBenchmarkClient) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	return m.maxBlock, nil
}

func (m *largeBenchmarkClient) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	result := make([]outbound.BlockData, len(blockNums))
	for i, num := range blockNums {
		header := outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", num),
			Hash:       fmt.Sprintf("0x%064x", num),
			ParentHash: fmt.Sprintf("0x%064x", num-1),
			Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
		}
		blockJSON, _ := json.Marshal(header)
		result[i] = outbound.BlockData{
			BlockNumber: num,
			Block:       blockJSON,
			Receipts:    json.RawMessage(`[]`),
			Traces:      json.RawMessage(`[]`),
			Blobs:       json.RawMessage(`[]`),
		}
	}
	return result, nil
}

func (m *largeBenchmarkClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	header := outbound.BlockHeader{
		Number:     fmt.Sprintf("0x%x", blockNum),
		Hash:       hash,
		ParentHash: fmt.Sprintf("0x%064x", blockNum-1),
		Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
	}
	blockJSON, _ := json.Marshal(header)
	return outbound.BlockData{
		BlockNumber: blockNum,
		Block:       blockJSON,
		Receipts:    json.RawMessage(`[]`),
		Traces:      json.RawMessage(`[]`),
		Blobs:       json.RawMessage(`[]`),
	}, nil
}

// BenchmarkLargePostgres_FindGaps benchmarks gap detection on a 10M row table.
func BenchmarkLargePostgres_FindGaps(b *testing.B) {
	db, repo, cleanup := setupLargePostgres(b)
	b.Cleanup(cleanup)

	// Define gap ranges scattered throughout the dataset
	gapRanges := []outbound.BlockRange{
		{From: 1_000_000, To: 1_000_099}, // 100 blocks at 1M
		{From: 5_000_000, To: 5_000_499}, // 500 blocks at 5M
		{From: 9_000_000, To: 9_000_999}, // 1000 blocks at 9M
	}

	seedLargeDataset(b, db, LargeBenchmarkRowCount, gapRanges)

	ctx := context.Background()

	b.Run("FindGaps_FullRange", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			gaps, err := repo.FindGaps(ctx, 1, LargeBenchmarkRowCount)
			if err != nil {
				b.Fatalf("FindGaps failed: %v", err)
			}
			if len(gaps) != len(gapRanges) {
				b.Fatalf("expected %d gaps, got %d", len(gapRanges), len(gaps))
			}
		}
	})

	b.Run("FindGaps_PartialRange_1M", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			gaps, err := repo.FindGaps(ctx, 900_000, 1_100_000)
			if err != nil {
				b.Fatalf("FindGaps failed: %v", err)
			}
			if len(gaps) != 1 {
				b.Fatalf("expected 1 gap, got %d", len(gaps))
			}
		}
	})

	b.Run("GetMinBlockNumber", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			min, err := repo.GetMinBlockNumber(ctx)
			if err != nil {
				b.Fatalf("GetMinBlockNumber failed: %v", err)
			}
			if min != 1 {
				b.Fatalf("expected min=1, got %d", min)
			}
		}
	})

	b.Run("GetMaxBlockNumber", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			max, err := repo.GetMaxBlockNumber(ctx)
			if err != nil {
				b.Fatalf("GetMaxBlockNumber failed: %v", err)
			}
			if max != LargeBenchmarkRowCount {
				b.Fatalf("expected max=%d, got %d", LargeBenchmarkRowCount, max)
			}
		}
	})

	b.Run("GetBlockByNumber_First", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			block, err := repo.GetBlockByNumber(ctx, 1)
			if err != nil {
				b.Fatalf("GetBlockByNumber failed: %v", err)
			}
			if block == nil {
				b.Fatal("expected block, got nil")
			}
		}
	})

	b.Run("GetBlockByNumber_Middle", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			block, err := repo.GetBlockByNumber(ctx, 5_000_501) // Just after a gap
			if err != nil {
				b.Fatalf("GetBlockByNumber failed: %v", err)
			}
			if block == nil {
				b.Fatal("expected block, got nil")
			}
		}
	})

	b.Run("GetBlockByNumber_Last", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			block, err := repo.GetBlockByNumber(ctx, LargeBenchmarkRowCount)
			if err != nil {
				b.Fatalf("GetBlockByNumber failed: %v", err)
			}
			if block == nil {
				b.Fatal("expected block, got nil")
			}
		}
	})

	b.Run("GetLastBlock", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			block, err := repo.GetLastBlock(ctx)
			if err != nil {
				b.Fatalf("GetLastBlock failed: %v", err)
			}
			if block == nil {
				b.Fatal("expected block, got nil")
			}
		}
	})

	b.Run("GetRecentBlocks_100", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			blocks, err := repo.GetRecentBlocks(ctx, 100)
			if err != nil {
				b.Fatalf("GetRecentBlocks failed: %v", err)
			}
			if len(blocks) != 100 {
				b.Fatalf("expected 100 blocks, got %d", len(blocks))
			}
		}
	})
}

// BenchmarkLargePostgres_BackfillService benchmarks the full backfill service on 10M rows.
func BenchmarkLargePostgres_BackfillService(b *testing.B) {
	pool, repo, cleanup := setupLargePostgres(b)
	b.Cleanup(cleanup)

	// Create a small gap to backfill (we don't want to backfill thousands of blocks each iteration)
	gapRanges := []outbound.BlockRange{
		{From: 5_000_000, To: 5_000_049}, // 50 blocks in the middle
	}

	seedLargeDataset(b, pool, LargeBenchmarkRowCount, gapRanges)

	client := newLargeBenchmarkClient(LargeBenchmarkRowCount)
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	config := BackfillConfig{
		ChainID:      1,
		BatchSize:    10,
		PollInterval: time.Hour,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	service, err := NewBackfillService(config, client, repo, cache, eventSink)
	if err != nil {
		b.Fatalf("failed to create backfill service: %v", err)
	}

	ctx := context.Background()

	// Track timing for different phases
	var findGapsDuration time.Duration
	var fillGapsDuration time.Duration
	var mu sync.Mutex

	b.Run("RunOnce_50BlockGap", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			// Clear the gap blocks we filled in previous iteration
			_, err := pool.Exec(ctx, `
				DELETE FROM block_states 
				WHERE number >= $1 AND number <= $2
			`, gapRanges[0].From, gapRanges[0].To)
			if err != nil {
				b.Fatalf("failed to clear gap: %v", err)
			}

			// Time the backfill
			start := time.Now()
			if err := service.RunOnce(ctx); err != nil {
				b.Fatalf("RunOnce failed: %v", err)
			}
			elapsed := time.Since(start)

			mu.Lock()
			fillGapsDuration += elapsed
			mu.Unlock()
		}
	})

	// Benchmark just the gap detection part separately
	b.Run("FindGapsOnly", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			start := time.Now()
			gaps, err := repo.FindGaps(ctx, 1, LargeBenchmarkRowCount)
			elapsed := time.Since(start)
			if err != nil {
				b.Fatalf("FindGaps failed: %v", err)
			}
			_ = gaps

			mu.Lock()
			findGapsDuration += elapsed
			mu.Unlock()
		}
	})
}

// BenchmarkLargePostgres_QueryAnalysis runs EXPLAIN ANALYZE on critical queries.
// This is not a traditional benchmark but provides query planning insights.
func BenchmarkLargePostgres_QueryAnalysis(b *testing.B) {
	pool, repo, cleanup := setupLargePostgres(b)
	b.Cleanup(cleanup)

	gapRanges := []outbound.BlockRange{
		{From: 5_000_000, To: 5_000_099},
	}

	seedLargeDataset(b, pool, LargeBenchmarkRowCount, gapRanges)

	ctx := context.Background()

	// Warm up the cache
	_, _ = repo.FindGaps(ctx, 1, LargeBenchmarkRowCount)

	b.Run("ExplainAnalyze_FindGaps", func(b *testing.B) {
		query := `
			EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
			WITH blocks AS (
				SELECT number
				FROM block_states
				WHERE NOT is_orphaned AND number >= $1 AND number <= $2
				ORDER BY number
			),
			gaps AS (
				SELECT 
					LAG(number) OVER (ORDER BY number) + 1 AS gap_start,
					number - 1 AS gap_end
				FROM blocks
			)
			SELECT gap_start, gap_end
			FROM gaps
			WHERE gap_start IS NOT NULL AND gap_end >= gap_start
			ORDER BY gap_start
		`

		rows, err := pool.Query(ctx, query, 1, LargeBenchmarkRowCount)
		if err != nil {
			b.Fatalf("EXPLAIN failed: %v", err)
		}
		defer rows.Close()

		b.Log("FindGaps EXPLAIN ANALYZE:")
		for rows.Next() {
			var line string
			rows.Scan(&line)
			b.Log("  ", line)
		}
	})

	b.Run("ExplainAnalyze_GetMinMax", func(b *testing.B) {
		queries := []struct {
			name  string
			query string
		}{
			{"GetMinBlockNumber", "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT COALESCE(MIN(number), 0) FROM block_states WHERE NOT is_orphaned"},
			{"GetMaxBlockNumber", "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT COALESCE(MAX(number), 0) FROM block_states WHERE NOT is_orphaned"},
		}

		for _, q := range queries {
			b.Logf("\n%s EXPLAIN ANALYZE:", q.name)
			rows, err := pool.Query(ctx, q.query)
			if err != nil {
				b.Fatalf("EXPLAIN failed: %v", err)
			}
			for rows.Next() {
				var line string
				rows.Scan(&line)
				b.Log("  ", line)
			}
			rows.Close()
		}
	})
}
