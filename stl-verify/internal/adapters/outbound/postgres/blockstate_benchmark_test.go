//go:build benchmark

package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Default row count for benchmarks. Can be overridden via BENCH_ROW_COUNT env var.
// Use 1M for PR checks, 10M for merge to main.
const defaultRowCount = 10_000_000

// getTotalRows returns the number of rows to use for benchmarks.
// Reads from BENCH_ROW_COUNT environment variable, defaults to defaultRowCount.
func getTotalRows() int64 {
	if v := os.Getenv("BENCH_ROW_COUNT"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return defaultRowCount
}

// =============================================================================
// Test Cases
// =============================================================================

// TestLargeDataset_QueryPerformance tests query performance with configurable row count.
// Set BENCH_ROW_COUNT env var to control dataset size (default: 10M).
func TestLargeDataset_QueryPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	totalRows := getTotalRows()
	t.Logf("Running benchmark with %d rows (set BENCH_ROW_COUNT to override)", totalRows)

	repo, cleanup := setupLargePostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Insert blocks
	bulkInsertBlocksCOPY(t, repo.Pool(), 1, totalRows)

	// Create indexes and analyze
	createIndexes(t, repo.Pool())

	// Verify row count
	var count int64
	if err := repo.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM block_states").Scan(&count); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	t.Logf("Total rows in database: %d", count)

	// Now run performance tests
	t.Run("GetLastBlock", func(t *testing.T) {
		runQueryBenchmark(t, "GetLastBlock", 100, func() error {
			_, err := repo.GetLastBlock(ctx)
			return err
		})
	})

	t.Run("GetBlockByNumber_First", func(t *testing.T) {
		runQueryBenchmark(t, "GetBlockByNumber(1)", 100, func() error {
			_, err := repo.GetBlockByNumber(ctx, 1)
			return err
		})
	})

	t.Run("GetBlockByNumber_Middle", func(t *testing.T) {
		mid := int64(totalRows / 2)
		runQueryBenchmark(t, fmt.Sprintf("GetBlockByNumber(%d)", mid), 100, func() error {
			_, err := repo.GetBlockByNumber(ctx, mid)
			return err
		})
	})

	t.Run("GetBlockByNumber_Last", func(t *testing.T) {
		runQueryBenchmark(t, fmt.Sprintf("GetBlockByNumber(%d)", totalRows), 100, func() error {
			_, err := repo.GetBlockByNumber(ctx, totalRows)
			return err
		})
	})

	t.Run("GetBlockByNumber_Random", func(t *testing.T) {
		runQueryBenchmark(t, "GetBlockByNumber(random)", 100, func() error {
			blockNum := rand.Int63n(totalRows) + 1
			_, err := repo.GetBlockByNumber(ctx, blockNum)
			return err
		})
	})

	t.Run("GetBlockByHash_Random", func(t *testing.T) {
		runQueryBenchmark(t, "GetBlockByHash(random)", 100, func() error {
			blockNum := rand.Int63n(totalRows) + 1
			hash := fmt.Sprintf("0x%064d", blockNum)
			_, err := repo.GetBlockByHash(ctx, hash)
			return err
		})
	})

	t.Run("GetMinBlockNumber", func(t *testing.T) {
		runQueryBenchmark(t, "GetMinBlockNumber", 100, func() error {
			_, err := repo.GetMinBlockNumber(ctx)
			return err
		})
	})

	t.Run("GetMaxBlockNumber", func(t *testing.T) {
		runQueryBenchmark(t, "GetMaxBlockNumber", 100, func() error {
			_, err := repo.GetMaxBlockNumber(ctx)
			return err
		})
	})

	t.Run("GetRecentBlocks_10", func(t *testing.T) {
		runQueryBenchmark(t, "GetRecentBlocks(10)", 100, func() error {
			_, err := repo.GetRecentBlocks(ctx, 10)
			return err
		})
	})

	t.Run("GetRecentBlocks_100", func(t *testing.T) {
		runQueryBenchmark(t, "GetRecentBlocks(100)", 100, func() error {
			_, err := repo.GetRecentBlocks(ctx, 100)
			return err
		})
	})

	t.Run("GetRecentBlocks_1000", func(t *testing.T) {
		runQueryBenchmark(t, "GetRecentBlocks(1000)", 50, func() error {
			_, err := repo.GetRecentBlocks(ctx, 1000)
			return err
		})
	})

	t.Run("GetBlockVersionCount", func(t *testing.T) {
		runQueryBenchmark(t, "GetBlockVersionCount(random)", 100, func() error {
			blockNum := rand.Int63n(totalRows) + 1
			_, err := repo.GetBlockVersionCount(ctx, blockNum)
			return err
		})
	})

	t.Run("GetBackfillWatermark", func(t *testing.T) {
		runQueryBenchmark(t, "GetBackfillWatermark", 100, func() error {
			_, err := repo.GetBackfillWatermark(ctx)
			return err
		})
	})

	t.Run("FindGaps_NoGaps", func(t *testing.T) {
		// Set watermark high to skip most blocks
		repo.SetBackfillWatermark(ctx, totalRows-1000)
		runQueryBenchmark(t, "FindGaps(last 1000 blocks, no gaps)", 50, func() error {
			_, err := repo.FindGaps(ctx, totalRows-1000, totalRows)
			return err
		})
	})

	t.Run("FindGaps_FullScan", func(t *testing.T) {
		// Reset watermark to force full scan
		repo.SetBackfillWatermark(ctx, 0)
		// Full scan of 10M rows is expected to be slow - use relaxed thresholds
		runQueryBenchmarkWithThresholds(t, "FindGaps(full range, no gaps)", 10, 5*time.Second, 10*time.Second, func() error {
			_, err := repo.FindGaps(ctx, 1, totalRows)
			return err
		})
	})

	t.Run("VerifyChainIntegrity_Small", func(t *testing.T) {
		runQueryBenchmark(t, "VerifyChainIntegrity(1000 blocks)", 50, func() error {
			start := rand.Int63n(totalRows-1000) + 1
			return repo.VerifyChainIntegrity(ctx, start, start+1000)
		})
	})

	t.Run("VerifyChainIntegrity_Large", func(t *testing.T) {
		runQueryBenchmark(t, "VerifyChainIntegrity(100000 blocks)", 10, func() error {
			start := rand.Int63n(totalRows-100000) + 1
			return repo.VerifyChainIntegrity(ctx, start, start+100000)
		})
	})

	t.Run("GetBlocksWithIncompletePublish_10", func(t *testing.T) {
		runQueryBenchmark(t, "GetBlocksWithIncompletePublish(10)", 100, func() error {
			_, err := repo.GetBlocksWithIncompletePublish(ctx, 10)
			return err
		})
	})

	t.Run("GetBlocksWithIncompletePublish_100", func(t *testing.T) {
		runQueryBenchmark(t, "GetBlocksWithIncompletePublish(100)", 50, func() error {
			_, err := repo.GetBlocksWithIncompletePublish(ctx, 100)
			return err
		})
	})

	// Test SaveBlock performance (appending to existing data)
	t.Run("SaveBlock_Append", func(t *testing.T) {
		nextBlock := int64(totalRows + 1)
		runQueryBenchmark(t, "SaveBlock(append)", 100, func() error {
			_, err := repo.SaveBlock(ctx, outbound.BlockState{
				Number:     nextBlock,
				Hash:       fmt.Sprintf("0xnew_%d_%d", nextBlock, time.Now().UnixNano()),
				ParentHash: fmt.Sprintf("0x%064d", nextBlock-1),
				ReceivedAt: time.Now().Unix(),
			})
			nextBlock++
			return err
		})
	})

	// Test MarkBlockOrphaned performance
	t.Run("MarkBlockOrphaned", func(t *testing.T) {
		// Create some blocks to orphan
		for i := 0; i < 100; i++ {
			blockNum := totalRows + 1000 + int64(i)
			repo.SaveBlock(ctx, outbound.BlockState{
				Number:     blockNum,
				Hash:       fmt.Sprintf("0xorphan_%d", blockNum),
				ParentHash: fmt.Sprintf("0x%064d", blockNum-1),
				ReceivedAt: time.Now().Unix(),
			})
		}

		orphanIdx := 0
		runQueryBenchmark(t, "MarkBlockOrphaned", 100, func() error {
			hash := fmt.Sprintf("0xorphan_%d", totalRows+1000+int64(orphanIdx))
			orphanIdx++
			return repo.MarkBlockOrphaned(ctx, hash)
		})
	})
}

// TestExplainAnalyze outputs EXPLAIN ANALYZE for key queries to understand query plans.
func TestExplainAnalyze(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EXPLAIN ANALYZE test in short mode")
	}

	repo, cleanup := setupLargePostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Insert enough data to see realistic query plans
	const explainRows = 100_000
	bulkInsertBlocksCOPY(t, repo.Pool(), 1, explainRows)
	createIndexes(t, repo.Pool())

	queries := []struct {
		name  string
		query string
	}{
		{
			name: "GetLastBlock",
			query: `EXPLAIN ANALYZE
				SELECT number, hash, parent_hash, received_at, is_orphaned, version,
					block_published
				FROM block_states
				WHERE NOT is_orphaned
				ORDER BY number DESC
				LIMIT 1`,
		},
		{
			name: "GetBlockByNumber",
			query: `EXPLAIN ANALYZE
				SELECT number, hash, parent_hash, received_at, is_orphaned, version,
					block_published
				FROM block_states
				WHERE number = 50000 AND NOT is_orphaned`,
		},
		{
			name: "GetBlockByHash",
			query: fmt.Sprintf(`EXPLAIN ANALYZE
				SELECT number, hash, parent_hash, received_at, is_orphaned, version,
					block_published
				FROM block_states
				WHERE hash = '0x%064d'`, 50000),
		},
		{
			name: "GetMinBlockNumber",
			query: `EXPLAIN ANALYZE
				SELECT COALESCE(MIN(number), 0) 
				FROM block_states 
				WHERE NOT is_orphaned`,
		},
		{
			name: "GetMaxBlockNumber",
			query: `EXPLAIN ANALYZE
				SELECT COALESCE(MAX(number), 0) 
				FROM block_states 
				WHERE NOT is_orphaned`,
		},
		{
			name: "GetRecentBlocks",
			query: `EXPLAIN ANALYZE
				SELECT number, hash, parent_hash, received_at, is_orphaned, version,
					block_published
				FROM block_states
				WHERE NOT is_orphaned
				ORDER BY number DESC
				LIMIT 100`,
		},
		{
			name: "FindGaps",
			query: `EXPLAIN ANALYZE
				WITH blocks AS (
					SELECT number
					FROM block_states
					WHERE NOT is_orphaned AND number >= 1 AND number <= 100000
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
				ORDER BY gap_start`,
		},
		{
			name: "VerifyChainIntegrity",
			query: `EXPLAIN ANALYZE
				WITH ordered_blocks AS (
					SELECT number, hash, parent_hash,
						LAG(hash) OVER (ORDER BY number) as prev_hash,
						LAG(number) OVER (ORDER BY number) as prev_number
					FROM block_states
					WHERE NOT is_orphaned AND number >= 1 AND number <= 10000
				)
				SELECT number, hash, parent_hash, prev_hash, prev_number
				FROM ordered_blocks
				WHERE prev_hash IS NOT NULL
					AND prev_number = number - 1
					AND parent_hash != prev_hash
				LIMIT 1`,
		},
		{
			name: "GetBlocksWithIncompletePublish",
			query: `EXPLAIN ANALYZE
				SELECT number, hash, parent_hash, received_at, is_orphaned, version,
					block_published
				FROM block_states
				WHERE NOT is_orphaned
					AND NOT block_published
				ORDER BY number ASC
				LIMIT 100`,
		},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			rows, err := repo.Pool().Query(ctx, q.query)
			if err != nil {
				t.Fatalf("failed to run EXPLAIN ANALYZE: %v", err)
			}
			defer rows.Close()

			t.Logf("\n=== %s ===", q.name)
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					t.Fatalf("failed to scan row: %v", err)
				}
				t.Log(line)
			}
		})
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

// setupLargePostgres creates a PostgreSQL container with optimized settings for benchmarks.
func setupLargePostgres(tb testing.TB) (*BlockStateRepository, func()) {
	tb.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		// Optimize PostgreSQL for bulk loading
		Cmd: []string{
			"postgres",
			"-c", "shared_buffers=256MB",
			"-c", "work_mem=64MB",
			"-c", "maintenance_work_mem=256MB",
			"-c", "synchronous_commit=off",
			"-c", "wal_level=minimal",
			"-c", "max_wal_senders=0",
			"-c", "checkpoint_timeout=1h",
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
		tb.Fatalf("failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		tb.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		tb.Fatalf("failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		tb.Fatalf("failed to parse config: %v", err)
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		tb.Fatalf("failed to connect to database: %v", err)
	}

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := pool.Ping(ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Run migrations
	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		tb.Fatalf("failed to apply migrations: %v", err)
	}

	repo := NewBlockStateRepository(pool, 1, nil)

	cleanup := func() {
		pool.Close()
		container.Terminate(ctx)
	}

	return repo, cleanup
}

// bulkInsertBlocksCOPY uses PostgreSQL COPY protocol for maximum insert performance.
// This is 10-100x faster than INSERT statements.
func bulkInsertBlocksCOPY(tb testing.TB, pool *pgxpool.Pool, startBlock, count int64) {
	tb.Helper()
	ctx := context.Background()

	tb.Logf("Bulk inserting %d blocks using COPY protocol starting from %d...", count, startBlock)
	startTime := time.Now()

	// Get a connection from the pool for COPY support
	conn, err := pool.Acquire(ctx)
	if err != nil {
		tb.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Release()

	// Use pgx's CopyFrom
	if err := bulkInsertWithPgxCopy(ctx, tb, conn.Conn(), startBlock, count); err != nil {
		tb.Fatalf("COPY failed: %v", err)
	}

	duration := time.Since(startTime)
	rate := float64(count) / duration.Seconds()
	tb.Logf("Inserted %d blocks in %v (%.0f rows/sec)", count, duration, rate)
}

// bulkInsertWithPgxCopy uses pgx's CopyFrom for maximum performance.
func bulkInsertWithPgxCopy(ctx context.Context, tb testing.TB, conn *pgx.Conn, startBlock, count int64) error {
	now := time.Now().Unix()
	const batchSize = 500_000 // Process in batches to show progress

	columns := []string{
		"number", "hash", "parent_hash", "received_at", "is_orphaned", "version",
		"block_published",
	}

	for batchStart := int64(0); batchStart < count; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > count {
			batchEnd = count
		}
		batchCount := batchEnd - batchStart

		// Create row source for this batch
		rows := make([][]any, batchCount)
		for i := int64(0); i < batchCount; i++ {
			blockNum := startBlock + batchStart + i
			idx := batchStart + i

			rows[i] = []any{
				blockNum,                           // number
				fmt.Sprintf("0x%064d", blockNum),   // hash
				fmt.Sprintf("0x%064d", blockNum-1), // parent_hash
				now,                                // received_at
				false,                              // is_orphaned
				0,                                  // version
				idx%10 < 8,                         // block_published (80%)
			}
		}

		copyCount, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{"block_states"},
			columns,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("CopyFrom failed at batch %d: %w", batchStart, err)
		}

		if copyCount != batchCount {
			return fmt.Errorf("expected to copy %d rows, got %d", batchCount, copyCount)
		}

		tb.Logf("  Inserted %d/%d blocks (%.1f%%)...",
			batchEnd, count, float64(batchEnd)/float64(count)*100)
	}

	return nil
}

// createIndexes ensures indexes are created and analyzed for optimal query performance.
func createIndexes(tb testing.TB, pool *pgxpool.Pool) {
	tb.Helper()
	ctx := context.Background()

	tb.Log("Creating additional indexes and analyzing tables...")
	startTime := time.Now()

	// The schema should already have indexes, but let's ensure they exist
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_block_states_number_not_orphaned 
		 ON block_states(number) WHERE NOT is_orphaned`,
		`CREATE INDEX IF NOT EXISTS idx_block_states_incomplete_publish
		 ON block_states(number)
		 WHERE NOT is_orphaned AND NOT block_published`,
	}

	for _, idx := range indexes {
		if _, err := pool.Exec(ctx, idx); err != nil {
			tb.Logf("Warning: failed to create index: %v", err)
		}
	}

	// Analyze the table for query planner optimization
	if _, err := pool.Exec(ctx, "ANALYZE block_states"); err != nil {
		tb.Fatalf("failed to analyze table: %v", err)
	}

	tb.Logf("Indexes created and analyzed in %v", time.Since(startTime))
}

// runQueryBenchmark runs a query function multiple times and reports timing statistics.
func runQueryBenchmark(t *testing.T, name string, iterations int, queryFn func() error) {
	runQueryBenchmarkWithThresholds(t, name, iterations, 100*time.Millisecond, 500*time.Millisecond, queryFn)
}

func runQueryBenchmarkWithThresholds(t *testing.T, name string, iterations int, avgThreshold, p99Threshold time.Duration, queryFn func() error) {
	t.Helper()

	// Warm up
	for i := 0; i < 5; i++ {
		if err := queryFn(); err != nil {
			t.Fatalf("warmup failed: %v", err)
		}
	}

	// Measure
	times := make([]time.Duration, iterations)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := queryFn(); err != nil {
			t.Fatalf("query failed on iteration %d: %v", i, err)
		}
		times[i] = time.Since(start)
	}

	// Calculate statistics
	var total time.Duration
	min := times[0]
	max := times[0]

	for _, d := range times {
		total += d
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}

	avg := total / time.Duration(iterations)

	// Calculate p50 and p99
	sortedTimes := make([]time.Duration, len(times))
	copy(sortedTimes, times)
	sortDurations(sortedTimes)

	p50 := sortedTimes[len(sortedTimes)*50/100]
	p99 := sortedTimes[len(sortedTimes)*99/100]

	t.Logf("%s: avg=%v, min=%v, max=%v, p50=%v, p99=%v (n=%d)",
		name, avg, min, max, p50, p99, iterations)

	// Performance assertions - queries should be fast
	if avg > avgThreshold {
		t.Errorf("SLOW QUERY: %s average time %v exceeds %v threshold", name, avg, avgThreshold)
	}
	if p99 > p99Threshold {
		t.Errorf("SLOW QUERY: %s p99 time %v exceeds %v threshold", name, p99, p99Threshold)
	}
}

// sortDurations sorts a slice of durations in ascending order.
func sortDurations(d []time.Duration) {
	for i := 0; i < len(d); i++ {
		for j := i + 1; j < len(d); j++ {
			if d[j] < d[i] {
				d[i], d[j] = d[j], d[i]
			}
		}
	}
}
