//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getMigrationsPath() string {
	_, filename, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(filename)
	return filepath.Join(testDir, "..", "migrations")
}

func setupPostgres(ctx context.Context, t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn, containerCleanup := testutil.StartTimescaleDB(t)
	pool := testutil.ConnectPool(t, dsn)
	cleanup := func() {
		pool.Close()
		containerCleanup()
	}
	return pool, cleanup
}

func TestMigrator_ApplyAll(t *testing.T) {
	ctx := context.Background()

	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'migrations'
		)`).Scan(&exists)
	if err != nil {
		t.Fatalf("failed to check migrations table: %v", err)
	}
	if !exists {
		t.Fatal("migrations table does not exist")
	}

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM migrations").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count migrations: %v", err)
	}
	if count == 0 {
		t.Fatal("no migrations were applied")
	}
	t.Logf("✓ Applied %d migrations", count)

	migrations, err := m.ListApplied(ctx)
	if err != nil {
		t.Fatalf("failed to list migrations: %v", err)
	}
	t.Logf("✓ Migrations applied:")
	for _, migration := range migrations {
		t.Logf("  - %s", migration)
	}

	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("second ApplyAll failed: %v", err)
	}
	t.Logf("✓ Migrations are idempotent")

	var newCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM migrations").Scan(&newCount)
	if err != nil {
		t.Fatalf("failed to count migrations after second run: %v", err)
	}
	if newCount != count {
		t.Fatalf("migration count changed: expected %d, got %d", count, newCount)
	}
	t.Logf("✓ No duplicate migrations applied")
}

func TestMigrator_VerifySchema(t *testing.T) {
	ctx := context.Background()

	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	expectedTables := []string{
		"migrations",
		"block_states",
		"reorg_events",
		"backfill_watermark",
	}

	for _, tableName := range expectedTables {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = $1
			)`, tableName).Scan(&exists)
		if err != nil {
			t.Fatalf("failed to check table %s: %v", tableName, err)
		}
		if !exists {
			t.Errorf("expected table %s does not exist", tableName)
		} else {
			t.Logf("✓ Table %s exists", tableName)
		}
	}
}

func TestMigrator_ChecksumVerification(t *testing.T) {
	ctx := context.Background()

	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	_, err := pool.Exec(ctx, `
		CREATE TABLE migrations (
			id SERIAL PRIMARY KEY,
			filename TEXT NOT NULL UNIQUE,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			checksum TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create migrations table: %v", err)
	}

	tempDir := t.TempDir()
	testMigrationFile := filepath.Join(tempDir, "20260122_150000_test.sql")

	originalContent := `
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT
);

INSERT INTO migrations (filename) 
VALUES ('20260122_150000_test.sql')
ON CONFLICT (filename) DO NOTHING;
`

	if err := os.WriteFile(testMigrationFile, []byte(originalContent), 0644); err != nil {
		t.Fatalf("failed to write test migration: %v", err)
	}

	m := migrator.New(pool, tempDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply initial migrations: %v", err)
	}
	t.Logf("✓ Initial migration applied")

	modifiedContent := `
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT
);

INSERT INTO migrations (filename) 
VALUES ('20260122_150000_test.sql')
ON CONFLICT (filename) DO NOTHING;
`

	if err := os.WriteFile(testMigrationFile, []byte(modifiedContent), 0644); err != nil {
		t.Fatalf("failed to modify test migration: %v", err)
	}
	t.Logf("✓ Migration file modified")

	err = m.ApplyAll(ctx)
	if err == nil {
		t.Fatal("expected error for modified migration, got nil")
	}

	if !strings.Contains(err.Error(), "checksum verification failed") &&
		!strings.Contains(err.Error(), "migration has been modified") {
		t.Fatalf("expected checksum error, got: %v", err)
	}

	t.Logf("✓ Checksum verification correctly detected modification: %v", err)
}

// TestMigrations_AuditabilityOnSelfHosted verifies that the full migration set
// (including auditability migrations) applies cleanly on self-hosted TimescaleDB
// (not TigerData Cloud). This catches Cloud-only API usage like
// timescaledb.enable_columnstore that doesn't exist on self-hosted.
func TestMigrations_AuditabilityOnSelfHosted(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed on self-hosted TimescaleDB: %v", err)
	}

	// Verify build_registry exists and has the pre-tracking seed row.
	var gitHash string
	err := pool.QueryRow(ctx, `SELECT git_hash FROM build_registry WHERE id = 0`).Scan(&gitHash)
	if err != nil {
		t.Fatalf("build_registry seed row missing: %v", err)
	}
	if gitHash != "pre-tracking" {
		t.Fatalf("expected git_hash 'pre-tracking', got %q", gitHash)
	}

	// Verify processing_version + build_id columns exist on a sample of state tables.
	tables := []string{
		"borrower", "morpho_market_state", "onchain_token_price",
		"anchorage_package_snapshot", "offchain_token_price",
	}
	for _, table := range tables {
		var hasPV, hasBID bool
		err := pool.QueryRow(ctx, `
			SELECT
				EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = 'processing_version'),
				EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = 'build_id')
		`, table).Scan(&hasPV, &hasBID)
		if err != nil {
			t.Fatalf("column check for %s: %v", table, err)
		}
		if !hasPV {
			t.Errorf("%s missing processing_version column", table)
		}
		if !hasBID {
			t.Errorf("%s missing build_id column", table)
		}
	}

	// Verify triggers exist on state tables.
	triggerTables := []string{
		"borrower", "morpho_market_state", "onchain_token_price",
		"anchorage_package_snapshot", "offchain_token_price",
	}
	for _, table := range triggerTables {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.triggers
				WHERE event_object_table = $1
				AND trigger_name = 'trigger_assign_processing_version'
			)
		`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("trigger check for %s: %v", table, err)
		}
		if !exists {
			t.Errorf("%s missing trigger_assign_processing_version", table)
		}
	}

	// Verify the trigger works: insert a row and check processing_version was assigned.
	_, err = pool.Exec(ctx, `
		INSERT INTO build_registry (git_hash) VALUES ('test-hash-abc123')
	`)
	if err != nil {
		t.Fatalf("inserting test build: %v", err)
	}

	var buildID int
	err = pool.QueryRow(ctx, `SELECT id FROM build_registry WHERE git_hash = 'test-hash-abc123'`).Scan(&buildID)
	if err != nil {
		t.Fatalf("looking up test build: %v", err)
	}

	// Insert into offchain_token_price — no FK constraints on this table
	// (incompatible with distributed hypertables), so we can use a fake token_id.
	_, err = pool.Exec(ctx, `
		INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, build_id)
		VALUES (999, 1, NOW(), 100.0, $1)
		ON CONFLICT (token_id, source_id, processing_version, timestamp) DO NOTHING
	`, buildID)
	if err != nil {
		t.Fatalf("inserting price: %v", err)
	}

	var pv int
	err = pool.QueryRow(ctx, `
		SELECT processing_version FROM offchain_token_price WHERE token_id = 999
	`).Scan(&pv)
	if err != nil {
		t.Fatalf("reading processing_version: %v", err)
	}
	if pv != 0 {
		t.Fatalf("expected processing_version 0, got %d", pv)
	}
	t.Logf("trigger assigned processing_version = %d", pv)
}

// TestProcessingVersion_ConcurrentBuilds verifies that two concurrent inserts for the
// same natural key with different build_ids produce two rows with processing_version 0
// and 1 respectively.
//
// Without the advisory lock in the trigger, tx2's BEFORE INSERT trigger would read
// MAX(processing_version) = -1 (tx1's uncommitted row is invisible under READ COMMITTED),
// assign processing_version = 0, and then block on the unique index. After tx1 commits,
// tx2's ON CONFLICT DO NOTHING would drop the row — silently losing data. The advisory
// lock serializes trigger execution so tx2 sees tx1's committed row before computing MAX.
//
// The test uses two explicit connections to guarantee truly concurrent transactions.
func TestProcessingVersion_ConcurrentBuilds(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	// Register two builds.
	var build1, build2 int
	err := pool.QueryRow(ctx, `INSERT INTO build_registry (git_hash) VALUES ('build-race-1') RETURNING id`).Scan(&build1)
	if err != nil {
		t.Fatalf("inserting build 1: %v", err)
	}
	err = pool.QueryRow(ctx, `INSERT INTO build_registry (git_hash) VALUES ('build-race-2') RETURNING id`).Scan(&build2)
	if err != nil {
		t.Fatalf("inserting build 2: %v", err)
	}

	// Fixed timestamp for both inserts (same natural key).
	fixedTS := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Acquire two separate connections from the pool to guarantee independent
	// transaction visibility (same connection cannot have two open transactions).
	conn1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn1: %v", err)
	}
	defer conn1.Release()

	conn2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn2: %v", err)
	}
	defer conn2.Release()

	// tx1: begin, insert, hold open.
	tx1, err := conn1.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)

	_, err = tx1.Exec(ctx, `
		INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, build_id)
		VALUES (888, 1, $1, 100.0, $2)
		ON CONFLICT (token_id, source_id, processing_version, timestamp) DO NOTHING`,
		fixedTS, build1)
	if err != nil {
		t.Fatalf("tx1 insert: %v", err)
	}
	// tx1 is now open with an uncommitted row (processing_version=0).
	// Under READ COMMITTED, this row is invisible to tx2.

	// tx2: insert same natural key with different build_id in a goroutine.
	// Without advisory lock: trigger reads MAX=-1, assigns processing_version=0,
	//   insert succeeds immediately (different connection, no blocking).
	// With advisory lock: trigger blocks on the lock until tx1 commits.
	errCh := make(chan error, 1)
	go func() {
		tx2, err := conn2.Begin(ctx)
		if err != nil {
			errCh <- fmt.Errorf("begin tx2: %w", err)
			return
		}
		defer tx2.Rollback(ctx)

		_, err = tx2.Exec(ctx, `
			INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, build_id)
			VALUES (888, 1, $1, 200.0, $2)
			ON CONFLICT (token_id, source_id, processing_version, timestamp) DO NOTHING`,
			fixedTS, build2)
		if err != nil {
			errCh <- fmt.Errorf("tx2 insert: %w", err)
			return
		}
		errCh <- tx2.Commit(ctx)
	}()

	// Wait for tx2 to enter the trigger. Without advisory lock it completes
	// in ~100us. With advisory lock it blocks. 200ms guarantees tx2 has entered
	// the trigger before we commit tx1.
	time.Sleep(200 * time.Millisecond)

	// Commit tx1 — if advisory lock is held, this unblocks tx2.
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}

	// Wait for tx2 to finish.
	if err := <-errCh; err != nil {
		t.Fatalf("tx2 failed: %v", err)
	}

	// Release connections before querying results.
	conn1.Release()
	conn2.Release()

	// Count rows for this natural key.
	var count int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM offchain_token_price
		WHERE token_id = 888 AND source_id = 1 AND timestamp = $1`, fixedTS).Scan(&count)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 rows (processing_version 0 and 1), got %d — "+
			"the race condition caused one row to be silently dropped", count)
	}

	// Verify distinct processing_versions.
	rows, err := pool.Query(ctx, `
		SELECT processing_version, build_id FROM offchain_token_price
		WHERE token_id = 888 AND source_id = 1 AND timestamp = $1
		ORDER BY processing_version`, fixedTS)
	if err != nil {
		t.Fatalf("querying versions: %v", err)
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var pv, bid int
		if err := rows.Scan(&pv, &bid); err != nil {
			t.Fatalf("scanning row: %v", err)
		}
		versions = append(versions, pv)
		t.Logf("processing_version=%d, build_id=%d", pv, bid)
	}

	if len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Fatalf("expected processing_versions [0, 1], got %v", versions)
	}
}

// BenchmarkProcessingVersionTrigger_WithLock measures per-insert overhead of the
// processing_version trigger (including advisory lock). Each iteration inserts one
// row with a unique natural key, so the lock is always uncontended — this measures
// the cost added to normal (non-reprocessing) operations.
func BenchmarkProcessingVersionTrigger_WithLock(b *testing.B) {
	ctx := context.Background()
	dsn, containerCleanup := testutil.StartTimescaleDBForMain()
	pool := testutil.ConnectPoolForMain(dsn)
	defer func() {
		pool.Close()
		containerCleanup()
	}()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		b.Fatalf("migrations failed: %v", err)
	}

	var buildID int
	err := pool.QueryRow(ctx, `INSERT INTO build_registry (git_hash) VALUES ('bench-build') RETURNING id`).Scan(&buildID)
	if err != nil {
		b.Fatalf("inserting build: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each iteration uses a unique token_id so the advisory lock is uncontended.
		ts := time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC)
		_, err := pool.Exec(ctx, `
			INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, build_id)
			VALUES ($1, 1, $2, 100.0, $3)
			ON CONFLICT (token_id, source_id, processing_version, timestamp) DO NOTHING`,
			int64(i+10000), ts, buildID)
		if err != nil {
			b.Fatalf("insert %d: %v", i, err)
		}
	}
}
