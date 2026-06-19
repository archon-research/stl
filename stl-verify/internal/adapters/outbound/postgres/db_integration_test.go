//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestOpenPool_RuntimeParamTimeouts verifies the lock_timeout/statement_timeout
// runtime params actually reach Postgres. The connRuntimeParams unit test only
// covers the string building; this proves a) the values are valid GUCs Postgres
// accepts at startup, and b) WorkerDBConfig opts in while DefaultDBConfig does
// not. A malformed value would fail pool creation here rather than ship green.
func TestOpenPool_RuntimeParamTimeouts(t *testing.T) {
	dsn, cleanup := testutil.StartTimescaleDB(t)
	defer cleanup()
	ctx := context.Background()

	worker := WorkerDBConfig(dsn)
	worker.StatementTimeout = 45 * time.Second
	wp, err := OpenPool(ctx, worker)
	if err != nil {
		t.Fatalf("OpenPool(worker): %v", err)
	}
	defer wp.Close()

	var lockTO, stmtTO string
	if err := wp.QueryRow(ctx, "SHOW lock_timeout").Scan(&lockTO); err != nil {
		t.Fatalf("SHOW lock_timeout: %v", err)
	}
	if lockTO != "10s" {
		t.Errorf("WorkerDBConfig lock_timeout = %q, want 10s", lockTO)
	}
	if err := wp.QueryRow(ctx, "SHOW statement_timeout").Scan(&stmtTO); err != nil {
		t.Fatalf("SHOW statement_timeout: %v", err)
	}
	if stmtTO != "45s" {
		t.Errorf("WorkerDBConfig statement_timeout = %q, want 45s", stmtTO)
	}

	dp, err := OpenPool(ctx, DefaultDBConfig(dsn))
	if err != nil {
		t.Fatalf("OpenPool(default): %v", err)
	}
	defer dp.Close()

	var defaultLockTO string
	if err := dp.QueryRow(ctx, "SHOW lock_timeout").Scan(&defaultLockTO); err != nil {
		t.Fatalf("SHOW lock_timeout (default): %v", err)
	}
	if defaultLockTO != "0" {
		t.Errorf("DefaultDBConfig lock_timeout = %q, want 0 (server default / disabled)", defaultLockTO)
	}
}
