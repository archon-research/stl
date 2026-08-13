//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestOpenPool_TimeoutsApplied verifies the lock_timeout/statement_timeout GUCs
// are actually applied to pooled connections via the AfterConnect SET. The
// timeoutGUCs unit test only covers the string building; this proves a) the
// SET statements are valid and applied to every connection, and b) WorkerDBConfig
// opts in while DefaultDBConfig does not. A malformed GUC would fail the
// AfterConnect hook, surfaced here by OpenPool's connectivity Ping (which
// acquires a connection and runs the hook synchronously) rather than ship green.
// pgxpool's background warm-up ignores AfterConnect errors, so the Ping is what
// guards this test.
//
// Note: this connects directly to Postgres, so it does not exercise the pooler
// that rejected the old startup-parameter form; TestBuildPoolConfig_TimeoutsNotStartupParams
// guards that the timeouts never ride the startup packet again.
func TestOpenPool_TimeoutsApplied(t *testing.T) {
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
