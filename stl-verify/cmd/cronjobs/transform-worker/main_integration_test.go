//go:build integration

package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestTransformWorker_RunOnce migrates a fresh DB (which creates the transformed
// schema, its _watermark rows, and the _run functions), wires the worker exactly
// as main() does via setupRunner, and runs it twice. On freshly-migrated tables
// the raw sources are empty, so each run upserts 0 rows and succeeds; the second
// run proves the worker is idempotent. It also confirms the migration seeded one
// watermark row per transformed table.
func TestTransformWorker_RunOnce(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("second run (idempotent): %v", err)
	}

	var watermarks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transformed._watermark`).Scan(&watermarks); err != nil {
		t.Fatalf("counting watermark rows: %v", err)
	}
	if watermarks != 13 {
		t.Fatalf("transformed._watermark rows = %d, want 13", watermarks)
	}
}
