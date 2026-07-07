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
// schema, its _watermark rows, the _run functions, and the grants), then wires
// the worker exactly as main() does via setupRunner and runs it end to end. It
// verifies the wiring: the worker lists sources from transformed._watermark and
// invokes every _run_<t>() through the real adapter without error, and a second
// run is still clean. On freshly-migrated tables the raw sources are empty, so
// the runs upsert 0 rows; the incremental upsert's idempotency is proven at the
// SQL-function level (VEC-484) and the RunOnce control flow by service_test.go.
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
		t.Fatalf("second run: %v", err)
	}

	// The migration seeds one watermark row per transformed table. Assert a lower
	// bound plus a known source rather than an exact count, so adding tables in a
	// later bucket does not break this test.
	var watermarks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transformed._watermark`).Scan(&watermarks); err != nil {
		t.Fatalf("counting watermark rows: %v", err)
	}
	if watermarks < 1 {
		t.Fatalf("transformed._watermark rows = %d, want at least 1 (migration not applied?)", watermarks)
	}

	var present bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM transformed._watermark WHERE source = 'morpho_market_state')`,
	).Scan(&present); err != nil {
		t.Fatalf("checking known source: %v", err)
	}
	if !present {
		t.Fatal("expected transformed._watermark to contain source 'morpho_market_state'")
	}
}
