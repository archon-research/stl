//go:build integration

package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	code := m.Run()

	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

// TestPositionMaterializer_RunOnce migrates a fresh DB (which creates
// position_state, position_classification, and the shared
// materialize_position_projection function), registers a contract-conforming
// projection view, then wires the worker exactly as main() does via setupRunner
// and runs it end to end: the run classifies the position, and a second run is a
// clean no-op. Depends on the position_state spine migration (#625) being on
// main; red until it lands.
func TestPositionMaterializer_RunOnce(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	ctx := context.Background()

	if _, err := pool.Exec(ctx, `CREATE VIEW position_itest AS SELECT
		1::int AS chain_id, 10::bigint AS protocol_id, 'itest-instrument'::text AS instrument_key,
		'aa'::text AS holder_id, 5::numeric AS quantity, 'LOAN'::text AS deal_type_code,
		100::bigint AS block_number, 0::int AS block_version, 0::int AS processing_version,
		'2026-01-01 00:00+00'::timestamptz AS block_timestamp`); err != nil {
		t.Fatalf("create projection view: %v", err)
	}

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()},
		[]string{"position_itest"}, "itest@run")
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	var code, reason string
	if err := pool.QueryRow(ctx, `SELECT deal_type_code, change_reason FROM position_classification
		WHERE position_id = position_id(1, 10, 'itest-instrument', 'aa')`).Scan(&code, &reason); err != nil {
		t.Fatalf("classification not written: %v", err)
	}
	if code != "LOAN" {
		t.Errorf("classification = %q; want LOAN", code)
	}
	if reason != "itest@run" {
		t.Errorf("change_reason = %q; want the runner's provenance itest@run", reason)
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("second run (idempotent rerun): %v", err)
	}

	// A misconfigured view name must fail the run loudly (regclass), not skip.
	badRunner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()},
		[]string{"position_itest", "no_such_view"}, "itest@run")
	if err != nil {
		t.Fatalf("setupRunner(bad): %v", err)
	}
	err = badRunner.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "no_such_view") {
		t.Errorf("bad view: got %v; want a loud failure naming no_such_view", err)
	}
}
