//go:build integration

package main

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}

// TestPositionDailyCrystallizer_RunOnce migrates a fresh database (which creates
// position_daily_observation, the procedure, the view and the grants), then wires the
// worker exactly as main() does via setupRunner and runs it end to end against the real
// adapter. It covers the wiring main() cannot be called for: the env-read settling
// window, the repository's CALL, and the INOUT row count coming back.
//
// A freshly migrated database has an empty spine, so the pass writes nothing. That is
// the steady state and must be reported as success, not as a failed tick. The write
// behaviour itself is covered at the SQL level in db/migrator, and the control flow by
// service_test.go.
func TestPositionDailyCrystallizer_RunOnce(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	ctx := context.Background()
	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	// Twice: the tick must be idempotent, because Temporal retries it.
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("second run: %v", err)
	}

	// The wiring reaches the real objects: an observation on a settled day is
	// crystallized by a tick, which an empty-database run alone would not show.
	if _, err := pool.Exec(ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256('cron'::bytea), 1, 1, 'inst-cron', repeat('a', 40), 42, 100, 0, 0,
		        '2026-01-01T10:00:00Z', 'public.proj-0', 0)`); err != nil {
		t.Fatalf("seed the spine: %v", err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("run after seeding: %v", err)
	}
	var qty int
	if err := pool.QueryRow(ctx,
		`SELECT quantity FROM position_daily WHERE position_id = sha256('cron'::bytea)`).Scan(&qty); err != nil {
		t.Fatalf("the tick did not crystallize the seeded day: %v", err)
	}
	if qty != 42 {
		t.Errorf("the crystallized day reads %d, want 42", qty)
	}
}

// The adapter returns the procedure's row count, which is what the worker logs and
// records. Without this the count could be constant and nothing would notice.
func TestPositionDailyCrystallizer_ReportsRowsWritten(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	ctx := context.Background()
	repo := postgres.NewPositionDailyCrystallizerRepository(pool)

	appended, err := repo.Crystallize(ctx, time.Hour)
	if err != nil {
		t.Fatalf("crystallize on an empty spine: %v", err)
	}
	if appended != 0 {
		t.Errorf("an empty spine reported %d rows written, want 0", appended)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256('count'::bytea), 1, 1, 'inst-count', repeat('b', 40), 7, 100, 0, 0,
		        '2026-01-01T10:00:00Z', 'public.proj-0', 0)`); err != nil {
		t.Fatalf("seed the spine: %v", err)
	}
	if appended, err = repo.Crystallize(ctx, time.Hour); err != nil {
		t.Fatalf("crystallize after seeding: %v", err)
	}
	if appended != 1 {
		t.Errorf("one settled day reported %d rows written, want 1", appended)
	}
	if appended, err = repo.Crystallize(ctx, time.Hour); err != nil {
		t.Fatalf("second crystallize: %v", err)
	}
	if appended != 0 {
		t.Errorf("a second pass reported %d rows written, want 0", appended)
	}
}

// A bad settling window must stop the worker at startup rather than let it tick with a
// silently defaulted one.
func TestPositionDailyCrystallizer_RejectsABadSettlingWindow(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	t.Setenv("POSITION_DAILY_SETTLE_AFTER", "not-a-duration")
	if _, err := setupRunner(context.Background(),
		temporal.Dependencies{Pool: pool, Logger: slog.Default()}); err == nil {
		t.Fatal("setupRunner accepted an unparseable POSITION_DAILY_SETTLE_AFTER")
	}
}
