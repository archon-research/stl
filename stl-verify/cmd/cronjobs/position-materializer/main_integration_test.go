//go:build integration

package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
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
// position_state and the shared materialize_position_projection function),
// registers a contract-conforming projection view, then wires the worker exactly
// as main() does via setupRunner and runs it end to end: the run appends the
// observation stamped with the resolved build_id, a wrapper's refusal surfaces without starving the
// others, and a second run is a clean
// no-op. Depends on the position_state spine migration (#625) being on main; red
// until it lands.
func TestPositionMaterializer_RunOnce(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	ctx := context.Background()

	if _, err := pool.Exec(ctx, `CREATE VIEW position_itest AS SELECT
		1::int AS chain_id, 10::bigint AS protocol_id, 'itest-instrument'::text AS instrument_key,
		'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text AS holder_id, 5::numeric AS quantity, 'LOAN'::text AS deal_type,
		100::bigint AS block_number, 0::int AS block_version, 0::int AS processing_version,
		'2026-01-01 00:00+00'::timestamptz AS block_timestamp`); err != nil {
		t.Fatalf("create projection view: %v", err)
	}
	// The wrappers the runner calls: one delegates to the shared function like every real projection;
	// the other refuses, the way materialize_aave_lending refuses an unmapped reserve. The refusal must
	// surface through the runner and must not stop the other projection from materializing.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION materialize_itest(p_build_id integer DEFAULT 0, p_max_skew interval DEFAULT '1 day',
		                                  p_run_id bigint DEFAULT NULL) RETURNS bigint LANGUAGE sql AS $fn$
			SELECT materialize_position_projection('position_itest'::regclass, p_build_id, p_run_id);
		$fn$;
		CREATE FUNCTION materialize_itest_refusing(p_build_id integer DEFAULT 0,
		                                           p_run_id bigint DEFAULT NULL) RETURNS bigint LANGUAGE plpgsql AS $fn$
			BEGIN RAISE EXCEPTION 'materialize_itest_refusing: unresolved inputs, refusing to run: reserve 7'; END
		$fn$;`); err != nil {
		t.Fatalf("create materializer wrappers: %v", err)
	}

	// setupRunner registers a build, which needs a git hash. `go test` does not stamp VCS info, so
	// without this the test fails before it reaches anything it asserts -- in CI as well as locally.
	// Every sibling integration test that registers a build does the same.
	t.Setenv("BUILD_GIT_HASH", "integration-test")

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()},
		[]string{"materialize_itest_refusing", "materialize_itest"})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	err = runner.Run(ctx)
	if err == nil {
		t.Fatal("first run: the refusing wrapper's error did not surface")
	}
	if !strings.Contains(err.Error(), "refusing to run: reserve 7") {
		t.Errorf("first run error %q does not carry the wrapper's refusal", err.Error())
	}
	// The observation is appended, stamped with the build the registry resolved for
	// this binary (non-zero: buildregistry inserts the git hash on first sight, and
	// build_registry.id is a SERIAL starting above the reserved 0 = pre-tracking row).
	var quantity int64
	var buildID int
	var runID *int64
	var projection string
	if err := pool.QueryRow(ctx, `SELECT quantity, build_id, run_id, projection FROM position_state
		WHERE position_id = position_id(1, 10, 'itest-instrument', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&quantity, &buildID, &runID, &projection); err != nil {
		t.Fatalf("observation not appended: %v", err)
	}
	if quantity != 5 {
		t.Errorf("quantity = %d; want 5", quantity)
	}
	if buildID <= 0 {
		t.Errorf("build_id = %d; want the registry-resolved build, not the pre-tracking 0", buildID)
	}
	if projection != "public.position_itest" {
		t.Errorf("projection = %q; want public.position_itest", projection)
	}
	// And with the writer run this process opened. The stub wrapper declares a parameter between the
	// two provenance ones, as materialize_maple_loan does, so a positional second argument would have
	// bound the run to a skew tolerance instead of reaching the spine.
	if runID == nil || *runID <= 0 {
		t.Errorf("run_id = %v; want the writer run this process opened", runID)
	} else {
		var openRuns int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM writer_run WHERE id = $1`, *runID).Scan(&openRuns); err != nil {
			t.Fatal(err)
		}
		if openRuns != 1 {
			t.Errorf("run_id %d names no writer_run row", *runID)
		}
	}

	// The rerun re-derives the same observation, so it must append nothing; the refusing wrapper
	// refuses again, and again without starving the other projection.
	err = runner.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "refusing to run: reserve 7") {
		t.Fatalf("second run: got %v; want the wrapper's refusal again", err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state
		WHERE position_id = position_id(1, 10, 'itest-instrument', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("observations after an idempotent rerun = %d; want 1", rows)
	}

	// A misconfigured entry must fail the run loudly as an unknown function, not skip.
	badRunner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()},
		[]string{"materialize_itest", "materialize_no_such"})
	if err != nil {
		t.Fatalf("setupRunner(bad): %v", err)
	}
	err = badRunner.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "materialize_no_such") {
		t.Errorf("bad entry: got %v; want a loud failure naming materialize_no_such", err)
	}
}

// The withheld level is read with SQL, so it needs to run against a real position_projection_run:
// the query takes the newest row per projection, and a projection that has never run must be absent
// rather than reported as zero, or an alert cannot tell a healthy projection from a missing one.
func TestPositionMaterializer_RefusedByProjection(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `
		INSERT INTO position_projection_run
		    (projection, created_at, build_id, block_timestamp, rows_emitted, rows_appended, positions_refused)
		VALUES ('public.position_a', '2026-09-01T00:00:00Z', 0, NULL, 10, 10, 0),
		       ('public.position_a', '2026-09-01T01:00:00Z', 0, NULL, 10,  0, 4),
		       ('public.position_b', '2026-09-01T00:30:00Z', 0, NULL,  5,  5, 0)`); err != nil {
		t.Fatalf("seeding runs: %v", err)
	}

	repo := postgres.NewPositionMaterializerRepository(pool, slog.Default())
	got, err := repo.RefusedByProjection(ctx)
	if err != nil {
		t.Fatalf("RefusedByProjection: %v", err)
	}
	if got["public.position_a"] != 4 {
		t.Errorf("position_a = %d, want 4 from its newest run, not 0 from the older one", got["public.position_a"])
	}
	if got["public.position_b"] != 0 {
		t.Errorf("position_b = %d, want 0", got["public.position_b"])
	}
	if _, ok := got["public.position_never_run"]; ok {
		t.Error("a projection with no run row is reported; it must be absent so absence stays distinguishable")
	}
}
