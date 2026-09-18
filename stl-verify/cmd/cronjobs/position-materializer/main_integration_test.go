//go:build integration

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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

	// Filler runs, so writer_run.id and build_registry.id cannot coincide: on a fresh database both
	// sequences would hand out 1, and a swap of the two named arguments would satisfy every assertion
	// below. These make the ids differ by construction.
	if _, err := pool.Exec(ctx, `
		INSERT INTO writer_run (build_id, reference_snapshot, reference_effective_at)
		SELECT 0, 'filler', now() FROM generate_series(1, 5)`); err != nil {
		t.Fatalf("seed filler writer runs: %v", err)
	}

	// setupRunner registers a build, which needs a git hash. `go test` does not stamp VCS info, so
	// without this the test fails before it reaches anything it asserts -- in CI as well as locally.
	// Every sibling integration test that registers a build does the same.
	t.Setenv("BUILD_GIT_HASH", "integration-test")

	// The run counters must exist at zero for every configured materializer before any run, or the
	// alerts reading increase() miss a process's first error. Read through the global provider, which
	// is what setupRunner builds its telemetry on.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetMeterProvider(prevMP); _ = mp.Shutdown(context.Background()) })

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()},
		[]string{"materialize_itest_refusing", "materialize_itest"})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}
	seeded := map[string]int64{}
	for _, dp := range testutil.CollectSumDataPoints(t, reader, "position_materializer.projection_runs.total") {
		seeded[testutil.AttrValue(dp, "materializer")+"/"+testutil.AttrValue(dp, "status")] = dp.Value
	}
	for _, key := range []string{"materialize_itest_refusing/error", "materialize_itest/error"} {
		if v, ok := seeded[key]; !ok || v != 0 {
			t.Errorf("before any run, projection_runs %s = %d (present %v); want a seeded 0", key, v, ok)
		}
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
	if runID == nil {
		t.Fatalf("run_id is NULL; want the writer run this process opened")
	}
	// The run must be the newest one, must not be the build id, and must belong to this build.
	var wantRun int64
	var runBuild int
	if err := pool.QueryRow(ctx, `SELECT id, build_id FROM writer_run ORDER BY id DESC LIMIT 1`).Scan(&wantRun, &runBuild); err != nil {
		t.Fatal(err)
	}
	if *runID != wantRun {
		t.Errorf("run_id = %d; want %d, the run this process opened", *runID, wantRun)
	}
	if *runID == int64(buildID) {
		t.Errorf("run_id and build_id are both %d, so this cannot tell the two arguments apart", buildID)
	}
	if runBuild != buildID {
		t.Errorf("writer_run %d names build %d, but the rows carry build %d", *runID, runBuild, buildID)
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

	// A configured wrapper that does not exist, or does not take the provenance arguments by name,
	// stops the worker at startup naming it, rather than failing every tick.
	// Each of these would fail on every tick, so each must stop the worker instead.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION materialize_itest_positional(integer, bigint) RETURNS bigint
			LANGUAGE sql AS $fn$ SELECT 0::bigint $fn$;
		CREATE FUNCTION materialize_itest_build_only(p_build_id integer) RETURNS bigint
			LANGUAGE sql AS $fn$ SELECT 0::bigint $fn$;
		CREATE FUNCTION materialize_itest_overloaded(p_build_id integer DEFAULT 0, p_run_id bigint DEFAULT NULL) RETURNS bigint
			LANGUAGE sql AS $fn$ SELECT 0::bigint $fn$;
		CREATE FUNCTION materialize_itest_overloaded(p_build_id integer, p_run_id bigint, p_extra text DEFAULT '') RETURNS bigint
			LANGUAGE sql AS $fn$ SELECT 0::bigint $fn$;
		CREATE FUNCTION materialize_itest_required_extra(p_build_id integer, p_chain integer, p_run_id bigint DEFAULT NULL) RETURNS bigint
			LANGUAGE sql AS $fn$ SELECT 0::bigint $fn$;
		CREATE FUNCTION materialize_itest_wrong_type(p_build_id text, p_run_id bigint DEFAULT NULL) RETURNS bigint
			LANGUAGE sql AS $fn$ SELECT 0::bigint $fn$;
		CREATE PROCEDURE materialize_itest_procedure(p_build_id integer, p_run_id bigint)
			LANGUAGE sql AS $fn$ SELECT 1 $fn$;
		CREATE FUNCTION materialize_itest_void(p_build_id integer DEFAULT 0, p_run_id bigint DEFAULT NULL) RETURNS void
			LANGUAGE sql AS $fn$ SELECT $fn$;
		CREATE FUNCTION materialize_itest_setof(p_build_id integer DEFAULT 0, p_run_id bigint DEFAULT NULL) RETURNS SETOF bigint
			LANGUAGE sql AS $fn$ SELECT 0::bigint $fn$;
		CREATE FUNCTION materialize_itest_out(p_build_id integer, p_run_id bigint, OUT n bigint, OUT m bigint)
			LANGUAGE sql AS $fn$ SELECT 0::bigint, 0::bigint $fn$;`); err != nil {
		t.Fatalf("create uncallable wrappers: %v", err)
	}
	bad := []string{"materialize_no_such", "materialize_itest_positional", "materialize_itest_build_only",
		"materialize_itest_overloaded", "materialize_itest_required_extra", "materialize_itest_wrong_type",
		"materialize_itest_procedure", "materialize_itest_void", "materialize_itest_setof", "materialize_itest_out"}
	_, err = setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()},
		append([]string{"materialize_itest"}, bad...))
	if err == nil || !strings.Contains(err.Error(), strings.Join(bad, ", ")) {
		t.Errorf("uncallable entries: got %v; want a startup failure naming, in order, %s", err, strings.Join(bad, ", "))
	}
	if err != nil && strings.Contains(err.Error(), "materialize_itest,") {
		t.Errorf("uncallable entries: %v also names materialize_itest, which is callable", err)
	}
}

// The withheld level is read with SQL, so it needs to run against a real position_projection_run:
// the query takes the newest row per projection written by one writer run, and a projection with no
// row under that run since the tick began is absent rather than zero. Rows from another run -- a retired projection, or one
// run by hand -- must not be reported, or their last level is republished every tick.
func TestPositionMaterializer_RefusedByProjection(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `
		INSERT INTO position_projection_run
		    (projection, created_at, build_id, run_id, block_timestamp, rows_emitted, rows_appended, positions_refused)
		VALUES ('public.position_a',       now() - interval '30 minutes', 0, 42, NULL, 10, 10, 0),
		       ('public.position_a',       now() - interval '10 minutes', 0, 42, NULL, 10,  0, 4),
		       ('public.position_a',       now() - interval '5 minutes',  0, 41, NULL, 10,  0, 9),
		       ('public.position_b',       now() - interval '20 minutes', 0, 42, NULL,  5,  5, 0),
		       ('public.position_retired', now() - interval '20 minutes', 0, 41, NULL,  5,  0, 7),
		       ('public.position_stale',   now() - interval '2 hours',    0, 42, NULL,  5,  0, 12)`); err != nil {
		t.Fatalf("seeding runs: %v", err)
	}

	repo := postgres.NewPositionMaterializerRepository(pool, slog.Default())
	got, err := repo.RefusedByProjection(ctx, 42, time.Hour)
	if err != nil {
		t.Fatalf("RefusedByProjection: %v", err)
	}
	if got["public.position_a"] != 4 {
		t.Errorf("position_a = %d, want 4 from its newest row under run 42, not 0 from the older one or 9 from run 41", got["public.position_a"])
	}
	if got["public.position_b"] != 0 {
		t.Errorf("position_b = %d, want 0", got["public.position_b"])
	}
	if _, ok := got["public.position_retired"]; ok {
		t.Error("a projection written only by another run is reported, so its level would never clear")
	}
	if _, ok := got["public.position_stale"]; ok {
		t.Error("a projection whose newest row predates this tick is reported, so a failing projection holds its old level")
	}
	if _, ok := got["public.position_never_run"]; ok {
		t.Error("a projection with no run row is reported; it must be absent so absence stays distinguishable")
	}
}

// withheldPairViewDDL stands in for the view the wrapper migrations create (VEC-402), which this branch
// does not carry. TestPositionProjectionWithheldPairView_MatchesTheContract in db/migrator asserts the
// real view exposes exactly these two columns with these types, so a drift there fails that test rather
// than silently diverging from this stand-in.
const withheldPairViewDDL = `
	CREATE VIEW position_projection_withheld_pair AS
	SELECT * FROM (VALUES %s) AS v(projection, pair)`

// A pair the wrapper cannot key is recorded after the run row is written, so it can never be in
// positions_refused. It is the one withholding class that reaches no alert unless the level adds it here.
func TestPositionMaterializer_RefusedByProjectionAddsUnkeyablePairs(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	// One row per pair per reason: pair 1:10 fails two checks and is still one withheld position, so the
	// count must be DISTINCT. position_b withholds nothing and keeps its own refused count untouched.
	if _, err := pool.Exec(ctx, fmt.Sprintf(withheldPairViewDDL,
		`('public.position_morpho_market'::text, '1:10'::text),
		 ('public.position_morpho_market', '1:10'),
		 ('public.position_morpho_market', '2:10'),
		 ('public.position_morpho_market', '3:11'),
		 ('public.position_other', '7:70')`)); err != nil {
		t.Fatalf("seeding the withheld view: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO position_projection_run
		    (projection, created_at, build_id, run_id, block_timestamp, rows_emitted, rows_appended, positions_refused)
		VALUES ('public.position_morpho_market', now() - interval '5 minutes', 0, 42, NULL, 10, 10, 2),
		       ('public.position_b',             now() - interval '5 minutes', 0, 42, NULL, 10, 10, 1)`); err != nil {
		t.Fatalf("seeding the runs: %v", err)
	}

	repo := postgres.NewPositionMaterializerRepository(pool, slog.Default())
	got, err := repo.RefusedByProjection(ctx, 42, time.Hour)
	if err != nil {
		t.Fatalf("RefusedByProjection: %v", err)
	}
	if got["public.position_morpho_market"] != 5 {
		t.Errorf("position_morpho_market = %d, want 5: 2 from positions_refused plus 3 DISTINCT unkeyable pairs",
			got["public.position_morpho_market"])
	}
	if got["public.position_b"] != 1 {
		t.Errorf("position_b = %d, want 1: a projection withholding nothing keeps its own refused count", got["public.position_b"])
	}
	if _, ok := got["public.position_other"]; ok {
		t.Error("a projection that withholds pairs but completed no run this tick is reported; withheld pairs must not resurrect it")
	}
}

// The withheld pairs are a level, not a reason to resurrect a projection that did not complete: the run
// row is what says the projection ran, and absence is what ViewFailing and ViewNotCompleting read.
func TestPositionMaterializer_RefusedByProjectionKeepsANonRunningProjectionAbsent(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	if _, err := pool.Exec(ctx, fmt.Sprintf(withheldPairViewDDL,
		`('public.position_morpho_market'::text, '1:10'::text)`)); err != nil {
		t.Fatalf("seeding the withheld view: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO position_projection_run
		    (projection, created_at, build_id, run_id, block_timestamp, rows_emitted, rows_appended, positions_refused)
		VALUES ('public.position_morpho_market', now() - interval '2 hours', 0, 42, NULL, 10, 10, 0)`); err != nil {
		t.Fatalf("seeding a stale run: %v", err)
	}

	repo := postgres.NewPositionMaterializerRepository(pool, slog.Default())
	got, err := repo.RefusedByProjection(ctx, 42, time.Hour)
	if err != nil {
		t.Fatalf("RefusedByProjection: %v", err)
	}
	if _, ok := got["public.position_morpho_market"]; ok {
		t.Errorf("a projection whose newest run predates the tick reports %d; want absent, or withheld pairs "+
			"alone would hold a level for a projection that is not completing", got["public.position_morpho_market"])
	}
}

// A projection that completed a run and is withholding nothing must read 0, not go absent: 0 is what
// says the alert is being answered, and absence is reserved for a projection that did not complete.
func TestPositionMaterializer_RefusedByProjectionWithAnEmptyWithheldView(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `
		CREATE VIEW position_projection_withheld_pair AS
		SELECT NULL::text AS projection, NULL::text AS pair WHERE false;
		INSERT INTO position_projection_run
		    (projection, created_at, build_id, run_id, block_timestamp, rows_emitted, rows_appended, positions_refused)
		VALUES ('public.position_morpho_market', now() - interval '5 minutes', 0, 42, NULL, 10, 10, 0)`); err != nil {
		t.Fatalf("seeding an empty withheld view and its run: %v", err)
	}

	repo := postgres.NewPositionMaterializerRepository(pool, slog.Default())
	got, err := repo.RefusedByProjection(ctx, 42, time.Hour)
	if err != nil {
		t.Fatalf("RefusedByProjection: %v", err)
	}
	if level, ok := got["public.position_morpho_market"]; !ok || level != 0 {
		t.Errorf("position_morpho_market = %d (present %t); want 0 and present", level, ok)
	}
}

// The runner ships independently of the wrappers that define the view, so it can start against a
// database that has it and one that does not. A missing view contributes nothing rather than failing
// the read, which would take every other projection's level down with it.
func TestPositionMaterializer_RefusedByProjectionWithoutTheWithheldView(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	var present bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.position_projection_withheld_pair') IS NOT NULL`).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if present {
		t.Fatal("position_projection_withheld_pair exists on this branch; this test no longer covers the missing-view path")
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO position_projection_run
		    (projection, created_at, build_id, run_id, block_timestamp, rows_emitted, rows_appended, positions_refused)
		VALUES ('public.position_morpho_market', now() - interval '5 minutes', 0, 42, NULL, 10, 10, 3)`); err != nil {
		t.Fatalf("seeding the run: %v", err)
	}

	repo := postgres.NewPositionMaterializerRepository(pool, slog.Default())
	got, err := repo.RefusedByProjection(ctx, 42, time.Hour)
	if err != nil {
		t.Fatalf("RefusedByProjection with no withheld view: %v", err)
	}
	if got["public.position_morpho_market"] != 3 {
		t.Errorf("position_morpho_market = %d, want 3 from positions_refused alone", got["public.position_morpho_market"])
	}
}
