//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-566: materialize_position_projection takes an optional window so a scheduled run re-reads only
// the tail. The window is the whole point of the parameter, so these pin both halves of it: that a
// bounded run still closes the position, and that it CANNOT discover history outside its window —
// which is why bootstrap has to pass NULL.

// windowFixture: a 1-day-chunk hypertable behind a DISTINCT ON (the shape of every real projection),
// 40 daily rows at quantity 100 then a closing zero, one row per UTC day at noon so a window of 36 hours
// always selects exactly the last two rows and three chunks, whatever the time of day the test runs.
func windowFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE wsrc (ts timestamptz NOT NULL, holder text NOT NULL, ik text NOT NULL,
		                   qty numeric NOT NULL, bn bigint NOT NULL);
		SELECT create_hypertable('wsrc', by_range('ts', INTERVAL '1 day'));
		INSERT INTO wsrc SELECT date_trunc('day', now()) + interval '12 hours' - (g||' days')::interval,
		       'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'WIN', 100, 5000-g FROM generate_series(1,40) g;
		INSERT INTO wsrc VALUES (date_trunc('day', now()) + interval '12 hours', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'WIN', 0, 9000);
		CREATE VIEW position_win AS
		SELECT DISTINCT ON (holder, ik, bn)
		       1::integer AS chain_id, NULL::bigint AS protocol_id, ik AS instrument_key, holder AS holder_id,
		       qty AS quantity, 'BORROW'::text AS deal_type, bn AS block_number, 0 AS block_version,
		       0 AS processing_version, ts AS block_timestamp
		FROM wsrc ORDER BY holder, ik, bn, ts;
		CREATE FUNCTION position_win_since(p_since timestamptz) RETURNS SETOF position_win
		LANGUAGE sql STABLE AS $$
		SELECT DISTINCT ON (holder, ik, bn)
		       1::integer, NULL::bigint, ik, holder, qty, 'BORROW'::text, bn, 0, 0, ts
		FROM wsrc WHERE ts > p_since ORDER BY holder, ik, bn, ts $$`); err != nil {
		t.Fatalf("build the window fixture: %v", err)
	}
}

// chunksTouched runs one materialize call and reports how many of the source's chunks it read, from
// the per-relation scan counters. The call and the stats flush share one connection, since the
// counters are the calling backend's.
func chunksTouched(t *testing.T, ctx context.Context, pool *pgxpool.Pool, window any) int {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_stat_reset()`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SELECT materialize_position_projection('public.position_win'::regclass, 0, NULL, $1)`, window); err != nil {
		t.Fatalf("materialize with window %v: %v", window, err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_stat_force_next_flush()`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_all_tables s
		JOIN timescaledb_information.chunks c ON c.chunk_schema = s.schemaname AND c.chunk_name = s.relname
		WHERE c.hypertable_name = 'wsrc' AND s.seq_scan + s.idx_scan > 0`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The window's whole purpose: a bounded run opens only the tail of the source. A bound applied outside
// the view stays at 41, because the predicate cannot push below the DISTINCT ON.
func TestProjectionWindowReadsOnlyTheTailOfTheSource(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	if got := chunksTouched(t, ctx, pool, nil); got != 41 {
		t.Fatalf("an unbounded run touched %d chunks, want all 41 (the fixture is not what this test assumes)", got)
	}
	if got := chunksTouched(t, ctx, pool, "36 hours"); got != 3 {
		t.Errorf("a 36-hour window touched %d chunks, want exactly 3; the bound is not reaching the source's scan", got)
	}
}

// A projection with no bounded source cannot honour a window, and running it unbounded instead would
// hide the very cost the caller asked to avoid, so it is refused by name rather than run.
func TestProjectionWindowRefusesAProjectionWithoutABoundedSource(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `DROP FUNCTION position_win_since(timestamptz)`); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `SELECT materialize_position_projection('public.position_win'::regclass, 0, NULL, interval '2 days')`)
	if err == nil || !strings.Contains(err.Error(), "position_win_since(timestamptz)") {
		t.Fatalf("want a refusal naming the missing bounded source, got %v", err)
	}
	// Unbounded still reads the view, so a projection without the function is not unusable.
	if got := materializeWindow(t, ctx, pool, nil); got != 41 {
		t.Errorf("unbounded run appended %d rows, want 41", got)
	}
}

// The column contract is checked on the view, so the bounded source must be RETURNS SETOF <view>: a
// _since with its own row type could pass a lossy quantity through the check that exists to refuse it.
func TestProjectionWindowRefusesABoundedSourceNotReturningTheView(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
		DROP FUNCTION position_win_since(timestamptz);
		CREATE FUNCTION position_win_since(p_since timestamptz)
		RETURNS TABLE (chain_id integer, protocol_id bigint, instrument_key text, holder_id text,
		               quantity double precision, deal_type text, block_number bigint, block_version integer,
		               processing_version integer, block_timestamp timestamptz)
		LANGUAGE sql STABLE AS $$
		SELECT DISTINCT ON (holder, ik, bn)
		       1::integer, NULL::bigint, ik, holder, qty::double precision, 'BORROW'::text, bn, 0, 0, ts
		FROM wsrc WHERE ts > p_since ORDER BY holder, ik, bn, ts $$`); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `SELECT materialize_position_projection('public.position_win'::regclass, 0, NULL, interval '2 days')`)
	if err == nil || !strings.Contains(err.Error(), "RETURNS SETOF public.position_win") {
		t.Fatalf("want a refusal naming the required return type, got %v", err)
	}
	if rows := storedWindowRows(t, ctx, pool); rows != 0 {
		t.Errorf("stored %d rows through a bounded source of the wrong type, want 0", rows)
	}
}

func materializeWindow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, window any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT materialize_position_projection('public.position_win'::regclass, 0, NULL, $1)`, window).Scan(&n); err != nil {
		t.Fatalf("materialize with window %v: %v", window, err)
	}
	return n
}

func storedWindowRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE instrument_key = 'WIN'`).Scan(&n); err != nil {
		t.Fatalf("count stored rows: %v", err)
	}
	return n
}

// A NULL window is unbounded, which is what bootstrap needs: every observation is discovered.
func TestProjectionWindowNullIsUnbounded(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	if appended := materializeWindow(t, ctx, pool, nil); appended != 41 {
		t.Errorf("unbounded run appended %d, want 41", appended)
	}
	if rows := storedWindowRows(t, ctx, pool); rows != 41 {
		t.Errorf("stored %d rows, want 41", rows)
	}
}

// The trap the parameter's COMMENT documents: bounded from cold, everything outside the window is
// silently never discovered. Exact, not a ceiling, so a wrong column or instant lands on another number.
func TestProjectionWindowBoundedFromColdCannotDiscoverHistory(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	if appended := materializeWindow(t, ctx, pool, "36 hours"); appended != 2 {
		t.Fatalf("bounded cold run appended %d, want exactly 2 (the g=1 row and the closing zero)", appended)
	}
	if rows := storedWindowRows(t, ctx, pool); rows != 2 {
		t.Errorf("stored %d rows from a bounded cold run, want 2; the pre-window history must be absent", rows)
	}
}

// The run table's COMMENT reads a trailing position as "swept and not re-observed", which holds only for
// an unbounded run, so each record carries the window it was called with: NULL for unbounded.
func TestProjectionWindowIsStampedOnTheRunRecord(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	materializeWindow(t, ctx, pool, nil)
	materializeWindow(t, ctx, pool, "2 days")
	var got string
	if err := pool.QueryRow(ctx, `
		SELECT string_agg(CASE WHEN window_interval IS NULL THEN 'NULL'
		                       WHEN window_interval = interval '2 days' THEN '2 days'
		                       ELSE window_interval::text END, ',' ORDER BY created_at)
		  FROM position_projection_run WHERE projection = 'public.position_win'`).Scan(&got); err != nil {
		t.Fatalf("read the run records: %v", err)
	}
	if want := "NULL,2 days"; got != want {
		t.Errorf("run records carry window_interval %q, want %q", got, want)
	}
}

// Steady state: bootstrap unbounded, then bounded. Closure still holds — the batch's LAG falls back to
// prev_qty/opened_before probed from stored position_state — and a bounded re-run appends nothing new.
func TestProjectionWindowBoundedAfterBootstrapClosesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	if appended := materializeWindow(t, ctx, pool, nil); appended != 41 {
		t.Fatalf("bootstrap appended %d, want 41", appended)
	}
	if appended := materializeWindow(t, ctx, pool, "2 days"); appended != 0 {
		t.Errorf("bounded re-run appended %d, want 0", appended)
	}
	var closing string
	if err := pool.QueryRow(ctx, `
		SELECT quantity::text FROM position_state WHERE instrument_key = 'WIN'
		 ORDER BY block_number DESC LIMIT 1`).Scan(&closing); err != nil {
		t.Fatalf("read the closing observation: %v", err)
	}
	if closing != "0" {
		t.Errorf("closing quantity is %s, want 0; the bounded run must still close the position", closing)
	}
	if rows := storedWindowRows(t, ctx, pool); rows != 41 {
		t.Errorf("stored %d rows after the bounded re-run, want 41 unchanged", rows)
	}
}

// The window must reach the planner as a SQL literal. A bound parameter is not constified at plan
// time, so the planner builds paths for every chunk of the real (hypertable) sources and prunes
// nothing -- 21.8 MB and 754 ms of planning on onchain_token_price when it was measured (VEC-672).
//
// Every other test here asserts row-level results, which a bound parameter does not change, so this is
// the only thing that fails on that mutation. It reads pg_proc.prosrc because the property IS the
// generated SQL: the predicate is built by format() at runtime and never exists as a plan this test
// could EXPLAIN. The repo pins other whole-class properties off prosrc the same way
// (20260818_130000_create_position_state.sql).
func TestProjectionWindowIsInterpolatedAsALiteral(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	var src string
	if err := pool.QueryRow(ctx, `
		SELECT prosrc FROM pg_proc WHERE proname = 'materialize_position_projection'`).Scan(&src); err != nil {
		t.Fatalf("read the function body: %v", err)
	}
	// Positive control: this is really the materializer's body, so the checks below cannot pass because
	// prosrc came back empty or from some other function. Anchored on the batch table the window
	// filters, which survives any rewrite of how the predicate itself is built.
	if !strings.Contains(src, "CREATE TEMP TABLE _mpp_src") {
		t.Fatalf("the function body does not build _mpp_src; this test is reading the wrong function")
	}
	if !strings.Contains(src, `format('%s(%L::timestamptz)', v_source, v_since)`) {
		t.Error("the bounded source's instant is not built with format(... %L ...); a bound instant prunes no chunks (AGENTS.md, \"A time window on a hypertable is a SQL literal\")")
	}
	// USING is how a bound parameter would reach the EXECUTE, which is the mutation this guards.
	if strings.Contains(src, "USING p_window") {
		t.Error("the window is passed to EXECUTE with USING, so it reaches the planner as a bind parameter rather than a literal")
	}
}

// A zero or negative window selects nothing, and an infinite one sweeps everything while stamping a
// non-NULL window on the run record; both are refused rather than run.
func TestProjectionWindowRefusesANonPositiveInterval(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	for _, w := range []string{"0", "-1 days", "infinity"} {
		var n int64
		err := pool.QueryRow(ctx,
			`SELECT materialize_position_projection('public.position_win'::regclass, 0, NULL, $1)`, w).Scan(&n)
		if err == nil || !strings.Contains(err.Error(), "p_window must be a finite positive interval") {
			t.Errorf("window %q: want a refusal naming p_window, got n=%d err=%v", w, n, err)
		}
	}
}

// The wrappers call this positionally with three arguments. The old three-argument signature is dropped
// rather than left beside the new one: with both present that call matches the old exactly AND the new
// one by default, which PostgreSQL refuses as ambiguous. Nothing on main calls it that way yet, so
// without this the migration could ship the ambiguity and no test would notice.
func TestProjectionWindowLeavesExactlyOneSignature(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	var signatures int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_proc WHERE proname = 'materialize_position_projection'`).Scan(&signatures); err != nil {
		t.Fatalf("count signatures: %v", err)
	}
	if signatures != 1 {
		t.Errorf("%d signatures of materialize_position_projection exist; a three-argument call is ambiguous unless exactly one remains", signatures)
	}
	// The call the wrappers actually make.
	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT materialize_position_projection('public.position_win'::regclass, 0, NULL)`).Scan(&n); err != nil {
		t.Fatalf("the wrappers' three-argument call must still resolve: %v", err)
	}
	if n != 41 {
		t.Errorf("three-argument call appended %d, want 41 (it must default to an unbounded run)", n)
	}
}
