//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-566: materialize_position_projection takes an optional window. These pin both halves of it: a
// bounded run still closes the position, and it cannot discover history outside its window.

// windowFixture: 40 daily rows at quantity 100 then a closing zero, one per UTC day at noon, so a 36-hour
// window selects exactly the last two whatever the time of day. A plain table behind a DISTINCT ON view.
func windowFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE wsrc (ts timestamptz NOT NULL, holder text NOT NULL, ik text NOT NULL,
		                   qty numeric NOT NULL, bn bigint NOT NULL);
		INSERT INTO wsrc SELECT date_trunc('day', now()) + interval '12 hours' - (g||' days')::interval,
		       'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'WIN', 100, 5000-g FROM generate_series(1,40) g;
		INSERT INTO wsrc VALUES (date_trunc('day', now()) + interval '12 hours', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'WIN', 0, 9000);
		CREATE VIEW position_win AS
		SELECT DISTINCT ON (holder, ik, bn)
		       1::integer AS chain_id, NULL::bigint AS protocol_id, ik AS instrument_key, holder AS holder_id,
		       qty AS quantity, 'BORROW'::text AS deal_type, bn AS block_number, 0 AS block_version,
		       0 AS processing_version, ts AS block_timestamp
		FROM wsrc ORDER BY holder, ik, bn, ts`); err != nil {
		t.Fatalf("build the window fixture: %v", err)
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

// The window must reach the planner as a SQL literal (AGENTS.md); row-level results do not change under
// a bind parameter, so this reads pg_proc.prosrc, the only place the property exists.
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
	if !strings.Contains(src, `format('WHERE block_timestamp > %L::timestamptz', v_since)`) {
		t.Error("the window predicate is not built with format(... %L ...); a bound instant prunes no chunks (AGENTS.md, \"A time window on a hypertable is a SQL literal\")")
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
