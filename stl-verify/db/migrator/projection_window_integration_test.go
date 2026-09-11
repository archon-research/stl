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
// bounded run prunes chunks and still closes correctly, and that a bounded run CANNOT discover history
// outside its window — which is why bootstrap has to pass NULL.

// windowFixture builds a projection over a 41-day daily-chunked source: 40 days at quantity 100, then a
// closing zero an hour ago. Block numbers ascend with time, as the spine's block-time invariant requires.
func windowFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE wsrc (ts timestamptz NOT NULL, holder text NOT NULL, ik text NOT NULL,
		                   qty numeric NOT NULL, bn bigint NOT NULL)
		  WITH (tsdb.hypertable, tsdb.partition_column='ts', tsdb.chunk_interval='1 day');
		INSERT INTO wsrc SELECT now() - (g||' days')::interval, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
		       'WIN', 100, 5000-g FROM generate_series(1,40) g;
		INSERT INTO wsrc VALUES (now() - interval '1 hour', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'WIN', 0, 9000);
		CREATE VIEW position_win AS
		SELECT 1::integer AS chain_id, NULL::bigint AS protocol_id, ik AS instrument_key, holder AS holder_id,
		       qty AS quantity, 'BORROW'::text AS deal_type, bn AS block_number, 0 AS block_version,
		       0 AS processing_version, ts AS block_timestamp FROM wsrc`); err != nil {
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
// silently never discovered. Pinned so the documented limitation cannot quietly stop being true.
func TestProjectionWindowBoundedFromColdCannotDiscoverHistory(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	appended := materializeWindow(t, ctx, pool, "2 days")
	if appended >= 41 {
		t.Fatalf("bounded run appended %d; it must see only the tail, not all 41 observations", appended)
	}
	if rows := storedWindowRows(t, ctx, pool); rows != int(appended) || rows == 41 {
		t.Errorf("stored %d rows from a bounded cold run (appended %d); the pre-window history must be absent", rows, appended)
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

// The window has to reach the planner as a literal or it prunes nothing, which is the only reason the
// parameter exists. Asserted on the plan: the bounded read must touch strictly fewer chunks.
func TestProjectionWindowPrunesChunks(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	chunks := func(query string) int {
		rows, err := pool.Query(ctx, "EXPLAIN (COSTS OFF) "+query)
		if err != nil {
			t.Fatalf("explain %q: %v", query, err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("scan plan: %v", err)
			}
			if strings.Contains(line, "_hyper_") {
				n++
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("plan rows: %v", err)
		}
		return n
	}
	unbounded := chunks(`SELECT * FROM position_win`)
	bounded := chunks(`SELECT * FROM position_win WHERE block_timestamp > now() - interval '2 days'`)
	if unbounded < 40 {
		t.Fatalf("unbounded plan references %d chunks, want the whole 41-chunk fixture", unbounded)
	}
	if bounded >= unbounded {
		t.Errorf("bounded plan references %d chunks against %d unbounded; the window must prune", bounded, unbounded)
	}
}

// A zero or negative window would silently select nothing at all, so it is refused rather than run.
func TestProjectionWindowRefusesANonPositiveInterval(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	windowFixture(t, ctx, pool)

	for _, w := range []string{"0", "-1 days"} {
		var n int64
		err := pool.QueryRow(ctx,
			`SELECT materialize_position_projection('public.position_win'::regclass, 0, NULL, $1)`, w).Scan(&n)
		if err == nil || !strings.Contains(err.Error(), "p_window must be positive") {
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
