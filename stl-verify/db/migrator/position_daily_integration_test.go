//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const positionDailyMigration = "20260824_120000_create_position_daily.sql"

// positionDailyEveryDate reads every date the fixtures use through the general read.
const positionDailyEveryDate = `position_daily_between('2000-01-01', '2100-01-01')`

// positionDailyFixture is one migrated database plus the seeding and reading each case needs.
type positionDailyFixture struct {
	ctx  context.Context
	t    *testing.T
	pool *pgxpool.Pool
}

func newPositionDailyFixture(t *testing.T) *positionDailyFixture {
	t.Helper()
	ctx := context.Background()
	// setupMigratedPostgres also disables the scheduled jobs: position_state registers a 2-day
	// compression policy and these fixtures are stamped 2026-01..12, immediately eligible, so
	// policy_compression could otherwise fire mid-test and take AccessExclusiveLock per chunk.
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
	return &positionDailyFixture{ctx: ctx, t: t, pool: pool}
}

// dailyObs is one observation. dealType "" stores NULL.
type dailyObs struct {
	qty, block, bv, pv int
	ts                 string
	dealType           string
}

// observe appends one observation to the spine. projection and build_id vary with processing_version
// and run_id with the coordinate, so the whole-row comparison against the spine covers every column.
func (f *positionDailyFixture) observe(id string, o dailyObs) {
	f.t.Helper()
	var dt any
	if o.dealType != "" {
		dt = o.dealType
	}
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type,
		     run_id)
		VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, substr(md5($1) || md5($1), 1, 40), $2, $3, $4, $5::int, $6,
		        'public.proj-' || ($5::int)::text, $5::int, $7, 7700 + ($3::bigint * 10) + $5::int)`,
		id, o.qty, o.block, o.bv, o.pv, o.ts, dt); err != nil {
		f.t.Fatalf("observe %s at block %d: %v", id, o.block, err)
	}
}

// column runs a one-column query and returns every value, failing on a mid-stream error: pgx ends
// Next() on one exactly as on end-of-rows, so without the check a truncated result reads as short.
func (f *positionDailyFixture) column(q string, args ...any) []string {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, q, args...)
	if err != nil {
		f.t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("%s iteration: %v", q, err)
	}
	return out
}

// daily returns one position's series as (as_of_date, quantity) pairs, oldest first, through position_daily_between.
func (f *positionDailyFixture) daily(id string) []string {
	f.t.Helper()
	return f.column(`SELECT as_of_date::text || '=' || quantity::text FROM `+positionDailyEveryDate+`
	                   WHERE position_id = sha256($1::bytea) ORDER BY as_of_date`, id)
}

// dayRow is the day's answer through position_daily_between, dayOnRow through position_daily_on, and dayWinner the
// same shape read from the spine by an ORDER BY ... LIMIT 1 the functions do not share.
func (f *positionDailyFixture) dayRow(id, date string) map[string]string {
	f.t.Helper()
	return f.rowOf(`SELECT to_jsonb(d) - 'position_id' - 'as_of_date' FROM position_daily_between($2::date, $2::date) d
	                 WHERE d.position_id = sha256($1::bytea)`, id, date)
}

func (f *positionDailyFixture) dayOnRow(id, date string) map[string]string {
	f.t.Helper()
	return f.rowOf(`SELECT to_jsonb(d) - 'position_id' - 'as_of_date' FROM position_daily_on($2::date) d
	                 WHERE d.position_id = sha256($1::bytea)`, id, date)
}

func (f *positionDailyFixture) dayWinner(id, date string) map[string]string {
	f.t.Helper()
	return f.rowOf(`
		SELECT to_jsonb(p) - 'position_id' FROM position_state p
		 WHERE p.position_id = sha256($1::bytea) AND (p.block_timestamp AT TIME ZONE 'utc')::date = $2::date
		 ORDER BY p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
		 LIMIT 1`, id, date)
}

func (f *positionDailyFixture) rowOf(q, id, date string) map[string]string {
	f.t.Helper()
	var raw map[string]any
	if err := f.pool.QueryRow(f.ctx, q, id, date).Scan(&raw); err != nil {
		f.t.Fatalf("read %s on %s: %v", id, date, err)
	}
	out := map[string]string{}
	for k, v := range raw {
		if v == nil {
			out[k] = "NULL"
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}

// dayQty is the day's answer through position_daily_between over every date, cross-checked against
// position_daily_on so every case in this file covers both reads.
func (f *positionDailyFixture) dayQty(id, date string) int {
	f.t.Helper()
	var viaBetween, viaOn int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT quantity FROM `+positionDailyEveryDate+` WHERE position_id = sha256($1::bytea) AND as_of_date = $2),
		       (SELECT quantity FROM position_daily_on($2::date) WHERE position_id = sha256($1::bytea))`,
		id, date).Scan(&viaBetween, &viaOn); err != nil {
		f.t.Fatalf("dayQty(%s, %s): %v", id, date, err)
	}
	if viaBetween != viaOn {
		f.t.Errorf("%s on %s: position_daily_between reads %d, position_daily_on reads %d", id, date, viaBetween, viaOn)
	}
	return viaBetween
}

// dayRows counts the readings one (position, date) has through each read; both must be 0 or 1.
func (f *positionDailyFixture) dayRows(id, date string) (viaBetween, viaOn int) {
	f.t.Helper()
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM `+positionDailyEveryDate+` WHERE position_id = sha256($1::bytea) AND as_of_date = $2),
		       (SELECT count(*) FROM position_daily_on($2::date) WHERE position_id = sha256($1::bytea))`,
		id, date).Scan(&viaBetween, &viaOn); err != nil {
		f.t.Fatalf("dayRows(%s, %s): %v", id, date, err)
	}
	return viaBetween, viaOn
}

// dbNow is a marker from the database's own clock: the container's clock is its own, and a host
// timestamp can sit ahead of or behind a created_at stamped moments later.
func (f *positionDailyFixture) dbNow() time.Time {
	f.t.Helper()
	var at time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT clock_timestamp()`).Scan(&at); err != nil {
		f.t.Fatalf("read the clock: %v", err)
	}
	return at
}

// A read pinned to the time a report ran returns the same answer after a correction lands, because
// the spine is append-only and its created_at is the as-of axis.
func TestPositionDailyAnswersAsOfATimeReproducibly(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, day = "d-asof", "2026-01-01"
	beforeAny := f.dbNow()
	f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T10:00:00Z", dealType: "LOAN"})
	reportRanAt := f.dbNow()
	f.observe(id, dailyObs{qty: 15, block: 100, pv: 1, ts: "2026-01-01T10:00:00Z", dealType: "BORROW"})

	if got := f.dayQty(id, day); got != 15 {
		t.Fatalf("the latest reading is %d; want 15, the correction", got)
	}
	for _, tc := range []struct {
		name string
		at   time.Time
		want []string
	}{
		{"before any observation", beforeAny, nil},
		{"at the report's run time", reportRanAt, []string{"10"}},
		{"now", f.dbNow(), []string{"15"}},
	} {
		asOf := f.column(`SELECT quantity::text FROM position_daily_between($3::date, $3::date, $1)
		                    WHERE position_id = sha256($2::bytea)`, tc.at, id, day)
		on := f.column(`SELECT quantity::text FROM position_daily_on($3::date, $1)
		                  WHERE position_id = sha256($2::bytea)`, tc.at, id, day)
		if !slices.Equal(asOf, tc.want) || !slices.Equal(on, tc.want) {
			t.Errorf("%s: position_daily_between reads %v and position_daily_on reads %v; want %v", tc.name, asOf, on, tc.want)
		}
	}
}

// Every read inlines. A Function Scan means it did not, and the read then materialises the whole
// function result before the caller's WHERE applies and loses chunk exclusion.
func TestPositionDailyReadsAreInlined(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-inline", dailyObs{qty: 1, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	for _, q := range []string{
		`EXPLAIN SELECT * FROM position_daily_between('2026-01-01', '2026-01-03') WHERE position_id = sha256('d-inline'::bytea)`,
		`EXPLAIN SELECT * FROM position_daily_between('2026-01-01', '2026-01-03', now())`,
		`EXPLAIN SELECT * FROM position_daily_on('2026-01-01') WHERE position_id = sha256('d-inline'::bytea)`,
		`EXPLAIN SELECT * FROM position_daily_on('2026-01-01', now())`,
	} {
		joined := strings.Join(f.column(q), "\n")
		if strings.Contains(joined, "Function Scan") {
			t.Errorf("%s\nplans as a Function Scan, so the read is not inlined:\n%s", q, joined)
		}
		if !strings.Contains(joined, "_hyper_") && !strings.Contains(joined, "position_state") {
			t.Errorf("%s\nplan touches no position_state chunk, so this control is not reading the right object:\n%s", q, joined)
		}
	}
}

// The windowed reads exist for this: one date reads one spine chunk, uncompressed or compressed, with
// the date as a literal or as a bind parameter. A window on (block_timestamp AT TIME ZONE 'utc')::date
// instead of on block_timestamp reads every chunk and fails every case here.
func TestPositionDailyOnReadsOneChunk(t *testing.T) {
	f := newPositionDailyFixture(t)
	const days = 12
	for d := range days {
		for p := range 3 {
			f.observe(fmt.Sprintf("d-chunk-%d", p), dailyObs{qty: d*10 + p, block: 1000 + d*10 + p,
				ts: fmt.Sprintf("2026-03-%02dT12:00:00Z", d+1), dealType: "LOAN"})
		}
	}
	if n := chunkCount(t, f.ctx, f.pool, "position_state"); n < days {
		t.Fatalf("the spine has %d chunk(s); want at least %d, one per seeded day, or one chunk read proves nothing", n, days)
	}

	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(f.ctx, `PREPARE pd_on(date) AS SELECT * FROM position_daily_on($1)`); err != nil {
		t.Fatalf("prepare the bind-parameter read: %v", err)
	}

	check := func(t *testing.T, label string, genericChunks int) {
		t.Helper()
		for _, tc := range []struct {
			name, mode, sql string
			wantChunks      int
		}{
			// Without ANALYZE the plan names every chunk it was built with, so a chunk dropped only at startup
			// still appears. One chunk here is exclusion at plan time.
			{name: "literal date, excluded at plan time", mode: "auto", wantChunks: 1,
				sql: `EXPLAIN (FORMAT JSON) SELECT * FROM position_daily_on('2026-03-05')`},
			{name: "literal range, excluded at plan time", mode: "auto", wantChunks: 3,
				sql: `EXPLAIN (FORMAT JSON) SELECT * FROM position_daily_between('2026-03-04', '2026-03-06')`},
			{name: "literal date", mode: "auto", wantChunks: 1,
				sql: `EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM position_daily_on('2026-03-05')`},
			{name: "literal date and bound", mode: "auto", wantChunks: 1,
				sql: `EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM position_daily_on('2026-03-05', now())`},
			{name: "custom plan over a bind parameter", mode: "force_custom_plan", wantChunks: 1,
				sql: `EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE pd_on('2026-03-05')`},
			// Pinned, not endorsed: which plan a generic plan picks depends on the data. On these uncompressed
			// chunks it is a Merge Append over every chunk; on compressed ones a ChunkAppend that excludes at
			// startup. This fails when either changes, which is when the COMMENT's "can read every chunk" is revisited.
			{name: "generic plan over a bind parameter", mode: "force_generic_plan", wantChunks: genericChunks,
				sql: `EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE pd_on('2026-03-05')`},
		} {
			if _, err := conn.Exec(f.ctx, `SET plan_cache_mode = `+tc.mode); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			plan := explainJSON(t, f.ctx, conn, tc.sql)
			want := tc.wantChunks
			if want < 0 {
				want = chunkCount(t, f.ctx, f.pool, "position_state")
			}
			if chunks := plan.chunkNames(); len(chunks) != want {
				t.Errorf("%s, %s: read %d chunk(s) %v, want %d\nplan:\n%s", label, tc.name, len(chunks), chunks, want, plan.raw)
			}
			if _, err := conn.Exec(f.ctx, `RESET plan_cache_mode`); err != nil {
				t.Fatal(err)
			}
		}
		var got []string
		rows, err := conn.Query(f.ctx, `SELECT quantity::text FROM position_daily_on('2026-03-05') ORDER BY quantity`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			got = append(got, v)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, []string{"40", "41", "42"}) {
			t.Errorf("%s: the one-chunk read returned %v, want the three positions' 2026-03-05 readings [40 41 42]", label, got)
		}
	}

	t.Run("uncompressed", func(t *testing.T) { check(t, "uncompressed", -1) })
	t.Run("compressed", func(t *testing.T) {
		var compressed int
		if err := f.pool.QueryRow(f.ctx,
			`SELECT count(compress_chunk(c)) FROM show_chunks('position_state') c`).Scan(&compressed); err != nil {
			t.Fatalf("compress the spine: %v", err)
		}
		if compressed < days {
			t.Fatalf("compressed %d chunk(s), want at least %d", compressed, days)
		}
		check(t, "compressed", 1)
	})
}

// A range reads one chunk per day in it, d_to inclusive, and over each single day it is position_daily_on.
// A window on the ::date expression reads every chunk; an exclusive d_to drops the last day.
func TestPositionDailyBetweenReadsOneChunkPerDay(t *testing.T) {
	f := newPositionDailyFixture(t)
	const days = 12
	for d := range days {
		for p := range 3 {
			f.observe(fmt.Sprintf("d-range-%d", p), dailyObs{qty: d*10 + p, block: 1000 + d*10 + p,
				ts: fmt.Sprintf("2026-03-%02dT12:00:00Z", d+1), dealType: "LOAN"})
		}
	}
	if n := chunkCount(t, f.ctx, f.pool, "position_state"); n < days {
		t.Fatalf("the spine has %d chunk(s); want at least %d", n, days)
	}
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	for _, tc := range []struct {
		from, to   string
		wantChunks int
		wantRows   int
	}{
		{"2026-03-03", "2026-03-07", 5, 15},
		{"2026-03-05", "2026-03-05", 1, 3},
		{"2026-03-11", "2026-03-20", 2, 6},
	} {
		plan := explainJSON(t, f.ctx, conn, fmt.Sprintf(
			`EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM position_daily_between('%s', '%s')`, tc.from, tc.to))
		if chunks := plan.chunkNames(); len(chunks) != tc.wantChunks {
			t.Errorf("%s..%s read %d chunk(s) %v, want %d\nplan:\n%s", tc.from, tc.to, len(chunks), chunks, tc.wantChunks, plan.raw)
		}
		var rows int
		if err := conn.QueryRow(f.ctx, `SELECT count(*) FROM position_daily_between($1::date, $2::date)`, tc.from, tc.to).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != tc.wantRows {
			t.Errorf("%s..%s returned %d reading(s), want %d", tc.from, tc.to, rows, tc.wantRows)
		}
	}
	var mismatched int
	if err := conn.QueryRow(f.ctx, `
		SELECT count(*) FROM (
		    (SELECT o.* FROM generate_series('2026-03-01'::date, '2026-03-12'::date, '1 day') g(d)
		      CROSS JOIN LATERAL position_daily_on(g.d::date) o
		     EXCEPT ALL SELECT * FROM position_daily_between('2026-03-01', '2026-03-12'))
		    UNION ALL
		    (SELECT * FROM position_daily_between('2026-03-01', '2026-03-12')
		     EXCEPT ALL SELECT o.* FROM generate_series('2026-03-01'::date, '2026-03-12'::date, '1 day') g(d)
		      CROSS JOIN LATERAL position_daily_on(g.d::date) o)) x`).Scan(&mismatched); err != nil {
		t.Fatal(err)
	}
	if mismatched != 0 {
		t.Errorf("position_daily_on over each day and position_daily_between over the range differ on %d row(s)", mismatched)
	}
}

// The window's edges are the UTC midnights: the last microsecond of a date is on it and midnight is
// on the next. A bound that is off by one either way moves an observation onto the wrong date.
func TestPositionDailyOnWindowEdgesAreUTCMidnight(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-edge-last", dailyObs{qty: 1, block: 100, ts: "2026-02-01T23:59:59.999999Z", dealType: "LOAN"})
	f.observe("d-edge-first", dailyObs{qty: 2, block: 101, ts: "2026-02-02T00:00:00Z", dealType: "LOAN"})
	f.observe("d-edge-prev", dailyObs{qty: 3, block: 99, ts: "2026-01-31T23:59:59.999999Z", dealType: "LOAN"})
	// The session zone must not move the window: the bound is built in UTC, not in the caller's zone.
	// One held connection, so the SET reaches the connection the reads run on.
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(f.ctx, `SET TIME ZONE 'Pacific/Kiritimati'`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := conn.Exec(context.Background(), `RESET TIME ZONE`); err != nil {
			t.Errorf("reset the time zone: %v", err)
		}
	}()
	read := func(q, date string) []string {
		t.Helper()
		rows, err := conn.Query(f.ctx, q, date)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			out = append(out, v)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s iteration: %v", q, err)
		}
		return out
	}
	var zone string
	if err := conn.QueryRow(f.ctx, `SHOW TIME ZONE`).Scan(&zone); err != nil || zone != "Pacific/Kiritimati" {
		t.Fatalf("the session zone is %q (%v); the reads below would not test a non-UTC session", zone, err)
	}
	for _, tc := range []struct {
		date string
		want []string
	}{
		{"2026-01-31", []string{"3@2026-01-31"}},
		{"2026-02-01", []string{"1@2026-02-01"}},
		{"2026-02-02", []string{"2@2026-02-02"}},
	} {
		on := read(`SELECT quantity::text || '@' || as_of_date::text FROM position_daily_on($1::date) ORDER BY 1`, tc.date)
		between := read(`SELECT quantity::text || '@' || as_of_date::text FROM position_daily_between($1::date, $1::date) ORDER BY 1`, tc.date)
		if !slices.Equal(on, tc.want) || !slices.Equal(between, tc.want) {
			t.Errorf("%s: position_daily_on reads %v and position_daily_between reads %v; want %v", tc.date, on, between, tc.want)
		}
	}
}

// Every column of the day's reading equals that day's winning spine row, through both reads.
func TestPositionDailyEqualsTheWinningSpineRowOnEveryColumn(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, date = "d-every-col", "2026-01-01"
	f.observe(id, dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	f.observe(id, dailyObs{qty: 22, block: 200, pv: 1, ts: "2026-01-01T05:00:00Z", dealType: "BORROW"})
	want := f.dayWinner(id, date)
	if want["deal_type"] != "BORROW" {
		t.Fatalf("the oracle picked deal_type %q, want BORROW; the fixture is not what this case needs", want["deal_type"])
	}
	for name, got := range map[string]map[string]string{"position_daily": f.dayRow(id, date), "position_daily_on": f.dayOnRow(id, date)} {
		if len(got) != len(want) {
			t.Errorf("%s carries %d columns beside the key, the spine %d", name, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s %s = %q, winning spine row = %q", name, k, got[k], v)
			}
		}
	}
}

// A NULL bound or date must raise. Left to the comparison it is NULL for every row, so the read returns
// an empty set and a caller with an unset value reads "held nothing" as an answer.
func TestPositionDailyRefusesNullArguments(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-null-arg", dailyObs{qty: 9, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	for _, tc := range []struct{ q, want string }{
		{`SELECT count(*) FROM position_daily_between('2026-01-01', '2026-01-01', NULL::timestamptz)`, "as-of time is required"},
		{`SELECT count(*) FROM position_daily_between(NULL::date, '2026-01-01')`, "date is required"},
		{`SELECT count(*) FROM position_daily_between('2026-01-01', NULL::date)`, "date is required"},
		{`SELECT count(*) FROM position_daily_on('2026-01-01', NULL::timestamptz)`, "as-of time is required"},
		{`SELECT count(*) FROM position_daily_on(NULL::date)`, "date is required"},
	} {
		var n int
		err := f.pool.QueryRow(f.ctx, tc.q).Scan(&n)
		if err == nil {
			t.Errorf("%s returned %d row(s) instead of raising", tc.q, n)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s raised for the wrong reason: %v", tc.q, err)
		}
	}
	// Negative control: real arguments still answer, so the guards reject NULL rather than everything.
	var asOf, on int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM position_daily_between('2026-01-01', '2026-01-01', 'infinity')), (SELECT count(*) FROM position_daily_on('2026-01-01', 'infinity'))`).
		Scan(&asOf, &on); err != nil {
		t.Fatalf("real arguments must still answer: %v", err)
	}
	if asOf != 1 || on != 1 {
		t.Errorf("real arguments read %d and %d row(s), want 1 each", asOf, on)
	}
}

// The COMMENT's holder series path: position_ids from position_current, then position_id = ANY(...) over a
// range. On compressed chunks that must reach each chunk's position_id segment index; the answer is the
// same without it, so only the plan can catch it.
func TestPositionDailyHolderSeriesUsesTheSegmentIndex(t *testing.T) {
	f := newPositionDailyFixture(t)
	for d := range 3 {
		for i := range 40 {
			f.observe(fmt.Sprintf("d-holder-%02d", i), dailyObs{qty: d*100 + i, block: 1000 + d*100 + i,
				ts: fmt.Sprintf("2026-04-%02dT01:00:00Z", d+1), dealType: "LOAN"})
		}
	}
	var compressed int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(compress_chunk(c)) FROM show_chunks('position_state') c`).Scan(&compressed); err != nil {
		t.Fatalf("compress the spine: %v", err)
	}
	if compressed < 3 {
		t.Fatalf("compressed %d chunk(s), want 3", compressed)
	}
	if _, err := f.pool.Exec(f.ctx, `ANALYZE position_state`); err != nil {
		t.Fatal(err)
	}
	const series = `SELECT %s FROM position_daily_between('2026-04-01', '2026-04-03')
	                 WHERE position_id = ANY (ARRAY(SELECT position_id FROM position_current WHERE holder_id = substr(md5('d-holder-07') || md5('d-holder-07'), 1, 40)))`

	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	plan := explainJSON(t, f.ctx, conn, "EXPLAIN (ANALYZE, FORMAT JSON) "+fmt.Sprintf(series, "*"))
	var segmentLookups int
	plan.walk(func(n explainNode) {
		if n.NodeType == "Index Scan" && strings.HasSuffix(n.RelationName, "_compressed") {
			segmentLookups++
		}
	})
	if segmentLookups != 3 {
		t.Errorf("the holder series made %d index lookup(s) into compressed chunks, want 3, one per day:\n%s", segmentLookups, plan.raw)
	}
	if got := f.column(fmt.Sprintf(series, "quantity::text") + " ORDER BY as_of_date"); !slices.Equal(got, []string{"7", "107", "207"}) {
		t.Errorf("the holder series read %v, want [7 107 207]", got)
	}
}

// The limit of the reproducibility claim: the spine's created_at is TRANSACTION START time but a row is
// visible at COMMIT, so a T taken while a spine write is open gains that row once it commits.
func TestPositionDailyAsOfIsUnstableWhileASpineWriteIsOpen(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, day = "d-inflight", "2026-01-01"

	writer, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Release()
	tx, err := writer.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(f.ctx, `
		INSERT INTO position_state (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		    block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256($1::bytea), 1, 1, 'inst-inflight', repeat('c', 40), 5, 100, 0, 0, '2026-01-01T10:00:00Z', 'public.p', 0)`, id); err != nil {
		t.Fatalf("append inside the open transaction: %v", err)
	}

	pinned := f.dbNow()
	countAt := func(at time.Time) int {
		t.Helper()
		var n int
		if err := f.pool.QueryRow(f.ctx,
			`SELECT count(*) FROM position_daily_on($3::date, $1) WHERE position_id = sha256($2::bytea)`,
			at, id, day).Scan(&n); err != nil {
			t.Fatalf("read at the pinned time: %v", err)
		}
		return n
	}
	if before := countAt(pinned); before != 0 {
		t.Fatalf("the open write was visible before it committed (%d rows); this case cannot show the window", before)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if after := countAt(pinned); after != 1 {
		t.Errorf("after the commit the same pinned T reads %d row(s), want 1 -- if this is now 0 the spine's stamp "+
			"became commit-ordered and the COMMENTs plus this test must be rewritten", after)
	}
}

// At bulk, one reading per position per date however many observations each day carried.
func TestPositionDailyOneReadingPerPositionPerDateAtBulk(t *testing.T) {
	f := newPositionDailyFixture(t)
	const positions = 2000
	seed := func(qty, block int, ts, dealType string) {
		t.Helper()
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO position_state
			    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type)
			SELECT sha256(g::text::bytea), 1, 1, 'inst-' || g, substr(md5(g::text) || md5(g::text), 1, 40), $1,
			       $2, 0, 0, $3::timestamptz, 'public.proj-0', 0, $4
			  FROM generate_series(1, $5) g`, qty, block, ts, dealType, positions); err != nil {
			t.Fatalf("seed the spine at block %d: %v", block, err)
		}
	}
	seed(5, 100, "2026-03-02T00:00:00Z", "LOAN")
	seed(9, 200, "2026-03-02T06:00:00Z", "BORROW")
	var viewNewest, viewTotal, onNewest, onTotal int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FILTER (WHERE quantity = 9 AND deal_type = 'BORROW') FROM position_daily_between('2026-03-02', '2026-03-02')),
		       (SELECT count(*) FROM position_daily_between('2026-03-02', '2026-03-02')),
		       (SELECT count(*) FILTER (WHERE quantity = 9 AND deal_type = 'BORROW') FROM position_daily_on('2026-03-02')),
		       (SELECT count(*) FROM position_daily_on('2026-03-02'))`).
		Scan(&viewNewest, &viewTotal, &onNewest, &onTotal); err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct {
		name          string
		newest, total int
	}{{"position_daily_between", viewNewest, viewTotal}, {"position_daily_on", onNewest, onTotal}} {
		if r.newest != positions || r.total != positions {
			t.Errorf("%s: %d reading(s), %d carrying the later observation; want %d of each", r.name, r.total, r.newest, positions)
		}
	}
}

// Applying the migration twice is a no-op, which a manual apply or a restore depends on.
func TestPositionDailyMigrationIsReRunnable(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-rerun", dailyObs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), positionDailyMigration))
	if err != nil {
		t.Fatalf("read %s: %v", positionDailyMigration, err)
	}
	if _, err := f.pool.Exec(f.ctx, string(raw)); err != nil {
		t.Fatalf("re-applying %s: %v", positionDailyMigration, err)
	}
	if got := f.dayQty("d-rerun", "2026-01-01"); got != 5 {
		t.Errorf("the day reads %d after a re-apply, want 5", got)
	}
}

// Both reads equal the argmax over position_state per (position, UTC date), on every column they share
// with it, over randomised out-of-order histories driven through the materializer. The oracle is a
// window function, not the functions' DISTINCT ON, and shared columns come from the catalogue.
func TestPositionDailyEqualsTheSpineArgmaxOverRandomHistories(t *testing.T) {
	const seeds = 8
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			ctx := context.Background()
			pool, cleanup := setupMigratedPostgres(ctx, t)
			defer cleanup()

			rng := rand.New(rand.NewSource(int64(seed) * 7919))
			rows := generateHistory(rng)
			view := fmt.Sprintf("pv_pd_%d", seed)

			var arrived []obsRow
			for bi, batch := range splitBatches(rng, rows) {
				arrived = append(arrived, batch...)
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+view+` AS `+valuesBody(arrived)); err != nil {
					t.Fatalf("create view (batch %d): %v", bi, err)
				}
				if _, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass, 0, $2)`, view, 9000+bi); err != nil {
					t.Fatalf("materialize batch %d: %v", bi, err)
				}
			}

			// A random history puts at most one observation on most dates. Append a later observation on
			// a date already present, per position, so the same-day pick is exercised across batches.
			if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+view+` AS `+valuesBody(arrived)+`
				UNION ALL
				-- An off-chain row's block_number IS its instant in epoch seconds, which the spine enforces.
				SELECT chain_id, protocol_id, instrument_key, holder_id, quantity + 7,
				       CASE WHEN chain_id IS NULL
				            THEN floor(extract(epoch FROM block_timestamp + interval '1 minute'))::bigint
				            ELSE block_number + 1 END,
				       block_version, processing_version, block_timestamp + interval '1 minute',
				       CASE deal_type WHEN 'LOAN' THEN 'BORROW' ELSE 'LOAN' END
				  FROM (SELECT DISTINCT ON (position_id) chain_id, protocol_id, instrument_key, holder_id,
				               quantity, block_number, block_version, processing_version, block_timestamp, deal_type
				          FROM position_state
				         WHERE ((block_timestamp + interval '1 minute') AT TIME ZONE 'utc')::date
				               = (block_timestamp AT TIME ZONE 'utc')::date
				         ORDER BY position_id, block_number DESC) latest`); err != nil {
				t.Fatalf("create the same-day view: %v", err)
			}
			if _, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass, 0, $2)`, view, 9500); err != nil {
				t.Fatalf("materialize the same-day observations: %v", err)
			}

			cols := dailySpineColumns(ctx, t, pool)
			if d := diffDailyAgainstSpineArgmax(ctx, t, pool, positionDailyEveryDate, cols); d != "" {
				t.Errorf("position_daily_between over every date: %s", d)
			}
			// position_daily_on over every observed date, unioned, is the same relation.
			onEveryDate := `(SELECT o.* FROM (SELECT DISTINCT (block_timestamp AT TIME ZONE 'utc')::date AS d FROM position_state) dates
			                 CROSS JOIN LATERAL position_daily_on(dates.d) o) x`
			if d := diffDailyAgainstSpineArgmax(ctx, t, pool, onEveryDate, cols); d != "" {
				t.Errorf("position_daily_on over every date: %s", d)
			}
		})
	}
}

// dailySpineColumns lists position_state's columns from the catalogue, so the comparison covers every
// column the reads return (TestPositionDailySchema pins that they return all of them) without naming one.
func dailySpineColumns(ctx context.Context, t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT attname FROM pg_attribute
		 WHERE attrelid = 'position_state'::regclass AND attnum > 0 AND NOT attisdropped
		 ORDER BY attname`)
	if err != nil {
		t.Fatalf("position_state columns: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(out, "deal_type") || len(out) < 9 {
		t.Fatalf("position_state has %d columns (deal_type present: %v); the comparison would be weak",
			len(out), slices.Contains(out, "deal_type"))
	}
	return out
}

// diffDailyAgainstSpineArgmax compares a relation (a name, a function call, or a parenthesised subquery with an alias)
// against the newest position_state row per (position, UTC date), over the given columns.
func diffDailyAgainstSpineArgmax(ctx context.Context, t *testing.T, pool *pgxpool.Pool, rel string, cols []string) string {
	t.Helper()
	sel := strings.Join(cols, ", ")
	var onlyOracle, onlyRead int
	var example string
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		WITH ranked AS (
		  SELECT %s, row_number() OVER (PARTITION BY position_id, (block_timestamp AT TIME ZONE 'utc')::date
		           ORDER BY block_number DESC, block_version DESC, processing_version DESC,
		                    block_timestamp DESC) rn
		    FROM position_state),
		     oracle AS (SELECT %s FROM ranked WHERE rn = 1),
		     reading AS (SELECT %s FROM %s)
		SELECT (SELECT count(*) FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM reading) a),
		       (SELECT count(*) FROM (SELECT * FROM reading EXCEPT ALL SELECT * FROM oracle) b),
		       COALESCE((SELECT a::text FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM reading) a LIMIT 1), '')`,
		sel, sel, sel, rel)).Scan(&onlyOracle, &onlyRead, &example); err != nil {
		t.Fatalf("oracle compare on %s: %v", rel, err)
	}
	if onlyOracle == 0 && onlyRead == 0 {
		return ""
	}
	return fmt.Sprintf("%d rows the spine implies the reading lacks, %d the reading holds that the spine does not; oracle-only e.g. %s",
		onlyOracle, onlyRead, example)
}

// The as-of bound is inclusive, which is what lets a report pin the exact instant it read.
func TestPositionDailyAsOfBoundIsInclusive(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, day = "d-bound", "2026-01-01"
	f.observe(id, dailyObs{qty: 12, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	var stamped time.Time
	if err := f.pool.QueryRow(f.ctx,
		`SELECT created_at FROM position_state WHERE position_id = sha256($1::bytea)`, id).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	var atStamp, justBefore, onAtStamp, onJustBefore int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM position_daily_between($2::date, $2::date, $1) WHERE position_id = sha256($3::bytea)),
		       (SELECT count(*) FROM position_daily_between($2::date, $2::date, $1 - interval '1 microsecond') WHERE position_id = sha256($3::bytea)),
		       (SELECT count(*) FROM position_daily_on($2::date, $1) WHERE position_id = sha256($3::bytea)),
		       (SELECT count(*) FROM position_daily_on($2::date, $1 - interval '1 microsecond') WHERE position_id = sha256($3::bytea))`,
		stamped, day, id).Scan(&atStamp, &justBefore, &onAtStamp, &onJustBefore); err != nil {
		t.Fatal(err)
	}
	if atStamp != 1 || onAtStamp != 1 {
		t.Errorf("a T equal to the row's own created_at reads %d/%d row(s), want 1 -- the bound must be inclusive", atStamp, onAtStamp)
	}
	if justBefore != 0 || onJustBefore != 0 {
		t.Errorf("a T one microsecond earlier reads %d/%d row(s), want 0; without this the case above passes on any bound", justBefore, onJustBefore)
	}
}

// The current UTC date is read like any other: there is no settling step, so an observation made today
// is today's reading as soon as it is on the spine.
func TestPositionDailyReadsTheCurrentDay(t *testing.T) {
	f := newPositionDailyFixture(t)
	var today string
	if err := f.pool.QueryRow(f.ctx, `SELECT ((now() AT TIME ZONE 'utc')::date)::text`).Scan(&today); err != nil {
		t.Fatal(err)
	}
	f.observe("d-today", dailyObs{qty: 7, block: 100, ts: today + "T00:00:01Z", dealType: "LOAN"})
	if v, on := f.dayRows("d-today", today); v != 1 || on != 1 {
		t.Errorf("today's observation has %d reading(s) through position_daily_between and %d through position_daily_on, want 1 each", v, on)
	}
}

// The last ordering leg. At equal (block, block_version, processing_version) the later instant wins.
// One pair can pass by scan order alone, so many positions insert the pair in both orders, and the
// read runs under a seq scan and under the primary key, which returns block_timestamp ascending.
func TestPositionDailyLaterInstantWinsAtEqualVersionsWhateverTheScanOrder(t *testing.T) {
	f := newPositionDailyFixture(t)
	const positions = 100
	for i := range positions {
		id := fmt.Sprintf("d-ts-order-%03d", i)
		early := dailyObs{qty: 1, block: 100, ts: "2026-05-05T01:00:00Z", dealType: "LOAN"}
		late := dailyObs{qty: 2, block: 100, ts: "2026-05-05T09:00:00Z", dealType: "LOAN"}
		if i%2 == 0 {
			f.observe(id, early)
			f.observe(id, late)
		} else {
			f.observe(id, late)
			f.observe(id, early)
		}
	}
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	for _, scan := range []struct{ name, set string }{
		{"seq scan", `SET enable_indexscan = off; SET enable_bitmapscan = off`},
		{"index scan", `SET enable_seqscan = off; SET enable_bitmapscan = off`},
	} {
		if _, err := conn.Exec(f.ctx, scan.set); err != nil {
			t.Fatalf("%s: %v", scan.name, err)
		}
		var onWrong, viewWrong int
		if err := conn.QueryRow(f.ctx, `
			SELECT (SELECT count(*) FROM position_daily_on('2026-05-05') WHERE quantity <> 2),
			       (SELECT count(*) FROM position_daily_between('2026-05-04', '2026-05-06') WHERE as_of_date = '2026-05-05' AND quantity <> 2)`).
			Scan(&onWrong, &viewWrong); err != nil {
			t.Fatalf("%s: %v", scan.name, err)
		}
		if onWrong != 0 || viewWrong != 0 {
			t.Errorf("%s: %d position(s) through position_daily_on and %d through position_daily_between read the earlier instant; "+
				"at equal versions the later one wins", scan.name, onWrong, viewWrong)
		}
		if _, err := conn.Exec(f.ctx, `RESET enable_indexscan; RESET enable_bitmapscan; RESET enable_seqscan`); err != nil {
			t.Fatal(err)
		}
	}
}

// These cases check catalogue-level guarantees and share one database.
func TestPositionDailySchema(t *testing.T) {
	f := newPositionDailyFixture(t)

	t.Run("app roles can call the reads", func(t *testing.T) {
		for _, role := range []string{"stl_readonly", "stl_readwrite"} {
			var between, on bool
			if err := f.pool.QueryRow(f.ctx, `
				SELECT has_function_privilege($1, 'position_daily_between(date, date, timestamptz)', 'EXECUTE'),
				       has_function_privilege($1, 'position_daily_on(date, timestamptz)', 'EXECUTE')`, role).
				Scan(&between, &on); err != nil {
				t.Fatal(err)
			}
			if !between || !on {
				t.Errorf("%s: EXECUTE position_daily_between=%v, EXECUTE position_daily_on=%v; want both true", role, between, on)
			}
		}
	})

	// No copy and no unbounded read: a migration that brings back a table, a view or a windowless function
	// fails here.
	t.Run("stores nothing and has no unbounded read", func(t *testing.T) {
		var relations, functions []string
		if err := f.pool.QueryRow(f.ctx, `
			SELECT (SELECT COALESCE(array_agg(relname::text ORDER BY relname), '{}') FROM pg_class
			         WHERE relnamespace = 'public'::regnamespace AND relname LIKE 'position_daily%'),
			       (SELECT COALESCE(array_agg(p.oid::regprocedure::text ORDER BY 1), '{}') FROM pg_proc p
			         WHERE p.pronamespace = 'public'::regnamespace AND p.proname LIKE 'position_daily%')`).
			Scan(&relations, &functions); err != nil {
			t.Fatal(err)
		}
		if len(relations) != 0 {
			t.Errorf("relations %v exist; position_daily is read only through its bounded functions", relations)
		}
		want := []string{
			"position_daily_as_of_bound(timestamp with time zone)",
			"position_daily_between(date,date,timestamp with time zone)",
			"position_daily_date_required(date)",
			"position_daily_on(date,timestamp with time zone)",
		}
		if !slices.Equal(functions, want) {
			t.Errorf("position_daily functions are %v, want exactly %v", functions, want)
		}
	})

	// The guards are IMMUTABLE so a constant argument folds at plan time. They pin search_path because they
	// are plpgsql and never inlined.
	t.Run("guards are immutable plpgsql with a pinned search_path", func(t *testing.T) {
		for _, fn := range []string{"position_daily_date_required(date)", "position_daily_as_of_bound(timestamptz)"} {
			var lang, volatility string
			var config []string
			if err := f.pool.QueryRow(f.ctx, `
				SELECT l.lanname, p.provolatile::text, COALESCE(p.proconfig, '{}')
				  FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang
				 WHERE p.oid = $1::regprocedure`, fn).Scan(&lang, &volatility, &config); err != nil {
				t.Fatalf("%s: %v", fn, err)
			}
			if lang != "plpgsql" || volatility != "i" || !slices.Equal(config, []string{"search_path=pg_catalog, public"}) {
				t.Errorf("%s is %s, volatility %s, config %v; want plpgsql, IMMUTABLE, search_path=pg_catalog, public",
					fn, lang, volatility, config)
			}
		}
	})

	// A SET clause or a non-SQL language on a read function stops it inlining, and with it chunk exclusion.
	t.Run("reads are inlinable SQL", func(t *testing.T) {
		for _, fn := range []string{"position_daily_between(date, date, timestamptz)", "position_daily_on(date, timestamptz)"} {
			var lang, volatility string
			var config []string
			var definer bool
			if err := f.pool.QueryRow(f.ctx, `
				SELECT l.lanname, p.provolatile::text, COALESCE(p.proconfig, '{}'), p.prosecdef
				  FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang
				 WHERE p.oid = $1::regprocedure`, fn).Scan(&lang, &volatility, &config, &definer); err != nil {
				t.Fatalf("%s: %v", fn, err)
			}
			if lang != "sql" || volatility != "s" || len(config) != 0 || definer {
				t.Errorf("%s is %s, volatility %s, config %v, security definer %v; want sql, STABLE, no config, invoker",
					fn, lang, volatility, config, definer)
			}
		}
	})

	// The functions declare their columns, so a column added to position_state does not reach them on
	// its own. This fails the day it happens, which is when the migration must be extended.
	t.Run("reads carry every spine column", func(t *testing.T) {
		spine := f.column(`SELECT attname::text FROM pg_attribute WHERE attrelid = 'position_state'::regclass
		                     AND attnum > 0 AND NOT attisdropped ORDER BY attname`)
		if len(spine) == 0 {
			t.Fatal("read no spine columns, so the comparison below is vacuous")
		}
		want := append(slices.Clone(spine), "as_of_date")
		slices.Sort(want)
		for name, q := range map[string]string{
			"position_daily_between": `SELECT n FROM unnest((SELECT proargnames[4:] FROM pg_proc
			                            WHERE oid = 'position_daily_between(date, date, timestamptz)'::regprocedure)) n ORDER BY n`,
			"position_daily_on": `SELECT n FROM unnest((SELECT proargnames[3:] FROM pg_proc
			                       WHERE oid = 'position_daily_on(date, timestamptz)'::regprocedure)) n ORDER BY n`,
		} {
			if got := f.column(q); !slices.Equal(got, want) {
				t.Errorf("%s exposes %v; want every position_state column plus as_of_date, %v", name, got, want)
			}
		}
	})
}

// These cases check the per-day semantics and share one database; every case owns its position.
func TestPositionDailySemantics(t *testing.T) {
	f := newPositionDailyFixture(t)

	// One case per leg of the ordering, each holding the earlier legs equal and both rows on the SAME UTC
	// date so they compete for the day and the ordering actually decides.
	t.Run("newer wins precedence", func(t *testing.T) {
		for _, tc := range []struct {
			name             string
			id               string
			base, challenger dailyObs
			keepBase         bool
			why              string
		}{
			{name: "a newer block wins", id: "d-block",
				base:       dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
				challenger: dailyObs{qty: 22, block: 200, ts: "2026-01-01T02:00:00Z", dealType: "LOAN"}},
			{name: "an older block does not win even at a higher processing_version", id: "d-order",
				base:       dailyObs{qty: 11, block: 200, ts: "2026-01-01T02:00:00Z", dealType: "LOAN"},
				challenger: dailyObs{qty: 22, block: 100, pv: 1, ts: "2026-01-01T03:00:00Z", dealType: "LOAN"},
				keepBase:   true, why: "the ordering must lead with block_number, or a reprocess of old history rolls the day back"},
			{name: "a newer block_version at the same block wins", id: "d-bv",
				base:       dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
				challenger: dailyObs{qty: 22, block: 100, bv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"}},
			{name: "a newer block_version wins over a higher processing_version", id: "d-bv-pv",
				base:       dailyObs{qty: 11, block: 100, pv: 5, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
				challenger: dailyObs{qty: 22, block: 100, bv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"}},
			{name: "a newer processing_version at the same block and block_version wins", id: "d-pv",
				base:       dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
				challenger: dailyObs{qty: 22, block: 100, pv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"}},
			{name: "a newer processing_version wins over a later instant", id: "d-pv-ts",
				base:       dailyObs{qty: 11, block: 100, ts: "2026-01-01T09:00:00Z", dealType: "LOAN"},
				challenger: dailyObs{qty: 22, block: 100, pv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"}},
			{name: "a later block_timestamp on the same day at equal versions wins", id: "d-ts",
				base:       dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
				challenger: dailyObs{qty: 22, block: 100, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f.observe(tc.id, tc.base)
				f.observe(tc.id, tc.challenger)
				want := tc.challenger.qty
				if tc.keepBase {
					want = tc.base.qty
				}
				if got := f.dayQty(tc.id, "2026-01-01"); got != want {
					t.Errorf("the day reads %d; want %d. %s", got, want, tc.why)
				}
				if v, on := f.dayRows(tc.id, "2026-01-01"); v != 1 || on != 1 {
					t.Errorf("the day has %d reading(s) through position_daily_between and %d through position_daily_on, want 1 each", v, on)
				}
			})
		}
	})

	t.Run("older observation arriving later cannot regress the day", func(t *testing.T) {
		f.observe("d-late", dailyObs{qty: 22, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		f.observe("d-late", dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		if got := f.dayQty("d-late", "2026-01-01"); got != 22 {
			t.Errorf("the day reads %d; want 22 -- an older observation arriving later must not win", got)
		}
	})

	// Only observed dates get a row, and every observed date gets one. No carry-forward.
	t.Run("retains every observed date and only those", func(t *testing.T) {
		for _, o := range []dailyObs{
			{qty: 10, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"},
			{qty: 20, block: 200, ts: "2026-01-03T00:00:00Z", dealType: "LOAN"},
			{qty: 30, block: 300, ts: "2026-01-06T00:00:00Z", dealType: "LOAN"},
		} {
			f.observe("d-dates", o)
		}
		if got := strings.Join(f.daily("d-dates"), ","); got != "2026-01-01=10,2026-01-03=20,2026-01-06=30" {
			t.Errorf("series = %s; want only the three observed dates, with no carry-forward into 01-02 or 01-04/05", got)
		}
		if v, on := f.dayRows("d-dates", "2026-01-02"); v != 0 || on != 0 {
			t.Errorf("an unobserved date has %d/%d reading(s), want none", v, on)
		}
	})

	// A same-day reprocess supersedes that day's reading; the next day is its own row.
	t.Run("corrections land on their own date", func(t *testing.T) {
		f.observe("d-corr", dailyObs{qty: 10, block: 100, ts: "2026-01-01T23:00:00Z", dealType: "LOAN"})
		f.observe("d-corr", dailyObs{qty: 15, block: 100, pv: 1, ts: "2026-01-01T23:30:00Z", dealType: "LOAN"})
		f.observe("d-corr", dailyObs{qty: 99, block: 200, ts: "2026-01-02T00:30:00Z", dealType: "LOAN"})
		if got := strings.Join(f.daily("d-corr"), ","); got != "2026-01-01=15,2026-01-02=99" {
			t.Errorf("series = %s; want the same-day reprocess to supersede 01-01 and the next day to be its own row", got)
		}
	})

	t.Run("carries a null deal type", func(t *testing.T) {
		f.observe("d-null", dailyObs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z"})
		if got := f.dayRow("d-null", "2026-01-01")["deal_type"]; got != "NULL" {
			t.Errorf("deal_type = %q, want NULL", got)
		}
	})

	// A read over a shadowing schema ahead of public must still read public.position_state.
	t.Run("resolves the spine under a shadowing search_path", func(t *testing.T) {
		f.observe("d-shadow", dailyObs{qty: 4, block: 100, ts: "2026-01-09T00:00:00Z", dealType: "LOAN"})
		conn, err := f.pool.Acquire(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(f.ctx, `
			CREATE SCHEMA IF NOT EXISTS shadow;
			CREATE TABLE IF NOT EXISTS shadow.position_state (LIKE public.position_state);
			SET search_path = shadow, public`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := conn.Exec(context.Background(), `RESET search_path`); err != nil {
				t.Errorf("reset search_path: %v", err)
			}
		}()
		var n int
		if err := conn.QueryRow(f.ctx, `SELECT count(*) FROM public.position_daily_on('2026-01-09')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("under a shadowing search_path position_daily_on read %d row(s), want 1 from public.position_state", n)
		}
	})
}
