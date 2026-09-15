//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	positionDailyMigration         = "20260824_120000_create_position_daily.sql"
	positionDailyBackfillMigration = "20260824_120100_backfill_position_daily.sql"
)

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

// observe appends one observation to the history in its own statement, which the trigger propagates
// to the table as one appended row. projection and build_id vary with processing_version and run_id
// with the coordinate, so the whole-row comparison against the spine covers every copied column.
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

// daily returns one position's series as (as_of_date, quantity) pairs, oldest first, through the
// position_daily view: the read a consumer makes.
func (f *positionDailyFixture) daily(id string) []string {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx,
		`SELECT as_of_date::text || '=' || quantity::text FROM position_daily
		  WHERE position_id = sha256($1::bytea) ORDER BY as_of_date`, id)
	if err != nil {
		f.t.Fatalf("daily(%s): %v", id, err)
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
		// pgx streams: a mid-stream failure ends Next() exactly like end-of-rows, so without this a
		// truncated slice reads as a short series and the caller asserts on partial data.
		f.t.Fatalf("daily(%s) iteration: %v", id, err)
	}
	return out
}

// dayRow is the day's answer for one (position, date) as the view gives it; dayWinner is the same
// shape read from the spine.
func (f *positionDailyFixture) dayRow(id, date string) map[string]string {
	f.t.Helper()
	return f.rowOf(`SELECT to_jsonb(d) - 'position_id' - 'as_of_date' - 'created_at' FROM position_daily d
	                 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2`, id, date)
}

func (f *positionDailyFixture) dayWinner(id, date string) map[string]string {
	f.t.Helper()
	return f.rowOf(`
		SELECT to_jsonb(p) - 'position_id' - 'created_at' FROM position_state p
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

// dayQty is the day's answer through the view.
func (f *positionDailyFixture) dayQty(id, date string) int {
	f.t.Helper()
	var q int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT quantity FROM position_daily WHERE position_id = sha256($1::bytea) AND as_of_date = $2`,
		id, date).Scan(&q); err != nil {
		f.t.Fatalf("dayQty(%s, %s): %v", id, date, err)
	}
	return q
}

// dayRows counts the rows the TABLE holds for one (position, date): one per batch that observed it.
func (f *positionDailyFixture) dayRows(id, date string) int {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM position_daily_observation WHERE position_id = sha256($1::bytea) AND as_of_date = $2`,
		id, date).Scan(&n); err != nil {
		f.t.Fatalf("dayRows(%s, %s): %v", id, date, err)
	}
	return n
}

func (f *positionDailyFixture) rowCount() int {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_daily_observation`).Scan(&n); err != nil {
		f.t.Fatalf("count position_daily_observation: %v", err)
	}
	return n
}

func (f *positionDailyFixture) rebuild() {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, `CALL rebuild_position_daily()`); err != nil {
		f.t.Fatalf("rebuild: %v", err)
	}
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

// One case per leg of the ordering, each holding the earlier legs equal and both rows on the SAME UTC
// date so they compete for the day and the ordering actually decides.
func TestPositionDailyNewerWinsPrecedence(t *testing.T) {
	f := newPositionDailyFixture(t)
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
		{name: "a newer processing_version at the same block and block_version wins", id: "d-pv",
			base:       dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
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
		})
	}
}

// An older observation arriving later cannot regress the day it lands on.
func TestPositionDailyOlderObservationArrivingLaterCannotRegressTheDay(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-late", dailyObs{qty: 22, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
	f.observe("d-late", dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	if got := f.dayQty("d-late", "2026-01-01"); got != 22 {
		t.Errorf("the day reads %d; want 22 -- an older observation arriving later must not win", got)
	}
}

// Only observed dates get a row, and every observed date gets one. No carry-forward.
func TestPositionDailyRetainsEveryObservedDateAndOnlyThose(t *testing.T) {
	f := newPositionDailyFixture(t)
	for _, o := range []dailyObs{
		{qty: 10, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"},
		{qty: 20, block: 200, ts: "2026-01-03T00:00:00Z", dealType: "LOAN"},
		{qty: 30, block: 300, ts: "2026-01-06T00:00:00Z", dealType: "LOAN"},
	} {
		f.observe("d-dates", o)
	}
	got := strings.Join(f.daily("d-dates"), ",")
	if got != "2026-01-01=10,2026-01-03=20,2026-01-06=30" {
		t.Errorf("series = %s; want only the three observed dates, with no carry-forward into 01-02 or 01-04/05", got)
	}
}

// A correction on the same day supersedes that day's reading; a correction across UTC midnight is a
// row on its own date and leaves the old date's reading standing.
func TestPositionDailyCorrectionsLandOnTheirOwnDate(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-corr", dailyObs{qty: 10, block: 100, ts: "2026-01-01T23:00:00Z", dealType: "LOAN"})
	f.observe("d-corr", dailyObs{qty: 15, block: 100, pv: 1, ts: "2026-01-01T23:30:00Z", dealType: "LOAN"})
	f.observe("d-corr", dailyObs{qty: 99, block: 200, ts: "2026-01-02T00:30:00Z", dealType: "LOAN"})
	got := strings.Join(f.daily("d-corr"), ",")
	if got != "2026-01-01=15,2026-01-02=99" {
		t.Errorf("series = %s; want the same-day reprocess to supersede 01-01 and the next day to be its own row", got)
	}
}

// The table is append-only in fact, not just in grants: every batch that observes a (position, day)
// leaves one row behind, the earlier rows keep the values they were written with, and nothing is
// ever rewritten. An upsert holds one row per day and fails the count.
func TestPositionDailyAppendsOneRowPerBatchAndNeverRewrites(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, day = "d-append", "2026-01-01"
	series := []dailyObs{
		{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
		{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "BORROW"},
		// An older observation arriving third: appended like any other, and it must not win.
		{qty: 5, block: 50, ts: "2026-01-01T00:30:00Z", dealType: "LOAN"},
		{qty: 30, block: 300, ts: "2026-01-01T09:00:00Z", dealType: "LOAN"},
	}
	for _, o := range series {
		f.observe(id, o)
	}
	if got := f.dayRows(id, day); got != len(series) {
		t.Fatalf("the table holds %d row(s) for the day after %d batches; want one per batch -- an upsert leaves one", got, len(series))
	}
	// Each row still carries the coordinate and quantity it was appended with, in ascending created_at.
	rows, err := f.pool.Query(f.ctx, `
		SELECT block_number, quantity::int FROM position_daily_observation
		 WHERE position_id = sha256($1::bytea) AND as_of_date = $2 ORDER BY created_at, block_number`, id, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var bn, q int
		if err := rows.Scan(&bn, &q); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d=%d", bn, q))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"100=10", "200=20", "50=5", "300=30"}; !slices.Equal(got, want) {
		t.Errorf("the appended rows read %v in created_at order; want %v, each as written", got, want)
	}
	if q := f.dayQty(id, day); q != 30 {
		t.Errorf("the day reads %d; want 30, the newest block", q)
	}
}

// The reason the table is append-only: a query pinned to the time it ran returns the same rows later,
// after a correction has landed. An upsert overwrites the row and its created_at, so the pinned read
// finds nothing where it found the original.
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

	// Pinned read over the raw table: what a report that recorded its own run time can reconstruct.
	var pinned []int
	rows, err := f.pool.Query(f.ctx, `
		SELECT DISTINCT ON (position_id, as_of_date) quantity::int FROM position_daily_observation
		 WHERE position_id = sha256($1::bytea) AND as_of_date = $2 AND created_at <= $3
		 ORDER BY position_id, as_of_date, block_number DESC, block_version DESC, processing_version DESC, block_timestamp DESC`,
		id, day, reportRanAt)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var q int
		if err := rows.Scan(&q); err != nil {
			t.Fatal(err)
		}
		pinned = append(pinned, q)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pinned, []int{10}) {
		t.Errorf("rows appended by the report's run time read %v; want [10], the original -- the correction rewrote or re-stamped it", pinned)
	}

	// The same through the function the COMMENT points consumers at, at three instants.
	for _, tc := range []struct {
		name string
		at   time.Time
		want []int
	}{
		{"before any observation", beforeAny, nil},
		{"at the report's run time", reportRanAt, []int{10}},
		{"now", f.dbNow(), []int{15}},
	} {
		var got []int
		rows, err := f.pool.Query(f.ctx,
			`SELECT quantity::int FROM position_daily_as_of($1) WHERE position_id = sha256($2::bytea) AND as_of_date = $3`,
			tc.at, id, day)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		for rows.Next() {
			var q int
			if err := rows.Scan(&q); err != nil {
				t.Fatal(err)
			}
			got = append(got, q)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("position_daily_as_of(%s) reads %v; want %v", tc.name, got, tc.want)
		}
	}
}

// The latest view is the function at 'infinity', and the planner inlines that function into the
// caller's query. A Function Scan in the plan means it did not, and every read pays a full
// materialisation of the table before its WHERE applies.
func TestPositionDailyLatestViewIsInlined(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-inline", dailyObs{qty: 1, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	for _, q := range []string{
		`EXPLAIN SELECT * FROM position_daily WHERE position_id = sha256('d-inline'::bytea)`,
		`EXPLAIN SELECT * FROM position_daily_as_of(now()) WHERE position_id = sha256('d-inline'::bytea)`,
	} {
		rows, err := f.pool.Query(f.ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var plan []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(plan, "\n")
		if strings.Contains(joined, "Function Scan") {
			t.Errorf("%s\nplans as a Function Scan, so position_daily_as_of is not inlined:\n%s", q, joined)
		}
		if !strings.Contains(joined, "position_daily_observation") {
			t.Errorf("%s\nplan does not touch position_daily_observation at all, so this control is not reading the right object:\n%s", q, joined)
		}
	}
}

// Every column of the day's reading equals that day's winning spine row, through the trigger and
// through a rebuild from empty.
func TestPositionDailyEqualsTheWinningSpineRowOnEveryColumn(t *testing.T) {
	for _, writer := range []string{"trigger", "rebuild"} {
		t.Run(writer, func(t *testing.T) {
			f := newPositionDailyFixture(t)
			const id, date = "d-every-col", "2026-01-01"
			f.observe(id, dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
			f.observe(id, dailyObs{qty: 22, block: 200, pv: 1, ts: "2026-01-01T05:00:00Z", dealType: "BORROW"})
			if writer == "rebuild" {
				if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_daily_observation`); err != nil {
					t.Fatalf("empty the table (superuser harness): %v", err)
				}
				f.rebuild()
			}
			got, want := f.dayRow(id, date), f.dayWinner(id, date)
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s: reading %s = %q, winning spine row = %q", writer, k, got[k], v)
				}
			}
			if got["deal_type"] != "BORROW" {
				t.Errorf("%s: deal_type = %q, want BORROW -- the day's winner flipped direction", writer, got["deal_type"])
			}
		})
	}
}

// A NULL deal_type is carried as NULL.
func TestPositionDailyCarriesANullDealType(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-null", dailyObs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z"})
	if got := f.dayRow("d-null", "2026-01-01")["deal_type"]; got != "NULL" {
		t.Errorf("deal_type = %q, want NULL", got)
	}
}

// One batch carrying several observations of one position on one day appends that day's newest, once.
func TestPositionDailyIntraBatchPick(t *testing.T) {
	f := newPositionDailyFixture(t)
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type)
		SELECT sha256('d-batch'::bytea), 1, 1, 'inst-d-batch', repeat('a', 40), v.qty, v.bn, 0, 0,
		       v.ts::timestamptz, 'public.proj-0', 0, v.dt
		FROM (VALUES (11, 100, '2026-01-01T01:00:00Z', 'LOAN'),
		             (33, 300, '2026-01-01T09:00:00Z', 'BORROW'),
		             (22, 200, '2026-01-01T05:00:00Z', 'LOAN')) AS v(qty, bn, ts, dt)`); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	if n := f.dayRows("d-batch", "2026-01-01"); n != 1 {
		t.Errorf("one batch left %d row(s) for the day; want 1, the batch's newest", n)
	}
	got := f.dayRow("d-batch", "2026-01-01")
	if got["quantity"] != "33" || got["block_number"] != "300" || got["deal_type"] != "BORROW" {
		t.Errorf("the day reads quantity %s at block %s deal_type %s; want 33 at 300 BORROW", got["quantity"], got["block_number"], got["deal_type"])
	}
}

// The app role reads and cannot write; the owner can only append. The owner half is read from the
// ACL rather than has_table_privilege because the harness's owner is a superuser, for whom that
// function answers true regardless.
func TestPositionDailyGrantsAreReadForTheAppRoleAndAppendOnlyForTheOwner(t *testing.T) {
	f := newPositionDailyFixture(t)
	for _, c := range []struct {
		role, priv string
		want       bool
	}{
		{"stl_readonly", "SELECT", true},
		{"stl_readwrite", "SELECT", true},
		{"stl_readwrite", "INSERT", false},
		{"stl_readwrite", "UPDATE", false},
		{"stl_readwrite", "DELETE", false},
		{"stl_readwrite", "TRUNCATE", false},
	} {
		var got bool
		if err := f.pool.QueryRow(f.ctx,
			`SELECT has_table_privilege($1, 'position_daily_observation', $2)`, c.role, c.priv).Scan(&got); err != nil {
			t.Fatalf("has_table_privilege(%s, %s): %v", c.role, c.priv, err)
		}
		if got != c.want {
			t.Errorf("%s %s on position_daily_observation = %v; want %v", c.role, c.priv, got, c.want)
		}
	}
	for _, view := range []string{"stl_readonly", "stl_readwrite"} {
		var got bool
		if err := f.pool.QueryRow(f.ctx,
			`SELECT has_table_privilege($1, 'position_daily', 'SELECT')`, view).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if !got {
			t.Errorf("%s cannot SELECT position_daily, the read the table exists for", view)
		}
	}

	var ownerPrivs []string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT COALESCE(array_agg(a.privilege_type ORDER BY a.privilege_type), '{}')
		  FROM pg_class c, aclexplode(c.relacl) a
		 WHERE c.oid = 'position_daily_observation'::regclass AND a.grantee = c.relowner`).Scan(&ownerPrivs); err != nil {
		t.Fatalf("read the owner's ACL: %v", err)
	}
	if !slices.Contains(ownerPrivs, "INSERT") || !slices.Contains(ownerPrivs, "SELECT") {
		t.Errorf("the owner's ACL is %v; the trigger and the rebuild need INSERT and SELECT", ownerPrivs)
	}
	for _, p := range []string{"UPDATE", "DELETE", "TRUNCATE"} {
		if slices.Contains(ownerPrivs, p) {
			t.Errorf("the owner still holds %s on position_daily_observation (ACL %v); the creating migration revokes it", p, ownerPrivs)
		}
	}
}

// End to end as the login user the workers really use: every direct write on the table is refused,
// while an append to the history still lands a row through the SECURITY DEFINER trigger. The third
// case is what makes the refusals meaningful: a REVOKE that also broke the trigger would look the same.
func TestPositionDailyIsWrittenOnlyByItsTriggerUnderTheRealRole(t *testing.T) {
	f := newPositionDailyFixture(t)
	appPool, err := pgxpool.New(f.ctx, loginRoleDSN(t, f.pool))
	if err != nil {
		t.Fatalf("connect as stl_read_write: %v", err)
	}
	defer appPool.Close()

	for name, stmt := range map[string]string{
		"INSERT": `INSERT INTO position_daily_observation (position_id, as_of_date, instrument_key, holder_id, quantity,
		               block_number, block_version, processing_version, block_timestamp, projection, build_id)
		           VALUES (sha256('x'::bytea), '2026-01-01', 'x', repeat('a', 40), 1, 1, 0, 0, '2026-01-01T00:00:00Z', 'p', 0)`,
		// A WHERE that matches nothing: privileges are checked at executor start.
		"UPDATE":   `UPDATE position_daily_observation SET quantity = quantity WHERE block_number = -1`,
		"DELETE":   `DELETE FROM position_daily_observation WHERE block_number = -1`,
		"TRUNCATE": `TRUNCATE position_daily_observation`,
	} {
		_, err := appPool.Exec(f.ctx, stmt)
		if err == nil {
			t.Errorf("%s on position_daily_observation succeeded as stl_read_write; only the trigger and the rebuild may write it", name)
			continue
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s failed with %v, want SQLSTATE 42501 (insufficient_privilege)", name, err)
		}
	}

	if _, err := appPool.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type)
		VALUES (sha256('d-role'::bytea), 1, 1, 'inst-d-role', repeat('c', 40), 42, 100, 0, 0,
		        '2026-01-01T10:00:00Z', 'public.proj-0', 0, 'LOAN')`); err != nil {
		t.Fatalf("append to position_state as stl_read_write: %v", err)
	}
	var qty int
	if err := appPool.QueryRow(f.ctx,
		`SELECT quantity FROM position_daily WHERE position_id = sha256('d-role'::bytea) AND as_of_date = '2026-01-01'`).
		Scan(&qty); err != nil {
		t.Fatalf("no row after the append -- the SECURITY DEFINER trigger did not write it, or the role lost SELECT: %v", err)
	}
	if qty != 42 {
		t.Errorf("the day reads %d, want 42", qty)
	}
}

// The maintainer runs as the owner with a pinned search_path, both mandatory for SECURITY DEFINER.
func TestPositionDailyTriggerFunctionIsSecurityDefinerWithAPinnedPath(t *testing.T) {
	f := newPositionDailyFixture(t)
	var definer bool
	var config []string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT prosecdef, proconfig FROM pg_proc WHERE proname = 'append_position_daily'`).Scan(&definer, &config); err != nil {
		t.Fatal(err)
	}
	if !definer {
		t.Error("append_position_daily is not SECURITY DEFINER, so the appending role would need a write grant on the table")
	}
	// The exact value, not merely the presence of the setting: the path IS the security boundary on a
	// SECURITY DEFINER function, and `SET search_path = pg_temp` satisfies a presence check.
	if !strings.Contains(strings.Join(config, " "), "search_path=pg_catalog, public") {
		t.Errorf("append_position_daily pins proconfig = %v; want search_path=pg_catalog, public", config)
	}
}

// The rebuild procedure pins the settings a hand-run region could forget.
func TestPositionDailyRebuildPinsItsSettings(t *testing.T) {
	f := newPositionDailyFixture(t)
	var config []string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT proconfig FROM pg_proc WHERE proname = 'rebuild_position_daily'`).Scan(&config); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(config, " ")
	for _, want := range []string{"timescaledb.enable_tiered_reads=on", "search_path=pg_catalog, public",
		"work_mem=64MB", "lock_timeout=10s"} {
		if !strings.Contains(joined, want) {
			t.Errorf("rebuild_position_daily does not pin %q (proconfig = %v)", want, config)
		}
	}
}

// The rebuild adds what the trigger missed and touches nothing else: over a complete table it is a
// no-op, over an emptied one it restores exactly the day winners, and the rows it left alone keep
// their created_at.
func TestPositionDailyRebuildAppendsOnlyWhatIsMissing(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id = "d-rebuild"
	f.observe(id, dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	f.observe(id, dailyObs{qty: 22, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
	f.observe(id, dailyObs{qty: 33, block: 300, ts: "2026-01-02T05:00:00Z", dealType: "LOAN"})

	before := dailyCacheDigest(f.ctx, t, f.pool, "position_daily_observation")
	f.rebuild()
	if after := dailyCacheDigest(f.ctx, t, f.pool, "position_daily_observation"); after != before {
		t.Error("a rebuild over a complete table changed it; it may only append rows that are missing")
	}

	// Emptied (superuser harness; the owner's DELETE is revoked in every deployed environment).
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_daily_observation`); err != nil {
		t.Fatalf("empty the table: %v", err)
	}
	f.rebuild()
	if n := f.rowCount(); n != 2 {
		t.Errorf("a rebuild from empty appended %d row(s); want 2, one winner per observed date -- the losing same-day observation is not a winner", n)
	}
	if got := strings.Join(f.daily(id), ","); got != "2026-01-01=22,2026-01-02=33" {
		t.Errorf("series after a rebuild from empty = %s; want 2026-01-01=22,2026-01-02=33", got)
	}

	// A row the trigger wrote keeps its created_at through a later rebuild: the as-of reading depends on it.
	var stamped time.Time
	if err := f.pool.QueryRow(f.ctx,
		`SELECT created_at FROM position_daily_observation WHERE position_id = sha256($1::bytea) AND as_of_date = '2026-01-02'`, id).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	f.rebuild()
	var again time.Time
	if err := f.pool.QueryRow(f.ctx,
		`SELECT created_at FROM position_daily_observation WHERE position_id = sha256($1::bytea) AND as_of_date = '2026-01-02'`, id).Scan(&again); err != nil {
		t.Fatal(err)
	}
	if !again.Equal(stamped) {
		t.Errorf("created_at moved %s -> %s across a rebuild that had nothing to add", stamped, again)
	}
}

// A shadowing schema ahead of public must not capture the rebuild's writes.
func TestPositionDailyRebuildResolvesUnderAShadowingSearchPath(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-shadow", dailyObs{qty: 7, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_daily_observation`); err != nil {
		t.Fatalf("empty the table: %v", err)
	}
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS shadow`,
		`CREATE TABLE IF NOT EXISTS shadow.position_daily_observation (LIKE public.position_daily_observation)`,
		`CREATE TABLE IF NOT EXISTS shadow.position_state (LIKE public.position_state)`,
		`SET search_path = shadow, public`,
	} {
		if _, err := f.pool.Exec(f.ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	f.rebuild()
	var public, shadowed int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM public.position_daily_observation), (SELECT count(*) FROM shadow.position_daily_observation)`).
		Scan(&public, &shadowed); err != nil {
		t.Fatal(err)
	}
	if public != 1 || shadowed != 0 {
		t.Errorf("under a shadowing search_path the rebuild wrote %d rows to public and %d to shadow; want 1 and 0", public, shadowed)
	}
}

// A plain postgres table, which is the house default: no hypertable, no chunks, and no native
// partitioning either. A migration that reaches for create_hypertable again fails here.
func TestPositionDailyIsAPlainTable(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-plain", dailyObs{qty: 1, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	f.observe("d-plain", dailyObs{qty: 2, block: 200, ts: "2026-01-20T00:00:00Z", dealType: "LOAN"})

	// position_state is a hypertable in this same database, read through the same views with the same
	// filter shape: without it, a mistyped name or a renamed catalogue column reads as "plain" and the
	// test passes on the pre-change code too.
	var dimensions, chunks, spineDimensions, spineChunks, jobs int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM timescaledb_information.dimensions WHERE hypertable_name = 'position_daily_observation'),
		       (SELECT count(*) FROM timescaledb_information.chunks     WHERE hypertable_name = 'position_daily_observation'),
		       (SELECT count(*) FROM timescaledb_information.dimensions WHERE hypertable_name = 'position_state'),
		       (SELECT count(*) FROM timescaledb_information.chunks     WHERE hypertable_name = 'position_state'),
		       (SELECT count(*) FROM timescaledb_information.jobs       WHERE hypertable_name = 'position_daily_observation')`).
		Scan(&dimensions, &chunks, &spineDimensions, &spineChunks, &jobs); err != nil {
		t.Fatal(err)
	}
	if spineDimensions < 1 || spineChunks < 1 {
		t.Fatalf("the control reads %d dimension(s) and %d chunk(s) for position_state, which IS a hypertable; "+
			"the catalogue views or the filter are not reporting, so zeroes below prove nothing", spineDimensions, spineChunks)
	}
	if dimensions != 0 || chunks != 0 {
		t.Errorf("position_daily_observation has %d partitioning dimension(s) and %d chunk(s); a plain table has neither", dimensions, chunks)
	}
	if jobs != 0 {
		t.Errorf("position_daily_observation carries %d scheduled policy job(s); the table COMMENT says it has none", jobs)
	}

	var relkind string
	var partitions int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT c.relkind::text, (SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid)
		  FROM pg_class c WHERE c.oid = 'public.position_daily_observation'::regclass`).Scan(&relkind, &partitions); err != nil {
		t.Fatal(err)
	}
	if relkind != "r" || partitions != 0 {
		t.Errorf("position_daily_observation is relkind %q with %d partition(s); want an ordinary table ('r') with none", relkind, partitions)
	}
	if rows := f.rowCount(); rows != 2 {
		t.Errorf("the two observations stored %d row(s), want 2", rows)
	}
}

// The as_of_date CHECK pins both writers' date derivation, and rejects a hand-written mismatch.
func TestPositionDailyAsOfDateIsPinnedToBlockTimestamp(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-date-chk", dailyObs{qty: 5, block: 100, ts: "2026-01-01T23:30:00Z", dealType: "LOAN"})
	if got := f.dayRow("d-date-chk", "2026-01-01")["block_timestamp"]; !strings.HasPrefix(got, "2026-01-01") {
		t.Errorf("block_timestamp = %q, want the 2026-01-01 instant", got)
	}
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily_observation (position_id, as_of_date, instrument_key, holder_id, quantity,
		    block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256('d-date-chk'::bytea), '2026-02-02', 'x', repeat('a', 40), 1, 1, 0, 0, '2026-01-01T23:30:00Z', 'p', 0)`); err == nil {
		t.Error("a row landed on a date its block_timestamp does not fall on; the CHECK is missing")
	} else if !strings.Contains(err.Error(), "check constraint") {
		t.Errorf("rejected for the wrong reason: %v", err)
	}
	var onParent int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_constraint WHERE conrelid = 'position_daily_observation'::regclass AND conname = 'position_daily_observation_as_of_date_chk'`).Scan(&onParent); err != nil {
		t.Fatal(err)
	}
	if onParent != 1 {
		t.Errorf("position_daily_observation_as_of_date_chk is not declared on position_daily_observation (%d), so nothing pins as_of_date to block_timestamp", onParent)
	}
}

// The PK leads with (position_id, as_of_date) so a day's rows are one prefix scan, and the two
// secondary indexes serve the holder series and the whole book on one date. Read from the catalogue
// rather than the indexdef text, so a partial, expression or INCLUDE-only index fails.
func TestPositionDailyIndexesCoverTheHolderAndDateReads(t *testing.T) {
	f := newPositionDailyFixture(t)
	for _, want := range []struct {
		name string
		cols []string
	}{
		{name: "position_daily_observation_pkey", cols: []string{"position_id", "as_of_date", "block_number", "block_version", "processing_version", "block_timestamp"}},
		{name: "position_daily_observation_holder_idx", cols: []string{"holder_id", "as_of_date"}},
		{name: "position_daily_observation_as_of_date_idx", cols: []string{"as_of_date", "position_id"}},
	} {
		var cols []string
		var notPartial, notExpression, noInclude, valid bool
		if err := f.pool.QueryRow(f.ctx, `
			SELECT (SELECT array_agg(a.attname ORDER BY k.ord)
			          FROM unnest(i.indkey[0:i.indnkeyatts-1]) WITH ORDINALITY k(attnum, ord)
			          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum),
			       i.indpred IS NULL, i.indexprs IS NULL, i.indnatts = i.indnkeyatts, i.indisvalid
			  FROM pg_index i WHERE i.indexrelid = ('public.' || $1)::regclass`, want.name).
			Scan(&cols, &notPartial, &notExpression, &noInclude, &valid); err != nil {
			t.Errorf("%s is missing: %v", want.name, err)
			continue
		}
		if !slices.Equal(cols, want.cols) {
			t.Errorf("%s keys on %v, want %v in that order", want.name, cols, want.cols)
		}
		if !notPartial || !notExpression || !noInclude || !valid {
			t.Errorf("%s: partial=%t expression=%t include=%t valid=%t; want a plain, complete, valid btree",
				want.name, !notPartial, !notExpression, !noInclude, valid)
		}
	}

	// Only the PK may be unique. Both writers name it as their ON CONFLICT target, so a UNIQUE index
	// added later would not silently swallow their rows -- but it would reject them outright, and the
	// two secondary indexes are over columns that legitimately repeat once rows are per-batch.
	for _, name := range []string{"position_daily_observation_holder_idx", "position_daily_observation_as_of_date_idx"} {
		var unique bool
		if err := f.pool.QueryRow(f.ctx,
			`SELECT indisunique FROM pg_index WHERE indexrelid = ('public.' || $1)::regclass`, name).Scan(&unique); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if unique {
			t.Errorf("%s is UNIQUE; holder_id and as_of_date both repeat across batches, so it would reject legitimate appends", name)
		}
	}
}

// A NULL as-of bound must raise. Left to the created_at comparison it is NULL for every row, so the
// read returns an empty set and a caller with an unset timestamp reads "held nothing" as an answer.
func TestPositionDailyAsOfRefusesANullBound(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-null-bound", dailyObs{qty: 9, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	var n int
	err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_daily_as_of(NULL::timestamptz)`).Scan(&n)
	if err == nil {
		t.Fatalf("position_daily_as_of(NULL) returned %d row(s) instead of raising; an unset bound reads as an empty series", n)
	}
	if !strings.Contains(err.Error(), "as-of time is required") {
		t.Errorf("raised for the wrong reason: %v", err)
	}
	// Negative control: a real bound still answers, so the guard rejects NULL rather than everything.
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM position_daily_as_of('infinity'::timestamptz)`).Scan(&n); err != nil {
		t.Fatalf("a real bound must still answer: %v", err)
	}
	if n != 1 {
		t.Errorf("a real bound read %d row(s), want 1", n)
	}
}

// The view must expose every column of the table. The function RETURNS SETOF the table, so a later
// ADD COLUMN widens the function; the view's SELECT * was expanded and frozen when it was created, so
// the two documented reads would then disagree on shape and CREATE OR REPLACE VIEW could not fix it.
func TestPositionDailyViewExposesEveryTableColumn(t *testing.T) {
	f := newPositionDailyFixture(t)
	var tableCols, viewCols []string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT array_agg(attname ORDER BY attnum) FROM pg_attribute
		         WHERE attrelid = 'position_daily_observation'::regclass AND attnum > 0 AND NOT attisdropped),
		       (SELECT array_agg(attname ORDER BY attnum) FROM pg_attribute
		         WHERE attrelid = 'position_daily'::regclass AND attnum > 0 AND NOT attisdropped)`).
		Scan(&tableCols, &viewCols); err != nil {
		t.Fatal(err)
	}
	if len(tableCols) == 0 {
		t.Fatal("read no columns for the table, so the comparison below is vacuous")
	}
	if !slices.Equal(tableCols, viewCols) {
		t.Errorf("the view exposes %v; the table has %v -- re-create the view so a consumer of either read sees the same shape",
			viewCols, tableCols)
	}
}

// A holder filter must reach the table as an index condition. holder_id is in the DISTINCT ON key only
// so the planner may push it below: left out, it becomes a post-filter over every row of every
// position, measured at 1.9s against 34ms. A plan assertion is the only thing that catches that,
// because the ANSWER is identical either way.
func TestPositionDailyHolderFilterIsPushedIntoTheScan(t *testing.T) {
	f := newPositionDailyFixture(t)
	for i := range 40 {
		f.observe(fmt.Sprintf("d-push-%02d", i), dailyObs{qty: i + 1, block: 100 + i, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	}
	var holder string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT holder_id FROM position_daily_observation ORDER BY holder_id LIMIT 1`).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	// Planning against a tiny table would choose a seq scan whatever the qual can do, so force the
	// index path: what is under test is whether the qual can descend at all, not the planner's choice.
	if _, err := f.pool.Exec(f.ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	rows, err := f.pool.Query(f.ctx,
		`EXPLAIN SELECT * FROM position_daily WHERE holder_id = $1`, holder)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "position_daily_observation_holder_idx") {
		t.Errorf("a holder filter does not reach position_daily_observation_holder_idx, so it is a post-filter over the whole table:\n%s", joined)
	}
	// And the answer is right, so a plan that reads the index is not reading it wrongly.
	var got int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM position_daily WHERE holder_id = $1`, holder).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Errorf("the holder read returned %d row(s), want 1", got)
	}
}

// The window created_at's COMMENT records, pinned so it cannot be quietly forgotten: created_at is
// TRANSACTION START time but a row becomes visible at COMMIT, so a T taken after a writer began and
// before it committed gains rows afterwards. This is the limit of the reproducibility claim.
func TestPositionDailyAsOfIsUnstableWhileAWriterIsOpen(t *testing.T) {
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
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type)
		VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, repeat('d', 40), 5, 100, 0, 0,
		        '2026-01-01T10:00:00Z', 'public.proj-0', 0, 'LOAN')`, id); err != nil {
		t.Fatalf("append inside the open writer: %v", err)
	}

	// T is taken while that writer is still open, so its row is invisible.
	pinned := f.dbNow()
	countAt := func(at time.Time) int {
		f.t.Helper()
		var n int
		if err := f.pool.QueryRow(f.ctx,
			`SELECT count(*) FROM position_daily_as_of($1) WHERE position_id = sha256($2::bytea) AND as_of_date = $3`,
			at, id, day).Scan(&n); err != nil {
			t.Fatalf("read at the pinned time: %v", err)
		}
		return n
	}
	if before := countAt(pinned); before != 0 {
		t.Fatalf("the open writer's row was visible before it committed (%d rows); this case cannot show the window", before)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	// Same T, and now the row is there: its created_at is the transaction's start, before T.
	after := countAt(pinned)
	if after != 1 {
		t.Errorf("after the commit the same pinned T reads %d row(s), want 1 -- if this is now 0 the stamp "+
			"became commit-ordered and created_at's COMMENT plus this test must be rewritten", after)
	}
}

// Two bulk batches over the same day for many positions: the table holds a row per batch per position,
// the reading is the second batch everywhere, and a rebuild over the populated table -- the DISTINCT ON
// sort the procedure pins work_mem for -- adds nothing.
func TestPositionDailyHoldsARowPerBatchAtBulkAndTheRebuildAddsNothing(t *testing.T) {
	f := newPositionDailyFixture(t)
	const positions = 20000
	seed := func(qty, block int, ts, dealType string) {
		f.t.Helper()
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
	if n := f.rowCount(); n != 2*positions {
		t.Fatalf("two batches over %d positions left %d rows; want %d, one per batch per position -- an upsert leaves %d",
			positions, n, 2*positions, positions)
	}
	f.rebuild()
	if n := f.rowCount(); n != 2*positions {
		t.Errorf("the rebuild changed the row count to %d from %d; every winner was already present", n, 2*positions)
	}
	var newest, total int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FILTER (WHERE quantity = 9 AND deal_type = 'BORROW'), count(*) FROM position_daily`).
		Scan(&newest, &total); err != nil {
		t.Fatal(err)
	}
	if newest != positions || total != positions {
		t.Errorf("the reading carries the second batch for %d of %d positions (%d readings in total); want all %d",
			newest, positions, total, positions)
	}
}

// Applying the migration twice is a no-op, which a manual apply or a restore depends on.
func TestPositionDailyMigrationIsReRunnable(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-rerun", dailyObs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	for _, name := range []string{positionDailyMigration, positionDailyBackfillMigration} {
		raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err := f.pool.Exec(f.ctx, string(raw)); err != nil {
			t.Fatalf("re-applying %s: %v", name, err)
		}
	}
	if got := f.dayQty("d-rerun", "2026-01-01"); got != 5 {
		t.Errorf("the day reads %d after a re-apply, want 5", got)
	}
	if n := f.rowCount(); n != 1 {
		t.Errorf("the re-apply's backfill left %d row(s), want 1: the winner was already present", n)
	}
}

// The reading equals the argmax over position_state per (position, UTC date), on every column the two
// tables share, over randomised out-of-order histories driven through the materializer. Shared
// columns come from the catalogue, so a column dropped from either writer is caught without naming it.
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

			// Re-project everything that has arrived so far, as the runner does, in random batch order.
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

			const dailyGrain = ", (block_timestamp AT TIME ZONE 'utc')::date"
			cols := dailySharedSpineColumns(ctx, t, pool, "position_daily")
			if d := diffDailyCacheAgainstSpineArgmax(ctx, t, pool, "position_daily", dailyGrain, cols); d != "" {
				t.Errorf("after the trigger: %s", d)
			}
			// Every table row is a spine row: the trigger copies, it does not invent.
			var orphans int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM position_daily_observation d
				 WHERE NOT EXISTS (SELECT 1 FROM position_state p
				                    WHERE (p.position_id, p.block_number, p.block_version, p.processing_version, p.block_timestamp)
				                        = (d.position_id, d.block_number, d.block_version, d.processing_version, d.block_timestamp))`).
				Scan(&orphans); err != nil {
				t.Fatal(err)
			}
			if orphans != 0 {
				t.Errorf("%d position_daily_observation row(s) have no position_state row at their coordinate", orphans)
			}

			// A rebuild over the complete table changes nothing; over one missing rows it fills them.
			before := dailyCacheDigest(ctx, t, pool, "position_daily_observation")
			if _, err := pool.Exec(ctx, `CALL rebuild_position_daily()`); err != nil {
				t.Fatalf("rebuild over a complete table: %v", err)
			}
			if dailyCacheDigest(ctx, t, pool, "position_daily_observation") != before {
				t.Error("a rebuild over a complete table changed it")
			}
			if _, err := pool.Exec(ctx, `
				DELETE FROM position_daily_observation WHERE (('x' || substr(md5(position_id::text), 1, 8))::bit(32)::int % 2) = 0`); err != nil {
				t.Fatalf("remove half the rows: %v", err)
			}
			if _, err := pool.Exec(ctx, `CALL rebuild_position_daily()`); err != nil {
				t.Fatalf("rebuild over a table missing rows: %v", err)
			}
			if d := diffDailyCacheAgainstSpineArgmax(ctx, t, pool, "position_daily", dailyGrain, cols); d != "" {
				t.Errorf("the rebuild did not restore the reading: %s", d)
			}
		})
	}
}

// dailySharedSpineColumns lists the columns the reading and position_state both carry, so a comparison
// over them covers deal_type without naming it and cannot silently narrow when a column is added.
func dailySharedSpineColumns(ctx context.Context, t *testing.T, pool *pgxpool.Pool, rel string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT a.attname FROM pg_attribute a
		 WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped AND a.attname <> 'created_at'
		   AND EXISTS (SELECT 1 FROM pg_attribute b
		                WHERE b.attrelid = 'position_state'::regclass AND b.attname = a.attname
		                  AND b.attnum > 0 AND NOT b.attisdropped)
		 ORDER BY a.attname`, rel)
	if err != nil {
		t.Fatalf("shared columns for %s: %v", rel, err)
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
		t.Fatalf("%s shares %d columns with position_state (deal_type present: %v); the comparison would be weak",
			rel, len(out), slices.Contains(out, "deal_type"))
	}
	return out
}

// diffDailyCacheAgainstSpineArgmax compares a relation against the newest position_state row per
// position, at the given grain, over the given columns.
func diffDailyCacheAgainstSpineArgmax(ctx context.Context, t *testing.T, pool *pgxpool.Pool, rel, grain string, cols []string) string {
	t.Helper()
	sel := strings.Join(cols, ", ")
	var onlyOracle, onlyCache int
	var example string
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		WITH ranked AS (
		  SELECT %s, row_number() OVER (PARTITION BY position_id%s
		           ORDER BY block_number DESC, block_version DESC, processing_version DESC,
		                    block_timestamp DESC) rn
		    FROM position_state),
		     oracle AS (SELECT %s FROM ranked WHERE rn = 1),
		     cached AS (SELECT %s FROM %s)
		SELECT (SELECT count(*) FROM (SELECT * FROM oracle EXCEPT SELECT * FROM cached) a),
		       (SELECT count(*) FROM (SELECT * FROM cached EXCEPT SELECT * FROM oracle) b),
		       COALESCE((SELECT a::text FROM (SELECT * FROM oracle EXCEPT SELECT * FROM cached) a LIMIT 1), '')`,
		sel, grain, sel, sel, rel)).Scan(&onlyOracle, &onlyCache, &example); err != nil {
		t.Fatalf("oracle compare on %s: %v", rel, err)
	}
	if onlyOracle == 0 && onlyCache == 0 {
		return ""
	}
	return fmt.Sprintf("%d rows the spine implies the reading lacks, %d the reading holds that the spine does not; oracle-only e.g. %s",
		onlyOracle, onlyCache, example)
}

func dailyCacheDigest(ctx context.Context, t *testing.T, pool *pgxpool.Pool, rel string) string {
	t.Helper()
	var d string
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COALESCE(md5(string_agg(x::text, '|' ORDER BY x::text)), '') FROM %s x`, rel)).Scan(&d); err != nil {
		t.Fatalf("digest %s: %v", rel, err)
	}
	return d
}

// rowImages is every stored row with its physical identity: the whole row, its ctid and its xmin.
// An UPDATE moves ctid and stamps a new xmin, so comparing these catches a rewrite that leaves the
// row COUNT and the winning values untouched -- which a count-based or winners-only check cannot.
func (f *positionDailyFixture) rowImages() map[string]string {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		SELECT (position_id, as_of_date, block_number, block_version, processing_version, block_timestamp)::text,
		       d.ctid::text, d.xmin::text, to_jsonb(d)::text
		  FROM position_daily_observation d`)
	if err != nil {
		f.t.Fatalf("read row images: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var key, ctid, xmin, body string
		if err := rows.Scan(&key, &ctid, &xmin, &body); err != nil {
			f.t.Fatal(err)
		}
		out[key] = ctid + " " + xmin + " " + body
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("row image iteration: %v", err)
	}
	return out
}

// The invariant the whole change rests on: a row, once appended, is never touched again. Every other
// assertion in this file reads the day's WINNER, so a writer that rewrote superseded rows in place
// would pass all of them while silently rewriting the history position_daily_as_of(T) reads.
func TestPositionDailyNeverTouchesAnAppendedRow(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, day = "d-frozen", "2026-01-01"
	f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "BORROW"})
	// A second position, so a batch that touches one cannot be excused for rewriting the other.
	f.observe("d-frozen-peer", dailyObs{qty: 77, block: 150, ts: "2026-01-01T03:00:00Z", dealType: "LOAN"})
	before := f.rowImages()
	if len(before) != 3 {
		t.Fatalf("seeded %d row(s), want 3", len(before))
	}

	for _, step := range []struct {
		name string
		run  func()
	}{
		{"a newer observation for the same day", func() {
			f.observe(id, dailyObs{qty: 30, block: 300, ts: "2026-01-01T09:00:00Z", dealType: "LOAN"})
		}},
		{"a correction at an existing coordinate's day", func() {
			f.observe(id, dailyObs{qty: 44, block: 300, pv: 1, ts: "2026-01-01T09:00:00Z", dealType: "BORROW"})
		}},
		{"an observation on another day", func() {
			f.observe(id, dailyObs{qty: 55, block: 400, ts: "2026-01-02T09:00:00Z", dealType: "LOAN"})
		}},
		{"a rebuild", func() { f.rebuild() }},
	} {
		t.Run(step.name, func(t *testing.T) {
			step.run()
			after := f.rowImages()
			for key, img := range before {
				got, ok := after[key]
				if !ok {
					t.Errorf("the row at %s is gone after %s; appended rows are never removed", key, step.name)
					continue
				}
				if got != img {
					t.Errorf("the row at %s changed after %s:\n  before %s\n  after  %s\nappended rows are never rewritten -- ctid or xmin moving means an UPDATE touched it",
						key, step.name, img, got)
				}
			}
			before = after
		})
	}

	// Controls: the appends really landed, and the day's answer really moved, so the frozen-row
	// assertions above are not passing over a writer that did nothing at all.
	if n := f.dayRows(id, day); n != 4 {
		t.Errorf("the day holds %d row(s) after four appends to it, want 4", n)
	}
	if q := f.dayQty(id, day); q != 44 {
		t.Errorf("the day reads %d, want 44 -- the correction is the newest coordinate", q)
	}
}

// Each leg of the REBUILD's ordering, which the from-empty tests exercise only on block_number. The
// rebuild is what repairs the documented replica-bypass window, and a reorg (block_version) and a
// correction (processing_version) are exactly what it has to get right there.
func TestPositionDailyRebuildOrderingPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		id               string
		base, challenger dailyObs
		want             int
	}{
		{name: "a newer block_version wins", id: "r-bv",
			base:       dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
			challenger: dailyObs{qty: 22, block: 100, bv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"}, want: 22},
		{name: "a newer processing_version wins", id: "r-pv",
			base:       dailyObs{qty: 33, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
			challenger: dailyObs{qty: 44, block: 100, pv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"}, want: 44},
		{name: "a later block_timestamp on the same day wins", id: "r-ts",
			base:       dailyObs{qty: 55, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"},
			challenger: dailyObs{qty: 66, block: 100, ts: "2026-01-01T07:00:00Z", dealType: "LOAN"}, want: 66},
		{name: "an older block does not win at a higher processing_version", id: "r-order",
			base:       dailyObs{qty: 77, block: 200, ts: "2026-01-01T02:00:00Z", dealType: "LOAN"},
			challenger: dailyObs{qty: 88, block: 100, pv: 1, ts: "2026-01-01T03:00:00Z", dealType: "LOAN"}, want: 77},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPositionDailyFixture(t)
			f.observe(tc.id, tc.base)
			f.observe(tc.id, tc.challenger)
			// Empty the table so the REBUILD, not the trigger, is the writer under test.
			if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_daily_observation`); err != nil {
				t.Fatalf("empty the table: %v", err)
			}
			f.rebuild()
			if n := f.rowCount(); n != 1 {
				t.Fatalf("the rebuild appended %d row(s) for one position on one date, want 1", n)
			}
			if got := f.dayQty(tc.id, "2026-01-01"); got != tc.want {
				t.Errorf("the rebuilt day reads %d, want %d", got, tc.want)
			}
		})
	}
}

// The TRIGGER's last ordering leg. position_state's PK carries block_timestamp, so one batch can hold
// two rows sharing every earlier leg on one UTC date; the loser is never appended, so getting this
// wrong makes the day permanently wrong with no trace.
func TestPositionDailyIntraBatchPicksTheLaterInstantAtEqualVersions(t *testing.T) {
	f := newPositionDailyFixture(t)
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type)
		SELECT sha256('d-ts-batch'::bytea), 1, 1, 'inst-d-ts-batch', repeat('a', 40), v.qty, 100, 0, 0,
		       v.ts::timestamptz, 'public.proj-0', 0, 'LOAN'
		FROM (VALUES (11, '2026-01-01T01:00:00Z'), (22, '2026-01-01T09:00:00Z')) AS v(qty, ts)`); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	if n := f.dayRows("d-ts-batch", "2026-01-01"); n != 1 {
		t.Fatalf("one batch left %d row(s) for the day, want 1", n)
	}
	if got := f.dayQty("d-ts-batch", "2026-01-01"); got != 22 {
		t.Errorf("the day reads %d, want 22 -- at equal versions the later instant is the day's observation, "+
			"and the loser is never appended, so this is unrecoverable", got)
	}
}

// The owner revoke's guard, under a NON-superuser owner. The harness owner is a superuser, for whom
// has_table_privilege answers true regardless, so the migration skips the check and its RAISE arm
// never runs in any other test: dropping the guard passes the whole suite.
func TestPositionDailyOwnerRevokeHoldsUnderANonSuperuserOwner(t *testing.T) {
	f := newPositionDailyFixture(t)
	if _, err := f.pool.Exec(f.ctx, `
		DROP ROLE IF EXISTS pd_owner;
		CREATE ROLE pd_owner NOSUPERUSER;
		GRANT pd_owner TO CURRENT_USER;
		ALTER TABLE position_daily_observation OWNER TO pd_owner;
		GRANT UPDATE, DELETE, TRUNCATE ON position_daily_observation TO pd_owner;`); err != nil {
		t.Fatalf("hand the table to a non-superuser owner: %v", err)
	}
	var owner string
	var superuser, hadUpdate bool
	if err := f.pool.QueryRow(f.ctx, `
		SELECT pg_get_userbyid(relowner), (SELECT rolsuper FROM pg_roles WHERE rolname = pg_get_userbyid(relowner)),
		       has_table_privilege(pg_get_userbyid(relowner), 'position_daily_observation', 'UPDATE')
		  FROM pg_class WHERE oid = 'position_daily_observation'::regclass`).Scan(&owner, &superuser, &hadUpdate); err != nil {
		t.Fatal(err)
	}
	if owner != "pd_owner" || superuser {
		t.Fatalf("owner is %q (superuser %v); the guard's arm only runs for a non-superuser owner", owner, superuser)
	}
	if !hadUpdate {
		t.Fatal("the owner does not hold UPDATE before the re-apply, so the revoke below has nothing to remove")
	}

	raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), positionDailyMigration))
	if err != nil {
		t.Fatal(err)
	}
	// The migration raises rather than returning quietly if the revoke did not land, so a failure here
	// IS the guard firing.
	if _, err := f.pool.Exec(f.ctx, string(raw)); err != nil {
		t.Fatalf("re-applying over a non-superuser-owned table: %v", err)
	}
	var stillHasUpdate bool
	if err := f.pool.QueryRow(f.ctx,
		`SELECT has_table_privilege('pd_owner', 'position_daily_observation', 'UPDATE')`).Scan(&stillHasUpdate); err != nil {
		t.Fatal(err)
	}
	if stillHasUpdate {
		t.Error("a non-superuser owner still holds UPDATE after the migration; append-only is not enforced where it matters")
	}
}

// The as-of bound is inclusive, which is what lets a report pin the exact instant it read. A caller
// passing a T it read back from a row must see that row.
func TestPositionDailyAsOfBoundIsInclusive(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, day = "d-bound", "2026-01-01"
	f.observe(id, dailyObs{qty: 12, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	var stamped time.Time
	if err := f.pool.QueryRow(f.ctx,
		`SELECT created_at FROM position_daily_observation WHERE position_id = sha256($1::bytea)`, id).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	var atStamp, justBefore int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM position_daily_as_of($1) WHERE as_of_date = $2),
		       (SELECT count(*) FROM position_daily_as_of($1 - interval '1 microsecond') WHERE as_of_date = $2)`,
		stamped, day).Scan(&atStamp, &justBefore); err != nil {
		t.Fatal(err)
	}
	if atStamp != 1 {
		t.Errorf("a T equal to the row's own created_at reads %d row(s), want 1 -- the bound must be inclusive", atStamp)
	}
	if justBefore != 0 {
		t.Errorf("a T one microsecond earlier reads %d row(s), want 0; without this the case above passes on any bound", justBefore)
	}
}

// The whole-book-on-one-date read must reach its index, as the holder read must reach its own. A
// catalogue check of the index columns passes whether or not any plan can use them.
func TestPositionDailyDateFilterIsPushedIntoTheScan(t *testing.T) {
	f := newPositionDailyFixture(t)
	for i := range 40 {
		f.observe(fmt.Sprintf("d-date-push-%02d", i), dailyObs{qty: i + 1, block: 100 + i, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	}
	f.observe("d-date-push-other", dailyObs{qty: 999, block: 900, ts: "2026-02-02T01:00:00Z", dealType: "LOAN"})
	if _, err := f.pool.Exec(f.ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	rows, err := f.pool.Query(f.ctx, `EXPLAIN SELECT * FROM position_daily WHERE as_of_date = '2026-01-01'`)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if joined := strings.Join(plan, "\n"); !strings.Contains(joined, "position_daily_observation_as_of_date_idx") {
		t.Errorf("a date filter does not reach position_daily_observation_as_of_date_idx:\n%s", joined)
	}
	var got int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM position_daily WHERE as_of_date = '2026-01-01'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 40 {
		t.Errorf("the date read returned %d row(s), want 40", got)
	}
}
