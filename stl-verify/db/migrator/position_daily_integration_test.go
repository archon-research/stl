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

	"github.com/jackc/pgx/v5/pgxpool"
)

const positionDailyMigration = "20260824_120000_create_position_daily.sql"

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

// observe appends one observation to the history, which the trigger propagates to the day's row.
// projection and build_id vary with processing_version, so a SET list that drops either is caught.
// run_id is seeded non-NULL and varies with the coordinate, so the whole-row comparison against the
// winning spine row covers it: a writer that forgets it leaves NULL in the cache.
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

// daily returns one position's series as (as_of_date, quantity) pairs, oldest first.
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

// dayRow is the whole cached row for one (position, date); winner is the same shape read from the spine.
func (f *positionDailyFixture) dayRow(id, date string) map[string]string {
	f.t.Helper()
	return f.rowOf(`SELECT to_jsonb(d) - 'position_id' - 'as_of_date' FROM position_daily d
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

func (f *positionDailyFixture) rebuild() {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, `CALL rebuild_position_daily()`); err != nil {
		f.t.Fatalf("rebuild: %v", err)
	}
}

// One case per leg of the newer-wins comparison, each holding the earlier legs equal and both rows on
// the SAME UTC date so they collide on (position_id, as_of_date) and the comparison actually runs.
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
			keepBase:   true, why: "the comparison must lead with block_number, or a reprocess of old history rolls the day back"},
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
				t.Errorf("the day holds %d; want %d. %s", got, want, tc.why)
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
		t.Errorf("the day holds %d; want 22 -- an older observation arriving later must not win", got)
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

// A correction on the same day replaces that day; a correction across UTC midnight leaves the old date.
func TestPositionDailyCorrectionsLandOnTheirOwnDate(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-corr", dailyObs{qty: 10, block: 100, ts: "2026-01-01T23:00:00Z", dealType: "LOAN"})
	f.observe("d-corr", dailyObs{qty: 15, block: 100, pv: 1, ts: "2026-01-01T23:30:00Z", dealType: "LOAN"})
	f.observe("d-corr", dailyObs{qty: 99, block: 200, ts: "2026-01-02T00:30:00Z", dealType: "LOAN"})
	got := strings.Join(f.daily("d-corr"), ",")
	if got != "2026-01-01=15,2026-01-02=99" {
		t.Errorf("series = %s; want the same-day reprocess to replace 01-01 and the next day to be its own row", got)
	}
}

// Every column of a day's row equals that day's winning spine row, through both writers.
func TestPositionDailyEqualsTheWinningSpineRowOnEveryColumn(t *testing.T) {
	for _, writer := range []string{"trigger", "rebuild"} {
		t.Run(writer, func(t *testing.T) {
			f := newPositionDailyFixture(t)
			const id, date = "d-every-col", "2026-01-01"
			f.observe(id, dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
			f.observe(id, dailyObs{qty: 22, block: 200, pv: 1, ts: "2026-01-01T05:00:00Z", dealType: "BORROW"})
			if writer == "rebuild" {
				f.rebuild()
			}
			got, want := f.dayRow(id, date), f.dayWinner(id, date)
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s: cache %s = %q, winning spine row = %q", writer, k, got[k], v)
				}
			}
			if got["deal_type"] != "BORROW" {
				t.Errorf("%s: deal_type = %q, want BORROW -- the day's winner flipped direction", writer, got["deal_type"])
			}
		})
	}
}

// A deal_type change at a newer coordinate on the same day reaches the cache through both writers.
func TestPositionDailyDealTypeChangeReachesTheCache(t *testing.T) {
	t.Run("trigger", func(t *testing.T) {
		f := newPositionDailyFixture(t)
		const id = "d-dt"
		f.observe(id, dailyObs{qty: 5, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.observe(id, dailyObs{qty: 5, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "BORROW"})
		if got := f.dayRow(id, "2026-01-01")["deal_type"]; got != "BORROW" {
			t.Errorf("the trigger left deal_type = %q, want BORROW", got)
		}
	})
	t.Run("rebuild over a day stale on deal_type alone", func(t *testing.T) {
		f := newPositionDailyFixture(t)
		const id = "d-dt"
		f.observe(id, dailyObs{qty: 5, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.observe(id, dailyObs{qty: 5, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "BORROW"})
		if _, err := f.pool.Exec(f.ctx, `
			UPDATE position_daily SET block_number = 100, block_timestamp = '2026-01-01T01:00:00Z', deal_type = 'LOAN'
			 WHERE position_id = sha256($1::bytea) AND as_of_date = '2026-01-01'`, id); err != nil {
			t.Fatalf("stale the day (owner role): %v", err)
		}
		f.rebuild()
		if got := f.dayRow(id, "2026-01-01")["deal_type"]; got != "BORROW" {
			t.Errorf("the rebuild left deal_type = %q, want BORROW -- is deal_type missing from its DO UPDATE SET list?", got)
		}
	})
}

// A NULL deal_type is carried as NULL.
func TestPositionDailyCarriesANullDealType(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-null", dailyObs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z"})
	if got := f.dayRow("d-null", "2026-01-01")["deal_type"]; got != "NULL" {
		t.Errorf("deal_type = %q, want NULL", got)
	}
}

// One batch carrying several observations of one position on one day picks that day's newest.
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
	got := f.dayRow("d-batch", "2026-01-01")
	if got["quantity"] != "33" || got["block_number"] != "300" || got["deal_type"] != "BORROW" {
		t.Errorf("the day holds quantity %s at block %s deal_type %s; want 33 at 300 BORROW", got["quantity"], got["block_number"], got["deal_type"])
	}
}

// The app role reads and cannot write.
func TestPositionDailyIsTriggerOnlyForTheAppRole(t *testing.T) {
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
			`SELECT has_table_privilege($1, 'position_daily', $2)`, c.role, c.priv).Scan(&got); err != nil {
			t.Fatalf("has_table_privilege(%s, %s): %v", c.role, c.priv, err)
		}
		if got != c.want {
			t.Errorf("%s %s on position_daily = %v; want %v", c.role, c.priv, got, c.want)
		}
	}
}

// The maintainer runs as the owner with a pinned search_path, both mandatory for SECURITY DEFINER.
func TestPositionDailyTriggerFunctionIsSecurityDefinerWithAPinnedPath(t *testing.T) {
	f := newPositionDailyFixture(t)
	var definer bool
	var config []string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT prosecdef, proconfig FROM pg_proc WHERE proname = 'upsert_position_daily'`).Scan(&definer, &config); err != nil {
		t.Fatal(err)
	}
	if !definer {
		t.Error("upsert_position_daily is not SECURITY DEFINER, so the appending role would need a write grant on the cache")
	}
	if !strings.Contains(strings.Join(config, " "), "search_path=") {
		t.Errorf("upsert_position_daily does not pin search_path (proconfig = %v); mandatory on SECURITY DEFINER", config)
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
	for _, want := range []string{"timescaledb.enable_tiered_reads=on", "search_path=pg_catalog, public", "work_mem="} {
		if !strings.Contains(joined, want) {
			t.Errorf("rebuild_position_daily does not pin %q (proconfig = %v)", want, config)
		}
	}
}

// The rebuild is forward-only and reconstructs the cache from history alone.
func TestPositionDailyRebuildIsForwardOnlyAndRebuildable(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-fwd", dailyObs{qty: 11, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	if _, err := f.pool.Exec(f.ctx, `
		UPDATE position_daily SET quantity = 99, block_number = 900, block_timestamp = '2026-01-01T09:00:00Z'
		 WHERE position_id = sha256($1::bytea)`, "d-fwd"); err != nil {
		t.Fatalf("push the day ahead of history (owner role): %v", err)
	}
	f.rebuild()
	if got := f.dayQty("d-fwd", "2026-01-01"); got != 99 {
		t.Errorf("the rebuild pulled a row ahead of history back to %d; forward-only means it must not", got)
	}
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_daily`); err != nil {
		t.Fatalf("empty the cache (owner role): %v", err)
	}
	f.rebuild()
	if got := f.dayQty("d-fwd", "2026-01-01"); got != 11 {
		t.Errorf("after a rebuild from empty the day holds %d; want 11 from history", got)
	}
}

// A shadowing schema ahead of public must not capture the rebuild's writes.
func TestPositionDailyRebuildResolvesUnderAShadowingSearchPath(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-shadow", dailyObs{qty: 7, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_daily`); err != nil {
		t.Fatalf("empty the cache: %v", err)
	}
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS shadow`,
		`CREATE TABLE IF NOT EXISTS shadow.position_daily (LIKE public.position_daily)`,
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
		SELECT (SELECT count(*) FROM public.position_daily), (SELECT count(*) FROM shadow.position_daily)`).
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
		SELECT (SELECT count(*) FROM timescaledb_information.dimensions WHERE hypertable_name = 'position_daily'),
		       (SELECT count(*) FROM timescaledb_information.chunks     WHERE hypertable_name = 'position_daily'),
		       (SELECT count(*) FROM timescaledb_information.dimensions WHERE hypertable_name = 'position_state'),
		       (SELECT count(*) FROM timescaledb_information.chunks     WHERE hypertable_name = 'position_state'),
		       (SELECT count(*) FROM timescaledb_information.jobs       WHERE hypertable_name = 'position_daily')`).
		Scan(&dimensions, &chunks, &spineDimensions, &spineChunks, &jobs); err != nil {
		t.Fatal(err)
	}
	if spineDimensions < 1 || spineChunks < 1 {
		t.Fatalf("the control reads %d dimension(s) and %d chunk(s) for position_state, which IS a hypertable; "+
			"the catalogue views or the filter are not reporting, so zeroes below prove nothing", spineDimensions, spineChunks)
	}
	if dimensions != 0 || chunks != 0 {
		t.Errorf("position_daily has %d partitioning dimension(s) and %d chunk(s); a plain table has neither", dimensions, chunks)
	}
	// The COMMENT promises no compression and no tiering; both are jobs, so this pins it directly.
	if jobs != 0 {
		t.Errorf("position_daily carries %d scheduled policy job(s); the table COMMENT says it has none", jobs)
	}

	// Native declarative partitioning reports zero dimensions and zero chunks too, so exclude it.
	var relkind string
	var partitions int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT c.relkind::text, (SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid)
		  FROM pg_class c WHERE c.oid = 'public.position_daily'::regclass`).Scan(&relkind, &partitions); err != nil {
		t.Fatal(err)
	}
	if relkind != "r" || partitions != 0 {
		t.Errorf("position_daily is relkind %q with %d partition(s); want an ordinary table ('r') with none", relkind, partitions)
	}

	// Negative control: the rows are really there, so the counts above are not zero for want of data.
	var rows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_daily`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Errorf("the two dates stored %d row(s), want 2", rows)
	}
}

// The as_of_date CHECK pins both writers' date derivation, and rejects a hand-written mismatch.
func TestPositionDailyAsOfDateIsPinnedToBlockTimestamp(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-date-chk", dailyObs{qty: 5, block: 100, ts: "2026-01-01T23:30:00Z", dealType: "LOAN"})
	if got := f.dayRow("d-date-chk", "2026-01-01")["block_timestamp"]; !strings.HasPrefix(got, "2026-01-01") {
		t.Errorf("block_timestamp = %q, want the 2026-01-01 instant", got)
	}
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE position_daily SET as_of_date = '2026-02-02' WHERE position_id = sha256($1::bytea)`, "d-date-chk"); err == nil {
		t.Error("as_of_date could be set to a date its block_timestamp does not fall on; the CHECK is missing")
	} else if !strings.Contains(err.Error(), "check constraint") {
		t.Errorf("rejected for the wrong reason: %v", err)
	}
	// And it is declared on the table, so it holds for every row rather than for a subset.
	var onParent int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM pg_constraint WHERE conrelid = 'position_daily'::regclass AND conname = 'position_daily_as_of_date_chk'`).Scan(&onParent); err != nil {
		t.Fatal(err)
	}
	if onParent != 1 {
		t.Errorf("position_daily_as_of_date_chk is not declared on position_daily (%d), so nothing pins as_of_date to block_timestamp", onParent)
	}
}

// Two indexes the PK cannot serve: the holder filter, and a cross-position query for one date -- which
// on a plain table has nothing else to read, where chunk exclusion once pruned it. Read from the
// catalogue rather than the indexdef text, so a partial, expression or INCLUDE-only index fails.
func TestPositionDailyIndexesCoverTheHolderAndDateReads(t *testing.T) {
	f := newPositionDailyFixture(t)
	for _, want := range []struct {
		name string
		cols []string
	}{
		{name: "position_daily_holder_idx", cols: []string{"holder_id", "as_of_date"}},
		{name: "position_daily_as_of_date_idx", cols: []string{"as_of_date"}},
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
		// A partial index covers a slice of the table, and an INCLUDE column is unordered payload, so
		// neither serves the read this index exists for even when the key list looks right.
		if !notPartial || !notExpression || !noInclude || !valid {
			t.Errorf("%s: partial=%t expression=%t include=%t valid=%t; want a plain, complete, valid btree",
				want.name, !notPartial, !notExpression, !noInclude, valid)
		}
	}
}

// The rebuild's DISTINCT ON sorts the whole spine, which is why the procedure pins work_mem. A rebuild
// over a handful of rows never reaches that path, so this one converges a spine big enough to sort.
func TestPositionDailyRebuildConvergesOverABulkSpine(t *testing.T) {
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
	// The trigger caches all of these, so the rebuild below meets rows that already exist and must take
	// its ON CONFLICT DO UPDATE arm in bulk -- emptying the cache first would exercise INSERTs only.
	seed(5, 100, "2026-03-02T00:00:00Z", "LOAN")
	seed(9, 200, "2026-03-02T06:00:00Z", "BORROW")
	var cached int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_daily`).Scan(&cached); err != nil {
		t.Fatal(err)
	}
	if cached != positions {
		t.Fatalf("the trigger cached %d rows before the rebuild, want %d; the rebuild would not meet a populated cache", cached, positions)
	}
	// Stale every cached row (owner role) so the rebuild has to raise all of them.
	if _, err := f.pool.Exec(f.ctx, `
		UPDATE position_daily SET quantity = 5, deal_type = 'LOAN', block_number = 100,
		       block_timestamp = '2026-03-02T00:00:00Z'`); err != nil {
		t.Fatalf("stale the cached rows: %v", err)
	}
	f.rebuild()

	var converged, total int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FILTER (WHERE quantity = 9 AND deal_type = 'BORROW'), count(*) FROM position_daily`).
		Scan(&converged, &total); err != nil {
		t.Fatal(err)
	}
	if converged != positions || total != positions {
		t.Errorf("after the rebuild %d of %d rows carry the newer observation, %d rows total; want all %d",
			converged, total, total, positions)
	}
}

// Applying the migration twice is a no-op, which a manual apply or a restore depends on.
func TestPositionDailyMigrationIsReRunnable(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("d-rerun", dailyObs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), positionDailyMigration))
	if err != nil {
		t.Fatalf("read the migration: %v", err)
	}
	src := string(raw)
	if _, err := f.pool.Exec(f.ctx, src); err != nil {
		t.Fatalf("re-applying the migration: %v", err)
	}
	if got := f.dayQty("d-rerun", "2026-01-01"); got != 5 {
		t.Errorf("the day holds %d after a re-apply, want 5", got)
	}
}

// The cache equals the argmax over position_state per (position, UTC date), on every column the two
// tables share, over randomised out-of-order histories. Shared columns come from the catalogue, so a column dropped from
// either writer's SET list is caught without naming one here -- the assertion the missing deal_type
// would have failed. The spine harness cannot check this: the cache does not exist in that PR.
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

			// A random history puts at most one observation on most dates, so the trigger's UPDATE arm
			// would barely run. Append a later observation on a date already present, per position, which
			// is what forces the same-day collision the arm exists for.
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

			// The daily grain is (position, UTC date), so the oracle partitions by both.
			const dailyGrain = ", (block_timestamp AT TIME ZONE 'utc')::date"
			cols := dailySharedSpineColumns(ctx, t, pool, "position_daily")
			if d := diffDailyCacheAgainstSpineArgmax(ctx, t, pool, "position_daily", dailyGrain, cols); d != "" {
				t.Errorf("after the trigger: %s", d)
			}

			// A rebuild over the converged cache changes nothing; over a lagging one it converges.
			before := dailyCacheDigest(ctx, t, pool, "position_daily")
			if _, err := pool.Exec(ctx, `CALL rebuild_position_daily()`); err != nil {
				t.Fatalf("rebuild over a converged cache: %v", err)
			}
			if dailyCacheDigest(ctx, t, pool, "position_daily") != before {
				t.Error("a rebuild over a converged cache changed it")
			}
			// block_timestamp must stay on as_of_date (a CHECK pins it), so the lag is created by
			// pulling the coordinates back within the row's own day.
			if _, err := pool.Exec(ctx, `
				UPDATE position_daily SET block_number = 0, block_version = 0, processing_version = 0,
				       block_timestamp = as_of_date::timestamptz, quantity = 0, deal_type = NULL`); err != nil {
				t.Fatalf("make the cache lag history: %v", err)
			}
			if _, err := pool.Exec(ctx, `CALL rebuild_position_daily()`); err != nil {
				t.Fatalf("rebuild over a lagging cache: %v", err)
			}
			if d := diffDailyCacheAgainstSpineArgmax(ctx, t, pool, "position_daily", dailyGrain, cols); d != "" {
				t.Errorf("the rebuild did not converge a lagging cache: %s", d)
			}
		})
	}
}

// dailySharedSpineColumns lists the columns the cache and position_state both carry, so a comparison over
// them covers deal_type without naming it and cannot silently narrow when a column is added.
func dailySharedSpineColumns(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT a.attname FROM pg_attribute a
		 WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped AND a.attname <> 'created_at'
		   AND EXISTS (SELECT 1 FROM pg_attribute b
		                WHERE b.attrelid = 'position_state'::regclass AND b.attname = a.attname
		                  AND b.attnum > 0 AND NOT b.attisdropped)
		 ORDER BY a.attname`, cache)
	if err != nil {
		t.Fatalf("shared columns for %s: %v", cache, err)
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
	var hasDealType bool
	for _, c := range out {
		if c == "deal_type" {
			hasDealType = true
		}
	}
	if !hasDealType || len(out) < 9 {
		t.Fatalf("%s shares %d columns with position_state (deal_type present: %v); the comparison would be weak",
			cache, len(out), hasDealType)
	}
	return out
}

// diffDailyCacheAgainstSpineArgmax compares the cache against the newest position_state row per position,
// at the cache's own grain, over the given columns.
func diffDailyCacheAgainstSpineArgmax(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache, grain string, cols []string) string {
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
		sel, grain, sel, sel, cache)).Scan(&onlyOracle, &onlyCache, &example); err != nil {
		t.Fatalf("oracle compare on %s: %v", cache, err)
	}
	if onlyOracle == 0 && onlyCache == 0 {
		return ""
	}
	return fmt.Sprintf("%d rows the spine implies the cache lacks, %d the cache holds that the spine does not; oracle-only e.g. %s",
		onlyOracle, onlyCache, example)
}

func dailyCacheDigest(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache string) string {
	t.Helper()
	var d string
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COALESCE(md5(string_agg(x::text, '|' ORDER BY x::text)), '') FROM %s x`, cache)).Scan(&d); err != nil {
		t.Fatalf("digest %s: %v", cache, err)
	}
	return d
}
