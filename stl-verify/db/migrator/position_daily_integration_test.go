//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// positionDailyMigration is the file the rebuild region is lifted from, so the tests execute the text
// that ships rather than a hand-copied duplicate that lets the migration drift.
const positionDailyMigration = "20260824_120000_create_position_daily.sql"

// positionDailyFixture is one migrated database plus the seeding and reading each case needs. Every
// test takes its own, so no case observes another's rows and no whole-table assertion depends on
// declaration order.
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

// observe appends one observation to the history, which the trigger propagates to the day's row.
// projection and build_id vary with processing_version, so a SET list that drops either is caught.
func (f *positionDailyFixture) observe(id string, qty, block, bv, pv int, ts string) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, substr(md5($1) || md5($1), 1, 40), $2, $3, $4, $5::int, $6,
		        'public.proj-' || ($5::int)::text, $5::int)`,
		id, qty, block, bv, pv, ts); err != nil {
		f.t.Fatalf("observe %s at block %d: %v", id, block, err)
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

// setDay rewrites a cached day's coordinates directly, standing in for a cache that lags history or
// runs ahead of it. block_timestamp must stay on as_of_date, which a CHECK pins.
func (f *positionDailyFixture) setDay(id, date string, qty, block, bv, pv int, ts string) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		UPDATE position_daily
		   SET quantity = $3, block_number = $4, block_version = $5, processing_version = $6,
		       block_timestamp = $7::timestamptz
		 WHERE position_id = sha256($1::bytea) AND as_of_date = $2`,
		id, date, qty, block, bv, pv, ts); err != nil {
		f.t.Fatalf("setDay %s %s: %v", id, date, err)
	}
}

func (f *positionDailyFixture) rebuild() {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, positionDailyRebuildRegion(f.t)); err != nil {
		f.t.Fatalf("rebuild: %v", err)
	}
}

func positionDailyRebuildRegion(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), positionDailyMigration))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	begin, end := "-- REBUILD-BEGIN position_daily\n", "\n-- REBUILD-END position_daily"
	i := strings.Index(string(raw), begin)
	if i < 0 {
		t.Fatalf("migration has no %q marker", begin)
	}
	rest := string(raw)[i+len(begin):]
	j := strings.Index(rest, end)
	if j < 0 {
		t.Fatalf("migration has no %q marker", end)
	}
	stmt := rest[:j]
	if !strings.Contains(stmt, "DO UPDATE") {
		t.Fatalf("rebuild carries no newer-wins DO UPDATE arm; DO NOTHING cannot repair a stale row:\n%s", stmt)
	}
	return stmt
}

// One case per leg of (block_number, block_version, processing_version, block_timestamp), each holding
// the earlier legs equal and both rows on the SAME UTC date so they collide on (position_id, as_of_date)
// and the comparison actually runs.
func TestPositionDailyNewerWinsPrecedence(t *testing.T) {
	cases := []struct {
		name          string
		id            string
		block, bv, pv int
		ts            string
	}{
		{"a newer block wins", "prec-block", 200, 0, 0, "2026-01-01T06:00:00Z"},
		{"a newer block_version at the same block wins", "prec-bv", 100, 1, 0, "2026-01-01T00:00:00Z"},
		{"a newer processing_version at the same block and version wins", "prec-pv", 100, 0, 1, "2026-01-01T00:00:00Z"},
		{"a later block_timestamp at the same block, version and processing_version wins", "prec-ts", 100, 0, 0, "2026-01-01T12:00:00Z"},
	}
	f := newPositionDailyFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.observe(tc.id, 11, 100, 0, 0, "2026-01-01T00:00:00Z")
			f.observe(tc.id, 22, tc.block, tc.bv, tc.pv, tc.ts)
			if got := f.dayQty(tc.id, "2026-01-01"); got != 22 {
				t.Errorf("2026-01-01 = %d; want 22. The comparison is ignoring this key column", got)
			}
			if got := f.daily(tc.id); len(got) != 1 {
				t.Errorf("series = %v; want a single row for the day", got)
			}
		})
	}
}

func TestPositionDailyOlderObservationArrivingLaterCannotRegressTheDay(t *testing.T) {
	// Newest-FIRST, so the guard's REJECT branch runs. With every same-date pair arriving oldest-first
	// only the accept branch executes and the entire WHERE can be deleted with the suite green.
	f := newPositionDailyFixture(t)
	f.observe("reject", 50, 700, 2, 3, "2026-02-01T12:00:00Z")
	f.observe("reject", 99, 700, 1, 9, "2026-02-01T06:00:00Z")
	if got := f.dayQty("reject", "2026-02-01"); got != 50 {
		t.Errorf("2026-02-01 = %d; want 50 -- the older observation regressed the day", got)
	}
}

func TestPositionDailyRetainsEveryObservedDateAndOnlyThose(t *testing.T) {
	// The grain: one row per date the position was OBSERVED, with no carry-forward. A gap day has no
	// row, which the COMMENT states explicitly so a consumer does not read absence as zero.
	f := newPositionDailyFixture(t)
	f.observe("series", 10, 100, 0, 0, "2026-03-01T00:00:00Z")
	f.observe("series", 20, 200, 0, 0, "2026-03-02T00:00:00Z")
	f.observe("series", 30, 300, 0, 0, "2026-03-05T00:00:00Z")
	want := []string{"2026-03-01=10", "2026-03-02=20", "2026-03-05=30"}
	got := f.daily("series")
	if len(got) != len(want) {
		t.Fatalf("series = %v; want %v -- three observed dates, and no row for the 03-03/03-04 gap", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("series[%d] = %s; want %s", i, got[i], want[i])
		}
	}
}

func TestPositionDailySameDayReprocessReplacesThatDay(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("sameday", 50, 800, 0, 0, "2026-04-10T00:00:00Z")
	f.observe("sameday", 55, 800, 0, 1, "2026-04-10T06:00:00Z")
	if got := f.daily("sameday"); len(got) != 1 || got[0] != "2026-04-10=55" {
		t.Errorf("series = %v; want one row for the day at the winning pv, [2026-04-10=55]", got)
	}
}

// as_of_date comes from the winning row's timestamp and the upsert is keyed on it, so a reprocess
// landing on another date writes a NEW row and nothing retracts the old one. Append-only means the
// original still sits on the old date, so this agrees with deriving the grain from position_state.
func TestPositionDailyCorrectionAcrossUTCMidnightLeavesTheOldDate(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("midnight", 10, 900, 0, 0, "2026-05-10T23:30:00Z")
	f.observe("midnight", 20, 900, 0, 1, "2026-05-11T00:30:00Z")
	got := f.daily("midnight")
	if len(got) != 2 || got[0] != "2026-05-10=10" || got[1] != "2026-05-11=20" {
		t.Errorf("series = %v; want both dates present, [2026-05-10=10 2026-05-11=20] -- the superseded row is NOT retracted", got)
	}
}

func TestPositionDailyAsOfDateIsTheWinningObservationsUTCDate(t *testing.T) {
	// 23:30Z pins the derivation westward: under a wrong zone literal or a non-UTC session this date
	// shifts, which a midday fixture cannot detect.
	f := newPositionDailyFixture(t)
	f.observe("asof", 7, 1000, 0, 0, "2026-06-01T23:30:00Z")
	if got := f.daily("asof"); len(got) != 1 || got[0] != "2026-06-01=7" {
		t.Errorf("series = %v; want [2026-06-01=7] -- 23:30Z belongs to 06-01 in UTC", got)
	}
}

func TestPositionDailyIsAHypertableOnAsOfDateWithSevenDayChunks(t *testing.T) {
	f := newPositionDailyFixture(t)
	var column, interval string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT column_name, time_interval::text FROM timescaledb_information.dimensions
		 WHERE hypertable_name = 'position_daily'`).Scan(&column, &interval); err != nil {
		t.Fatalf("position_daily is not a hypertable: %v", err)
	}
	if column != "as_of_date" {
		t.Errorf("partition column = %q; want as_of_date", column)
	}
	if interval != "7 days" {
		t.Errorf("chunk interval = %q; want \"7 days\" -- 1-day chunks measured 2.1x the buffers on a position's history", interval)
	}
}

// position_daily deliberately carries NO compression policy: adding one in good faith, because the
// house rule says to, makes the first whole-day reprocess fail in production. Asserted on the job
// and compression_enabled, not hypertable_compression_settings, which lists it either way.
func TestPositionDailyHasNoCompressionPolicy(t *testing.T) {
	f := newPositionDailyFixture(t)

	const reason = "position_daily's maintainer upserts in place with ON CONFLICT DO UPDATE, and a " +
		"reprocess rewriting a whole day for every position exceeds max_tuples_decompressed_per_dml_" +
		"transaction (measured 100,050 against the 100,000 limit, where the same rows via DO NOTHING " +
		"are free). Compression and this write path are incompatible, not merely costly. VEC-566 " +
		"carries the sanctioned exception or the write-path change; until one lands, a policy here " +
		"breaks the first bulk reprocess in production"

	var jobs int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM timescaledb_information.jobs
		 WHERE hypertable_name = 'position_daily' AND proc_name = 'policy_compression'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 {
		t.Errorf("position_daily has %d compression policy job(s); want none. %s", jobs, reason)
	}

	var enabled bool
	if err := f.pool.QueryRow(f.ctx, `
		SELECT compression_enabled FROM timescaledb_information.hypertables
		 WHERE hypertable_name = 'position_daily'`).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Errorf("position_daily has compression enabled; want it off. %s", reason)
	}

	// Positive control: position_state does carry both, so neither query above is one that returns
	// nothing whatever the state of the database.
	var stateJobs int
	var stateEnabled bool
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM timescaledb_information.jobs
		         WHERE hypertable_name = 'position_state' AND proc_name = 'policy_compression'),
		       (SELECT compression_enabled FROM timescaledb_information.hypertables
		         WHERE hypertable_name = 'position_state')`).Scan(&stateJobs, &stateEnabled); err != nil {
		t.Fatal(err)
	}
	if stateJobs == 0 || !stateEnabled {
		t.Errorf("the control failed: position_state reports %d compression jobs and enabled=%v, so "+
			"these queries cannot detect a policy and the assertions above prove nothing",
			stateJobs, stateEnabled)
	}
}

func TestPositionDailyRowsRouteToChunksByAsOfDate(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("routing", 60, 1100, 0, 0, "2026-08-01T00:00:00Z")
	f.observe("routing", 61, 1200, 0, 0, "2026-08-09T00:00:00Z")
	var chunks int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM timescaledb_information.chunks
		 WHERE hypertable_name = 'position_daily'
		   AND range_start <= '2026-08-09'::timestamptz AND range_end > '2026-08-01'::timestamptz`,
	).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if chunks != 2 {
		t.Errorf("dates 8 days apart occupy %d chunks; want 2 with a 7-day interval", chunks)
	}
}

func TestPositionDailyReprocessIntoAnOlderChunkLeavesTheNewerOne(t *testing.T) {
	f := newPositionDailyFixture(t)
	f.observe("crosschunk", 60, 1300, 0, 0, "2026-09-01T00:00:00Z")
	f.observe("crosschunk", 61, 1400, 0, 0, "2026-09-09T00:00:00Z")
	f.observe("crosschunk", 99, 1300, 0, 1, "2026-09-01T06:00:00Z")
	if got := f.dayQty("crosschunk", "2026-09-01"); got != 99 {
		t.Errorf("2026-09-01 = %d; want 99 -- the reprocess must win in the older chunk", got)
	}
	if got := f.dayQty("crosschunk", "2026-09-09"); got != 61 {
		t.Errorf("2026-09-09 = %d; want 61 -- the newer chunk must be untouched", got)
	}
}

func TestPositionDailyTriggerFunctionPinsSearchPath(t *testing.T) {
	// pg_temp is searched first for RELATION names whatever search_path says, so qualification in the
	// body defends the writes; this pin defends the rest. Asserted from pg_proc because dropping the SET
	// has no reachable behavioural consequence here.
	f := newPositionDailyFixture(t)
	var cfg []string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT coalesce(proconfig, '{}') FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		  WHERE n.nspname = 'public' AND p.proname = 'upsert_position_daily'`).Scan(&cfg); err != nil {
		t.Fatal(err)
	}
	var got string
	for _, kv := range cfg {
		if strings.HasPrefix(kv, "search_path=") {
			got = strings.TrimPrefix(kv, "search_path=")
		}
	}
	if got == "" {
		t.Fatalf("upsert_position_daily has no search_path in proconfig (%v)", cfg)
	}
	if strings.Contains(got, `"$user"`) {
		t.Errorf("search_path = %q includes \"$user\", which resolves per CALLER for a SECURITY "+
			"INVOKER function", got)
	}
}

// The statement trigger decides twice: which row of a batch wins for a (position, date), and whether
// that winner beats the cached row. One row per statement exercises only the second.
func TestPositionDailyIntraBatchPickPerKeyColumn(t *testing.T) {
	holder := strings.Repeat("a", 40)
	cases := []struct {
		name, id string
		rows     [][4]any // (qty, block, bv, pv) -- all on the same UTC date
	}{
		{"block_number", "bd-bn", [][4]any{{1, 700, 0, 0}, {99, 900, 0, 0}, {2, 800, 0, 0}}},
		{"block_version", "bd-bv", [][4]any{{1, 700, 0, 0}, {99, 700, 2, 0}, {2, 700, 1, 0}}},
		{"processing_version", "bd-pv", [][4]any{{1, 700, 0, 0}, {99, 700, 0, 3}, {2, 700, 0, 1}}},
	}
	f := newPositionDailyFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var vals []string
			for _, r := range tc.rows {
				vals = append(vals, fmt.Sprintf(
					`(sha256($1::bytea), 1, 1, $1, $2, %d, %d, %d, %d, '2026-02-01T06:00:00Z'::timestamptz, 'public.p', 0)`,
					r[0], r[1], r[2], r[3]))
			}
			if _, err := f.pool.Exec(f.ctx, `
				INSERT INTO position_state
				    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
				     block_number, block_version, processing_version, block_timestamp, projection, build_id)
				VALUES `+strings.Join(vals, ","), tc.id, holder); err != nil {
				t.Fatal(err)
			}
			var qty int
			if err := f.pool.QueryRow(f.ctx, `
				SELECT quantity FROM position_daily
				 WHERE position_id = sha256($1::bytea) AND as_of_date = '2026-02-01'`, tc.id).Scan(&qty); err != nil {
				t.Fatal(err)
			}
			if qty != 99 {
				t.Errorf("cached quantity = %d; want 99. The intra-batch pick is ignoring %s", qty, tc.name)
			}
		})
	}
}

func TestPositionDailyHolderIndexExists(t *testing.T) {
	f := newPositionDailyFixture(t)
	var def string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'position_daily' AND indexname = 'position_daily_holder_idx'`,
	).Scan(&def); err != nil {
		t.Fatalf("position_daily_holder_idx: %v", err)
	}
	if !strings.Contains(def, "(holder_id, as_of_date)") {
		t.Errorf("index = %q; want columns (holder_id, as_of_date) so a holder's series comes back ordered", def)
	}
}

func TestPositionDailyGrantsAreSelectInsertUpdateOnly(t *testing.T) {
	// Asserted from the catalogue, not from reading the GRANT list: ALTER DEFAULT PRIVILEGES already
	// grants DELETE on every new public table, so only the explicit REVOKE closes it.
	f := newPositionDailyFixture(t)
	for _, c := range []struct {
		role, priv string
		want       bool
	}{
		{"stl_readonly", "SELECT", true},
		{"stl_readonly", "INSERT", false},
		{"stl_readwrite", "SELECT", true},
		{"stl_readwrite", "INSERT", true},
		{"stl_readwrite", "UPDATE", true},
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

func TestPositionDailyConstraintsRejectWhatPositionStateRejects(t *testing.T) {
	const validHolder = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cols := `(position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
	          block_number, block_version, processing_version, block_timestamp, projection, build_id)`
	valid := `sha256('chk'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`
	cases := []struct{ name, values, constraint string }{
		{"a wrong-width position_id", `'\x00'::bytea, '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "id_len"},
		{"a NaN quantity", `sha256('c2'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 'NaN'::numeric, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
		{"a negative quantity", `sha256('c3'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', -1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
		{"an uppercase holder_id", `sha256('c4'::bytea), '2026-01-01', 1, 1, 'i', '` + strings.Repeat("A", 40) + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
		// 40 chars so the length quantifier cannot be what rejects it; this pins the ANCHORING, which a
		// bare-length case leaves open.
		{"a valid holder embedded in a longer string", `sha256('c4b'::bytea), '2026-01-01', 1, 1, 'i', '0x` + strings.Repeat("a", 40) + `!', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
		{"an over-long instrument_key", `sha256('c9'::bytea), '2026-01-01', 1, 1, repeat('k', 2001), '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "instrument_key_len"},
		{"a negative block_number", `sha256('c5'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, -1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
		{"a zero chain_id", `sha256('c6'::bytea), '2026-01-01', 0, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "chain_pos"},
		{"a pre-genesis block_timestamp", `sha256('c7'::bytea), '2008-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2008-01-01T00:00:00Z'::timestamptz, 'p', 0`, "ts_sane"},
		{"an as_of_date that disagrees with block_timestamp", `sha256('c8'::bytea), '2026-01-02', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T23:30:00Z'::timestamptz, 'p', 0`, "as_of_date"},
	}
	f := newPositionDailyFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.pool.Exec(f.ctx, `INSERT INTO position_daily `+cols+` VALUES (`+tc.values+`)`)
			if err == nil {
				t.Fatalf("accepted %s; want rejection by position_daily_%s_chk", tc.name, tc.constraint)
			}
			if !strings.Contains(err.Error(), tc.constraint) {
				t.Errorf("rejected %s with %v; want the %s constraint to fire", tc.name, err, tc.constraint)
			}
		})
	}
	// Control: the same shape with every field valid must be accepted, so the cases above fail for the
	// reason named rather than because the INSERT was malformed.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO position_daily `+cols+` VALUES (`+valid+`) ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("the all-valid control was rejected, so the cases above prove nothing: %v", err)
	}
}

func TestPositionDailyCopiesEveryColumnFromTheWinner(t *testing.T) {
	// projection and build_id are not identity-invariant -- they change when a new build reprocesses --
	// so a DO UPDATE SET omitting either silently freezes provenance on the first build that wrote the day.
	f := newPositionDailyFixture(t)
	f.observe("cols", 10, 1500, 0, 0, "2026-10-01T00:00:00Z")
	f.observe("cols", 20, 1500, 0, 4, "2026-10-01T06:00:00Z")
	var projection string
	var buildID int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT projection, build_id FROM position_daily
		  WHERE position_id = sha256('cols'::bytea) AND as_of_date = '2026-10-01'`,
	).Scan(&projection, &buildID); err != nil {
		t.Fatal(err)
	}
	if projection != "public.proj-4" || buildID != 4 {
		t.Errorf("projection=%q build_id=%d; want public.proj-4 and 4 from the winning observation", projection, buildID)
	}
}

// One case per leg of the BACKFILL's newer-wins WHERE, which was only exercised where its own pick
// was already newest. Each accept case leaves one leg unequal; the reject case covers ordering.
// Both timestamps stay on as_of_date, which a CHECK pins, so the ts leg moves no row to another day.
func TestPositionDailyRebuildNewerWinsPerKeyColumn(t *testing.T) {
	const date = "2026-07-01"
	type coords struct {
		block, bv, pv int
		ts            string
	}
	cases := []struct {
		name       string
		id         string
		history    coords
		cache      coords
		wantRaised bool
	}{
		{
			name:       "a newer block_number in history raises the day",
			id:         "leg-block",
			history:    coords{200, 0, 0, date + "T06:00:00Z"},
			cache:      coords{100, 0, 0, date + "T06:00:00Z"},
			wantRaised: true,
		},
		{
			name:       "a newer block_version in history raises the day",
			id:         "leg-bv",
			history:    coords{100, 1, 0, date + "T06:00:00Z"},
			cache:      coords{100, 0, 0, date + "T06:00:00Z"},
			wantRaised: true,
		},
		{
			name:       "a newer processing_version in history raises the day",
			id:         "leg-pv",
			history:    coords{100, 0, 1, date + "T06:00:00Z"},
			cache:      coords{100, 0, 0, date + "T06:00:00Z"},
			wantRaised: true,
		},
		{
			// Both timestamps stay on as_of_date, which a CHECK pins, so this leg is isolated without
			// moving the row to another day.
			name:       "a later block_timestamp in history raises the day",
			id:         "leg-ts",
			history:    coords{100, 0, 0, date + "T12:00:00Z"},
			cache:      coords{100, 0, 0, date + "T06:00:00Z"},
			wantRaised: true,
		},
		{
			name:    "older history at a higher processing_version does not lower the day",
			id:      "leg-order",
			history: coords{100, 0, 1, date + "T06:00:00Z"},
			cache:   coords{200, 0, 0, date + "T12:00:00Z"},
		},
	}
	const historyQty, cacheQty = 41, 77
	f := newPositionDailyFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.observe(tc.id, historyQty, tc.history.block, tc.history.bv, tc.history.pv, tc.history.ts)
			f.setDay(tc.id, date, cacheQty, tc.cache.block, tc.cache.bv, tc.cache.pv, tc.cache.ts)
			f.rebuild()
			want, why := cacheQty, "the rebuild lowered a cached day; its newer-wins WHERE is not constraining it"
			if tc.wantRaised {
				want = historyQty
				why = "the rebuild did not raise the day, so its comparison is ignoring this key column"
			}
			if got := f.dayQty(tc.id, date); got != want {
				t.Errorf("%s = %d; want %d -- %s", date, got, want, why)
			}
		})
	}
}

// An orphan row -- one whose position has no history -- survives the rebuild, because the merge is
// forward-only. Removing it needs the owner: DELETE and TRUNCATE are both revoked.
func TestPositionDailyRebuildCannotRemoveAnOrphanRow(t *testing.T) {
	f := newPositionDailyFixture(t)
	const date = "2026-07-15"
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256('orphan'::bytea), $1::date, 1, 1, 'inst-orphan', $2, 123, 500, 0, 0,
		        ($1 || 'T06:00:00Z')::timestamptz, 'public.p', 0)`,
		date, strings.Repeat("b", 40)); err != nil {
		t.Fatalf("seed the orphan: %v", err)
	}
	var history int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM position_state WHERE position_id = sha256('orphan'::bytea)`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if history != 0 {
		t.Fatalf("the orphan has %d history rows; the case requires none", history)
	}

	f.rebuild()

	if got := f.dayQty("orphan", date); got != 123 {
		t.Errorf("orphan quantity = %d after the rebuild; want 123 -- the merge is forward-only and must "+
			"not remove a row, so this class stays until the owner deletes it", got)
	}
}

func TestPositionDailyIsRebuildableFromHistory(t *testing.T) {
	// Seeds its own series -- a multi-date position, a same-day reprocess and a closed position -- so the
	// rebuild has version noise to collapse.
	f := newPositionDailyFixture(t)
	f.observe("rb-1", 10, 1800, 0, 0, "2026-12-01T00:00:00Z")
	f.observe("rb-1", 20, 1900, 0, 0, "2026-12-02T00:00:00Z")
	f.observe("rb-2", 30, 2000, 0, 0, "2026-12-03T00:00:00Z")
	f.observe("rb-2", 35, 2000, 0, 1, "2026-12-03T06:00:00Z")
	f.observe("rb-3", 0, 2100, 0, 0, "2026-12-04T00:00:00Z")

	before := f.snapshot()
	if len(before) == 0 {
		t.Fatal("position_daily is empty; nothing to rebuild")
	}
	if _, err := f.pool.Exec(f.ctx, `TRUNCATE position_daily`); err != nil {
		t.Fatal(err)
	}
	f.rebuild()

	after := f.snapshot()
	if len(after) != len(before) {
		t.Errorf("rebuild produced %d (position, date) rows; want %d", len(after), len(before))
	}
	for k, want := range before {
		if got, ok := after[k]; !ok {
			t.Errorf("%s lost by the rebuild", k)
		} else if got != want {
			t.Errorf("%s = %d after rebuild; want %d", k, got, want)
		}
	}
}

func (f *positionDailyFixture) snapshot() map[string]int {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		SELECT encode(position_id,'hex') || '|' || as_of_date::text, quantity FROM position_daily`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var q int
		if err := rows.Scan(&k, &q); err != nil {
			f.t.Fatal(err)
		}
		out[k] = q
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("iterating the position_daily snapshot: %v", err)
	}
	return out
}

func TestPositionDailyRebuildOverwritesARowLandedInTheWindow(t *testing.T) {
	// The window: TRUNCATE commits, ingest appends an OLDER observation, the trigger lands it in the
	// now-empty cache with no conflict, and only then does the rebuild run. Under DO NOTHING the older
	// row is kept and no later insert can correct it.
	f := newPositionDailyFixture(t)
	f.observe("rebuild-race", 10, 1600, 0, 0, "2026-11-01T00:00:00Z")
	f.observe("rebuild-race", 20, 1700, 0, 0, "2026-11-01T06:00:00Z")

	if _, err := f.pool.Exec(f.ctx, `TRUNCATE position_daily`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		SELECT p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
		       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
		       p.processing_version, p.block_timestamp, p.projection, p.build_id
		FROM position_state p
		WHERE p.position_id = sha256('rebuild-race'::bytea) AND p.block_number = 1600`); err != nil {
		t.Fatalf("seed the window: %v", err)
	}
	f.rebuild()
	if got := f.dayQty("rebuild-race", "2026-11-01"); got != 20 {
		t.Errorf("2026-11-01 = %d; want 20. DO NOTHING leaves the older 10 in place, unrepairably", got)
	}
}
