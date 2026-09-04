//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// positionStateCols is the insert column list for position_state, shared so a schema change lands in one
// place rather than in every test that writes history.
const positionStateCols = "position_id, chain_id, protocol_id, instrument_key, holder_id, quantity, " +
	"block_number, block_version, processing_version, block_timestamp, projection, build_id"

const (
	// positionCurrentDDL creates the table, the grants and the maintainer; positionCurrentBackfill
	// carries the marked REBUILD region. They are separate migrations because the migrator runs a whole
	// file in one transaction, which held CREATE TRIGGER's lock on position_state across a full-history
	// scan.
	positionCurrentDDL      = "20260819_150000_create_position_current.sql"
	positionCurrentBackfill = "20260819_150100_backfill_position_current.sql"
)

// orderByPositionIDRE matches an ORDER BY whose FIRST key is position_id, qualified or not. Both writers
// into position_current must sweep its PK in that order: it is the only total order a block_timestamp
// cannot permute, and ordering either writer by time reopens the deadlock class.
var orderByPositionIDRE = regexp.MustCompile(`(?i)ORDER\s+BY\s+(?:[a-z_]+\.)?position_id\b`)

// rebuildOrderByRE matches the rebuild's DISTINCT ON ordering with all four legs in precedence order,
// tolerant of whitespace so reformatting the migration does not read as a behaviour change.
// triggerOrderByRE is the same ordering inside upsert_position_current, over its transition table.
var triggerOrderByRE = regexp.MustCompile(`(?i)ORDER\s+BY\s+n\.position_id\s*,\s*` +
	`n\.block_number\s+DESC\s*,\s*n\.block_version\s+DESC\s*,\s*` +
	`n\.processing_version\s+DESC\s*,\s*n\.block_timestamp\s+DESC`)

var rebuildOrderByRE = regexp.MustCompile(`(?i)ORDER\s+BY\s+p\.position_id\s*,\s*` +
	`p\.block_number\s+DESC\s*,\s*p\.block_version\s+DESC\s*,\s*` +
	`p\.processing_version\s+DESC\s*,\s*p\.block_timestamp\s+DESC`)

// positionCurrentFixture is one migrated database plus the seeding and reading each position_current
// case needs. Every test takes its own, so no case can observe another's rows and no whole-table
// assertion depends on declaration order.
type positionCurrentFixture struct {
	ctx  context.Context
	t    *testing.T
	pool *pgxpool.Pool
}

func newPositionCurrentFixture(t *testing.T) *positionCurrentFixture {
	t.Helper()
	ctx := context.Background()
	// setupMigratedPostgres also disables the scheduled jobs: position_state registers a 2-day
	// compression policy and every fixture here is stamped 2026-01..10, immediately eligible, so
	// policy_compression could otherwise fire mid-test and take AccessExclusiveLock per chunk.
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
	return &positionCurrentFixture{ctx: ctx, t: t, pool: pool}
}

// observe appends one observation of id to the history, which the trigger propagates to the cache.
func (f *positionCurrentFixture) observe(id string, qty, block, bv, pv int, ts string) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (`+positionStateCols+`)
		VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, substr(md5($1) || md5($1), 1, 40), $2, $3, $4, $5::int, $6,
		        'public.proj-' || ($5::int)::text, $5::int)`,
		id, qty, block, bv, pv, ts); err != nil {
		f.t.Fatalf("observe %s at block %d: %v", id, block, err)
	}
}

func (f *positionCurrentFixture) current(id string) (qty, block, bv int) {
	f.t.Helper()
	if err := f.pool.QueryRow(f.ctx,
		`SELECT quantity, block_number, block_version FROM position_current
		  WHERE position_id = sha256($1::bytea)`, id).Scan(&qty, &block, &bv); err != nil {
		f.t.Fatalf("current(%s): %v", id, err)
	}
	return
}

func (f *positionCurrentFixture) currentDate(id string) string {
	f.t.Helper()
	var d string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT as_of_date::text FROM position_current WHERE position_id = sha256($1::bytea)`,
		id).Scan(&d); err != nil {
		f.t.Fatalf("currentDate(%s): %v", id, err)
	}
	return d
}

// setCache rewrites the cached row's coordinates directly, standing in for a cache that lags history or
// runs ahead of it. as_of_date is derived here too, because a CHECK pins the two together.
func (f *positionCurrentFixture) setCache(id string, qty, block, bv, pv int, ts string) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		UPDATE position_current
		   SET quantity = $2, block_number = $3, block_version = $4, processing_version = $5,
		       block_timestamp = $6::timestamptz, as_of_date = ($6::timestamptz AT TIME ZONE 'utc')::date
		 WHERE position_id = sha256($1::bytea)`, id, qty, block, bv, pv, ts); err != nil {
		f.t.Fatalf("setCache %s: %v", id, err)
	}
}

func (f *positionCurrentFixture) rebuild() {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, rebuildRegion(f.t)); err != nil {
		f.t.Fatalf("rebuild: %v", err)
	}
}

func migrationSource(t *testing.T, filename string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), filename))
	if err != nil {
		t.Fatalf("read %s: %v", filename, err)
	}
	return string(raw)
}

// rebuildRegion lifts the rebuild statement out of the migration by its markers, so the tests execute
// the text that actually ships. A hand-copied duplicate lets the migration drift: change its backfill
// and a test asserting on its own copy stays green.
func rebuildRegion(t *testing.T) string {
	t.Helper()
	src := migrationSource(t, positionCurrentBackfill)
	begin, end := "-- REBUILD-BEGIN position_current\n", "\n-- REBUILD-END position_current"
	i := strings.Index(src, begin)
	if i < 0 {
		t.Fatalf("%s has no %q marker", positionCurrentBackfill, begin)
	}
	rest := src[i+len(begin):]
	j := strings.Index(rest, end)
	if j < 0 {
		t.Fatalf("%s has no %q marker", positionCurrentBackfill, end)
	}
	stmt := rest[:j]
	if !strings.Contains(stmt, "DO UPDATE") {
		t.Fatalf("rebuild carries no newer-wins DO UPDATE arm; DO NOTHING cannot repair a stale row:\n%s", stmt)
	}
	return stmt
}

// rebuildGuard returns a statement calling the shipped precondition check on its own, so a test can run
// it in a transaction of its own choosing. The check lives in the DDL migration as a function and the
// rebuild calls it BOTH standalone and from INSIDE its INSERT: a preceding statement alone is steppable
// by any client with ON_ERROR_STOP off, and an in-INSERT qual alone is planned away when every chunk is
// excluded at plan time.
func rebuildGuard(t *testing.T) string {
	t.Helper()
	// Comment text is STRIPPED first, per this file's convention: without that, commenting out the
	// statement while leaving the literal in a comment satisfies every check here. The DDL carries the
	// name twice -- the CREATE and its COMMENT ON -- so the CREATE is matched in full.
	ddl := lineCommentRE.ReplaceAllString(migrationSource(t, positionCurrentDDL), "")
	if !strings.Contains(ddl, "CREATE OR REPLACE FUNCTION public.position_current_rebuild_guard()") {
		t.Fatalf("%s does not create position_current_rebuild_guard; the rebuild has no enforceable guard", positionCurrentDDL)
	}
	region := lineCommentRE.ReplaceAllString(rebuildRegion(t), "")
	if !strings.Contains(region, "WHERE (SELECT public.position_current_rebuild_guard())") {
		t.Fatal("the REBUILD region does not call the guard from inside its INSERT, so a client with " +
			"ON_ERROR_STOP off would step over it and rebuild anyway")
	}
	if !strings.Contains(region, "\nSELECT public.position_current_rebuild_guard();") {
		t.Fatal("the REBUILD region does not call the guard as a standalone statement, so a zero-chunk " +
			"or fully tiered position_state plans the in-INSERT call away and never evaluates it")
	}
	return "SELECT public.position_current_rebuild_guard();"
}

// hasMarkerLine reports whether marker appears as a line of its own, ignoring surrounding whitespace.
func hasMarkerLine(src, marker string) bool {
	for _, line := range strings.Split(src, "\n") {
		if strings.TrimSpace(line) == marker {
			return true
		}
	}
	return false
}

// Newer-wins compares (block_number, block_version, processing_version, block_timestamp) in that order.
// One case per leg, each isolating that leg by holding the earlier ones equal, so a comparison that
// drops any single leg fails a specific case. The challenger always carries quantity 22.
func TestPositionCurrentNewerWinsPrecedence(t *testing.T) {
	type observation struct {
		qty, block, bv, pv int
		ts                 string
	}
	cases := []struct {
		name       string
		id         string
		base       observation
		challenger observation
		// keepBase inverts the expectation: the challenger must LOSE and the base stay current.
		keepBase bool
	}{
		{
			// block_timestamp is held EQUAL, so the block_number leg is the only thing that can decide.
			name:       "a newer block wins",
			id:         "prec-block",
			base:       observation{11, 100, 0, 0, "2026-01-01T00:00:00Z"},
			challenger: observation{22, 200, 0, 0, "2026-01-01T00:00:00Z"},
		},
		{
			// The ordering the design rests on: block_number FIRST. An OLDER block carrying a HIGHER
			// processing_version must NOT win, or a reprocess of old history rolls a position's balance
			// back. This is the case that fails if the comparison leads with anything else.
			name:       "an older block does not win even at a higher processing_version",
			id:         "prec-order",
			base:       observation{11, 200, 0, 0, "2026-01-02T00:00:00Z"},
			challenger: observation{22, 100, 0, 1, "2026-01-03T00:00:00Z"},
			keepBase:   true,
		},
		{
			// The documented reorg consequence, pinned: a replacement landing at a LOWER block does not
			// win, so the orphaned observation stays current.
			name:       "a lower-block reorg replacement does not displace the orphan",
			id:         "prec-reorg",
			base:       observation{500, 200, 0, 0, "2026-01-02T00:00:00Z"},
			challenger: observation{0, 150, 1, 0, "2026-01-03T00:00:00Z"},
			keepBase:   true,
		},
		{
			name:       "a newer block_version at the same block wins",
			id:         "prec-bv",
			base:       observation{11, 100, 0, 0, "2026-01-01T00:00:00Z"},
			challenger: observation{22, 100, 1, 0, "2026-01-01T00:00:00Z"},
		},
		{
			name:       "a newer processing_version at the same block and block_version wins",
			id:         "prec-pv",
			base:       observation{11, 100, 0, 0, "2026-01-01T00:00:00Z"},
			challenger: observation{22, 100, 0, 1, "2026-01-01T00:00:00Z"},
		},
		{
			// position_state's PK has five columns because block_timestamp is the partition column, so a
			// pair differing only in it is legal even though the sanctioned write path does not produce
			// one. The fourth leg is defensive; without it this case is a coin flip.
			name:       "a later block_timestamp at the same block, block_version and processing_version wins",
			id:         "prec-ts",
			base:       observation{11, 100, 0, 0, "2026-01-01T00:00:00Z"},
			challenger: observation{22, 100, 0, 0, "2026-01-02T00:00:00Z"},
		},
	}
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.observe(tc.id, tc.base.qty, tc.base.block, tc.base.bv, tc.base.pv, tc.base.ts)
			f.observe(tc.id, tc.challenger.qty, tc.challenger.block, tc.challenger.bv, tc.challenger.pv, tc.challenger.ts)
			want := tc.challenger.qty
			why := "The comparison is ignoring this key column"
			if tc.keepBase {
				want = tc.base.qty
				why = "The challenger is older on block_number and must not win; the comparison is not leading with block_number"
			}
			if qty, _, _ := f.current(tc.id); qty != want {
				t.Errorf("current = %d; want %d. %s", qty, want, why)
			}
		})
	}
}

func TestPositionCurrentWinnerIsNotDecidedByArrivalOrder(t *testing.T) {
	// The mirror of the precedence cases: the same pair inserted newest-FIRST must still resolve to the
	// newest, so an older row arriving late cannot regress the cache. Without this, only the guard's
	// accept branch is ever executed and the WHERE could be deleted outright.
	f := newPositionCurrentFixture(t)
	f.observe("arrival", 30, 600, 0, 0, "2026-03-01T00:00:00Z")
	f.observe("arrival", 99, 400, 0, 0, "2026-02-01T00:00:00Z")
	if qty, block, _ := f.current("arrival"); qty != 30 || block != 600 {
		t.Errorf("current = qty %d at block %d; want 30 at 600, the older backfill regressed it", qty, block)
	}
}

func TestPositionCurrentOlderObservationAtSameBlockCannotRegressTheCache(t *testing.T) {
	// Newest-first at equal block_number, so the reject branch of every later leg is exercised too.
	f := newPositionCurrentFixture(t)
	f.observe("reject", 50, 700, 2, 3, "2026-04-02T00:00:00Z")
	f.observe("reject", 99, 700, 1, 9, "2026-04-01T00:00:00Z")
	if qty, _, bv := f.current("reject"); qty != 50 || bv != 2 {
		t.Errorf("current = qty %d at bv %d; want 50 at bv 2", qty, bv)
	}
}

func TestPositionCurrentClosingZeroIsCurrentNotSkipped(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("closing", 5, 100, 0, 0, "2026-01-01T00:00:00Z")
	f.observe("closing", 0, 150, 0, 0, "2026-01-01T12:00:00Z")
	if qty, block, _ := f.current("closing"); qty != 0 || block != 150 {
		t.Errorf("current = qty %d at block %d; want the closing 0 at 150", qty, block)
	}
}

func TestPositionCurrentAsOfDateIsTheWinningObservationsUTCDate(t *testing.T) {
	// 23:30Z pins the UTC derivation in the westward direction; a midday fixture cannot detect a shift.
	f := newPositionCurrentFixture(t)
	f.observe("asof", 30, 600, 0, 0, "2026-03-01T23:30:00Z")
	f.observe("asof", 99, 400, 0, 0, "2026-02-01T23:30:00Z")
	if got := f.currentDate("asof"); got != "2026-03-01" {
		t.Errorf("currentDate(asof) = %s; want 2026-03-01, the winning observation's UTC date", got)
	}
}

func TestPositionCurrentIsAMaterializedTableNotAView(t *testing.T) {
	f := newPositionCurrentFixture(t)
	var relkind string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT relkind FROM pg_class WHERE oid = 'public.position_current'::regclass`).Scan(&relkind); err != nil {
		t.Fatal(err)
	}
	if relkind != "r" {
		t.Errorf("position_current relkind = %q; want \"r\" (an ordinary table). \"v\" means it is still a view", relkind)
	}
}

func TestPositionCurrentIsAPlainTableNotAHypertable(t *testing.T) {
	// One row per position and no time dimension, so the hypertable rule does not engage and there is no
	// compression or tiering policy to want. Same shape as #733's current-position caches.
	f := newPositionCurrentFixture(t)
	var n int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'position_current'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("position_current is a hypertable; want a plain table (no time dimension to partition on)")
	}
}

func TestPositionCurrentHolderIndexExists(t *testing.T) {
	// The PK serves position_id lookups; enriched views on this layer filter by holder, which it cannot
	// serve. Measured at 200,000 positions: 4,652 buffers -> 1 with this index.
	f := newPositionCurrentFixture(t)
	var def string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'position_current' AND indexname = 'position_current_holder_idx'`,
	).Scan(&def); err != nil {
		t.Fatalf("position_current_holder_idx: %v", err)
	}
	if !strings.Contains(def, "(holder_id)") {
		t.Errorf("index = %q; want columns (holder_id)", def)
	}
}

func TestPositionCurrentGrantsAreSelectInsertUpdateOnly(t *testing.T) {
	// The trigger and the backfill only insert or overwrite, so a delete channel would be unused reach,
	// and the remove-rows path stays owner-only. Asserted from the catalogue because the harness
	// superuser bypasses ACLs.
	f := newPositionCurrentFixture(t)
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
			`SELECT has_table_privilege($1, 'position_current', $2)`, c.role, c.priv).Scan(&got); err != nil {
			t.Fatalf("has_table_privilege(%s, %s): %v", c.role, c.priv, err)
		}
		if got != c.want {
			t.Errorf("%s %s on position_current = %v; want %v", c.role, c.priv, got, c.want)
		}
	}
}

// The trigger's upsert carries an ON CONFLICT ... DO UPDATE arm and the function is SECURITY INVOKER, so
// the arm is refused at executor start -- with the writer's own privileges, conflict or not. UPDATE here
// is therefore a precondition for INSERT into position_state, and a narrower materializer role (VEC-562)
// granted only INSERT on the spine would fail every statement.
func TestPositionStateWritersHoldUpdateOnPositionCurrent(t *testing.T) {
	f := newPositionCurrentFixture(t)
	const uncoupled = `
		SELECT r.rolname FROM pg_roles r
		 WHERE has_table_privilege(r.oid, 'public.position_state', 'INSERT')
		   AND NOT has_table_privilege(r.oid, 'public.position_current', 'UPDATE')
		 ORDER BY r.rolname`
	list := func() []string {
		t.Helper()
		rows, err := f.pool.Query(f.ctx, uncoupled)
		if err != nil {
			t.Fatalf("query uncoupled roles: %v", err)
		}
		defer rows.Close()
		var names []string
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			names = append(names, n)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return names
	}

	if got := list(); len(got) != 0 {
		t.Errorf("roles hold INSERT on position_state without UPDATE on position_current: %v. Every "+
			"statement they run against the spine fails at executor start, because the trigger's "+
			"ON CONFLICT DO UPDATE arm is checked with the writer's privileges", got)
	}

	// Negative control: without it this passes on any database where nothing holds INSERT at all.
	if _, err := f.pool.Exec(f.ctx, `REVOKE UPDATE ON position_current FROM stl_readwrite`); err != nil {
		t.Fatal(err)
	}
	broken := list()
	if _, err := f.pool.Exec(f.ctx, `GRANT UPDATE ON position_current TO stl_readwrite`); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range broken {
		if n == "stl_readwrite" {
			found = true
		}
	}
	if !found {
		t.Errorf("with UPDATE revoked the query reported %v, not stl_readwrite; it cannot detect the "+
			"coupling it exists to assert", broken)
	}
}

func TestPositionCurrentUTCDerivationIgnoresSessionTimeZone(t *testing.T) {
	// A westward zone leaves the derivation's result unchanged, so only an EASTWARD zone separates UTC
	// from session-local: at UTC+9, 23:30Z is the next local day.
	f := newPositionCurrentFixture(t)
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(f.ctx, `SET TIME ZONE 'Asia/Tokyo'`); err != nil {
		t.Fatal(err)
	}
	// The pool has no AfterRelease reset, so without this the connection returns carrying Asia/Tokyo and
	// any later caller drawing it runs under a non-default TimeZone.
	defer func() {
		if _, err := conn.Exec(f.ctx, `RESET TIME ZONE`); err != nil {
			t.Errorf("reset TimeZone before releasing: %v", err)
		}
	}()
	if _, err := conn.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (position_id(1,1,'i-tz',$1), 1, 1, 'i-tz', $1, 5,
		        900, 0, 0, '2026-05-01T23:30:00Z'::timestamptz, 'public.p', 0)`,
		strings.Repeat("a", 40)); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT as_of_date::text FROM position_current WHERE position_id = position_id(1,1,'i-tz',$1)`,
		strings.Repeat("a", 40)).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "2026-05-01" {
		t.Errorf("as_of_date = %s under Asia/Tokyo; want 2026-05-01 (the UTC date). The derivation is "+
			"following the session TimeZone, so the same observation dates differently per writer", got)
	}
}

// A cache-only correction is self-reverting, which is why the plain UPDATE this table's record
// documents repairs the ahead-of-history class ONLY. An UPDATE that changes a payload column without
// moving the coordinates leaves the row at coordinates EQUAL to history, which the converge arm exists
// to overwrite, so the next rebuild reinstates the spine's value. A wrong value at correct coordinates
// is corrected by appending a higher-processing_version spine row instead.
func TestPositionCurrentCacheOnlyCorrectionSelfReverts(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("selfrevert", 100, 100, 0, 0, "2026-01-01T00:00:00Z")
	if qty, _, _ := f.current("selfrevert"); qty != 100 {
		t.Fatalf("cache = %d before the operator write; want 100 from history", qty)
	}

	// The operator "fix": payload only, coordinates untouched.
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE position_current SET quantity = 999 WHERE position_id = sha256($1::bytea)`,
		"selfrevert"); err != nil {
		t.Fatal(err)
	}
	if qty, _, _ := f.current("selfrevert"); qty != 999 {
		t.Fatalf("the operator UPDATE did not take (%d); the assertion below would prove nothing", qty)
	}

	f.rebuild()
	if qty, _, _ := f.current("selfrevert"); qty != 100 {
		t.Errorf("cache = %d after the rebuild; want 100. A cache-only correction must self-revert, "+
			"otherwise the converge arm is not overwriting an equal-coordinate payload drift and the "+
			"record's \"append a higher processing_version instead\" guidance is wrong", qty)
	}
}

// The same coordinates cannot simply be re-appended to the spine, so the trigger reverts a cache-only
// correction only when a LATER observation happens to land at exactly the coordinates the operator wrote.
func TestPositionStateRejectsASameCoordinateReAppend(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("dupcoord", 100, 100, 0, 0, "2026-01-01T00:00:00Z")
	_, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, substr(md5($1) || md5($1), 1, 40), 555,
		        100, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'public.p', 0)`, "dupcoord")
	if err == nil {
		t.Fatal("the spine accepted a second row at identical coordinates; the PK no longer covers " +
			"(position_id, block_number, block_version, processing_version, block_timestamp)")
	}
	if !strings.Contains(err.Error(), "23505") && !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("re-append failed with %v; want a unique-violation from the spine PK", err)
	}
}

func TestPositionCurrentRebuildDoesNotRegressARowAheadOfHistory(t *testing.T) {
	// The state the backfill's own newer-wins WHERE exists for: a cached row NEWER than anything history
	// holds. It must survive the rebuild: the merge is forward-only, and lowering a cached row on the
	// strength of older history is the one write it must never make. Repairing it is a plain in-role UPDATE.
	f := newPositionCurrentFixture(t)
	f.observe("ahead", 10, 100, 0, 0, "2026-01-01T00:00:00Z")
	f.setCache("ahead", 77, 5000, 0, 0, "2026-02-01T00:00:00Z")
	f.rebuild()
	if qty, block, _ := f.current("ahead"); qty != 77 || block != 5000 {
		t.Errorf("rebuild regressed a row ahead of history to qty=%d block=%d; want 77/5000. The "+
			"backfill's newer-wins WHERE is not constraining it", qty, block)
	}
}

// One case per leg of the BACKFILL's newer-wins WHERE. The trigger's four-leg pick is covered elsewhere;
// this statement's own comparison was only ever exercised where its pick was already the newest thing in
// the table, so dropping or reordering a leg here passed. Each accept case leaves exactly one leg
// unequal, so dropping that leg makes the comparison false and the cache is not raised; the reject case
// covers ordering, where a leading pv would let older history win.
func TestPositionCurrentRebuildNewerWinsPerKeyColumn(t *testing.T) {
	type coords struct {
		block, bv, pv int
		ts            string
	}
	cases := []struct {
		name    string
		id      string
		history coords
		cache   coords
		// wantRaised expects the rebuild to lift the cache to history's quantity; otherwise the cache's
		// own marker quantity must survive untouched.
		wantRaised bool
	}{
		{
			name:       "a newer block_number in history raises the cache",
			id:         "leg-block",
			history:    coords{200, 0, 0, "2026-07-01T00:00:00Z"},
			cache:      coords{100, 0, 0, "2026-07-01T00:00:00Z"},
			wantRaised: true,
		},
		{
			name:       "a newer block_version in history raises the cache",
			id:         "leg-bv",
			history:    coords{100, 1, 0, "2026-07-01T00:00:00Z"},
			cache:      coords{100, 0, 0, "2026-07-01T00:00:00Z"},
			wantRaised: true,
		},
		{
			name:       "a newer processing_version in history raises the cache",
			id:         "leg-pv",
			history:    coords{100, 0, 1, "2026-07-01T00:00:00Z"},
			cache:      coords{100, 0, 0, "2026-07-01T00:00:00Z"},
			wantRaised: true,
		},
		{
			name:       "a later block_timestamp in history raises the cache",
			id:         "leg-ts",
			history:    coords{100, 0, 0, "2026-07-02T00:00:00Z"},
			cache:      coords{100, 0, 0, "2026-07-01T00:00:00Z"},
			wantRaised: true,
		},
		{
			name:    "older history at a higher processing_version does not lower the cache",
			id:      "leg-order",
			history: coords{100, 0, 1, "2026-07-03T00:00:00Z"},
			cache:   coords{200, 0, 0, "2026-07-01T00:00:00Z"},
		},
	}
	const historyQty, cacheQty = 41, 77
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.observe(tc.id, historyQty, tc.history.block, tc.history.bv, tc.history.pv, tc.history.ts)
			f.setCache(tc.id, cacheQty, tc.cache.block, tc.cache.bv, tc.cache.pv, tc.cache.ts)
			f.rebuild()
			want, why := cacheQty, "the rebuild lowered a cached row; its newer-wins WHERE is not constraining it"
			if tc.wantRaised {
				want = historyQty
				why = "the rebuild did not raise the cache, so its comparison is ignoring this key column"
			}
			if qty, _, _ := f.current(tc.id); qty != want {
				t.Errorf("current = %d; want %d -- %s", qty, want, why)
			}
		})
	}
}

// The guard the rebuild region calls, executed here on its own so the transaction shape is the test's to
// choose. Outside one transaction every SET LOCAL in the region is an inert warning, which would run the
// statement with no tiered-read guarantee and no lock bound while reporting a healthy INSERT 0 N.
func TestPositionCurrentRebuildGuardRequiresOneTransaction(t *testing.T) {
	f := newPositionCurrentFixture(t)
	guard := rebuildGuard(t)

	// No stamp in this transaction, which is what a statement-at-a-time apply produces.
	if _, err := f.pool.Exec(f.ctx, guard); err == nil {
		t.Error("the guard passed with no transaction stamp; a statement-at-a-time apply would run the " +
			"rebuild with every SET LOCAL inert")
	} else if !strings.Contains(err.Error(), "inside ONE transaction") {
		t.Errorf("guard raised %v; want the one-transaction message", err)
	}

	// A session carrying pre-seeded values, which is the case a fixed sentinel or a lock_timeout proxy
	// cannot tell from a correct run: both read back the value they expect while every SET LOCAL in the
	// region was an inert warning. The stamp cannot be faked this way because it must equal the LIVE
	// transaction id.
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`SET lock_timeout = '10s'`,
		`SET position_current.rebuild_xid = '999999'`,
	} {
		if _, err := conn.Exec(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	_, guardErr := conn.Exec(f.ctx, guard)
	if _, err := conn.Exec(f.ctx, `RESET ALL`); err != nil {
		t.Errorf("reset the session before releasing: %v", err)
	}
	conn.Release()
	if guardErr == nil {
		t.Error("the guard passed on a session with a pre-seeded stamp and lock_timeout, so it is not " +
			"proving the SET LOCALs took effect -- only that some value is readable")
	}

	// Control: stamped and read back inside one transaction, it must pass -- otherwise the cases above
	// prove nothing about the stamp.
	tx, err2 := f.pool.Begin(f.ctx)
	if err2 != nil {
		t.Fatal(err2)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	if _, err := tx.Exec(f.ctx, `SELECT set_config('position_current.rebuild_xid', pg_current_xact_id()::text, true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(f.ctx, guard); err != nil {
		t.Errorf("the guard rejected a correctly stamped transaction: %v", err)
	}
}

func TestPositionCurrentRebuildGuardRejectsTieredReadsOff(t *testing.T) {
	// The failure this exists for reports INSERT 0 N with no error: a rebuild over local chunks computes
	// "newest per key" across a PARTIAL table once cold chunks are tiered.
	f := newPositionCurrentFixture(t)
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	for _, q := range []string{
		`SELECT set_config('position_current.rebuild_xid', pg_current_xact_id()::text, true)`,
		`SET LOCAL timescaledb.enable_tiered_reads = 'off'`,
	} {
		if _, err := tx.Exec(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(f.ctx, rebuildGuard(t))
	if err == nil {
		t.Fatal("the guard passed with tiered reads off; the rebuild would compute newest-per-key over " +
			"local chunks only and report a byte-identical INSERT 0 N")
	}
	if !strings.Contains(err.Error(), "enable_tiered_reads") {
		t.Errorf("guard raised %v; want the tiered-reads message", err)
	}
}

// Documented invariants no behavioural case inside one transaction can reach, asserted over the source.
// Comment text is STRIPPED first: without that every check here is satisfiable by leaving the literal in
// a comment while commenting out the statement.
func TestPositionCurrentMigrationPinsItsLoadBearingInvariants(t *testing.T) {
	ddlRaw := migrationSource(t, positionCurrentDDL)
	ddl := lineCommentRE.ReplaceAllString(ddlRaw, "")
	backfillRaw := migrationSource(t, positionCurrentBackfill)
	backfill := lineCommentRE.ReplaceAllString(backfillRaw, "")

	if positionCurrentDDL >= positionCurrentBackfill {
		t.Fatalf("%s does not sort before %s, so the migrator would backfill before the trigger exists",
			positionCurrentDDL, positionCurrentBackfill)
	}

	t.Run("the DDL and the backfill are separate migrations", func(t *testing.T) {
		// Together, CREATE TRIGGER's SHARE ROW EXCLUSIVE on position_state was held for the length of
		// the full-history scan, queueing every ingest INSERT behind it for the whole apply.
		if !strings.Contains(ddl, "CREATE TRIGGER trigger_upsert_position_current") {
			t.Error("the DDL migration does not create the trigger")
		}
		// The marker as a STANDALONE line: the table COMMENT names it in prose to point operators at the
		// backfill migration, and a substring match would count that reference as the region itself.
		if hasMarkerLine(ddlRaw, "-- REBUILD-BEGIN position_current") {
			t.Error("the DDL migration still carries the REBUILD region, so CREATE TRIGGER's lock on " +
				"position_state is again held across a full-history scan in one transaction")
		}
		if strings.Contains(backfill, "CREATE TRIGGER") {
			t.Error("the backfill migration creates a trigger; its transaction must not take SHARE ROW " +
				"EXCLUSIVE on position_state")
		}
	})

	t.Run("the trigger creation is idempotent", func(t *testing.T) {
		drop := strings.Index(ddl, "DROP TRIGGER IF EXISTS trigger_upsert_position_current")
		create := strings.Index(ddl, "CREATE TRIGGER trigger_upsert_position_current")
		if drop < 0 {
			t.Error("CREATE TRIGGER is not preceded by DROP TRIGGER IF EXISTS, so re-running this file " +
				"fails with \"trigger already exists\" -- the only non-idempotent statement in it")
		} else if drop > create {
			t.Error("DROP TRIGGER IF EXISTS comes after CREATE TRIGGER, so it drops the trigger it just created")
		}
	})

	t.Run("the REBUILD region carries its settings as executable sql", func(t *testing.T) {
		region := lineCommentRE.ReplaceAllString(rebuildRegion(t), "")
		for _, want := range []string{
			"SET LOCAL lock_timeout",
			"SET LOCAL timescaledb.enable_tiered_reads",
			"SET LOCAL search_path",
			"position_current.rebuild_xid",
		} {
			if !strings.Contains(region, want) {
				t.Errorf("the REBUILD region omits %q as EXECUTABLE sql. The table COMMENT tells "+
					"operators to re-run that region and SET LOCAL dies with its transaction", want)
			}
		}
	})

	t.Run("the rebuild orders by the whole version tuple", func(t *testing.T) {
		// Pinned as text because deleting this leg kills no behavioural case and cannot be made to; it is
		// still load-bearing, since the PK admits a tie on the first three. Measurements in #644.
		if !triggerOrderByRE.MatchString(ddl) {
			t.Errorf("the trigger's DISTINCT ON ordering does not match %v; promoting block_timestamp "+
				"ahead of block_number reinstates the lower-block reorg win the table COMMENT calls the "+
				"load-bearing rule, and every behavioural case still passes", triggerOrderByRE)
		}
		if !rebuildOrderByRE.MatchString(backfill) {
			t.Errorf("the rebuild's DISTINCT ON ordering does not match %v; a dropped or reordered leg "+
				"seats the wrong observation as current, and after a TRUNCATE-first rebuild the "+
				"newer-wins arm never runs to repair it", rebuildOrderByRE)
		}
	})

	t.Run("both writers sweep the PK in position_id order", func(t *testing.T) {
		region := lineCommentRE.ReplaceAllString(rebuildRegion(t), "")
		if !orderByPositionIDRE.MatchString(region) {
			t.Error("the rebuild does not order by p.position_id first, so it can cross the trigger's " +
				"upsert order and deadlock live ingest")
		}
		if strings.Contains(region, "ORDER BY block_timestamp") || strings.Contains(region, "ORDER BY p.block_timestamp") {
			t.Error("the rebuild leads its ORDER BY with block_timestamp; that chases the row-trigger " +
				"order which live ingest does not reproduce")
		}
		for _, want := range []string{"REFERENCING NEW TABLE AS newrows", "FOR EACH STATEMENT", "FROM newrows"} {
			if !strings.Contains(ddl, want) {
				t.Errorf("the trigger is not statement-level over a transition table (missing %q); a row "+
					"trigger fires in the writer's insertion order and cannot be ordered by position_id", want)
			}
		}
		if strings.Contains(ddl, "FOR EACH ROW") {
			t.Error("the trigger is FOR EACH ROW; that reintroduces both the deadlock class and the " +
				"per-row upsert cost")
		}
	})

	t.Run("the backfill leaves no session state behind and defers its index", func(t *testing.T) {
		// The marker is a comment, so it is located in the raw source; what follows it is then stripped.
		endAt := strings.Index(backfillRaw, "-- REBUILD-END position_current")
		if endAt < 0 {
			t.Fatal("the backfill migration has no REBUILD-END marker")
		}
		tail := backfillRaw[endAt:]
		if !strings.Contains(lineCommentRE.ReplaceAllString(tail, ""), "RESET search_path") {
			t.Error("search_path is not reset after REBUILD-END; SET LOCAL lives until the transaction " +
				"ends, so ANALYZE and the migrations INSERT would resolve under a hardcoded public")
		}
		idx := strings.Index(backfill, "CREATE INDEX IF NOT EXISTS position_current_holder_idx")
		if idx < 0 {
			t.Fatal("the holder index is not created in the backfill migration")
		}
		if insert := strings.Index(backfill, "INSERT INTO public.position_current"); idx < insert {
			t.Error("the holder index is created BEFORE the backfill, so every backfilled row pays a " +
				"random btree insert with its own WAL instead of one bulk build")
		}
	})

	t.Run("neither file is marked no-transaction", func(t *testing.T) {
		// It would reduce every SET LOCAL to a warning: applyMigrationNoTx runs each statement in its own
		// implicit transaction.
		for name, raw := range map[string]string{positionCurrentDDL: ddlRaw, positionCurrentBackfill: backfillRaw} {
			for _, line := range strings.Split(raw, "\n") {
				if strings.TrimSpace(line) == "-- migrate: no-transaction" {
					t.Errorf("%s is marked no-transaction, which reduces every SET LOCAL to a warning", name)
				}
			}
		}
	})
}

// The statement trigger decides in TWO places: which row of the batch wins (its DISTINCT ON order) and
// whether that winner beats the cache (the ON CONFLICT guard). One row per statement exercises only the
// guard, so these insert a whole batch in one statement with the other legs held equal.
func TestPositionCurrentIntraBatchPickPerKeyColumn(t *testing.T) {
	holder := strings.Repeat("a", 40)
	cases := []struct {
		name, id string
		// rows are (qty, block, bv, pv, ts); the row with qty 99 must win.
		rows [][5]any
	}{
		{"block_number", "batch-bn", [][5]any{
			{1, 700, 0, 0, "2026-01-01T00:00:00Z"},
			{99, 900, 0, 0, "2026-01-01T00:00:00Z"},
			{2, 800, 0, 0, "2026-01-01T00:00:00Z"},
		}},
		{"block_version", "batch-bv", [][5]any{
			{1, 700, 0, 0, "2026-01-01T00:00:00Z"},
			{99, 700, 2, 0, "2026-01-01T00:00:00Z"},
			{2, 700, 1, 0, "2026-01-01T00:00:00Z"},
		}},
		{"processing_version", "batch-pv", [][5]any{
			{1, 700, 0, 0, "2026-01-01T00:00:00Z"},
			{99, 700, 0, 3, "2026-01-01T00:00:00Z"},
			{2, 700, 0, 1, "2026-01-01T00:00:00Z"},
		}},
		{"block_timestamp", "batch-ts", [][5]any{
			{1, 700, 0, 0, "2026-01-01T00:00:00Z"},
			{99, 700, 0, 0, "2026-01-03T00:00:00Z"},
			{2, 700, 0, 0, "2026-01-02T00:00:00Z"},
		}},
	}
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var vals []string
			args := []any{tc.id, holder}
			for _, r := range tc.rows {
				vals = append(vals, fmt.Sprintf(
					`(position_id(1,1,$1,$2), 1, 1, $1, $2, %d, %d, %d, %d, '%s'::timestamptz, 'public.p', 0)`,
					r[0], r[1], r[2], r[3], r[4]))
			}
			if _, err := f.pool.Exec(f.ctx, `
				INSERT INTO position_state
				    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
				     block_number, block_version, processing_version, block_timestamp, projection, build_id)
				VALUES `+strings.Join(vals, ","), args...); err != nil {
				t.Fatal(err)
			}
			var qty int
			if err := f.pool.QueryRow(f.ctx,
				`SELECT quantity FROM position_current WHERE position_id = position_id(1,1,$1,$2)`,
				tc.id, holder).Scan(&qty); err != nil {
				t.Fatal(err)
			}
			if qty != 99 {
				t.Errorf("cached quantity = %d; want 99. The trigger's intra-batch pick is ignoring %s",
					qty, tc.name)
			}
		})
	}
}

func TestPositionCurrentTriggerFunctionPinsSearchPath(t *testing.T) {
	// pg_temp is searched first for RELATION names whatever search_path says, so qualification in the
	// body is what defends the writes; this pin defends everything else. Asserted from pg_proc because
	// dropping the SET has no reachable behavioural consequence in this suite.
	f := newPositionCurrentFixture(t)
	// Both permanent functions this pair of migrations installs, not just the trigger's: the guard
	// carries the same pin and dropping it has no reachable behavioural consequence here either.
	for _, fn := range []string{"upsert_position_current", "position_current_rebuild_guard"} {
		t.Run(fn, func(t *testing.T) { assertSearchPathPinned(t, f, fn) })
	}
}

func assertSearchPathPinned(t *testing.T, f *positionCurrentFixture, fn string) {
	t.Helper()
	var cfg []string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT coalesce(proconfig, '{}') FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		  WHERE n.nspname = 'public' AND p.proname = $1::name`, fn).Scan(&cfg); err != nil {
		t.Fatal(err)
	}
	var got string
	for _, kv := range cfg {
		if strings.HasPrefix(kv, "search_path=") {
			got = strings.TrimPrefix(kv, "search_path=")
		}
	}
	if got == "" {
		t.Fatalf("%s has no search_path in proconfig (%v); a caller's path would "+
			"then resolve anything the body leaves unqualified", fn, cfg)
	}
	if strings.Contains(got, `"$user"`) {
		t.Errorf("search_path = %q includes \"$user\", which resolves per CALLER for a SECURITY "+
			"INVOKER function, so a role-named schema could shadow a reference", got)
	}
}

// stl_readwrite can INSERT here directly, not only through the trigger, so the cache needs the history's
// guards or a hand-written INSERT can seat garbage in the copy consumers read.
func TestPositionCurrentConstraintsRejectWhatPositionStateRejects(t *testing.T) {
	const validHolder = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	valid := `sha256('chk'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`
	cols := `(position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
	          block_number, block_version, processing_version, block_timestamp, projection, build_id)`
	cases := []struct{ name, values, constraint string }{
		{"a wrong-width position_id", `'\x00'::bytea, '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "id_len"},
		{"a NaN quantity", `sha256('c2'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 'NaN'::numeric, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
		{"a negative quantity", `sha256('c3'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', -1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
		// 40 characters, so the length quantifier cannot be what rejects these: they constrain case and
		// anchoring specifically.
		{"an uppercase holder_id", `sha256('c4'::bytea), '2026-01-01', 1, 1, 'i', '` + strings.Repeat("A", 40) + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
		{"a valid holder embedded in a longer string", `sha256('c4b'::bytea), '2026-01-01', 1, 1, 'i', '0x` + strings.Repeat("a", 40) + `!', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
		{"an Infinity quantity", `sha256('c9'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 'Infinity'::numeric, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
		{"an over-long instrument_key", `sha256('c10'::bytea), '2026-01-01', 1, 1, repeat('k', 2001), '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "instrument_key_len"},
		{"a zero protocol_id", `sha256('c11'::bytea), '2026-01-01', 1, 0, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "protocol_pos"},
		{"a negative block_version", `sha256('c12'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, -1, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
		{"a negative processing_version", `sha256('c13'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, -1, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
		{"a negative build_id", `sha256('c14'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', -1`, "coord_nonneg"},
		// NOT NULL, not a CHECK: a NULL satisfies every CHECK vacuously, so the column declaration is the
		// only guard. 23502 is the NOT NULL SQLSTATE.
		{"a NULL quantity", `sha256('c15'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', NULL, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "quantity"},
		{"a NULL holder_id", `sha256('c16'::bytea), '2026-01-01', 1, 1, 'i', NULL, 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_id"},
		{"a negative block_number", `sha256('c5'::bytea), '2026-01-01', 1, 1, 'i', '` + validHolder + `', 1, -1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
		{"a zero chain_id", `sha256('c6'::bytea), '2026-01-01', 0, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "chain_pos"},
		{"a pre-genesis block_timestamp", `sha256('c7'::bytea), '2008-01-01', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2008-01-01T00:00:00Z'::timestamptz, 'p', 0`, "ts_sane"},
		{"an as_of_date that disagrees with block_timestamp", `sha256('c8'::bytea), '2026-01-02', 1, 1, 'i', '` + validHolder + `', 1, 1, 0, 0, '2026-01-01T23:30:00Z'::timestamptz, 'p', 0`, "as_of_date"},
	}
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.pool.Exec(f.ctx, `INSERT INTO position_current `+cols+` VALUES (`+tc.values+`)`)
			if err == nil {
				t.Fatalf("accepted %s; want rejection by position_current_%s_chk", tc.name, tc.constraint)
			}
			if !strings.Contains(err.Error(), tc.constraint) {
				t.Errorf("rejected %s with %v; want the %s constraint to be the one that fired", tc.name, err, tc.constraint)
			}
		})
	}
	// Control: the same shape with every field valid must be accepted, so the cases above fail for the
	// reason named rather than because the INSERT was malformed.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO position_current `+cols+` VALUES (`+valid+`) ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("the all-valid control was rejected, so the cases above prove nothing: %v", err)
	}
}

func TestPositionCurrentIsExactlyOneRowPerPosition(t *testing.T) {
	// Seeds its own positions with several observations each, so it does not pass vacuously on an empty
	// table.
	f := newPositionCurrentFixture(t)
	f.observe("onerow-1", 10, 100, 0, 0, "2026-04-01T00:00:00Z")
	f.observe("onerow-1", 20, 200, 0, 0, "2026-04-02T00:00:00Z")
	f.observe("onerow-2", 30, 100, 0, 0, "2026-04-01T00:00:00Z")
	f.observe("onerow-2", 40, 100, 1, 0, "2026-04-01T00:00:00Z")

	var rows, positions int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*), count(DISTINCT position_id) FROM position_current`).Scan(&rows, &positions); err != nil {
		t.Fatal(err)
	}
	if rows != positions {
		t.Errorf("position_current = %d rows over %d distinct positions; want one each", rows, positions)
	}
	if rows == 0 {
		t.Error("position_current is empty; its trigger is not firing on position_state")
	}
}

func TestPositionCurrentCopiesEveryColumnFromTheWinner(t *testing.T) {
	// build_id legitimately changes when a new build reprocesses, so a DO UPDATE SET omitting it freezes
	// provenance on the first build that ever wrote the position. projection cannot vary per position
	// through the sanctioned path -- one view owns a position_id -- but is copied so the cache does not
	// diverge from the spine after a projection-rename re-stamp.
	f := newPositionCurrentFixture(t)
	f.observe("cols", 10, 100, 0, 0, "2026-05-01T00:00:00Z")
	f.observe("cols", 20, 100, 0, 4, "2026-05-01T00:00:00Z")
	var projection string
	var buildID int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT projection, build_id FROM position_current WHERE position_id = sha256('cols'::bytea)`,
	).Scan(&projection, &buildID); err != nil {
		t.Fatal(err)
	}
	if projection != "public.proj-4" || buildID != 4 {
		t.Errorf("projection=%q build_id=%d; want public.proj-4 and 4 from the winning observation", projection, buildID)
	}
}

func TestPositionCurrentIsRebuildableFromHistory(t *testing.T) {
	// Seeds a multi-observation series and a closed position so the rebuild has version noise to
	// collapse, then compares every position present rather than a hardcoded list.
	f := newPositionCurrentFixture(t)
	f.observe("rebuild-1", 10, 100, 0, 0, "2026-06-01T00:00:00Z")
	f.observe("rebuild-1", 20, 200, 0, 0, "2026-06-02T00:00:00Z")
	f.observe("rebuild-2", 30, 300, 0, 0, "2026-06-03T00:00:00Z")
	f.observe("rebuild-2", 35, 300, 0, 1, "2026-06-03T06:00:00Z")
	f.observe("rebuild-3", 0, 400, 0, 0, "2026-06-04T00:00:00Z")

	before := f.snapshot()
	if len(before) == 0 {
		t.Fatal("position_current is empty; nothing to rebuild")
	}
	if _, err := f.pool.Exec(f.ctx, `TRUNCATE position_current`); err != nil {
		t.Fatal(err)
	}
	f.rebuild()

	after := f.snapshot()
	if len(after) != len(before) {
		t.Errorf("rebuild produced %d positions; want %d", len(after), len(before))
	}
	for id, want := range before {
		if got, ok := after[id]; !ok {
			t.Errorf("position %s lost by the rebuild", id)
		} else if got != want {
			t.Errorf("position %s = qty %d after rebuild; want %d", id, got, want)
		}
	}
}

func (f *positionCurrentFixture) snapshot() map[string]int {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `SELECT encode(position_id,'hex'), quantity FROM position_current`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var id string
		var qty int
		if err := rows.Scan(&id, &qty); err != nil {
			f.t.Fatal(err)
		}
		out[id] = qty
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("iterating the position_current snapshot: %v", err)
	}
	return out
}

func TestPositionCurrentRebuildOverwritesARowLandedInTheWindow(t *testing.T) {
	// The window a rebuild has to survive: the TRUNCATE commits, ingest appends an OLDER observation, the
	// trigger lands it in the now-empty cache (no conflict, so newer-wins never runs), and only then does
	// the rebuild execute. With ON CONFLICT DO NOTHING the older row is kept and no later insert can
	// correct it, because every later insert compares against the poisoned row and loses.
	f := newPositionCurrentFixture(t)
	f.observe("rebuild-race", 10, 100, 0, 0, "2026-10-01T00:00:00Z")
	f.observe("rebuild-race", 20, 200, 0, 0, "2026-10-01T06:00:00Z")

	if _, err := f.pool.Exec(f.ctx, `TRUNCATE position_current`); err != nil {
		t.Fatal(err)
	}
	// Stand in for the trigger firing during the window, landing only the OLDER observation.
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_current
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		SELECT p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
		       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
		       p.processing_version, p.block_timestamp, p.projection, p.build_id
		FROM position_state p
		WHERE p.position_id = sha256('rebuild-race'::bytea) AND p.block_number = 100`); err != nil {
		t.Fatalf("seed the window: %v", err)
	}
	f.rebuild()
	if qty, block, _ := f.current("rebuild-race"); qty != 20 || block != 200 {
		t.Errorf("current(rebuild-race) = qty %d at block %d; want 20 at 200. DO NOTHING leaves the "+
			"older 10 in place, unrepairably", qty, block)
	}
}

// Asserts the row count after the guard raises, not just that it raises: a check in its own statement is
// stepped over by a client with ON_ERROR_STOP off, which then writes every row and leaves an ERROR
// followed by a healthy INSERT 0 N. Statement-at-a-time is the copy-paste path the backfill documents.

func TestPositionCurrentRebuildWritesNothingWhenItsGuardFires(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("guard-nowrite", 7, 100, 0, 0, "2026-01-01T00:00:00Z")
	if _, err := f.pool.Exec(f.ctx, `TRUNCATE position_current`); err != nil {
		t.Fatal(err)
	}

	// Statement-at-a-time: each Exec is its own implicit transaction, so the region's stamp never
	// survives to the INSERT -- exactly the shape psql -f produces.
	var lastErr error
	for _, stmt := range migrator.SplitStatements(rebuildRegion(t)) {
		if _, err := f.pool.Exec(f.ctx, stmt); err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		t.Fatal("no statement in the region raised; the guard did not fire at all")
	}
	if !strings.Contains(lastErr.Error(), "inside ONE transaction") {
		t.Errorf("region raised %v; want the one-transaction message", lastErr)
	}

	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_current`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("the guard raised but the rebuild wrote %d row(s); an operator sees an ERROR followed "+
			"by a healthy INSERT 0 N and has no reason to think the cache is now partial", n)
	}
}

// The backfill's DISTINCT ON pick, a different comparison from the newer-wins WHERE that
// RebuildNewerWinsPerKeyColumn covers. Coordinates are ANTI-correlated -- the winner is behind on every
// leg except the one under test -- so no leg is redundant with another, and the cache is emptied first so
// no ON CONFLICT guard can mask the pick. Deleting each leg in turn kills its own case and no other.
func TestPositionCurrentRebuildPickPerKeyColumn(t *testing.T) {
	type coords struct {
		block, bv, pv int
		ts            string
	}
	cases := []struct {
		name          string
		id            string
		winner, loser coords
	}{
		{
			name:   "block_number outranks a later timestamp at a higher pv",
			id:     "pick-block",
			winner: coords{200, 0, 0, "2026-07-01T00:00:00Z"},
			loser:  coords{100, 0, 9, "2026-07-09T00:00:00Z"},
		},
		{
			name:   "block_version outranks a later timestamp at a higher pv, at one block",
			id:     "pick-bv",
			winner: coords{100, 1, 0, "2026-07-01T00:00:00Z"},
			loser:  coords{100, 0, 9, "2026-07-09T00:00:00Z"},
		},
		{
			name:   "processing_version outranks a later timestamp, at one block and version",
			id:     "pick-pv",
			winner: coords{100, 0, 1, "2026-07-01T00:00:00Z"},
			loser:  coords{100, 0, 0, "2026-07-09T00:00:00Z"},
		},
		{
			// Same leg as the case above -- deleting block_version kills both -- kept because it is the
			// scenario the design is actually defending, at a realistic height rather than a synthetic one.
			name:   "a same-block reorg replacement beats the branch it orphans",
			id:     "pick-reorg",
			winner: coords{950, 1, 0, "2026-07-01T00:00:00Z"},
			loser:  coords{950, 0, 0, "2026-07-09T00:00:00Z"},
		},
		// No block_timestamp case: the leg cannot be made to fail behaviourally, so pinning it is left to
		// TestPositionCurrentMigrationPinsItsLoadBearingInvariants, which asserts the ORDER BY text.
	}
	const winnerQty, loserQty = 41, 77
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.observe(tc.id, loserQty, tc.loser.block, tc.loser.bv, tc.loser.pv, tc.loser.ts)
			f.observe(tc.id, winnerQty, tc.winner.block, tc.winner.bv, tc.winner.pv, tc.winner.ts)
			if _, err := f.pool.Exec(f.ctx, `TRUNCATE position_current`); err != nil {
				t.Fatal(err)
			}
			f.rebuild()
			if qty, _, _ := f.current(tc.id); qty != winnerQty {
				t.Errorf("rebuild picked quantity %d; want %d -- its DISTINCT ON ordering is ignoring this key column",
					qty, winnerQty)
			}
		})
	}
}

// A batch spanning several positions: the intra-batch cases put their rows on a single position_id, so
// nothing else exercises the transition table's partitioning. Narrowing the trigger to the lowest
// position_id of its transition table kills this and nothing else.
func TestPositionCurrentMultiPositionBatch(t *testing.T) {
	f := newPositionCurrentFixture(t)
	const n = 25
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		SELECT sha256(('multi-' || g)::bytea), 1, 1, 'inst-' || g,
		       substr(md5(g::text) || md5(g::text), 1, 40), g, 100 + g, 0, 0,
		       '2026-01-01T00:00:00Z'::timestamptz, 'public.proj-0', 0
		FROM generate_series(1, $1) g`, n); err != nil {
		t.Fatalf("multi-position batch: %v", err)
	}

	var cached int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_current`).Scan(&cached); err != nil {
		t.Fatal(err)
	}
	if cached != n {
		t.Fatalf("one statement inserting %d positions cached %d; the trigger is not processing its whole "+
			"transition table", n, cached)
	}
	// Each cached row must be its own position's observation, not another's.
	var mismatched int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM position_current c JOIN position_state p USING (position_id)
		 WHERE c.quantity IS DISTINCT FROM p.quantity OR c.block_number IS DISTINCT FROM p.block_number`).Scan(&mismatched); err != nil {
		t.Fatal(err)
	}
	if mismatched != 0 {
		t.Errorf("%d cached rows do not match their own history row", mismatched)
	}
}

// The replica-role gap and its recovery. Measured on 2.25.1-pg17: at the ORIGIN default this trigger
// does not fire under session_replication_role = 'replica' -- a plain INSERT and a COPY both land their
// rows and skip it, with no error, so history advances and the cache does not. ENABLE ALWAYS is not
// available to fix it: TimescaleDB refuses to enable or disable triggers on a hypertable at all. What we
// do offer is the REBUILD region, and this asserts it actually repairs the case, which is the only
// reason the gap is acceptable. If TimescaleDB ever lifts the restriction the first half stops being
// true and this test says so.
func TestPositionCurrentReplicaRoleGapIsRepairedByRebuild(t *testing.T) {
	f := newPositionCurrentFixture(t)

	var enabled string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT tgenabled FROM pg_trigger
		 WHERE tgrelid = 'position_state'::regclass AND tgname = 'trigger_upsert_position_current'`).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != "O" {
		// The gap being CLOSED is good news, not a regression: it would mean TimescaleDB lifted the
		// restriction or someone made ENABLE ALWAYS stick. Skip rather than fail, and say the migration
		// comment is now stale.
		t.Skipf("tgenabled = %q, not the ORIGIN default -- the documented gap no longer applies and the "+
			"comment in %s claiming TimescaleDB forbids ENABLE ALWAYS should be revisited", enabled, positionCurrentDDL)
	}

	f.observeUnderReplicaRole("replica-write", 9, 100, "2026-01-01T00:00:00Z")

	var history, cached int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM position_state   WHERE position_id = sha256($1::bytea)),
		       (SELECT count(*) FROM position_current WHERE position_id = sha256($1::bytea))`,
		"replica-write").Scan(&history, &cached); err != nil {
		t.Fatal(err)
	}
	if history != 1 {
		t.Fatalf("the replica-role write did not reach history at all (%d rows); this test is measuring "+
			"the wrong thing", history)
	}
	if cached != 0 {
		t.Skipf("the trigger fired under the replica role (cache rows = %d); the gap does not reproduce "+
			"on this engine, so the recovery below would assert nothing", cached)
	}

	// Only reached with the cache genuinely empty, so the row asserted after this is one the rebuild
	// wrote -- not one that was already there.
	f.rebuild()
	if qty, block, _ := f.current("replica-write"); qty != 9 || block != 100 {
		t.Errorf("after the rebuild the cache holds quantity %d at block %d; want 9 at 100 -- the "+
			"documented recovery for a replica-role bulk load does not recover it", qty, block)
	}
}

// observeUnderReplicaRole appends one observation with session_replication_role = 'replica', the setting
// pg_restore --disable-triggers uses. The reset is deferred: the pool has no AfterRelease hook, so an
// early exit here would return a triggers-disabled connection to this test's own pool and silently
// skip the cache for whatever ran next in it.
func (f *positionCurrentFixture) observeUnderReplicaRole(id string, qty, block int, ts string) {
	f.t.Helper()
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(f.ctx, `SET session_replication_role = 'replica'`); err != nil {
		f.t.Fatal(err)
	}
	defer func() {
		if _, err := conn.Exec(f.ctx, `RESET session_replication_role`); err != nil {
			f.t.Errorf("resetting session_replication_role: %v", err)
		}
	}()
	if _, err := conn.Exec(f.ctx, `
		INSERT INTO position_state
		    (`+positionStateCols+`)
		VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, substr(md5($1) || md5($1), 1, 40), $2, $3, 0, 0,
		        $4, 'public.proj-0', 0)`, id, qty, block, ts); err != nil {
		f.t.Fatalf("replica-role insert: %v", err)
	}
}

// The state the migration actually ships in. position_state is a hypertable, and with no chunks -- a
// first apply, and every CI run -- TimescaleDB marks the relation dummy at plan time, so the guard call
// in the INSERT's WHERE collapses to "One-Time Filter: false" and is never evaluated. Measured on
// 2.25.1-pg17: the region run statement-at-a-time reported INSERT 0 0 with no error, while the same shape
// over an empty PLAIN table raised. The standalone call in the region is what covers this; without it the
// guard is absent from precisely the state it ships in.
func TestPositionCurrentRebuildGuardFiresWithNoChunks(t *testing.T) {
	f := newPositionCurrentFixture(t)

	var chunks int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'position_state'`).
		Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if chunks != 0 {
		t.Fatalf("position_state already has %d chunk(s); this test needs the zero-chunk shape", chunks)
	}

	var lastErr error
	for _, stmt := range migrator.SplitStatements(rebuildRegion(t)) {
		if _, err := f.pool.Exec(f.ctx, stmt); err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		t.Fatal("the region ran statement-at-a-time against a zero-chunk position_state without raising; " +
			"the in-INSERT guard is planned away in this shape, so only a standalone call catches it")
	}
	if !strings.Contains(lastErr.Error(), "inside ONE transaction") {
		t.Errorf("region raised %v; want the one-transaction message", lastErr)
	}
}

// The other half: with no chunks the region must still SUCCEED on the correct path. A guard that fired
// here would make every first apply fail, since the migrator backfills before any history exists.
func TestPositionCurrentRebuildSucceedsWithNoChunks(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.rebuild()
	var cached int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_current`).Scan(&cached); err != nil {
		t.Fatal(err)
	}
	if cached != 0 {
		t.Errorf("rebuild over an empty history cached %d rows; want 0", cached)
	}
}

// The third drift class. With a strict > alone, a cache row at the SAME coordinates as history with a
// different payload would be left alone and the rebuild would report INSERT 0 0 -- byte-identical to a
// healthy converged run, the signature this migration is built to avoid. The shipped arm adds an
// equal-coordinates branch; this pins it. It is reachable through the sanctioned grants: stl_readwrite holds UPDATE here, and the
// table takes direct writes by design. Both files enumerate the unrepairable classes as exactly two,
// ahead-of-history and orphan, so this one has to be repairable for that enumeration to be true.
func TestPositionCurrentRebuildRepairsDriftAtEqualCoordinates(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("equal-coords", 500, 100, 0, 0, "2026-01-01T00:00:00Z")
	// Same coordinates as history, wrong payload -- what a direct UPDATE in role produces.
	f.setCache("equal-coords", 999999, 100, 0, 0, "2026-01-01T00:00:00Z")

	f.rebuild()

	if qty, block, _ := f.current("equal-coords"); qty != 500 || block != 100 {
		t.Errorf("after the rebuild the cache holds quantity %d at block %d; want 500 at 100 -- a payload "+
			"that drifted at equal coordinates is not repaired, and the rebuild reported no error", qty, block)
	}
}

// The equal-coordinates arm compares the WHOLE tuple. Each leg is checked on its own because a leg
// dropped from the `=` turns the convergence branch into a partial match, and a partial match LOWERS:
// it treats a cache row that is ahead on that leg as equal and overwrites it from older history. Every
// other "must not lower" fixture in this file moves two or more legs at once, so none of them can see it.
// Fixtures here are ahead on exactly one leg with the other three equal and the payload differing.
func TestPositionCurrentRebuildWillNotConvergeOnAPartialCoordinateMatch(t *testing.T) {
	cases := []struct {
		leg           string
		block, bv, pv int
		ts            string
	}{
		{"block_number", 200, 0, 0, "2026-01-01T00:00:00Z"},
		{"block_version", 100, 1, 0, "2026-01-01T00:00:00Z"},
		{"processing_version", 100, 0, 1, "2026-01-01T00:00:00Z"},
		{"block_timestamp", 100, 0, 0, "2026-09-01T00:00:00Z"},
	}
	const cachedQty, historyQty = 777, 500
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.leg, func(t *testing.T) {
			id := "partial-" + tc.leg
			f.observe(id, historyQty, 100, 0, 0, "2026-01-01T00:00:00Z")
			f.setCache(id, cachedQty, tc.block, tc.bv, tc.pv, tc.ts)

			f.rebuild()

			if qty, _, _ := f.current(id); qty != cachedQty {
				t.Errorf("the rebuild overwrote a cache row that is ahead on %s alone (quantity %d, want "+
					"%d); the arm's equality is matching a prefix of the tuple, so it lowered the row",
					tc.leg, qty, cachedQty)
			}
		})
	}
}

// The trigger's copy of the arm, same per-leg reasoning in the other direction: an observation arriving
// OLDER on exactly one leg, with the other three equal and a differing payload, must not displace the
// cached row. A leg missing from the trigger's `=` regresses the cache on ordinary ingest.
func TestPositionCurrentTriggerWillNotConvergeOnAPartialCoordinateMatch(t *testing.T) {
	cases := []struct {
		leg           string
		block, bv, pv int
		ts            string
	}{
		{"block_number", 200, 0, 0, "2026-01-01T00:00:00Z"},
		{"block_version", 100, 1, 0, "2026-01-01T00:00:00Z"},
		{"processing_version", 100, 0, 1, "2026-01-01T00:00:00Z"},
		{"block_timestamp", 100, 0, 0, "2026-09-01T00:00:00Z"},
	}
	const winnerQty, olderQty = 777, 500
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.leg, func(t *testing.T) {
			id := "trig-partial-" + tc.leg
			// The newer observation lands first and seats the cache.
			f.observe(id, winnerQty, tc.block, tc.bv, tc.pv, tc.ts)
			// Then one older on exactly this leg, differing only in payload otherwise.
			f.observe(id, olderQty, 100, 0, 0, "2026-01-01T00:00:00Z")

			if qty, _, _ := f.current(id); qty != winnerQty {
				t.Errorf("an observation older on %s alone displaced the cached row (quantity %d, want "+
					"%d); the trigger's equality is matching a prefix of the tuple", tc.leg, qty, winnerQty)
			}
		})
	}
}

// A converged rebuild must still touch nothing. The equal-coordinates arm fires only on an actual payload
// difference, so the "re-running is a no-op" property the table COMMENT and the trigger both rely on has
// to survive it -- otherwise every rebuild rewrites every row and takes a row lock on each.
func TestPositionCurrentConvergedRebuildUpdatesNothing(t *testing.T) {
	f := newPositionCurrentFixture(t)
	for i, id := range []string{"conv-a", "conv-b", "conv-c"} {
		f.observe(id, 10+i, 100+i, 0, 0, "2026-01-01T00:00:00Z")
	}
	// A fourth position whose payload is drifted, as the positive control: it proves xmin is sensitive in
	// this fixture, so "the other three did not change" means the arm declined rather than that nothing ran.
	f.observe("conv-drifted", 42, 200, 0, 0, "2026-01-01T00:00:00Z")
	f.setCache("conv-drifted", 99999, 200, 0, 0, "2026-01-01T00:00:00Z")

	before, xminBefore := f.snapshot(), f.rowVersions()
	if len(xminBefore) != 4 {
		t.Fatalf("read %d row versions, want 4; the comparison below would pass on an empty read",
			len(xminBefore))
	}
	control := f.hexID("conv-drifted")
	delete(before, control)

	f.rebuild()

	after := f.snapshot()
	if qty := after[control]; qty != 42 {
		t.Fatalf("the drifted control row is %d, want 42; the rebuild did not run, so the assertions "+
			"below prove nothing", qty)
	}
	delete(after, control)
	if !maps.Equal(before, after) {
		t.Errorf("a converged rebuild changed the cache contents: %v -> %v", before, after)
	}
	xminAfter := f.rowVersions()
	same := 0
	for i := range xminBefore {
		if i < len(xminAfter) && xminBefore[i] == xminAfter[i] {
			same++
		}
	}
	if same != 3 {
		t.Errorf("%d of 4 rows kept their xmin, want exactly 3 (the drifted control rewritten, the three "+
			"converged rows untouched): %v -> %v", same, xminBefore, xminAfter)
	}
}

// hexID is the snapshot map's key for a fixture label: snapshot is keyed by the hex position_id.
func (f *positionCurrentFixture) hexID(id string) string {
	f.t.Helper()
	var hex string
	if err := f.pool.QueryRow(f.ctx, `SELECT encode(sha256($1::bytea), 'hex')`, id).Scan(&hex); err != nil {
		f.t.Fatalf("hexID(%s): %v", id, err)
	}
	return hex
}

// rowVersions returns each cached row's xmin in position_id order, so a caller can tell a row that was
// rewritten from one that was left alone. Errors are fatal: a silent read failure would make any
// before/after comparison over the result pass on two empty slices.
func (f *positionCurrentFixture) rowVersions() []uint32 {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `SELECT xmin::text::bigint FROM position_current ORDER BY position_id`)
	if err != nil {
		f.t.Fatalf("reading row versions: %v", err)
	}
	defer rows.Close()
	var out []uint32
	for rows.Next() {
		var x uint32
		if err := rows.Scan(&x); err != nil {
			f.t.Fatalf("scanning a row version: %v", err)
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("iterating row versions: %v", err)
	}
	return out
}

// The trigger carries the same equal-coordinates arm, and it is reachable without a rebuild: a row written
// straight into the cache with no history behind it is an orphan, and the moment real history arrives at
// the coordinates that orphan claims, the trigger is the only thing that can correct it. With a strict >
// the wrong payload survives an ingest that knows better, silently.
func TestPositionCurrentTriggerRepairsAnOrphanAtEqualCoordinates(t *testing.T) {
	f := newPositionCurrentFixture(t)
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_current
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id)
		VALUES (sha256($1::bytea), '2026-01-01', 1, 1, 'inst-' || $1,
		        substr(md5($1) || md5($1), 1, 40), 999999, 100, 0, 0,
		        '2026-01-01T00:00:00Z'::timestamptz, 'public.proj-0', 0)`, "orphan-eq"); err != nil {
		t.Fatalf("seeding the orphan: %v", err)
	}

	// History arrives at exactly the coordinates the orphan claims.
	f.observe("orphan-eq", 500, 100, 0, 0, "2026-01-01T00:00:00Z")

	if qty, block, _ := f.current("orphan-eq"); qty != 500 || block != 100 {
		t.Errorf("the cache holds quantity %d at block %d after history arrived at the same coordinates; "+
			"want 500 at 100 -- the trigger left a wrong payload standing with no error", qty, block)
	}
}

// Volatility is load-bearing for both functions and neither is reachable behaviourally, so it is asserted
// from pg_proc, following TestProcessingVersionHelpersAreVolatile. The guard calls pg_current_xact_id(),
// which assigns an xid when the transaction has none, so STABLE would be a false side-effect-free claim;
// the trigger function writes. Marking either non-volatile survives every other test in this package.
func TestPositionCurrentFunctionsAreVolatile(t *testing.T) {
	f := newPositionCurrentFixture(t)
	for _, fn := range []string{"upsert_position_current", "position_current_rebuild_guard"} {
		t.Run(fn, func(t *testing.T) {
			var vol string
			if err := f.pool.QueryRow(f.ctx,
				`SELECT provolatile FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
				  WHERE n.nspname = 'public' AND p.proname = $1::name`, fn).Scan(&vol); err != nil {
				t.Fatal(err)
			}
			if vol != "v" {
				t.Errorf("%s is provolatile %q, want \"v\" (VOLATILE); a non-volatile label asserts the "+
					"body has no side effects, which is false for both of these", fn, vol)
			}
		})
	}
}

// Per-column coverage of the arm's payload tuple, and of IS DISTINCT FROM over <>. Both fixtures that
// drift a payload drift quantity, so six of the eight terms can be deleted with the suite green; and
// because a row-wise <> returns TRUE whenever any non-null pair differs, quantity-drift also hides the
// operator choice. chain_id is nullable, so drifting it to NULL and nothing else is the one shape where
// <> yields NULL, the arm declines, and the repair silently does not happen.
func TestPositionCurrentRebuildRepairsEachDriftedPayloadColumn(t *testing.T) {
	cases := []struct {
		column string
		drift  string // SQL setting the cache column to a wrong value at unchanged coordinates
		want   string // SQL reading it back; must equal history's value
	}{
		{"chain_id", "chain_id = NULL", "chain_id::text"},
		{"protocol_id", "protocol_id = NULL", "protocol_id::text"},
		{"build_id", "build_id = 99", "build_id::text"},
		{"projection", "projection = 'public.wrong'", "projection"},
		{"instrument_key", "instrument_key = 'wrong'", "instrument_key"},
		{"holder_id", "holder_id = " + "repeat('b', 40)", "holder_id"},
		{"quantity", "quantity = 99999", "quantity::text"},
	}
	f := newPositionCurrentFixture(t)
	for _, tc := range cases {
		t.Run(tc.column, func(t *testing.T) {
			id := "payload-" + tc.column
			f.observe(id, 500, 100, 0, 0, "2026-01-01T00:00:00Z")
			var want string
			if err := f.pool.QueryRow(f.ctx,
				`SELECT `+tc.want+` FROM position_state WHERE position_id = sha256($1::bytea)`, id).
				Scan(&want); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(f.ctx,
				`UPDATE position_current SET `+tc.drift+` WHERE position_id = sha256($1::bytea)`, id); err != nil {
				t.Fatal(err)
			}

			f.rebuild()

			var got *string
			if err := f.pool.QueryRow(f.ctx,
				`SELECT `+tc.want+` FROM position_current WHERE position_id = sha256($1::bytea)`, id).
				Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got == nil || *got != want {
				t.Errorf("after the rebuild %s is %v, want %q -- this column is missing from the arm's "+
					"payload tuple, or the comparison is <> rather than IS DISTINCT FROM", tc.column, got, want)
			}
		})
	}
}

// The statements after RESET search_path must be schema-qualified. RESET restores the applying session's
// path, so an unqualified position_current there resolves under whatever that path names: with a schema
// ahead of public that shadows the table, CREATE INDEX IF NOT EXISTS builds the holder index on the
// shadow and reports success, leaving the real table with only its PK and the file's own 4,652-buffer seq
// scan intact. Executed against the shipped text, not a copy.
func TestPositionCurrentBackfillTailResolvesUnderAShadowingSearchPath(t *testing.T) {
	f := newPositionCurrentFixture(t)
	src := migrationSource(t, positionCurrentBackfill)
	tail := src[strings.Index(src, "RESET search_path;"):]
	if !strings.Contains(tail, "CREATE INDEX") {
		t.Fatalf("no tail after RESET search_path in %s", positionCurrentBackfill)
	}

	// The shadow has to be reachable through the ROLE default, not a session SET: the tail's first
	// statement is RESET search_path, which discards anything set on the session and restores the role
	// default. That is the same property the migration comment records.
	var role string
	if err := f.pool.QueryRow(f.ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS shadow`,
		`CREATE TABLE IF NOT EXISTS shadow.position_current (LIKE public.position_current)`,
		`CREATE TABLE IF NOT EXISTS shadow.migrations (LIKE public.migrations)`,
		`ALTER ROLE ` + role + ` SET search_path = shadow, public`,
		// so IF NOT EXISTS cannot mask where the tail actually builds it
		`DROP INDEX IF EXISTS public.position_current_holder_idx`,
	} {
		if _, err := f.pool.Exec(f.ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		if _, err := f.pool.Exec(f.ctx, `ALTER ROLE `+role+` RESET search_path`); err != nil {
			t.Errorf("restoring the role search_path: %v", err)
		}
	})

	// A brand-new pool, so the connection reads the role default at connect time. DISCARD ALL on a pooled
	// connection restores the path captured when IT connected, which predates the ALTER ROLE above.
	fresh, err := pgxpool.New(f.ctx, f.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	var path string
	if err := fresh.QueryRow(f.ctx, `SHOW search_path`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(path, "shadow") {
		t.Fatalf("the fresh connection resolves %q, which does not shadow public; this test cannot "+
			"reproduce the hazard it exists for", path)
	}
	for _, stmt := range migrator.SplitStatements(tail) {
		if _, err := fresh.Exec(f.ctx, stmt); err != nil {
			t.Fatalf("executing the shipped tail: %v", err)
		}
	}

	var schema string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relname = 'position_current_holder_idx'`).Scan(&schema); err != nil {
		t.Fatalf("the holder index was not created at all: %v", err)
	}
	if schema != "public" {
		t.Errorf("the holder index landed in %q, not public; a statement after RESET search_path is "+
			"unqualified, so it resolved against the shadowing schema and reported success", schema)
	}
}
