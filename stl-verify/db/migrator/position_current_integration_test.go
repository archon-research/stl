//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// orderByPositionIDRE matches an ORDER BY whose FIRST key is position_id, qualified or not. Both writers
// into position_current must sweep its PK in that order: it is the only total order a block_timestamp
// cannot permute, and ordering either writer by time reopens the deadlock class.
var orderByPositionIDRE = regexp.MustCompile(`(?i)ORDER\s+BY\s+(?:[a-z_]+\.)?position_id\b`)

// TestPositionCurrent is the VEC-409 contract test for position_current: one row per position, its
// latest observation, maintained by an AFTER INSERT trigger on the append-only position_state history
// and rebuildable by re-running the migration's own backfill statement.
//
// Every subtest seeds the positions it asserts on and passes when run alone. Two caveats, stated rather
// than glossed: the two whole-table assertions ("one row per position", and the rebuild) are invariants
// over whatever is present rather than fixed counts, and the rebuild subtest MUTATES shared state by
// truncating the cache before repopulating it from history. It is declared after every whole-table
// assertion and Go runs subtests in declaration order, so that placement is load-bearing -- a new
// whole-table assertion belongs ABOVE it.
//
// Fail-first, each verified by mutating the migration: the index subtest fails without the CREATE INDEX;
// each precedence case fails when its own leg is dropped from the comparison, and the ordering cases fail
// when the comparison is reordered to lead with anything other than block_number; the rebuild subtests
// fail when the backfill's conflict arm becomes DO NOTHING and when its newer-wins WHERE is removed; and
// each constraint subtest fails when its CHECK, or the individual leg it names, is removed.
func TestPositionCurrent(t *testing.T) {
	ctx := context.Background()
	// setupMigratedPostgres: it also disables the scheduled jobs, and position_state registers a
	// 2-day compression policy while every fixture here is stamped 2026-01..10 -- immediately eligible,
	// so policy_compression could fire mid-subtest and take AccessExclusiveLock per chunk.
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	insert := func(t *testing.T, id string, qty, block, bv, pv int, ts string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO position_state
			    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id)
			VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, substr(md5($1) || md5($1), 1, 40), $2, $3, $4, $5::int, $6,
			        'public.proj-' || ($5::int)::text, $5::int)`,
			id, qty, block, bv, pv, ts); err != nil {
			t.Fatalf("insert %s block %d: %v", id, block, err)
		}
	}
	current := func(t *testing.T, id string) (qty, block, bv int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT quantity, block_number, block_version FROM position_current
			  WHERE position_id = sha256($1::bytea)`, id).Scan(&qty, &block, &bv); err != nil {
			t.Fatalf("current(%s): %v", id, err)
		}
		return
	}
	currentDate := func(t *testing.T, id string) string {
		t.Helper()
		var d string
		if err := pool.QueryRow(ctx,
			`SELECT as_of_date::text FROM position_current WHERE position_id = sha256($1::bytea)`,
			id).Scan(&d); err != nil {
			t.Fatalf("currentDate(%s): %v", id, err)
		}
		return d
	}

	// rebuildSQL lifts the rebuild statement out of the migration by its markers, so the tests below
	// execute the text that actually ships. A hand-copied duplicate lets the migration drift: change its
	// backfill and a test asserting on its own copy stays green.
	rebuildSQL := func(t *testing.T) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), "20260819_150000_create_position_current.sql"))
		if err != nil {
			t.Fatalf("read migration: %v", err)
		}
		begin, end := "-- REBUILD-BEGIN position_current\n", "\n-- REBUILD-END position_current"
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

	// Newer-wins compares (block_number, block_version, processing_version, block_timestamp) in that
	// order. One case per leg, each isolating that leg by holding the earlier ones equal, so a comparison
	// that drops any single leg fails a specific case here. The challenger always carries quantity 22.
	t.Run("newer-wins precedence, one case per key column", func(t *testing.T) {
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
				// block_timestamp is held EQUAL so the block_number leg is the only thing that can decide
				// this. It previously advanced both, which let the timestamp leg carry the case: dropping
				// block_number from the comparison left the whole suite green (measured).
				name:       "a newer block wins",
				id:         "prec-block",
				base:       observation{11, 100, 0, 0, "2026-01-01T00:00:00Z"},
				challenger: observation{22, 200, 0, 0, "2026-01-01T00:00:00Z"},
			},
			{
				// The ordering the design rests on: block_number FIRST. An OLDER block carrying a HIGHER
				// processing_version must NOT win -- otherwise a reprocess of old history rolls a
				// position's current balance back. This is the case that fails if the comparison is
				// reordered to lead with block_timestamp or processing_version.
				name:       "an older block does not win even at a higher processing_version",
				id:         "prec-order",
				base:       observation{11, 200, 0, 0, "2026-01-02T00:00:00Z"},
				challenger: observation{22, 100, 0, 1, "2026-01-03T00:00:00Z"},
				keepBase:   true,
			},
			{
				// The documented reorg consequence, pinned: a replacement landing at a LOWER block does
				// not win, so the orphaned observation stays current. Asserting it means a future
				// ordering change cannot silently reverse the trade-off the migration documents.
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
				// position_state's PK has five columns because block_timestamp is the partition column,
				// so a pair differing only in it is legal even though the sanctioned write path does not
				// produce one. The fourth leg is defensive; without it this case is a coin flip.
				name:       "a later block_timestamp at the same block, block_version and processing_version wins",
				id:         "prec-ts",
				base:       observation{11, 100, 0, 0, "2026-01-01T00:00:00Z"},
				challenger: observation{22, 100, 0, 0, "2026-01-02T00:00:00Z"},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				insert(t, tc.id, tc.base.qty, tc.base.block, tc.base.bv, tc.base.pv, tc.base.ts)
				insert(t, tc.id, tc.challenger.qty, tc.challenger.block, tc.challenger.bv, tc.challenger.pv, tc.challenger.ts)
				want := tc.challenger.qty
				why := "The comparison is ignoring this key column"
				if tc.keepBase {
					want = tc.base.qty
					why = "The challenger is older on block_number and must not win; the comparison is not leading with block_number"
				}
				if qty, _, _ := current(t, tc.id); qty != want {
					t.Errorf("current = %d; want %d. %s", qty, want, why)
				}
			})
		}
	})

	t.Run("the winner is taken on its own merits, not by arrival order", func(t *testing.T) {
		// The mirror of the precedence cases: the same pair inserted newest-FIRST must still resolve to
		// the newest, so an older row arriving late cannot regress the cache. Without this, only the
		// guard's accept branch is ever executed and the WHERE could be deleted outright.
		insert(t, "arrival", 30, 600, 0, 0, "2026-03-01T00:00:00Z")
		insert(t, "arrival", 99, 400, 0, 0, "2026-02-01T00:00:00Z")
		if qty, block, _ := current(t, "arrival"); qty != 30 || block != 600 {
			t.Errorf("current = qty %d at block %d; want 30 at 600, the older backfill regressed it", qty, block)
		}
	})

	t.Run("an older observation at the same block cannot regress the cache", func(t *testing.T) {
		// Newest-first at equal block_number, so the reject branch of every later leg is exercised too.
		insert(t, "reject", 50, 700, 2, 3, "2026-04-02T00:00:00Z")
		insert(t, "reject", 99, 700, 1, 9, "2026-04-01T00:00:00Z")
		if qty, _, bv := current(t, "reject"); qty != 50 || bv != 2 {
			t.Errorf("current = qty %d at bv %d; want 50 at bv 2", qty, bv)
		}
	})

	t.Run("a closing zero is current, not skipped", func(t *testing.T) {
		insert(t, "closing", 5, 100, 0, 0, "2026-01-01T00:00:00Z")
		insert(t, "closing", 0, 150, 0, 0, "2026-01-01T12:00:00Z")
		if qty, block, _ := current(t, "closing"); qty != 0 || block != 150 {
			t.Errorf("current = qty %d at block %d; want the closing 0 at 150", qty, block)
		}
	})

	t.Run("as_of_date is the winning observation's UTC date, not the arrival order's", func(t *testing.T) {
		// 23:30Z pins the UTC derivation in the westward direction: under a non-UTC session or a
		// different zone literal this date shifts, which a midday fixture cannot detect.
		insert(t, "asof", 30, 600, 0, 0, "2026-03-01T23:30:00Z")
		insert(t, "asof", 99, 400, 0, 0, "2026-02-01T23:30:00Z")
		if got := currentDate(t, "asof"); got != "2026-03-01" {
			t.Errorf("currentDate(asof) = %s; want 2026-03-01, the winning observation's UTC date", got)
		}
	})

	t.Run("position_current is a materialized table, not a view", func(t *testing.T) {
		var relkind string
		if err := pool.QueryRow(ctx,
			`SELECT relkind FROM pg_class WHERE oid = 'public.position_current'::regclass`).Scan(&relkind); err != nil {
			t.Fatal(err)
		}
		if relkind != "r" {
			t.Errorf("position_current relkind = %q; want \"r\" (an ordinary table). \"v\" means it is still a view", relkind)
		}
	})

	t.Run("position_current is a plain table, deliberately not a hypertable", func(t *testing.T) {
		// One row per position and no time dimension, so the hypertable rule does not engage and there
		// is no compression or tiering policy to want. Same shape as #733's current-position caches.
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'position_current'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("position_current is a hypertable; want a plain table (no time dimension to partition on)")
		}
	})

	t.Run("the holder index exists", func(t *testing.T) {
		// The PK serves position_id lookups; enriched views on this layer filter by holder, which it
		// cannot serve. Measured at 200,000 positions: 4,652 buffers -> 1 with this index.
		var def string
		if err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE tablename = 'position_current' AND indexname = 'position_current_holder_idx'`,
		).Scan(&def); err != nil {
			t.Fatalf("position_current_holder_idx: %v", err)
		}
		if !strings.Contains(def, "(holder_id)") {
			t.Errorf("index = %q; want columns (holder_id)", def)
		}
	})

	t.Run("the grants are SELECT+INSERT+UPDATE, with no DELETE and no TRUNCATE", func(t *testing.T) {
		// The trigger and the backfill only insert or overwrite, so a delete channel would be unused
		// reach, and the remove-rows path stays owner-only by construction. Asserted from the catalogue
		// because the test harness superuser bypasses ACLs.
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
			if err := pool.QueryRow(ctx,
				`SELECT has_table_privilege($1, 'position_current', $2)`, c.role, c.priv).Scan(&got); err != nil {
				t.Fatalf("has_table_privilege(%s, %s): %v", c.role, c.priv, err)
			}
			if got != c.want {
				t.Errorf("%s %s on position_current = %v; want %v", c.role, c.priv, got, c.want)
			}
		}
	})

	t.Run("the UTC date derivation does not follow the session TimeZone", func(t *testing.T) {
		// The 23:30Z fixture alone does not pin this: under a WESTWARD zone the derivation still yields
		// the same date, so that subtest passes even if every writer switches to session-local
		// block_timestamp::date. An EASTWARD zone separates them -- at UTC+9, 23:30Z is the next local
		// day -- so this is the case that fails if the derivation stops naming UTC.
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, `SET TIME ZONE 'Asia/Tokyo'`); err != nil {
			t.Fatal(err)
		}
		// The pool has no AfterRelease reset, so without this the connection goes back carrying
		// Asia/Tokyo and every later subtest that draws it runs under a non-default TimeZone --
		// measured: all three rebuild-executing subtests did.
		defer func() {
			if _, err := conn.Exec(ctx, `RESET TIME ZONE`); err != nil {
				t.Errorf("reset TimeZone before releasing: %v", err)
			}
		}()
		if _, err := conn.Exec(ctx, `
			INSERT INTO position_state
			    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id)
			VALUES (position_id(1,1,'i-tz',$1), 1, 1, 'i-tz', $1, 5,
			        900, 0, 0, '2026-05-01T23:30:00Z'::timestamptz, 'public.p', 0)`,
			strings.Repeat("a", 40)); err != nil {
			t.Fatal(err)
		}
		var got string
		if err := pool.QueryRow(ctx,
			`SELECT as_of_date::text FROM position_current WHERE position_id = position_id(1,1,'i-tz',$1)`,
			strings.Repeat("a", 40)).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != "2026-05-01" {
			t.Errorf("as_of_date = %s under Asia/Tokyo; want 2026-05-01 (the UTC date). The derivation is "+
				"following the session TimeZone, so the same observation dates differently per writer", got)
		}
	})

	t.Run("the rebuild does not regress a cache row that is ahead of history", func(t *testing.T) {
		// The backfill carries its own newer-wins WHERE. Both existing rebuild subtests pass with that
		// guard deleted, because in both the backfill's own pick is the newest thing in the table. This
		// builds the state the guard exists for: a cached row NEWER than anything history holds.
		insert(t, "ahead", 10, 100, 0, 0, "2026-01-01T00:00:00Z")
		if _, err := pool.Exec(ctx, `
			UPDATE position_current
			   SET block_number = 5000, quantity = 77,
			       block_timestamp = '2026-02-01T00:00:00Z'::timestamptz, as_of_date = '2026-02-01'
			 WHERE position_id = sha256('ahead'::bytea)`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, rebuildSQL(t)); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		var qty, block int
		if err := pool.QueryRow(ctx,
			`SELECT quantity, block_number FROM position_current WHERE position_id = sha256('ahead'::bytea)`).
			Scan(&qty, &block); err != nil {
			t.Fatal(err)
		}
		if qty != 77 || block != 5000 {
			t.Errorf("rebuild regressed a row ahead of history to qty=%d block=%d; want 77/5000. The "+
				"backfill's newer-wins WHERE is not constraining it", qty, block)
		}
		// Remove it. A row ahead of history never converges back (that is the point of the case), so
		// leaving it would break every later whole-table assertion. Needs the owner: DELETE is revoked
		// from stl_readwrite, which is itself the drift the migration header documents.
		for _, q := range []string{
			`DELETE FROM position_current WHERE position_id = sha256('ahead'::bytea)`,
			`DELETE FROM position_state WHERE position_id = sha256('ahead'::bytea)`,
		} {
			if _, err := pool.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("the migration pins the invariants its own prose calls load-bearing", func(t *testing.T) {
		// Documented invariants no behavioural case inside one transaction can reach, asserted over the
		// source. Comment text is STRIPPED first: without that every check here is satisfiable by leaving
		// the literal in a comment while commenting out the statement, which was measured for all of them.
		src, err := os.ReadFile(filepath.Join(getMigrationsPath(), "20260819_150000_create_position_current.sql"))
		if err != nil {
			t.Fatal(err)
		}
		raw := string(src)
		code := lineCommentRE.ReplaceAllString(raw, "")

		const beginMarker, endMarker = "REBUILD-BEGIN position_current", "REBUILD-END position_current"
		beginAt, endAt := strings.Index(raw, "\n-- "+beginMarker), strings.Index(raw, "\n-- "+endMarker)
		if beginAt < 0 || endAt < 0 {
			t.Fatalf("REBUILD markers not found (begin=%d end=%d)", beginAt, endAt)
		}
		region := lineCommentRE.ReplaceAllString(raw[beginAt:endAt], "")

		for _, want := range []string{"SET LOCAL lock_timeout", "SET LOCAL timescaledb.enable_tiered_reads"} {
			if !strings.Contains(region, want) {
				t.Errorf("the REBUILD region omits %q as EXECUTABLE sql. The table COMMENT tells operators "+
					"to re-run that region and SET LOCAL dies with its transaction", want)
			}
		}
		// Both writers must sweep position_current's PK in position_id order: that is the only total
		// order a timestamp cannot permute, and ordering either by block_timestamp reopens the deadlock.
		if !orderByPositionIDRE.MatchString(region) {
			t.Error("the rebuild does not order by p.position_id first, so it can cross the trigger's " +
				"upsert order and deadlock live ingest")
		}
		if strings.Contains(region, "ORDER BY block_timestamp") || strings.Contains(region, "ORDER BY p.block_timestamp") {
			t.Error("the rebuild leads its ORDER BY with block_timestamp; that chases the row-trigger " +
				"order which live ingest does not reproduce (measured 3/20 deadlocks)")
		}
		// The trigger has to be statement-level over the transition table for the above to hold.
		for _, want := range []string{"REFERENCING NEW TABLE AS newrows", "FOR EACH STATEMENT", "FROM newrows"} {
			if !strings.Contains(code, want) {
				t.Errorf("the trigger is not statement-level over a transition table (missing %q); a row "+
					"trigger fires in the writer's insertion order and cannot be ordered by position_id", want)
			}
		}
		if strings.Contains(code, "FOR EACH ROW") {
			t.Error("the trigger is FOR EACH ROW; that reintroduces both the deadlock class and the " +
				"per-row upsert cost")
		}
		// The backfill's INSERT, not the trigger's: the trigger's carries "AS cur".
		trigAt := strings.Index(code, "CREATE TRIGGER trigger_upsert_position_current")
		backfillAt := strings.Index(code, "INSERT INTO public.position_current\n")
		if trigAt < 0 || backfillAt < 0 {
			t.Fatalf("could not locate both statements (trigger=%d backfill=%d)", trigAt, backfillAt)
		}
		if trigAt > backfillAt {
			t.Error("the trigger is created AFTER the backfill, so observations landing in between are missed")
		}
		for _, line := range strings.Split(raw, "\n") {
			if strings.TrimSpace(line) == "-- migrate: no-transaction" {
				t.Error("this file is marked no-transaction, which reduces both SET LOCALs to warnings")
			}
		}
	})

	t.Run("within ONE statement the newest observation wins, per key column", func(t *testing.T) {
		// The statement trigger decides in TWO places: which row of the batch wins (its DISTINCT ON
		// order) and whether that winner beats the cache (the ON CONFLICT guard). Every precedence case
		// above inserts one row per statement, so it only ever exercises the guard -- dropping a leg from
		// the pick survived the whole suite. These insert a whole batch in one statement, with the other
		// legs held equal so only the named one can decide.
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
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var vals []string
				args := []any{tc.id, holder}
				for _, r := range tc.rows {
					vals = append(vals, fmt.Sprintf(
						`(position_id(1,1,$1,$2), 1, 1, $1, $2, %d, %d, %d, %d, '%s'::timestamptz, 'public.p', 0)`,
						r[0], r[1], r[2], r[3], r[4]))
				}
				if _, err := pool.Exec(ctx, `
					INSERT INTO position_state
					    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
					     block_number, block_version, processing_version, block_timestamp, projection, build_id)
					VALUES `+strings.Join(vals, ","), args...); err != nil {
					t.Fatal(err)
				}
				var qty int
				if err := pool.QueryRow(ctx,
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
	})

	t.Run("the trigger function pins search_path in the catalogue", func(t *testing.T) {
		// pg_temp is searched first for RELATION names whatever search_path says, so qualification in the
		// body is what defends the writes; this pin defends everything else and removes the dependency on
		// whichever session applied the migration. Asserted from pg_proc because dropping the SET has no
		// reachable behavioural consequence in this suite -- it was a surviving mutation.
		var cfg []string
		if err := pool.QueryRow(ctx,
			`SELECT coalesce(proconfig, '{}') FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
			  WHERE n.nspname = 'public' AND p.proname = 'upsert_position_current'`).Scan(&cfg); err != nil {
			t.Fatal(err)
		}
		var got string
		for _, kv := range cfg {
			if strings.HasPrefix(kv, "search_path=") {
				got = strings.TrimPrefix(kv, "search_path=")
			}
		}
		if got == "" {
			t.Fatalf("upsert_position_current has no search_path in proconfig (%v); a caller's path would "+
				"then resolve anything the body leaves unqualified", cfg)
		}
		if strings.Contains(got, `"$user"`) {
			t.Errorf("search_path = %q includes \"$user\", which resolves per CALLER for a SECURITY "+
				"INVOKER function, so a role-named schema could shadow a reference", got)
		}
	})

	t.Run("the CHECK constraints reject what position_state rejects", func(t *testing.T) {
		// stl_readwrite can INSERT here directly, not only through the trigger, so the cache needs the
		// history's guards or a hand-written INSERT can seat garbage in the copy consumers read.
		valid := `sha256('chk'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`
		cols := `(position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		          block_number, block_version, processing_version, block_timestamp, projection, build_id)`
		cases := []struct{ name, values, constraint string }{
			{"a wrong-width position_id", `'\x00'::bytea, '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "id_len"},
			{"a NaN quantity", `sha256('c2'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'NaN'::numeric, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
			{"a negative quantity", `sha256('c3'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', -1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
			// 40 characters, so the LENGTH quantifier cannot be what rejects these -- the previous
			// 2-character 'AA' passed for that reason and left both case-sensitivity and anchoring
			// unconstrained (unanchoring the regex kept the suite green).
			{"an uppercase holder_id", `sha256('c4'::bytea), '2026-01-01', 1, 1, 'i', '` + strings.Repeat("A", 40) + `', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
			{"a valid holder embedded in a longer string", `sha256('c4b'::bytea), '2026-01-01', 1, 1, 'i', '0x` + strings.Repeat("a", 40) + `!', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
			{"an Infinity quantity", `sha256('c9'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'Infinity'::numeric, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
			{"an over-long instrument_key", `sha256('c10'::bytea), '2026-01-01', 1, 1, repeat('k', 2001), 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "instrument_key_len"},
			{"a zero protocol_id", `sha256('c11'::bytea), '2026-01-01', 1, 0, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "protocol_pos"},
			{"a negative block_version", `sha256('c12'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, -1, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
			{"a negative processing_version", `sha256('c13'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, -1, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
			{"a negative build_id", `sha256('c14'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', -1`, "coord_nonneg"},
			// NOT NULL, not a CHECK: a NULL satisfies every CHECK vacuously, so the column declaration is
			// the only guard. 23502 is the NOT NULL SQLSTATE.
			{"a NULL quantity", `sha256('c15'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "quantity"},
			{"a NULL holder_id", `sha256('c16'::bytea), '2026-01-01', 1, 1, 'i', NULL, 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_id"},
			{"a negative block_number", `sha256('c5'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, -1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
			{"a zero chain_id", `sha256('c6'::bytea), '2026-01-01', 0, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "chain_pos"},
			{"a pre-genesis block_timestamp", `sha256('c7'::bytea), '2008-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2008-01-01T00:00:00Z'::timestamptz, 'p', 0`, "ts_sane"},
			{"an as_of_date that disagrees with block_timestamp", `sha256('c8'::bytea), '2026-01-02', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T23:30:00Z'::timestamptz, 'p', 0`, "as_of_date"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := pool.Exec(ctx, `INSERT INTO position_current `+cols+` VALUES (`+tc.values+`)`)
				if err == nil {
					t.Fatalf("accepted %s; want rejection by position_current_%s_chk", tc.name, tc.constraint)
				}
				if !strings.Contains(err.Error(), tc.constraint) {
					t.Errorf("rejected %s with %v; want the %s constraint to be the one that fired", tc.name, err, tc.constraint)
				}
			})
		}
		// The control: the same shape with every field valid must be accepted, so the cases above fail
		// for the reason named rather than because the INSERT was malformed.
		if _, err := pool.Exec(ctx, `INSERT INTO position_current `+cols+` VALUES (`+valid+`) ON CONFLICT DO NOTHING`); err != nil {
			t.Fatalf("the all-valid control was rejected, so the cases above prove nothing: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM position_current WHERE position_id = sha256('chk'::bytea)`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("position_current is exactly one row per position", func(t *testing.T) {
		// Seeds its own positions with several observations each, so it neither depends on other subtests
		// nor passes vacuously on an empty table.
		insert(t, "onerow-1", 10, 100, 0, 0, "2026-04-01T00:00:00Z")
		insert(t, "onerow-1", 20, 200, 0, 0, "2026-04-02T00:00:00Z")
		insert(t, "onerow-2", 30, 100, 0, 0, "2026-04-01T00:00:00Z")
		insert(t, "onerow-2", 40, 100, 1, 0, "2026-04-01T00:00:00Z")

		var rows, positions int
		if err := pool.QueryRow(ctx,
			`SELECT count(*), count(DISTINCT position_id) FROM position_current`).Scan(&rows, &positions); err != nil {
			t.Fatal(err)
		}
		if rows != positions {
			t.Errorf("position_current = %d rows over %d distinct positions; want one each", rows, positions)
		}
		if rows == 0 {
			t.Error("position_current is empty; its trigger is not firing on position_state")
		}
	})

	t.Run("every copied column tracks the winning observation", func(t *testing.T) {
		// build_id legitimately changes when a new build reprocesses, so a DO UPDATE SET omitting it
		// freezes provenance on the first build that ever wrote the position. projection is different:
		// under the materializer one view owns a position_id and check (4) raises otherwise, so it cannot
		// vary per position through the sanctioned path -- it is copied so the cache does not diverge from
		// the spine after the documented projection-rename re-stamp. This helper INSERTs directly, which
		// is why it can vary both.
		insert(t, "cols", 10, 100, 0, 0, "2026-05-01T00:00:00Z")
		insert(t, "cols", 20, 100, 0, 4, "2026-05-01T00:00:00Z")
		var projection string
		var buildID int
		if err := pool.QueryRow(ctx,
			`SELECT projection, build_id FROM position_current WHERE position_id = sha256('cols'::bytea)`,
		).Scan(&projection, &buildID); err != nil {
			t.Fatal(err)
		}
		if projection != "public.proj-4" || buildID != 4 {
			t.Errorf("projection=%q build_id=%d; want public.proj-4 and 4 from the winning observation", projection, buildID)
		}
	})

	t.Run("rebuildable from history by re-running the migration's own statement", func(t *testing.T) {
		// Seeds a multi-observation series and a closed position so the rebuild has version noise to
		// collapse, then compares every position present rather than a hardcoded list.
		insert(t, "rebuild-1", 10, 100, 0, 0, "2026-06-01T00:00:00Z")
		insert(t, "rebuild-1", 20, 200, 0, 0, "2026-06-02T00:00:00Z")
		insert(t, "rebuild-2", 30, 300, 0, 0, "2026-06-03T00:00:00Z")
		insert(t, "rebuild-2", 35, 300, 0, 1, "2026-06-03T06:00:00Z")
		insert(t, "rebuild-3", 0, 400, 0, 0, "2026-06-04T00:00:00Z")

		before := map[string]int{}
		rows, err := pool.Query(ctx, `SELECT encode(position_id,'hex'), quantity FROM position_current`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id string
			var qty int
			if err := rows.Scan(&id, &qty); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			before[id] = qty
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("iterating the pre-rebuild snapshot: %v", err)
		}
		if len(before) == 0 {
			t.Fatal("position_current is empty; nothing to rebuild")
		}

		if _, err := pool.Exec(ctx, `TRUNCATE position_current`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, rebuildSQL(t)); err != nil {
			t.Fatalf("rebuild: %v", err)
		}

		after := map[string]int{}
		rows, err = pool.Query(ctx, `SELECT encode(position_id,'hex'), quantity FROM position_current`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var qty int
			if err := rows.Scan(&id, &qty); err != nil {
				t.Fatal(err)
			}
			after[id] = qty
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterating the post-rebuild snapshot: %v", err)
		}
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
	})

	t.Run("a rebuild overwrites a row that landed in the window with an older observation", func(t *testing.T) {
		// The window a rebuild has to survive: the TRUNCATE commits, ingest appends an OLDER observation,
		// the trigger lands it in the now-empty cache (no conflict, so newer-wins never runs), and only
		// then does the rebuild execute. With ON CONFLICT DO NOTHING the older row is kept and NO later
		// insert can correct it, because every later insert compares against the poisoned row and loses.
		insert(t, "rebuild-race", 10, 100, 0, 0, "2026-10-01T00:00:00Z")
		insert(t, "rebuild-race", 20, 200, 0, 0, "2026-10-01T06:00:00Z")

		if _, err := pool.Exec(ctx, `TRUNCATE position_current`); err != nil {
			t.Fatal(err)
		}
		// Stand in for the trigger firing during the window, landing only the OLDER observation.
		if _, err := pool.Exec(ctx, `
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
		if _, err := pool.Exec(ctx, rebuildSQL(t)); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		if qty, block, _ := current(t, "rebuild-race"); qty != 20 || block != 200 {
			t.Errorf("current(rebuild-race) = qty %d at block %d; want 20 at 200. DO NOTHING leaves the older 10 in place, unrepairably", qty, block)
		}
	})
}
