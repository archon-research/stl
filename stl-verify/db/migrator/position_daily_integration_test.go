//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestPositionDaily is the VEC-636 contract test for position_daily: one row per (position, UTC date)
// holding that day's winning observation, maintained by an AFTER INSERT trigger on the append-only
// position_state history and rebuildable by re-running the migration's own backfill statement.
//
// Every subtest seeds the positions it asserts on and passes when run alone. Two subtests MUTATE shared
// state (the rebuild ones truncate the cache before repopulating it from history); they are declared
// after every whole-table assertion and Go runs subtests in declaration order, so that placement is
// load-bearing -- a new whole-table assertion belongs ABOVE them.
//
// Three gaps in the pre-split version of this suite are closed here, each found by a mutation sweep:
// the precedence cases asserted through position_current so position_daily's own comparison had NO
// coverage; every same-date pair arrived oldest-first so only the accept branch ever ran, which meant
// the entire WHERE could be deleted with the suite green; and projection/build_id were constant across
// seeds, so a DO UPDATE SET omitting either was invisible.
func TestPositionDaily(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// projection and build_id vary with processing_version, so a SET list that drops either is caught.
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
	// daily returns one position's series as (as_of_date, quantity) pairs, oldest first.
	daily := func(t *testing.T, id string) []string {
		t.Helper()
		rows, err := pool.Query(ctx,
			`SELECT as_of_date::text || '=' || quantity::text FROM position_daily
			  WHERE position_id = sha256($1::bytea) ORDER BY as_of_date`, id)
		if err != nil {
			t.Fatalf("daily(%s): %v", id, err)
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
			// pgx streams: a mid-stream failure ends Next() exactly like end-of-rows, so without this a
			// truncated slice reads as a short series and the caller asserts on partial data.
			t.Fatalf("daily(%s) iteration: %v", id, err)
		}
		return out
	}
	dayQty := func(t *testing.T, id, date string) int {
		t.Helper()
		var q int
		if err := pool.QueryRow(ctx,
			`SELECT quantity FROM position_daily WHERE position_id = sha256($1::bytea) AND as_of_date = $2`,
			id, date).Scan(&q); err != nil {
			t.Fatalf("dayQty(%s, %s): %v", id, date, err)
		}
		return q
	}

	// rebuildSQL lifts the rebuild statement out of the migration by its markers, so the tests execute
	// the text that ships. A hand-copied duplicate lets the migration drift silently.
	rebuildSQL := func(t *testing.T) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), "20260824_120000_create_position_daily.sql"))
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

	// One case per leg of (block_number, block_version, processing_version, block_timestamp), each
	// holding the earlier legs equal and both rows on the SAME UTC date so they collide on
	// (position_id, as_of_date) and the comparison actually runs. Asserted on position_daily itself --
	// the pre-split version asserted through position_current, so this table's comparison was untested.
	t.Run("newer-wins precedence, one case per key column", func(t *testing.T) {
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
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				insert(t, tc.id, 11, 100, 0, 0, "2026-01-01T00:00:00Z")
				insert(t, tc.id, 22, tc.block, tc.bv, tc.pv, tc.ts)
				if got := dayQty(t, tc.id, "2026-01-01"); got != 22 {
					t.Errorf("2026-01-01 = %d; want 22. The comparison is ignoring this key column", got)
				}
				if got := daily(t, tc.id); len(got) != 1 {
					t.Errorf("series = %v; want a single row for the day", got)
				}
			})
		}
	})

	t.Run("an older observation arriving later cannot regress the day", func(t *testing.T) {
		// Newest-FIRST, so the guard's REJECT branch runs. Every same-date pair in the pre-split suite
		// arrived oldest-first, which meant only the accept branch executed and deleting the entire
		// WHERE left the suite green.
		insert(t, "reject", 50, 700, 2, 3, "2026-02-01T12:00:00Z")
		insert(t, "reject", 99, 700, 1, 9, "2026-02-01T06:00:00Z")
		if got := dayQty(t, "reject", "2026-02-01"); got != 50 {
			t.Errorf("2026-02-01 = %d; want 50 -- the older observation regressed the day", got)
		}
	})

	t.Run("every observed date is retained, and only observed dates", func(t *testing.T) {
		// The grain: one row per date the position was OBSERVED, with no carry-forward. A gap day has no
		// row, which the COMMENT states explicitly so a consumer does not read absence as zero.
		insert(t, "series", 10, 100, 0, 0, "2026-03-01T00:00:00Z")
		insert(t, "series", 20, 200, 0, 0, "2026-03-02T00:00:00Z")
		insert(t, "series", 30, 300, 0, 0, "2026-03-05T00:00:00Z")
		want := []string{"2026-03-01=10", "2026-03-02=20", "2026-03-05=30"}
		got := daily(t, "series")
		if len(got) != len(want) {
			t.Fatalf("series = %v; want %v -- three observed dates, and no row for the 03-03/03-04 gap", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("series[%d] = %s; want %s", i, got[i], want[i])
			}
		}
	})

	t.Run("a same-day reprocess replaces that day rather than adding one", func(t *testing.T) {
		insert(t, "sameday", 50, 800, 0, 0, "2026-04-10T00:00:00Z")
		insert(t, "sameday", 55, 800, 0, 1, "2026-04-10T06:00:00Z")
		if got := daily(t, "sameday"); len(got) != 1 || got[0] != "2026-04-10=55" {
			t.Errorf("series = %v; want one row for the day at the winning pv, [2026-04-10=55]", got)
		}
	})

	t.Run("a correction across UTC midnight leaves the old date's row, as documented", func(t *testing.T) {
		// Limit 2 in the migration header. as_of_date comes from the winning row's timestamp and the
		// upsert is keyed on it, so a reprocess landing on a different date writes a NEW row and nothing
		// retracts the old one -- the backfill reproduces the same state, so a rebuild cannot repair it.
		// Pinned as a test so the behaviour is a decision rather than a surprise: if it is ever made to
		// retract, this subtest is the one that should change.
		insert(t, "midnight", 10, 900, 0, 0, "2026-05-10T23:30:00Z")
		insert(t, "midnight", 20, 900, 0, 1, "2026-05-11T00:30:00Z")
		got := daily(t, "midnight")
		if len(got) != 2 || got[0] != "2026-05-10=10" || got[1] != "2026-05-11=20" {
			t.Errorf("series = %v; want both dates present, [2026-05-10=10 2026-05-11=20] -- the superseded row is NOT retracted", got)
		}
	})

	t.Run("as_of_date is the winning observation's UTC date", func(t *testing.T) {
		// 23:30Z pins the derivation westward: under a wrong zone literal or a non-UTC session this date
		// shifts, which a midday fixture cannot detect.
		insert(t, "asof", 7, 1000, 0, 0, "2026-06-01T23:30:00Z")
		if got := daily(t, "asof"); len(got) != 1 || got[0] != "2026-06-01=7" {
			t.Errorf("series = %v; want [2026-06-01=7] -- 23:30Z belongs to 06-01 in UTC", got)
		}
	})

	t.Run("position_daily is a hypertable on as_of_date with 7-day chunks", func(t *testing.T) {
		var column, interval string
		if err := pool.QueryRow(ctx, `
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
	})

	t.Run("rows route to chunks by as_of_date", func(t *testing.T) {
		insert(t, "routing", 60, 1100, 0, 0, "2026-08-01T00:00:00Z")
		insert(t, "routing", 61, 1200, 0, 0, "2026-08-09T00:00:00Z")
		var chunks int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM timescaledb_information.chunks
			 WHERE hypertable_name = 'position_daily'
			   AND range_start <= '2026-08-09'::timestamptz AND range_end > '2026-08-01'::timestamptz`,
		).Scan(&chunks); err != nil {
			t.Fatal(err)
		}
		if chunks != 2 {
			t.Errorf("dates 8 days apart occupy %d chunks; want 2 with a 7-day interval", chunks)
		}
	})

	t.Run("a reprocess into an older chunk resolves without touching the newer one", func(t *testing.T) {
		insert(t, "crosschunk", 60, 1300, 0, 0, "2026-09-01T00:00:00Z")
		insert(t, "crosschunk", 61, 1400, 0, 0, "2026-09-09T00:00:00Z")
		insert(t, "crosschunk", 99, 1300, 0, 1, "2026-09-01T06:00:00Z")
		if got := dayQty(t, "crosschunk", "2026-09-01"); got != 99 {
			t.Errorf("2026-09-01 = %d; want 99 -- the reprocess must win in the older chunk", got)
		}
		if got := dayQty(t, "crosschunk", "2026-09-09"); got != 61 {
			t.Errorf("2026-09-09 = %d; want 61 -- the newer chunk must be untouched", got)
		}
	})

	t.Run("the trigger function pins search_path in the catalogue", func(t *testing.T) {
		// pg_temp is searched first for RELATION names whatever search_path says, so qualification in the
		// body defends the writes; this pin defends the rest and removes the dependency on whichever
		// session applied the migration. Asserted from pg_proc because dropping the SET has no reachable
		// behavioural consequence here -- it was a surviving mutation.
		var cfg []string
		if err := pool.QueryRow(ctx,
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
	})

	t.Run("within ONE statement the newest observation for a date wins, per key column", func(t *testing.T) {
		// The statement trigger decides twice: which row of the batch wins for a (position, date) -- its
		// DISTINCT ON order -- and whether that winner beats the cached row -- the ON CONFLICT guard.
		// Inserting one row per statement only ever exercises the guard, so a leg dropped from the pick
		// survives. These insert a whole batch in one statement with the other legs held equal.
		holder := strings.Repeat("a", 40)
		for _, tc := range []struct {
			name, id string
			rows     [][4]any // (qty, block, bv, pv) -- all on the same UTC date
		}{
			{"block_number", "bd-bn", [][4]any{{1, 700, 0, 0}, {99, 900, 0, 0}, {2, 800, 0, 0}}},
			{"block_version", "bd-bv", [][4]any{{1, 700, 0, 0}, {99, 700, 2, 0}, {2, 700, 1, 0}}},
			{"processing_version", "bd-pv", [][4]any{{1, 700, 0, 0}, {99, 700, 0, 3}, {2, 700, 0, 1}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var vals []string
				for _, r := range tc.rows {
					vals = append(vals, fmt.Sprintf(
						`(sha256($1::bytea), 1, 1, $1, $2, %d, %d, %d, %d, '2026-02-01T06:00:00Z'::timestamptz, 'public.p', 0)`,
						r[0], r[1], r[2], r[3]))
				}
				if _, err := pool.Exec(ctx, `
					INSERT INTO position_state
					    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
					     block_number, block_version, processing_version, block_timestamp, projection, build_id)
					VALUES `+strings.Join(vals, ","), tc.id, holder); err != nil {
					t.Fatal(err)
				}
				var qty int
				if err := pool.QueryRow(ctx, `
					SELECT quantity FROM position_daily
					 WHERE position_id = sha256($1::bytea) AND as_of_date = '2026-02-01'`, tc.id).Scan(&qty); err != nil {
					t.Fatal(err)
				}
				if qty != 99 {
					t.Errorf("cached quantity = %d; want 99. The intra-batch pick is ignoring %s", qty, tc.name)
				}
			})
		}
	})

	t.Run("the holder index exists", func(t *testing.T) {
		var def string
		if err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE tablename = 'position_daily' AND indexname = 'position_daily_holder_idx'`,
		).Scan(&def); err != nil {
			t.Fatalf("position_daily_holder_idx: %v", err)
		}
		if !strings.Contains(def, "(holder_id, as_of_date)") {
			t.Errorf("index = %q; want columns (holder_id, as_of_date) so a holder's series comes back ordered", def)
		}
	})

	t.Run("the grants are SELECT+INSERT+UPDATE, with no DELETE and no TRUNCATE", func(t *testing.T) {
		// Asserted from the catalogue, not from reading the GRANT list: ALTER DEFAULT PRIVILEGES already
		// grants DELETE on every new public table, so only the explicit REVOKE closes it. That is how the
		// same defect was found on position_current.
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
				`SELECT has_table_privilege($1, 'position_daily', $2)`, c.role, c.priv).Scan(&got); err != nil {
				t.Fatalf("has_table_privilege(%s, %s): %v", c.role, c.priv, err)
			}
			if got != c.want {
				t.Errorf("%s %s on position_daily = %v; want %v", c.role, c.priv, got, c.want)
			}
		}
	})

	t.Run("the CHECK constraints reject what position_state rejects", func(t *testing.T) {
		cols := `(position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		          block_number, block_version, processing_version, block_timestamp, projection, build_id)`
		valid := `sha256('chk'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`
		cases := []struct{ name, values, constraint string }{
			{"a wrong-width position_id", `'\x00'::bytea, '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "id_len"},
			{"a NaN quantity", `sha256('c2'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'NaN'::numeric, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
			{"a negative quantity", `sha256('c3'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', -1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "qty_nonneg"},
			{"an uppercase holder_id", `sha256('c4'::bytea), '2026-01-01', 1, 1, 'i', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
			// 40 chars so the length quantifier cannot be what rejects it; this pins the ANCHORING, which
			// a bare-length case leaves open (unanchoring the regex survived the suite).
			{"a valid holder embedded in a longer string", `sha256('c4b'::bytea), '2026-01-01', 1, 1, 'i', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "holder_hex"},
			{"an over-long instrument_key", `sha256('c9'::bytea), '2026-01-01', 1, 1, repeat('k', 2001), 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "instrument_key_len"},
			{"a negative block_number", `sha256('c5'::bytea), '2026-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, -1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "coord_nonneg"},
			{"a zero chain_id", `sha256('c6'::bytea), '2026-01-01', 0, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T00:00:00Z'::timestamptz, 'p', 0`, "chain_pos"},
			{"a pre-genesis block_timestamp", `sha256('c7'::bytea), '2008-01-01', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2008-01-01T00:00:00Z'::timestamptz, 'p', 0`, "ts_sane"},
			{"an as_of_date that disagrees with block_timestamp", `sha256('c8'::bytea), '2026-01-02', 1, 1, 'i', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1, 0, 0, '2026-01-01T23:30:00Z'::timestamptz, 'p', 0`, "as_of_date"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := pool.Exec(ctx, `INSERT INTO position_daily `+cols+` VALUES (`+tc.values+`)`)
				if err == nil {
					t.Fatalf("accepted %s; want rejection by position_daily_%s_chk", tc.name, tc.constraint)
				}
				if !strings.Contains(err.Error(), tc.constraint) {
					t.Errorf("rejected %s with %v; want the %s constraint to fire", tc.name, err, tc.constraint)
				}
			})
		}
		// Control: the same shape with every field valid must be accepted, so the cases above fail for
		// the reason named rather than because the INSERT was malformed.
		if _, err := pool.Exec(ctx, `INSERT INTO position_daily `+cols+` VALUES (`+valid+`) ON CONFLICT DO NOTHING`); err != nil {
			t.Fatalf("the all-valid control was rejected, so the cases above prove nothing: %v", err)
		}
	})

	t.Run("every copied column tracks the winning observation", func(t *testing.T) {
		// projection and build_id are not identity-invariant -- they change when a new build reprocesses
		// -- so a DO UPDATE SET omitting either silently freezes provenance on the first build that wrote
		// the day.
		insert(t, "cols", 10, 1500, 0, 0, "2026-10-01T00:00:00Z")
		insert(t, "cols", 20, 1500, 0, 4, "2026-10-01T06:00:00Z")
		var projection string
		var buildID int
		if err := pool.QueryRow(ctx,
			`SELECT projection, build_id FROM position_daily
			  WHERE position_id = sha256('cols'::bytea) AND as_of_date = '2026-10-01'`,
		).Scan(&projection, &buildID); err != nil {
			t.Fatal(err)
		}
		if projection != "public.proj-4" || buildID != 4 {
			t.Errorf("projection=%q build_id=%d; want public.proj-4 and 4 from the winning observation", projection, buildID)
		}
	})

	t.Run("rebuildable from history by re-running the migration's own statement", func(t *testing.T) {
		// Seeds its own series -- a multi-date position, a same-day reprocess and a closed position -- so
		// the rebuild has version noise to collapse and the subtest works alone as well as in-suite.
		insert(t, "rb-1", 10, 1800, 0, 0, "2026-12-01T00:00:00Z")
		insert(t, "rb-1", 20, 1900, 0, 0, "2026-12-02T00:00:00Z")
		insert(t, "rb-2", 30, 2000, 0, 0, "2026-12-03T00:00:00Z")
		insert(t, "rb-2", 35, 2000, 0, 1, "2026-12-03T06:00:00Z")
		insert(t, "rb-3", 0, 2100, 0, 0, "2026-12-04T00:00:00Z")

		before := map[string]int{}
		rows, err := pool.Query(ctx, `
			SELECT encode(position_id,'hex') || '|' || as_of_date::text, quantity FROM position_daily
			 WHERE position_id IN (SELECT position_id FROM position_state)`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var k string
			var q int
			if err := rows.Scan(&k, &q); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			before[k] = q
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("iterating the pre-rebuild snapshot: %v", err)
		}
		if len(before) == 0 {
			t.Fatal("position_daily is empty; nothing to rebuild")
		}

		if _, err := pool.Exec(ctx, `TRUNCATE position_daily`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, rebuildSQL(t)); err != nil {
			t.Fatalf("rebuild: %v", err)
		}

		after := map[string]int{}
		rows, err = pool.Query(ctx, `SELECT encode(position_id,'hex') || '|' || as_of_date::text, quantity FROM position_daily`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var k string
			var q int
			if err := rows.Scan(&k, &q); err != nil {
				t.Fatal(err)
			}
			after[k] = q
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterating the post-rebuild snapshot: %v", err)
		}
		// `before` was already filtered to positions present in position_state, so the CHECK subtest's
		// direct-INSERT control row is excluded -- a rebuild from history cannot reproduce a row that has
		// no history behind it, and should not be asked to.
		for k, want := range before {
			if got, ok := after[k]; !ok {
				t.Errorf("%s lost by the rebuild", k)
			} else if got != want {
				t.Errorf("%s = %d after rebuild; want %d", k, got, want)
			}
		}
	})

	t.Run("a rebuild overwrites a row that landed in the window with an older observation", func(t *testing.T) {
		// The window: TRUNCATE commits, ingest appends an OLDER observation, the trigger lands it in the
		// now-empty cache (no conflict, so newer-wins never runs), and only then does the rebuild run.
		// With ON CONFLICT DO NOTHING the older row is kept and NO later insert can correct it, because
		// every later insert compares against the poisoned row and loses.
		insert(t, "rebuild-race", 10, 1600, 0, 0, "2026-11-01T00:00:00Z")
		insert(t, "rebuild-race", 20, 1700, 0, 0, "2026-11-01T06:00:00Z")

		if _, err := pool.Exec(ctx, `TRUNCATE position_daily`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
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
		if _, err := pool.Exec(ctx, rebuildSQL(t)); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		if got := dayQty(t, "rebuild-race", "2026-11-01"); got != 20 {
			t.Errorf("2026-11-01 = %d; want 20. DO NOTHING leaves the older 10 in place, unrepairably", got)
		}
	})
}
