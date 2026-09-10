//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// positionStateCols is the insert column list for position_state. Every test that writes history uses
// it, so a schema change lands in one place.
const positionStateCols = "position_id, chain_id, protocol_id, instrument_key, holder_id, quantity, " +
	"block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type"

// positionCurrentFixture is one migrated database plus the seeding and reading each case needs. Every
// test takes its own, so no case can observe another's rows.
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

// obs is one observation of a position. dealType "" stores NULL.
type obs struct {
	qty, block, bv, pv int
	ts                 string
	dealType           string
}

// observe appends one observation to the history, which the trigger propagates to the cache.
func (f *positionCurrentFixture) observe(id string, o obs) {
	f.t.Helper()
	var dt any
	if o.dealType != "" {
		dt = o.dealType
	}
	// run_id is seeded non-NULL and varies with the coordinate, so the whole-row comparison against
	// the winning spine row covers it: a writer that forgets it leaves NULL in the cache.
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state
		    (`+positionStateCols+`, run_id)
		VALUES (sha256($1::bytea), 1, 1, 'inst-' || $1, substr(md5($1) || md5($1), 1, 40), $2, $3, $4, $5::int, $6,
		        'public.proj-' || ($5::int)::text, $5::int, $7, 7700 + $5::int)`,
		id, o.qty, o.block, o.bv, o.pv, o.ts, dt); err != nil {
		f.t.Fatalf("observe %s at block %d: %v", id, o.block, err)
	}
}

// row is the whole cached row for a position, as a comparable map.
func (f *positionCurrentFixture) row(id string) map[string]string {
	f.t.Helper()
	return f.rowOf(`SELECT to_jsonb(c) - 'position_id' FROM position_current c WHERE c.position_id = sha256($1::bytea)`, id)
}

// winner is the same shape read from the spine: the newest observation for that position. The cache
// must equal this on every shared column, which is the assertion that covers a forgotten SET column.
func (f *positionCurrentFixture) winner(id string) map[string]string {
	f.t.Helper()
	return f.rowOf(`
		SELECT to_jsonb(p) - 'position_id' - 'created_at' FROM position_state p
		 WHERE p.position_id = sha256($1::bytea)
		 ORDER BY p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
		 LIMIT 1`, id)
}

func (f *positionCurrentFixture) rowOf(q, id string) map[string]string {
	f.t.Helper()
	var raw map[string]any
	if err := f.pool.QueryRow(f.ctx, q, id).Scan(&raw); err != nil {
		f.t.Fatalf("read %s: %v", id, err)
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

// asReadWritePool connects as stl_readwrite, the role the app uses, so a grant test measures what
// production does rather than what a superuser can do.
func asReadWritePool(ctx context.Context, t *testing.T, owner *pgxpool.Pool) (*pgxpool.Pool, func()) {
	t.Helper()
	if _, err := owner.Exec(ctx, `ALTER ROLE stl_readwrite WITH LOGIN PASSWORD 'pc-test'`); err != nil {
		t.Fatalf("grant stl_readwrite a login: %v", err)
	}
	cfg := owner.Config().Copy()
	cfg.ConnConfig.User = "stl_readwrite"
	cfg.ConnConfig.Password = "pc-test"
	rw, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect as stl_readwrite: %v", err)
	}
	return rw, func() {
		rw.Close()
		if _, err := owner.Exec(ctx, `ALTER ROLE stl_readwrite WITH NOLOGIN`); err != nil {
			t.Errorf("restoring stl_readwrite to NOLOGIN: %v", err)
		}
	}
}

func readMigration(name string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), name))
	return string(raw), err
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

func (f *positionCurrentFixture) rebuild() {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, `CALL rebuild_position_current()`); err != nil {
		f.t.Fatalf("rebuild: %v", err)
	}
}

// The newer-wins comparison, one case per leg, each holding the earlier legs equal.
func TestPositionCurrentNewerWinsPrecedence(t *testing.T) {
	f := newPositionCurrentFixture(t)
	for _, tc := range []struct {
		name             string
		id               string
		base, challenger obs
		keepBase         bool
		why              string
	}{
		{name: "a newer block wins", id: "prec-block",
			base:       obs{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"},
			challenger: obs{qty: 22, block: 200, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"}},
		{name: "an older block does not win even at a higher processing_version", id: "prec-order",
			base:       obs{qty: 11, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "LOAN"},
			challenger: obs{qty: 22, block: 100, pv: 1, ts: "2026-01-03T00:00:00Z", dealType: "LOAN"},
			keepBase:   true, why: "the comparison must lead with block_number, or a reprocess of old history rolls the balance back"},
		{name: "a lower-block reorg replacement does not displace the orphan", id: "prec-reorg",
			base:       obs{qty: 500, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "LOAN"},
			challenger: obs{qty: 0, block: 150, bv: 1, ts: "2026-01-03T00:00:00Z", dealType: "LOAN"},
			keepBase:   true, why: "the documented reorg limit: the orphaned observation stays current"},
		{name: "a newer block_version at the same block wins", id: "prec-bv",
			base:       obs{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"},
			challenger: obs{qty: 22, block: 100, bv: 1, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"}},
		{name: "a newer processing_version at the same block and block_version wins", id: "prec-pv",
			base:       obs{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"},
			challenger: obs{qty: 22, block: 100, pv: 1, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"}},
		{name: "a later block_timestamp at the same block, version and processing_version wins", id: "prec-ts",
			base:       obs{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"},
			challenger: obs{qty: 22, block: 100, ts: "2026-01-02T00:00:00Z", dealType: "LOAN"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.observe(tc.id, tc.base)
			f.observe(tc.id, tc.challenger)
			want := tc.challenger.qty
			if tc.keepBase {
				want = tc.base.qty
			}
			if qty, _, _ := f.current(tc.id); qty != want {
				t.Errorf("current = %d; want %d. %s", qty, want, tc.why)
			}
		})
	}
}

// The winner is decided by the comparison, not by which observation arrived last.
func TestPositionCurrentWinnerIsNotDecidedByArrivalOrder(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("order", obs{qty: 22, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "LOAN"})
	f.observe("order", obs{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	if qty, block, _ := f.current("order"); qty != 22 || block != 200 {
		t.Errorf("current = %d at block %d; want 22 at 200 -- an older observation arriving later must not win", qty, block)
	}
}

// A closing zero is the current value, not an absence.
func TestPositionCurrentClosingZeroIsCurrent(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("close", obs{qty: 100, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "BORROW"})
	f.observe("close", obs{qty: 0, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "BORROW"})
	if qty, block, _ := f.current("close"); qty != 0 || block != 200 {
		t.Errorf("current = %d at block %d; want 0 at 200", qty, block)
	}
}

// Every column of the cache row equals the winning spine row, through both writers. This is the
// assertion a column missing from either DO UPDATE SET list fails.
func TestPositionCurrentEqualsTheWinningSpineRowOnEveryColumn(t *testing.T) {
	for _, writer := range []string{"trigger", "rebuild"} {
		t.Run(writer, func(t *testing.T) {
			f := newPositionCurrentFixture(t)
			const id = "every-col"
			// First observation, then a newer one differing in every payload column including deal_type,
			// so the UPDATE arm (not the INSERT arm) is what has to carry them all.
			f.observe(id, obs{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
			f.observe(id, obs{qty: 22, block: 200, pv: 1, ts: "2026-01-02T00:00:00Z", dealType: "BORROW"})
			if writer == "rebuild" {
				// Drive the rebuild over a cache the trigger has already filled, so it takes the same arm.
				f.rebuild()
			}
			got, want := f.row(id), f.winner(id)
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s: cache %s = %q, winning spine row = %q", writer, k, got[k], v)
				}
			}
			if got["deal_type"] != "BORROW" {
				t.Errorf("%s: deal_type = %q, want BORROW -- the newer observation flipped direction", writer, got["deal_type"])
			}
		})
	}
}

// A deal_type change at a newer coordinate must reach the cache through both writers. The rebuild path
// fails when deal_type is missing from its SET list, which is what this pins.
func TestPositionCurrentDealTypeChangeReachesTheCache(t *testing.T) {
	t.Run("trigger", func(t *testing.T) {
		f := newPositionCurrentFixture(t)
		const id = "dt-flip"
		f.observe(id, obs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
		if got := f.row(id)["deal_type"]; got != "LOAN" {
			t.Fatalf("after the first observation deal_type = %q, want LOAN", got)
		}
		f.observe(id, obs{qty: 5, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "BORROW"})
		if got := f.row(id)["deal_type"]; got != "BORROW" {
			t.Errorf("the trigger left deal_type = %q, want BORROW", got)
		}
	})
	t.Run("rebuild over a cache stale on deal_type alone", func(t *testing.T) {
		f := newPositionCurrentFixture(t)
		const id = "dt-flip"
		f.observe(id, obs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
		f.observe(id, obs{qty: 5, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "BORROW"})
		if _, err := f.pool.Exec(f.ctx,
			`UPDATE position_current SET block_number = 100, block_timestamp = '2026-01-01T00:00:00Z', deal_type = 'LOAN'
			  WHERE position_id = sha256($1::bytea)`, id); err != nil {
			t.Fatalf("stale the cache (owner role): %v", err)
		}
		f.rebuild()
		if got := f.row(id)["deal_type"]; got != "BORROW" {
			t.Errorf("the rebuild left deal_type = %q, want BORROW -- is deal_type missing from its DO UPDATE SET list?", got)
		}
	})
}

// A NULL deal_type is carried as NULL, not skipped.
func TestPositionCurrentCarriesANullDealType(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("dt-null", obs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z"})
	if got := f.row("dt-null")["deal_type"]; got != "NULL" {
		t.Errorf("deal_type = %q, want NULL", got)
	}
}

// One batch carrying several observations of one position picks the newest per position, in-statement.
func TestPositionCurrentIntraBatchPick(t *testing.T) {
	f := newPositionCurrentFixture(t)
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state (`+positionStateCols+`)
		SELECT sha256('batch'::bytea), 1, 1, 'inst-batch', repeat('a', 40), v.qty, v.bn, v.bv, v.pv,
		       v.ts::timestamptz, 'public.proj-0', 0, v.dt
		FROM (VALUES (11, 100, 0, 0, '2026-01-01T00:00:00Z', 'LOAN'),
		             (33, 300, 0, 0, '2026-01-03T00:00:00Z', 'BORROW'),
		             (22, 200, 0, 0, '2026-01-02T00:00:00Z', 'LOAN')) AS v(qty, bn, bv, pv, ts, dt)`); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	got := f.row("batch")
	if got["quantity"] != "33" || got["block_number"] != "300" || got["deal_type"] != "BORROW" {
		t.Errorf("cache holds quantity %s at block %s deal_type %s; want 33 at 300 BORROW -- the in-statement pick is not the newest",
			got["quantity"], got["block_number"], got["deal_type"])
	}
}

// A batch touching several positions writes one row for each.
func TestPositionCurrentMultiPositionBatch(t *testing.T) {
	f := newPositionCurrentFixture(t)
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_state (`+positionStateCols+`)
		SELECT sha256(v.id::bytea), 1, 1, 'inst-' || v.id, substr(md5(v.id) || md5(v.id), 1, 40), v.qty, v.bn, 0, 0,
		       v.ts::timestamptz, 'public.proj-0', 0, 'LOAN'
		FROM (VALUES ('mp-a', 1, 100, '2026-01-01T00:00:00Z'),
		             ('mp-b', 2, 200, '2026-01-02T00:00:00Z'),
		             ('mp-c', 3, 300, '2026-01-03T00:00:00Z')) AS v(id, qty, bn, ts)`); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	var rows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_current`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Errorf("cache holds %d rows after a three-position batch, want 3", rows)
	}
	for _, id := range []string{"mp-a", "mp-b", "mp-c"} {
		if _, _, _ = f.current(id); t.Failed() {
			return
		}
	}
}

// The app role reads and cannot write: the cache is maintained only by the trigger and the procedure.
func TestPositionCurrentIsTriggerOnlyForTheAppRole(t *testing.T) {
	f := newPositionCurrentFixture(t)
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
			`SELECT has_table_privilege($1, 'position_current', $2)`, c.role, c.priv).Scan(&got); err != nil {
			t.Fatalf("has_table_privilege(%s, %s): %v", c.role, c.priv, err)
		}
		if got != c.want {
			t.Errorf("%s %s on position_current = %v; want %v", c.role, c.priv, got, c.want)
		}
	}
}

// The app role can still append to position_state, which is the point of the SECURITY DEFINER trigger:
// the appending role needs no write grant on the cache.
func TestPositionStateWriterNeedsNoGrantOnTheCache(t *testing.T) {
	f := newPositionCurrentFixture(t)
	rw, done := asReadWritePool(f.ctx, f.t, f.pool)
	defer done()
	if _, err := rw.Exec(f.ctx, `
		INSERT INTO position_state (`+positionStateCols+`)
		VALUES (sha256('rw'::bytea), 1, 1, 'inst-rw', repeat('a', 40), 5, 100, 0, 0,
		        '2026-01-01T00:00:00Z'::timestamptz, 'public.proj-0', 0, 'LOAN')`); err != nil {
		t.Fatalf("stl_readwrite could not append to position_state, so the cache trigger is not running as the owner: %v", err)
	}
	if qty, _, _ := f.current("rw"); qty != 5 {
		t.Errorf("the cache holds quantity %d after an app-role append, want 5", qty)
	}
}

// The rebuild is forward-only: it raises a stale row and leaves a row ahead of history alone.
func TestPositionCurrentRebuildIsForwardOnly(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("fwd", obs{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE position_current SET quantity = 99, block_number = 900, block_timestamp = '2026-02-01T00:00:00Z'
		  WHERE position_id = sha256($1::bytea)`, "fwd"); err != nil {
		t.Fatalf("push the cache ahead of history (owner role): %v", err)
	}
	f.rebuild()
	if qty, block, _ := f.current("fwd"); qty != 99 || block != 900 {
		t.Errorf("the rebuild pulled a row ahead of history back to %d at %d; forward-only means it must not", qty, block)
	}
}

// The rebuild reconstructs the cache from history alone.
func TestPositionCurrentIsRebuildableFromHistory(t *testing.T) {
	f := newPositionCurrentFixture(t)
	for _, o := range []obs{
		{qty: 11, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"},
		{qty: 22, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "BORROW"},
	} {
		f.observe("rebuildable", o)
	}
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_current`); err != nil {
		t.Fatalf("empty the cache (owner role): %v", err)
	}
	f.rebuild()
	got, want := f.row("rebuildable"), f.winner("rebuildable")
	for k, v := range want {
		if got[k] != v {
			t.Errorf("after a rebuild from empty, %s = %q, want %q", k, got[k], v)
		}
	}
}

// An empty spine rebuilds without error and writes nothing.
func TestPositionCurrentRebuildOnAnEmptySpine(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.rebuild()
	var rows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_current`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Errorf("rebuild over an empty spine wrote %d rows, want 0", rows)
	}
}

// The procedure pins the settings a hand-run region could forget: tiered reads on, and a pinned
// search_path so a shadowing schema cannot take the write.
func TestPositionCurrentRebuildPinsItsSettings(t *testing.T) {
	f := newPositionCurrentFixture(t)
	var config []string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT proconfig FROM pg_proc WHERE proname = 'rebuild_position_current'`).Scan(&config); err != nil {
		t.Fatalf("read proconfig: %v", err)
	}
	joined := strings.Join(config, " ")
	for _, want := range []string{"timescaledb.enable_tiered_reads=on", "search_path=pg_catalog, public"} {
		if !strings.Contains(joined, want) {
			t.Errorf("rebuild_position_current does not pin %q (proconfig = %v); a caller could compute newest-per-key over a partial table or write into a shadowing schema", want, config)
		}
	}
}

// A shadowing schema ahead of public must not capture the rebuild's writes.
func TestPositionCurrentRebuildResolvesUnderAShadowingSearchPath(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("shadowed", obs{qty: 7, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM position_current`); err != nil {
		t.Fatalf("empty the cache: %v", err)
	}
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS shadow`,
		`CREATE TABLE IF NOT EXISTS shadow.position_current (LIKE public.position_current)`,
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
		SELECT (SELECT count(*) FROM public.position_current), (SELECT count(*) FROM shadow.position_current)`).
		Scan(&public, &shadowed); err != nil {
		t.Fatal(err)
	}
	if public != 1 || shadowed != 0 {
		t.Errorf("under a shadowing search_path the rebuild wrote %d rows to public and %d to shadow; want 1 and 0", public, shadowed)
	}
}

// The trigger is a statement trigger over the transition table, which is what keeps one insert of N
// observations to one upsert pass rather than N.
func TestPositionCurrentTriggerIsPerStatement(t *testing.T) {
	f := newPositionCurrentFixture(t)
	var perRow, hasTransition bool
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (tgtype & 1) = 1, tgnewtable IS NOT NULL
		  FROM pg_trigger WHERE tgrelid = 'position_state'::regclass
		   AND tgname = 'trigger_upsert_position_current'`).Scan(&perRow, &hasTransition); err != nil {
		t.Fatalf("read the trigger: %v", err)
	}
	if perRow || !hasTransition {
		t.Errorf("trigger is per-row=%v, transition table=%v; want a FOR EACH STATEMENT trigger with REFERENCING NEW TABLE", perRow, hasTransition)
	}
}

// The maintainer runs as the owner with a pinned search_path, both mandatory for a SECURITY DEFINER
// function reading unqualified names.
func TestPositionCurrentTriggerFunctionIsSecurityDefinerWithAPinnedPath(t *testing.T) {
	f := newPositionCurrentFixture(t)
	var definer bool
	var config []string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT prosecdef, proconfig FROM pg_proc WHERE proname = 'upsert_position_current'`).Scan(&definer, &config); err != nil {
		t.Fatal(err)
	}
	if !definer {
		t.Error("upsert_position_current is not SECURITY DEFINER, so the appending role would need a write grant on the cache")
	}
	if !strings.Contains(strings.Join(config, " "), "search_path=") {
		t.Errorf("upsert_position_current does not pin search_path (proconfig = %v); mandatory on SECURITY DEFINER", config)
	}
}

// The documented replica-role gap, and that the rebuild is its recovery.
func TestPositionCurrentReplicaRoleGapIsRepairedByRebuild(t *testing.T) {
	f := newPositionCurrentFixture(t)
	var enabled string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT tgenabled FROM pg_trigger
		 WHERE tgrelid = 'position_state'::regclass AND tgname = 'trigger_upsert_position_current'`).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != "O" {
		t.Skipf("tgenabled = %q, not the ORIGIN default -- the documented gap no longer applies and the migration comment should be revisited", enabled)
	}
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(f.ctx, `SET session_replication_role = 'replica'`); err != nil {
		t.Fatal(err)
	}
	// Deferred, so an early exit cannot return a triggers-disabled connection to this pool.
	defer func() {
		if _, err := conn.Exec(f.ctx, `RESET session_replication_role`); err != nil {
			t.Errorf("resetting session_replication_role: %v", err)
		}
	}()
	if _, err := conn.Exec(f.ctx, `
		INSERT INTO position_state (`+positionStateCols+`)
		VALUES (sha256('replica'::bytea), 1, 1, 'inst-replica', repeat('a', 40), 9, 100, 0, 0,
		        '2026-01-01T00:00:00Z'::timestamptz, 'public.proj-0', 0, 'LOAN')`); err != nil {
		t.Fatalf("replica-role insert: %v", err)
	}
	var cached int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM position_current WHERE position_id = sha256($1::bytea)`, "replica").Scan(&cached); err != nil {
		t.Fatal(err)
	}
	if cached != 0 {
		t.Skipf("the trigger fired under the replica role (cache rows = %d); the gap does not reproduce here", cached)
	}
	f.rebuild()
	if qty, block, _ := f.current("replica"); qty != 9 || block != 100 {
		t.Errorf("after the rebuild the cache holds %d at block %d; want 9 at 100 -- the documented recovery does not recover it", qty, block)
	}
}

// A re-observation identical to what the cache holds must not rewrite the row.
func TestPositionCurrentTriggerDoesNotRewriteAnIdenticalRow(t *testing.T) {
	f := newPositionCurrentFixture(t)
	const id = "noop"
	o := obs{qty: 100, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"}
	f.observe(id, o)
	if _, err := f.pool.Exec(f.ctx,
		`DELETE FROM position_state WHERE position_id = sha256($1::bytea)`, id); err != nil {
		t.Fatalf("withdraw the spine row (owner role): %v", err)
	}
	var before string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT ctid::text FROM position_current WHERE position_id = sha256($1::bytea)`, id).Scan(&before); err != nil {
		t.Fatal(err)
	}
	f.observe(id, o)
	var after string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT ctid::text FROM position_current WHERE position_id = sha256($1::bytea)`, id).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Errorf("ctid moved %s -> %s: the trigger rewrote a row at identical coordinates, so every re-observation costs a heap write and WAL", before, after)
	}
	// Negative control: a genuinely newer observation must still be written.
	f.observe(id, obs{qty: 200, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "LOAN"})
	if qty, _, _ := f.current(id); qty != 200 {
		t.Errorf("quantity = %d after a newer observation; want 200 -- the arm does not fire at all, so the no-op assertion above proves nothing", qty)
	}
}

// Applying the migrations twice is a no-op, which a manual apply or a restore depends on.
func TestPositionCurrentMigrationsAreReRunnable(t *testing.T) {
	f := newPositionCurrentFixture(t)
	f.observe("rerun", obs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	for _, name := range []string{"20260819_150000_create_position_current.sql", "20260819_150100_backfill_position_current.sql"} {
		src, err := readMigration(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err := f.pool.Exec(f.ctx, src); err != nil {
			t.Fatalf("re-applying %s: %v", name, err)
		}
	}
	if qty, _, _ := f.current("rerun"); qty != 5 {
		t.Errorf("quantity = %d after re-applying both migrations, want 5", qty)
	}
}

// The cache equals the argmax over position_state, on every column the two tables share, over
// randomised out-of-order histories. Shared columns come from the catalogue, so a column dropped from
// either writer's SET list is caught without naming one here -- the assertion the missing deal_type
// would have failed. The spine harness cannot check this: the cache does not exist in that PR.
func TestPositionCurrentEqualsTheSpineArgmaxOverRandomHistories(t *testing.T) {
	const seeds = 8
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			ctx := context.Background()
			pool, cleanup := setupMigratedPostgres(ctx, t)
			defer cleanup()

			rng := rand.New(rand.NewSource(int64(seed) * 7919))
			rows := generateHistory(rng)
			view := fmt.Sprintf("pv_pc_%d", seed)

			// Re-project everything that has arrived so far, as the runner does, in random batch order.
			var arrived []obsRow
			for bi, batch := range splitBatches(rng, rows) {
				arrived = append(arrived, batch...)
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+view+` AS `+valuesBody(arrived)); err != nil {
					t.Fatalf("create view (batch %d): %v", bi, err)
				}
				if _, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass)`, view); err != nil {
					t.Fatalf("materialize batch %d: %v", bi, err)
				}
			}

			cols := sharedSpineColumns(ctx, t, pool, "position_current")
			if d := diffCacheAgainstSpineArgmax(ctx, t, pool, "position_current", "", cols); d != "" {
				t.Errorf("after the trigger: %s", d)
			}

			// A rebuild over the converged cache changes nothing; over a lagging one it converges.
			before := cacheDigest(ctx, t, pool, "position_current")
			if _, err := pool.Exec(ctx, `CALL rebuild_position_current()`); err != nil {
				t.Fatalf("rebuild over a converged cache: %v", err)
			}
			if cacheDigest(ctx, t, pool, "position_current") != before {
				t.Error("a rebuild over a converged cache changed it")
			}
			if _, err := pool.Exec(ctx, `
				UPDATE position_current SET block_number = 0, block_version = 0, processing_version = 0,
				       block_timestamp = '2009-01-04T00:00:00Z', quantity = 0, deal_type = NULL`); err != nil {
				t.Fatalf("make the cache lag history: %v", err)
			}
			if _, err := pool.Exec(ctx, `CALL rebuild_position_current()`); err != nil {
				t.Fatalf("rebuild over a lagging cache: %v", err)
			}
			if d := diffCacheAgainstSpineArgmax(ctx, t, pool, "position_current", "", cols); d != "" {
				t.Errorf("the rebuild did not converge a lagging cache: %s", d)
			}
		})
	}
}

// sharedSpineColumns lists the columns the cache and position_state both carry, so a comparison over
// them covers deal_type without naming it and cannot silently narrow when a column is added.
func sharedSpineColumns(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache string) []string {
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

// diffCacheAgainstSpineArgmax compares the cache against the newest position_state row per position,
// at the cache's own grain, over the given columns.
func diffCacheAgainstSpineArgmax(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache, grain string, cols []string) string {
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

func cacheDigest(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache string) string {
	t.Helper()
	var d string
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COALESCE(md5(string_agg(x::text, '|' ORDER BY x::text)), '') FROM %s x`, cache)).Scan(&d); err != nil {
		t.Fatalf("digest %s: %v", cache, err)
	}
	return d
}
