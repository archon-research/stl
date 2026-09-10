//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-408 contract: materialize_anchorage_custody() projects Anchorage custody snapshots into
// position_state as OFF-CHAIN observations. chain_id and protocol_id are NULL and block_number is the
// snapshot instant in whole epoch seconds, which the spine enforces for a chain_id-NULL row. The
// instrument is (package, asset) because the source carries one row per asset, and every package is
// pledged, so deal_type is CUSTODY_COLLATERAL. One behaviour per function, own database each.

const (
	anchorageHolder     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	anchorageProjection = "public.position_anchorage_custody"
	// Hand-computed from the fixture's instants, so the epoch encoding has an oracle the SQL does not
	// supply: 2026-04-07T00:00:00Z and the day after.
	anchorageEpochDay1 = 1775520000
	anchorageEpochDay2 = 1775606400
)

func anchorageKey(pkg, asset string) string { return "anchorage:" + pkg + ":" + asset }

// seedAnchorageBase gives a test its own migrated database with one prime and nothing else.
func seedAnchorageBase(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
	if _, err := pool.Exec(ctx,
		`INSERT INTO prime (name, vault_address) VALUES ('itest-anchorage', decode($1, 'hex'))`, anchorageHolder); err != nil {
		t.Fatalf("seed prime: %v", err)
	}
	return ctx, pool
}

// anchorageSnap is one custody snapshot. asset, custody, the LTV instant and the snapshot instant are
// all parameters, because each one changes the projection's identity, grain or observation time.
type anchorageSnap struct {
	pkg, asset, custody string
	qty                 float64
	snapTS, ltvTS       string
	build               int
}

func addSnap(t *testing.T, ctx context.Context, pool *pgxpool.Pool, s anchorageSnap) {
	t.Helper()
	if s.asset == "" {
		s.asset = "BTC"
	}
	if s.custody == "" {
		s.custody = "AnchorageCustody"
	}
	if s.ltvTS == "" {
		// Deliberately NOT the snapshot instant: the projection must observe at snapshot_time.
		s.ltvTS = "2026-01-01T00:00:00Z"
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO anchorage_package_snapshot
		    (prime_id, package_id, pledgor_id, secured_party_id, active, state, current_ltv,
		     exposure_value, package_value, margin_call_ltv, critical_ltv, margin_return_ltv,
		     asset_type, custody_type, asset_price, asset_quantity, asset_weighted_value,
		     ltv_timestamp, snapshot_time, build_id)
		SELECT p.id, $1, 'pledgor', 'secured', true, 'HEALTHY', 0.5,
		       1, 1, 0.7, 0.8, 0.6,
		       $2, $3, 1, $4, 1,
		       $5::timestamptz, $6::timestamptz, $7
		FROM prime p WHERE p.name = 'itest-anchorage'`,
		s.pkg, s.asset, s.custody, s.qty, s.ltvTS, s.snapTS, s.build); err != nil {
		t.Fatalf("snapshot %s/%s at %s: %v", s.pkg, s.asset, s.snapTS, err)
	}
}

// anchorageFixture seeds the history without materializing, so a test can inspect the source first.
//
//	PKG-1/BTC: 3 -> 5    two observations, both kept
//	PKG-2/BTC: 2 -> 0    a package emptied: the open and one closing zero-row
//	PKG-3/BTC: 0 -> 0    never held anything: leading zeros, emits nothing
func anchorageFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, s := range []anchorageSnap{
		{pkg: "PKG-1", qty: 3, snapTS: "2026-04-07T00:00:00Z"},
		{pkg: "PKG-1", qty: 5, snapTS: "2026-04-08T00:00:00Z"},
		{pkg: "PKG-2", qty: 2, snapTS: "2026-04-07T00:00:00Z"},
		{pkg: "PKG-2", qty: 0, snapTS: "2026-04-08T00:00:00Z"},
		{pkg: "PKG-3", qty: 0, snapTS: "2026-04-07T00:00:00Z"},
		{pkg: "PKG-3", qty: 0, snapTS: "2026-04-08T00:00:00Z"},
	} {
		addSnap(t, ctx, pool, s)
	}
}

func seedAnchorage(t *testing.T) (context.Context, *pgxpool.Pool, int64) {
	t.Helper()
	ctx, pool := seedAnchorageBase(t)
	anchorageFixture(t, ctx, pool)
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written); err != nil {
		t.Fatalf("materialize_anchorage_custody: %v", err)
	}
	return ctx, pool, written
}

// PKG-1 two rows + PKG-2 open and close = 4 rows over 2 positions; PKG-3 emits nothing. Every count is
// scoped to this projection, so another projection seeding the shared spine cannot change the result.
func TestMaterializeAnchorageCustodyProjectionShape(t *testing.T) {
	ctx, pool, written := seedAnchorage(t)
	var rows, positions, badIDs, wrongProjection, badVersion int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT position_id),
		       count(*) FILTER (WHERE octet_length(position_id) <> 32),
		       count(*) FILTER (WHERE projection <> $1),
		       count(*) FILTER (WHERE block_version <> 0)
		FROM position_state WHERE projection = $1`, anchorageProjection).
		Scan(&rows, &positions, &badIDs, &wrongProjection, &badVersion); err != nil {
		t.Fatalf("shape: %v", err)
	}
	if written != 4 || rows != 4 || positions != 2 || badIDs != 0 || wrongProjection != 0 || badVersion != 0 {
		t.Errorf("written=%d rows=%d positions=%d bad ids=%d wrong projection=%d block_version<>0=%d; want 4/4/2/0/0/0",
			written, rows, positions, badIDs, wrongProjection, badVersion)
	}
}

// Off-chain rows carry no chain and no protocol, and block_number equals the instant in whole epoch
// seconds — asserted against literals computed from the fixture, not against the view's own expression.
func TestMaterializeAnchorageCustodyIsOffChainAtTheInstant(t *testing.T) {
	ctx, pool, _ := seedAnchorage(t)
	var rows, withChain, withProtocol int
	var blocks []int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE chain_id IS NOT NULL), count(*) FILTER (WHERE protocol_id IS NOT NULL),
		       coalesce(array_agg(DISTINCT block_number ORDER BY block_number), '{}')
		FROM position_state WHERE projection = $1`, anchorageProjection).
		Scan(&rows, &withChain, &withProtocol, &blocks); err != nil {
		t.Fatalf("off-chain query: %v", err)
	}
	if rows == 0 {
		t.Fatal("the projection stored nothing, so the assertions below would pass vacuously")
	}
	if withChain != 0 || withProtocol != 0 {
		t.Errorf("%d rows carry a chain and %d a protocol; want none of either", withChain, withProtocol)
	}
	if len(blocks) != 2 || blocks[0] != anchorageEpochDay1 || blocks[1] != anchorageEpochDay2 {
		t.Errorf("block numbers = %v; want exactly [%d %d], the fixture's two instants in whole epoch seconds",
			blocks, anchorageEpochDay1, anchorageEpochDay2)
	}
}

// The observation instant is snapshot_time, not ltv_timestamp: the fixture holds them apart, since
// ltv_timestamp is when the LTV was priced and can legitimately lag the snapshot.
func TestMaterializeAnchorageCustodyObservesAtSnapshotTimeNotLtvTime(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-T", qty: 4,
		snapTS: "2026-04-07T00:00:00Z", ltvTS: "2026-04-01T00:00:00Z"})
	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody()`); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var ts string
	var block int64
	if err := pool.QueryRow(ctx, `
		SELECT block_timestamp::text, block_number FROM position_state WHERE projection = $1`,
		anchorageProjection).Scan(&ts, &block); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ts, "2026-04-07") || block != anchorageEpochDay1 {
		t.Errorf("observed at %s (block %d); want the 2026-04-07 snapshot instant and block %d, not the 2026-04-01 LTV time",
			ts, block, anchorageEpochDay1)
	}
}

// Every row is CUSTODY_COLLATERAL, not CUSTODY: the source cannot express an unpledged package, so
// CUSTODY would report pledged collateral as unencumbered.
func TestMaterializeAnchorageCustodyIsCollateralNotUnencumbered(t *testing.T) {
	ctx, pool, _ := seedAnchorage(t)
	var deals []string
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(array_agg(DISTINCT deal_type), '{}') FROM position_state WHERE projection = $1`,
		anchorageProjection).Scan(&deals); err != nil {
		t.Fatal(err)
	}
	if len(deals) != 1 || deals[0] != "CUSTODY_COLLATERAL" {
		t.Errorf("deal types = %v; want only CUSTODY_COLLATERAL (pledgor_id, secured_party_id and current_ltv are all NOT NULL in the source)", deals)
	}
}

func TestMaterializeAnchorageCustodyPerPosition(t *testing.T) {
	ctx, pool, _ := seedAnchorage(t)
	for _, c := range []struct {
		name     string
		key      string
		wantRows int
		wantQty  string
	}{
		{"PKG-1 keeps both observations, latest quantity 5", anchorageKey("PKG-1", "BTC"), 2, "5"},
		{"PKG-2 emptied: the open and one closing zero-row", anchorageKey("PKG-2", "BTC"), 2, "0"},
		{"PKG-3 never held anything, so nothing is emitted", anchorageKey("PKG-3", "BTC"), 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			var n int
			var qty string
			if err := pool.QueryRow(ctx, `
				SELECT count(*), coalesce((SELECT quantity::text FROM position_state
				  WHERE instrument_key = $1 ORDER BY block_number DESC LIMIT 1), '')
				FROM position_state WHERE instrument_key = $1`, c.key).Scan(&n, &qty); err != nil {
				t.Fatalf("query: %v", err)
			}
			if n != c.wantRows || qty != c.wantQty {
				t.Errorf("rows=%d latest quantity=%q; want %d and %q", n, qty, c.wantRows, c.wantQty)
			}
		})
	}
}

// The instrument carries the asset, so a package holding two assets is two positions rather than two
// rows on one observation key, which the materializer would refuse as a double emit.
func TestMaterializeAnchorageCustodyKeysTheAssetIntoTheInstrument(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-M", asset: "BTC", qty: 3, snapTS: "2026-04-07T00:00:00Z"})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-M", asset: "ETH", qty: 9, snapTS: "2026-04-07T00:00:00Z"})
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written); err != nil {
		t.Fatalf("a two-asset package must project, not collide: %v", err)
	}
	var keys []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(instrument_key || '=' || quantity::text ORDER BY instrument_key), '{}')
		FROM position_state WHERE projection = $1`, anchorageProjection).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	want := anchorageKey("PKG-M", "BTC") + "=3," + anchorageKey("PKG-M", "ETH") + "=9"
	if written != 2 || strings.Join(keys, ",") != want {
		t.Errorf("written=%d keys=%v; want 2 and %s", written, keys, want)
	}
}

// The holder is the prime's vault address and there is exactly one of them.
func TestMaterializeAnchorageCustodyHolderIsTheVaultAddress(t *testing.T) {
	ctx, pool, _ := seedAnchorage(t)
	var distinct int
	var holder string
	if err := pool.QueryRow(ctx, `
		SELECT count(DISTINCT holder_id), min(holder_id) FROM position_state WHERE projection = $1`,
		anchorageProjection).Scan(&distinct, &holder); err != nil {
		t.Fatal(err)
	}
	if distinct != 1 || holder != anchorageHolder {
		t.Errorf("%d distinct holders, first %s; want exactly 1, the prime's vault address %s", distinct, holder, anchorageHolder)
	}
}

// A package emptied and later refilled keeps the open, the close and the refill.
func TestMaterializeAnchorageCustodySurvivesARefill(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	for _, s := range []anchorageSnap{
		{pkg: "PKG-R", qty: 5, snapTS: "2026-04-07T00:00:00Z"},
		{pkg: "PKG-R", qty: 0, snapTS: "2026-04-08T00:00:00Z"},
		{pkg: "PKG-R", qty: 7, snapTS: "2026-04-09T00:00:00Z"},
		{pkg: "PKG-R", qty: 0, snapTS: "2026-04-10T00:00:00Z"},
		{pkg: "PKG-R", qty: 0, snapTS: "2026-04-11T00:00:00Z"},
	} {
		addSnap(t, ctx, pool, s)
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody()`); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var series []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(quantity::text ORDER BY block_number), '{}')
		FROM position_state WHERE instrument_key = $1`, anchorageKey("PKG-R", "BTC")).Scan(&series); err != nil {
		t.Fatal(err)
	}
	if strings.Join(series, ",") != "5,0,7,0" {
		t.Errorf("series = %v; want 5,0,7,0 — the open, its close, the refill and its close, with the repeated zero dropped", series)
	}
}

// Each input the view cannot place is refused by name, before anything is written.
func TestMaterializeAnchorageCustodyRefusesWhatItCannotPlace(t *testing.T) {
	for _, c := range []struct {
		name  string
		snaps []anchorageSnap
		want  string
	}{
		{"an unknown custodian, which the view would drop",
			[]anchorageSnap{{pkg: "PKG-X", qty: 7, snapTS: "2026-04-07T00:00:00Z", custody: "SomeOtherCustodian"}},
			`custody_type 'SomeOtherCustodian' on 1 snapshot(s) is not a known custodian`},
		{"two snapshots inside one second, which collapse onto one block",
			[]anchorageSnap{
				{pkg: "PKG-S", qty: 1, snapTS: "2026-04-07T00:00:00.100Z"},
				{pkg: "PKG-S", qty: 2, snapTS: "2026-04-07T00:00:00.900Z"}},
			"has 2 snapshots within one second"},
		{"a blank asset id, which position_key would reject without naming the row",
			[]anchorageSnap{{pkg: "PKG-B", asset: " ", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		{"a package id carrying the key delimiter",
			[]anchorageSnap{{pkg: "PKG;B", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		// ':' is legal in a native instrument_key (VEC-400 names Sky's registry:ilk), so a lone colon
		// must PROJECT. Only a genuine collision refuses: 'A:B'+'C' and 'A'+'B:C' both render
		// 'anchorage:A:B:C', which would give two assets one position_id.
		{"two identities that render one instrument_key",
			[]anchorageSnap{
				{pkg: "A:B", asset: "C", qty: 1, snapTS: "2026-04-07T00:00:00Z"},
				{pkg: "A", asset: "B:C", qty: 2, snapTS: "2026-04-08T00:00:00Z"}},
			"render the instrument_key"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, pool := seedAnchorageBase(t)
			for _, s := range c.snaps {
				addSnap(t, ctx, pool, s)
			}
			var written int64
			err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written)
			if err == nil {
				t.Fatalf("the run succeeded writing %d rows; want a refusal naming %q", written, c.want)
			}
			if !strings.Contains(err.Error(), "refusing to run") || !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not refuse naming %q", err.Error(), c.want)
			}
			var rows int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 0 {
				t.Errorf("a refused run wrote %d rows, want 0", rows)
			}
		})
	}
}

// The same fixture without the offending row projects normally, so the refusal above is the row's
// fault and not an implementation that always raises.
func TestMaterializeAnchorageCustodyRunsOnceTheOffenderIsRemoved(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-OK", qty: 4, snapTS: "2026-04-07T00:00:00Z"})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-X", qty: 7, snapTS: "2026-04-07T00:00:00Z", custody: "SomeOtherCustodian"})
	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody()`); err == nil {
		t.Fatal("expected the unknown custodian to refuse the run")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM anchorage_package_snapshot WHERE custody_type = 'SomeOtherCustodian'`); err != nil {
		t.Fatal(err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written); err != nil {
		t.Fatalf("after removing the offender the run must proceed: %v", err)
	}
	if written != 1 {
		t.Errorf("written=%d; want the one mapped snapshot", written)
	}
}

// Several offenders are reported in a stable order, so an operator sees the same message twice.
func TestMaterializeAnchorageCustodyReportsOffendersInOrder(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	for _, c := range []string{"ZCustodian", "ACustodian", "MCustodian"} {
		addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-" + c, qty: 1, snapTS: "2026-04-07T00:00:00Z", custody: c})
	}
	err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(new(int64))
	if err == nil {
		t.Fatal("expected a refusal")
	}
	a, m, z := strings.Index(err.Error(), "ACustodian"), strings.Index(err.Error(), "MCustodian"), strings.Index(err.Error(), "ZCustodian")
	if a < 0 || m < 0 || z < 0 || !(a < m && m < z) {
		t.Errorf("offenders are not reported in a stable sorted order (A=%d M=%d Z=%d): %s", a, m, z, err.Error())
	}
}

// p_build_id is stamped on every appended row and on the run record.
func TestMaterializeAnchorageCustodyStampsTheBuildID(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	anchorageFixture(t, ctx, pool)
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody(77)`).Scan(&written); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var stamped, rows, runs int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FILTER (WHERE build_id = 77) FROM position_state WHERE projection = $1),
		       (SELECT count(*) FROM position_state WHERE projection = $1),
		       (SELECT count(*) FROM position_projection_run WHERE projection = $1 AND build_id = 77)`,
		anchorageProjection).Scan(&stamped, &rows, &runs); err != nil {
		t.Fatal(err)
	}
	if written != 4 || stamped != 4 || rows != 4 || runs != 1 {
		t.Errorf("written=%d stamped=%d rows=%d run records=%d; want 4/4/4/1", written, stamped, rows, runs)
	}
}

// A reprocess at the same instant under a new build is its own observation.
func TestMaterializeAnchorageCustodyKeepsEachProcessingVersion(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-P", qty: 4, snapTS: "2026-04-07T00:00:00Z", build: 0})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-P", qty: 6, snapTS: "2026-04-07T00:00:00Z", build: 1})
	var pv0, pv1 int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE processing_version = 0), count(*) FILTER (WHERE processing_version = 1)
		FROM anchorage_package_snapshot WHERE package_id = 'PKG-P'`).Scan(&pv0, &pv1); err != nil {
		t.Fatal(err)
	}
	if pv0 != 1 || pv1 != 1 {
		t.Fatalf("the source holds pv0=%d pv1=%d; the fixture must produce one of each or this tests nothing", pv0, pv1)
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody()`); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var pvs []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(processing_version::text || '=' || quantity::text ORDER BY processing_version), '{}')
		FROM position_state WHERE instrument_key = $1`, anchorageKey("PKG-P", "BTC")).Scan(&pvs); err != nil {
		t.Fatal(err)
	}
	if strings.Join(pvs, ",") != "0=4,1=6" {
		t.Errorf("stored %v; want both processing versions, 0=4 and 1=6", pvs)
	}
}

// A second run appends nothing but still records that a sweep completed.
func TestMaterializeAnchorageCustodyIsIdempotent(t *testing.T) {
	ctx, pool, _ := seedAnchorage(t)
	var second int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&second); err != nil {
		t.Fatalf("second run: %v", err)
	}
	var rows, runs int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM position_state WHERE projection = $1),
		       (SELECT count(*) FROM position_projection_run WHERE projection = $1)`,
		anchorageProjection).Scan(&rows, &runs); err != nil {
		t.Fatal(err)
	}
	if second != 0 || rows != 4 || runs != 2 {
		t.Errorf("second run appended %d rows leaving %d, with %d run records; want 0/4/2", second, rows, runs)
	}
}

// A lone sub-second snapshot is legal (only a pair inside one second collides), and its block is the
// instant FLOORED to the second. The materializer hardcodes floor, so a projection that rounded or
// took the ceiling would be refused outright on exactly this row.
func TestMaterializeAnchorageCustodyFloorsASubSecondInstant(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-F", qty: 4, snapTS: "2026-04-07T00:00:00.750Z"})
	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody()`); err != nil {
		t.Fatalf("a single sub-second snapshot must project: %v", err)
	}
	var block int64
	if err := pool.QueryRow(ctx,
		`SELECT block_number FROM position_state WHERE projection = $1`, anchorageProjection).Scan(&block); err != nil {
		t.Fatal(err)
	}
	if block != anchorageEpochDay1 {
		t.Errorf("block_number = %d for a .750 instant; want %d, the instant floored (ceil or round would give %d and the materializer would refuse it)",
			block, anchorageEpochDay1, anchorageEpochDay1+1)
	}
}

// The view itself drops an unknown custodian rather than projecting it under pledge semantics that may
// not hold. A direct reader of the view sees the same set the materializer would.
func TestMaterializeAnchorageCustodyViewDropsAnUnknownCustodian(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-OK", qty: 3, snapTS: "2026-04-07T00:00:00Z"})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-X", qty: 7, snapTS: "2026-04-07T00:00:00Z", custody: "SomeOtherCustodian"})
	var rows int
	var keys []string
	if err := pool.QueryRow(ctx, `
		SELECT count(*), coalesce(array_agg(instrument_key ORDER BY instrument_key), '{}')
		FROM position_anchorage_custody`).Scan(&rows, &keys); err != nil {
		t.Fatalf("view: %v", err)
	}
	if rows != 1 || len(keys) != 1 || keys[0] != anchorageKey("PKG-OK", "BTC") {
		t.Errorf("the view emitted %d rows %v; want only the known custodian's %s", rows, keys, anchorageKey("PKG-OK", "BTC"))
	}
}

// A lone ':' in a native identity must PROJECT, not refuse: VEC-400 names Sky's registry:ilk as a
// native instrument_key form, so banning the character would reject valid identities.
func TestAnchorageColonIsLegalWhenUnambiguous(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG:SEG", asset: "BTC", qty: 4, snapTS: "2026-04-07T00:00:00Z"})
	var n int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&n); err != nil {
		t.Fatalf("a colon-bearing identity must project: %v", err)
	}
	var key string
	if err := pool.QueryRow(ctx, `SELECT instrument_key FROM position_state`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != "anchorage:PKG:SEG:BTC" {
		t.Errorf("instrument_key = %q; want anchorage:PKG:SEG:BTC", key)
	}
}

// This projection never closes from absence, so a package that stops being reported keeps its last
// quantity and reads as live pledged collateral. One package going is a delisting; several going at
// once is the feed failing, and carrying that forward silently is worse than stopping.
func TestMaterializeAnchorageRefusesWhenSeveralPackagesStopBeingReported(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	for _, p := range []string{"PKG-A", "PKG-B", "PKG-C"} {
		addSnap(t, ctx, pool, anchorageSnap{pkg: p, qty: 5, snapTS: "2026-06-01T00:00:00Z", ltvTS: "2026-06-01T00:00:00Z"})
	}
	// The next snapshot carries only one of the three.
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-A", qty: 5, snapTS: "2026-06-01T01:00:00Z", ltvTS: "2026-06-01T01:00:00Z"})

	_, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody()`)
	if err == nil {
		t.Fatal("the run succeeded while two live packages stopped being reported")
	}
	if !strings.Contains(err.Error(), "stopped being reported") {
		t.Errorf("refused with %v; want the feed-failure refusal", err)
	}
	for _, p := range []string{"PKG-B", "PKG-C"} {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("the refusal does not name %s: %v", p, err)
		}
	}
}

// The control: a single package leaving is a delisting and stays open by design, so the run must
// not stop. Without this the guard would refuse on ordinary custody churn.
func TestMaterializeAnchorageOnePackageLeavingDoesNotRefuse(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	for _, p := range []string{"PKG-A", "PKG-B", "PKG-C"} {
		addSnap(t, ctx, pool, anchorageSnap{pkg: p, qty: 5, snapTS: "2026-06-01T00:00:00Z", ltvTS: "2026-06-01T00:00:00Z"})
	}
	for _, p := range []string{"PKG-A", "PKG-B"} {
		addSnap(t, ctx, pool, anchorageSnap{pkg: p, qty: 5, snapTS: "2026-06-01T01:00:00Z", ltvTS: "2026-06-01T01:00:00Z"})
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody()`); err != nil {
		t.Fatalf("the run refused although only one package left: %v", err)
	}
}
