//go:build integration

package migrator_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-408 contract: materialize_anchorage_custody() projects Anchorage custody snapshots into
// position_state as OFF-CHAIN observations. chain_id and protocol_id are NULL and block_number is the
// snapshot instant in whole epoch seconds, which the spine enforces for a chain_id-NULL row. The
// instrument is (package, asset) because the source carries one row per asset, and every package is
// pledged, so deal_type is CUSTODY_COLLATERAL. One behaviour per function, own database each.

const (
	anchoragePrime      = "itest-anchorage"
	anchorageHolder     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	anchoragePrime2     = "itest-anchorage-2"
	anchorageCustody2   = "SecondCustody"
	anchorageHolder2    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	anchorageProjection = "public.position_anchorage_custody"
	// Hand-computed from the fixture's instants, so the epoch encoding has an oracle the SQL does not
	// supply: 2026-04-07T00:00:00Z and the day after.
	anchorageEpochDay1 = 1775520000
	anchorageEpochDay2 = 1775606400
)

func anchorageKey(pkg, asset string) string { return "anchorage:" + pkg + ":" + asset }

// admitSecondCustodian widens anchorage_known_custody_type the way a later migration would. The shipped
// view carries one custodian, which is what makes every two-custodian collision unreachable today.
func admitSecondCustodian(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW anchorage_known_custody_type AS
		SELECT * FROM (VALUES ('AnchorageCustody'), ('`+anchorageCustody2+`')) AS m(custody_type)`); err != nil {
		t.Fatalf("admit a second custodian: %v", err)
	}
}

// seedAnchorageBase gives a test its own migrated database with one prime and nothing else.
func seedAnchorageBase(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
	addPrime(t, ctx, pool, anchoragePrime, anchorageHolder)
	return ctx, pool
}

func addPrime(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name, vaultHex string) {
	t.Helper()
	tag, err := pool.Exec(ctx,
		`INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), $1, decode($2, 'hex'))`,
		name, vaultHex)
	if err != nil {
		t.Fatalf("seed prime %s: %v", name, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("seeding prime %s inserted %d rows, want 1", name, tag.RowsAffected())
	}
}

// anchorageSnap is one custody snapshot. prime, asset, custody, the LTV instant and the snapshot
// instant are all parameters, because each changes the projection's identity, grain or observation time.
type anchorageSnap struct {
	pkg, asset, custody, prime string
	qty                        float64
	snapTS, ltvTS              string
	build                      int
}

func addSnap(t *testing.T, ctx context.Context, pool *pgxpool.Pool, s anchorageSnap) {
	t.Helper()
	if s.asset == "" {
		s.asset = "BTC"
	}
	if s.custody == "" {
		s.custody = "AnchorageCustody"
	}
	if s.prime == "" {
		s.prime = anchoragePrime
	}
	if s.ltvTS == "" {
		// Deliberately NOT the snapshot instant: the projection must observe at snapshot_time.
		s.ltvTS = "2026-01-01T00:00:00Z"
	}
	tag, err := pool.Exec(ctx, `
		INSERT INTO anchorage_package_snapshot
		    (prime_id, package_id, pledgor_id, secured_party_id, active, state, current_ltv,
		     exposure_value, package_value, margin_call_ltv, critical_ltv, margin_return_ltv,
		     asset_type, custody_type, asset_price, asset_quantity, asset_weighted_value,
		     ltv_timestamp, snapshot_time, build_id)
		SELECT p.id, $1, 'pledgor', 'secured', true, 'HEALTHY', 0.5,
		       1, 1, 0.7, 0.8, 0.6,
		       $2, $3, 1, $4, 1,
		       $5::timestamptz, $6::timestamptz, $7
		FROM prime p WHERE p.name = $8`,
		s.pkg, s.asset, s.custody, s.qty, s.ltvTS, s.snapTS, s.build, s.prime)
	if err != nil {
		t.Fatalf("snapshot %s/%s at %s: %v", s.pkg, s.asset, s.snapTS, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("snapshot %s/%s resolved prime %q to %d rows, want 1", s.pkg, s.asset, s.prime, tag.RowsAffected())
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
		setup func(*testing.T, context.Context, *pgxpool.Pool)
		snaps []anchorageSnap
		want  string
	}{
		{"an unknown custodian, which the view would drop", nil,
			[]anchorageSnap{{pkg: "PKG-X", qty: 7, snapTS: "2026-04-07T00:00:00Z", custody: "SomeOtherCustodian"}},
			`custody_type 'SomeOtherCustodian' on 1 snapshot(s) is not a known custodian`},
		{"two snapshots inside one second, which collapse onto one block", nil,
			[]anchorageSnap{
				{pkg: "PKG-S", qty: 1, snapTS: "2026-04-07T00:00:00.100Z"},
				{pkg: "PKG-S", qty: 2, snapTS: "2026-04-07T00:00:00.900Z"}},
			"has 2 snapshots within one second"},
		// custody_type is not on the observation key, so a second-sharing pair collides however the rows
		// differ. Both cases need a second custodian admitted before the view will carry them.
		{"two custody types apart inside one second", admitSecondCustodian,
			[]anchorageSnap{
				{pkg: "PKG-C", qty: 1, snapTS: "2026-04-07T00:00:00.100Z"},
				{pkg: "PKG-C", qty: 2, snapTS: "2026-04-07T00:00:00.900Z", custody: anchorageCustody2}},
			"has 2 snapshots within one second"},
		{"two custody types on the same instant", admitSecondCustodian,
			[]anchorageSnap{
				{pkg: "PKG-C", qty: 1, snapTS: "2026-04-07T00:00:00Z"},
				{pkg: "PKG-C", qty: 2, snapTS: "2026-04-07T00:00:00Z", custody: anchorageCustody2}},
			"has 2 snapshots within one second"},
		// holder_id is the vault address in hex, so a short one renders fewer than 40 characters and
		// fails the spine's CHECK with a 23514 naming no row. Guarded here instead (VEC-819).
		{"a 19-byte vault address", func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			tag, err := pool.Exec(ctx, `UPDATE prime SET vault_address = decode($1, 'hex') WHERE name = $2`,
				anchorageHolder[:38], anchoragePrime)
			if err != nil {
				t.Fatalf("shorten the vault address: %v", err)
			}
			if tag.RowsAffected() != 1 {
				t.Fatalf("shortened %d prime rows, want 1", tag.RowsAffected())
			}
		},
			[]anchorageSnap{{pkg: "PKG-V", qty: 3, snapTS: "2026-04-07T00:00:00Z"}},
			"has a vault address of 19 bytes"},
		{"a blank asset id", nil,
			[]anchorageSnap{{pkg: "PKG-B", asset: " ", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		{"a tab-only package id, which btrim would let through", nil,
			[]anchorageSnap{{pkg: "\t", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		{"a newline-only asset id", nil,
			[]anchorageSnap{{pkg: "PKG-N", asset: "\n", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		{"a blank package id", nil,
			[]anchorageSnap{{pkg: " ", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		{"a package id carrying the key delimiter", nil,
			[]anchorageSnap{{pkg: "PKG;B", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		{"an asset id carrying the key delimiter", nil,
			[]anchorageSnap{{pkg: "PKG-D", asset: "BT;C", qty: 1, snapTS: "2026-04-07T00:00:00Z"}},
			"blank or delimiter-bearing identity"},
		// ':' is legal in a native instrument_key (VEC-400 names Sky's registry:ilk), so a lone colon
		// must PROJECT. Only a genuine collision refuses: 'A:B'+'C' and 'A'+'B:C' both render
		// 'anchorage:A:B:C', which would give two assets one position_id.
		{"two identities that render one instrument_key", nil,
			[]anchorageSnap{
				{pkg: "A:B", asset: "C", qty: 1, snapTS: "2026-04-07T00:00:00Z"},
				{pkg: "A", asset: "B:C", qty: 2, snapTS: "2026-04-08T00:00:00Z"}},
			"render the instrument_key"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, pool := seedAnchorageBase(t)
			if c.setup != nil {
				c.setup(t, ctx, pool)
			}
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

// The wrapper is the only path the runner calls, so it has to forward the writer run to the spine or
// every row this projection appends is provenance-free (ADR-0006 §2). The run record is the witness:
// its run_id can only have arrived through the wrapper's own parameter.
func TestMaterializeAnchorageCustodyForwardsTheWriterRun(t *testing.T) {
	ctx, pool, _ := seedAnchorage(t)
	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody(7, 9182)`); err != nil {
		t.Fatalf("materialize_anchorage_custody with a run: %v", err)
	}
	var runID *int64
	var buildID int
	if err := pool.QueryRow(ctx, `
		SELECT run_id, build_id FROM position_projection_run
		 WHERE projection = 'public.position_anchorage_custody'
		 ORDER BY created_at DESC LIMIT 1`).Scan(&runID, &buildID); err != nil {
		t.Fatalf("read the run record: %v", err)
	}
	if runID == nil || *runID != 9182 || buildID != 7 {
		t.Errorf("run record = run_id %v build_id %d, want 9182 and 7", runID, buildID)
	}

	// The runner passes the two provenance arguments BY NAME, so these parameter names are the
	// contract: renaming one here leaves this migration valid and breaks that projection only.
	var args []string
	if err := pool.QueryRow(ctx, `
		SELECT proargnames::text[] FROM pg_proc WHERE proname = 'materialize_anchorage_custody'`).Scan(&args); err != nil {
		t.Fatalf("read the wrapper's parameter names: %v", err)
	}
	for _, want := range []string{"p_build_id", "p_run_id"} {
		found := false
		for _, a := range args {
			if a == want {
				found = true
			}
		}
		if !found {
			t.Errorf("materialize_anchorage_custody declares %v, missing %s -- the runner calls it by name", args, want)
		}
	}
}

func TestMaterializeAnchorageCustodySeparatesTwoPrimesOnOnePackage(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addPrime(t, ctx, pool, anchoragePrime2, anchorageHolder2)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-J", qty: 3, snapTS: "2026-04-07T00:00:00Z"})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-J", qty: 8, snapTS: "2026-04-07T00:00:00Z", prime: anchoragePrime2})
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written); err != nil {
		t.Fatalf("one package held by two primes must project as two positions, not collide: %v", err)
	}
	var positions int
	var held []string
	if err := pool.QueryRow(ctx, `
		SELECT count(DISTINCT position_id),
		       coalesce(array_agg(holder_id || '=' || quantity::text ORDER BY holder_id), '{}')
		FROM position_state WHERE instrument_key = $1`, anchorageKey("PKG-J", "BTC")).Scan(&positions, &held); err != nil {
		t.Fatal(err)
	}
	want := anchorageHolder + "=3," + anchorageHolder2 + "=8"
	if written != 2 || positions != 2 || strings.Join(held, ",") != want {
		t.Errorf("written=%d positions=%d holders=%v; want 2, 2 distinct position_ids and %s", written, positions, held, want)
	}
}

// Two custody types one second apart are two observations of ONE position, so the refusal above must
// key on the second rather than on the presence of two custody types.
func TestMaterializeAnchorageCustodyAllowsTwoCustodyTypesAtDifferentSeconds(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	admitSecondCustodian(t, ctx, pool)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-E", qty: 1, snapTS: "2026-04-07T00:00:00Z"})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-E", qty: 2, snapTS: "2026-04-07T00:00:01Z", custody: anchorageCustody2})
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written); err != nil {
		t.Fatalf("two custody types a second apart are two observations, not a collision: %v", err)
	}
	var positions int
	var series []string
	if err := pool.QueryRow(ctx, `
		SELECT count(DISTINCT position_id),
		       coalesce(array_agg(block_number::text || '=' || quantity::text ORDER BY block_number), '{}')
		FROM position_state WHERE instrument_key = $1`, anchorageKey("PKG-E", "BTC")).Scan(&positions, &series); err != nil {
		t.Fatal(err)
	}
	want := strconv.FormatInt(anchorageEpochDay1, 10) + "=1," + strconv.FormatInt(anchorageEpochDay1+1, 10) + "=2"
	if written != 2 || positions != 1 || strings.Join(series, ",") != want {
		t.Errorf("written=%d positions=%d series=%v; want 2 observations of 1 position, %s", written, positions, series, want)
	}
}

// The cap names the five alphabetically-first offenders, so an operator paging through a broken feed
// sees a stable prefix rather than an arbitrary sample.
func TestMaterializeAnchorageCustodyNamesTheFirstFiveOffenders(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	for _, c := range []string{"FCustodian", "BCustodian", "DCustodian", "ACustodian", "ECustodian", "CCustodian"} {
		addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-" + c, qty: 1, snapTS: "2026-04-07T00:00:00Z", custody: c})
	}
	err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(new(int64))
	if err == nil {
		t.Fatal("expected a refusal")
	}
	for _, named := range []string{"ACustodian", "BCustodian", "CCustodian", "DCustodian", "ECustodian"} {
		if !strings.Contains(err.Error(), named) {
			t.Errorf("%s is among the first five offenders but was not named: %s", named, err.Error())
		}
	}
	if strings.Contains(err.Error(), "FCustodian") {
		t.Errorf("FCustodian is the sixth offender and must fall outside the cap: %s", err.Error())
	}
}

// The wrapper is the only path the runner calls, so a window it cannot forward is a window this
// projection can never run with. The run record stamps what the spine actually received.
func TestMaterializeAnchorageCustodyForwardsTheWindow(t *testing.T) {
	ctx, pool, _ := seedAnchorage(t)

	if _, err := pool.Exec(ctx, `SELECT materialize_anchorage_custody(0, NULL, interval '36 hours')`); err != nil {
		t.Fatalf("calling with a window: %v", err)
	}

	var window *string
	if err := pool.QueryRow(ctx, `
		SELECT window_interval::text FROM position_projection_run
		 WHERE projection = $1 ORDER BY created_at DESC LIMIT 1`, anchorageProjection).Scan(&window); err != nil {
		t.Fatalf("reading the run record: %v", err)
	}
	if window == nil {
		t.Fatal("the run recorded no window, so the wrapper dropped it")
	}
	if *window != "36:00:00" {
		t.Errorf("the run recorded window %q; want the 36 hours the wrapper was called with", *window)
	}
}

// The guard reads only the primes the batch actually references, so a prime with a malformed vault
// and no snapshots must not stop the run. Dropping that scoping passes every other test here.
func TestMaterializeAnchorageCustodyIgnoresAnIdlePrimeWithABadVault(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addPrime(t, ctx, pool, "itest-anchorage-idle", anchorageHolder2[:38])
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-OK", qty: 4, snapTS: "2026-04-07T00:00:00Z"})

	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written); err != nil {
		t.Fatalf("an idle prime with a 19-byte vault must not refuse the run: %v", err)
	}
	if written == 0 {
		t.Error("the run appended nothing, so it cannot show the good prime still projected")
	}
}

// Every width case is short, so <> 20 weakened to < 20 would survive. This is the case above it.
func TestMaterializeAnchorageCustodyRefusesAnOversizeVault(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	if _, err := pool.Exec(ctx, `UPDATE prime SET vault_address = decode($1, 'hex') WHERE name = $2`,
		anchorageHolder+"ff", anchoragePrime); err != nil {
		t.Fatalf("lengthen the vault address: %v", err)
	}
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-W", qty: 2, snapTS: "2026-04-07T00:00:00Z"})

	err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(new(int64))
	if err == nil {
		t.Fatal("a 21-byte vault address must refuse by name")
	}
	if !strings.Contains(err.Error(), "has a vault address of 21 bytes") {
		t.Errorf("error %q does not name the oversize vault", err.Error())
	}
}

// VEC-809. The key was spelled twice: once in the view, once in the wrapper's injectivity guard, which
// re-derived it from the source rather than reading the view's. The two agreed, so nothing was wrong
// today — but changing the view alone left the guard validating the old shape, still passing and
// protecting nothing. This pins the structural property the fix buys: exactly one definition.
func TestAnchorageInstrumentKeyIsSpelledInOnePlace(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)

	var helper, view, wrapper string
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_functiondef('anchorage_instrument_key(text,text)'::regprocedure),
		       pg_get_viewdef('position_anchorage_custody'::regclass),
		       pg_get_functiondef('materialize_anchorage_custody(integer,bigint,interval)'::regprocedure)`).
		Scan(&helper, &view, &wrapper); err != nil {
		t.Fatalf("reading the catalogue definitions: %v", err)
	}

	if !strings.Contains(helper, "'anchorage:'") {
		t.Error("anchorage_instrument_key does not build the prefix, so it is not the definition")
	}
	for _, c := range []struct{ what, def string }{
		{"the view", view},
		{"the wrapper's guard", wrapper},
	} {
		if strings.Contains(c.def, "'anchorage:'") {
			t.Errorf("%s carries its own copy of the key expression; it must call anchorage_instrument_key", c.what)
		}
	}
}

// The structural test above can be satisfied by a helper nothing meaningfully depends on. This one
// moves the single definition and requires BOTH the projected key and the guard to follow it: two
// assets of one package render one key, which is precisely what the injectivity guard exists to catch.
// Against the two-expression version the guard keeps deriving with asset_type, sees no collision, and
// lets two assets interleave under one position_id with no error.
func TestAnchorageGuardFollowsTheKeyDefinition(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-K", asset: "BTC", qty: 3, snapTS: "2026-04-07T00:00:00Z"})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-K", asset: "ETH", qty: 7, snapTS: "2026-04-08T00:00:00Z"})

	// Negative control: under the shipped key these are two instruments and the run is clean, so the
	// refusal below is caused by the redefinition and not by the fixture.
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&written); err != nil {
		t.Fatalf("two assets of one package are two instruments and must project: %v", err)
	}
	if written != 2 {
		t.Fatalf("appended %d rows, want 2 — the fixture is not what this test assumes", written)
	}

	// Move the one definition so the key no longer separates assets.
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION anchorage_instrument_key(p_package_id text, p_asset_type text)
		    RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path FROM CURRENT AS
		$fn$ SELECT 'anchorage:' || p_package_id $fn$`); err != nil {
		t.Fatalf("redefining the key helper: %v", err)
	}

	// The view must follow it.
	var keys []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(DISTINCT instrument_key), '{}') FROM position_anchorage_custody`).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "anchorage:PKG-K" {
		t.Errorf("the view projects %v; it does not build instrument_key from the helper", keys)
	}

	// And so must the guard, which is the half that used to drift.
	err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(new(int64))
	if err == nil {
		t.Fatal("two assets now render one instrument_key and the guard permitted the run: it is not reading the key definition")
	}
	if !strings.Contains(err.Error(), "render the instrument_key") {
		t.Errorf("the run failed, but not as the injectivity refusal: %s", err.Error())
	}
}

// VEC-811. The unit and scale of a normalised quantity is not recoverable from NUMERIC, and
// position_state.quantity is deliberately NOT normalised across protocols, so the source column has to
// say which of the two it is.
func TestAnchorageAssetQuantityDocumentsItsUnit(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)

	var comment *string
	if err := pool.QueryRow(ctx, `
		SELECT col_description('anchorage_package_snapshot'::regclass, attnum)
		FROM pg_attribute
		WHERE attrelid = 'anchorage_package_snapshot'::regclass AND attname = 'asset_quantity'`).Scan(&comment); err != nil {
		t.Fatalf("reading the column comment: %v", err)
	}
	if comment == nil {
		t.Fatal("asset_quantity carries no COMMENT, so its unit and scale are recorded nowhere")
	}
	for _, want := range []string{"NORMALISED", "whole units"} {
		if !strings.Contains(*comment, want) {
			t.Errorf("the comment does not say the quantity is %q: %s", want, *comment)
		}
	}
}

// TestMaterializeAnchorageCustodyForwardsTheWindow proves the argument REACHES the spine, by reading
// it back off the run record. It does not prove the window bounds anything. This does: the spine
// applies `block_timestamp > now() - p_window` to the view's output, so an observation outside the
// window must not reach position_state at all.
func TestMaterializeAnchorageCustodyWindowBoundsWhatItReads(t *testing.T) {
	ctx, pool := seedAnchorageBase(t)
	recent := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-NEW", qty: 5, snapTS: recent})
	addSnap(t, ctx, pool, anchorageSnap{pkg: "PKG-OLD", qty: 9, snapTS: "2026-04-07T00:00:00Z"})

	var written int64
	if err := pool.QueryRow(ctx,
		`SELECT materialize_anchorage_custody(0, NULL, interval '1 day')`).Scan(&written); err != nil {
		t.Fatalf("bounded run: %v", err)
	}

	var keys []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(DISTINCT instrument_key ORDER BY instrument_key), '{}')
		FROM position_state`).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if written != 1 || len(keys) != 1 || keys[0] != anchorageKey("PKG-NEW", "BTC") {
		t.Errorf("a 1-day window appended %d rows %v; want only the observation inside it, %s",
			written, keys, anchorageKey("PKG-NEW", "BTC"))
	}

	// Negative control: unbounded, the same fixture carries both, so the exclusion above is the
	// window's doing and not something else dropping the old row.
	var total int64
	if err := pool.QueryRow(ctx, `SELECT materialize_anchorage_custody()`).Scan(&total); err != nil {
		t.Fatalf("unbounded run: %v", err)
	}
	if total != 1 {
		t.Errorf("the unbounded run appended %d rows; want the 1 the window had excluded", total)
	}
}
