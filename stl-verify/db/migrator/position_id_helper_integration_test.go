//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestPositionIdHelper is the VEC-400 contract test: after migrations, public.position_key
// produces the canonical identity string, public.position_id is its 32-byte sha256, and the
// required-field guards fail hard. Identity is holder + instrument only (no classifications).
// Every per-protocol materializer depends on this helper, so the contract is pinned here.
func TestPositionIdHelper(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// The holder is the native on-chain holder id (a wallet/vault address), not a resolved entity.
	const holder = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	// Chain + protocol present -> the canonical string per the spec.
	var key string
	if err := pool.QueryRow(ctx,
		`SELECT position_key(1, 3, 'morpho_market:7', $1)`, holder).Scan(&key); err != nil {
		t.Fatalf("position_key: %v", err)
	}
	if want := "1;3;morpho_market:7;" + holder; key != want {
		t.Errorf("position_key = %q, want %q", key, want)
	}

	// position_id is the 32-byte sha256 of exactly that string.
	var idIsSha256 bool
	var idLen int
	if err := pool.QueryRow(ctx, `
		SELECT position_id(1,3,'morpho_market:7',$1)
		         = sha256(convert_to('1;3;morpho_market:7;' || $1,'UTF8')),
		       length(position_id(1,3,'morpho_market:7',$1))`, holder).Scan(&idIsSha256, &idLen); err != nil {
		t.Fatalf("position_id: %v", err)
	}
	if !idIsSha256 {
		t.Error("position_id is not the sha256 of position_key")
	}
	if idLen != 32 {
		t.Errorf("position_id length = %d bytes, want 32", idLen)
	}

	// Nullable structural fields render empty between the delimiters, positions stay stable.
	for _, c := range []struct{ name, sql, want string }{
		{"null protocol", `SELECT position_key(1, NULL, 'sky_ilk:ALLOCATOR-SPARK-A', $1)`, "1;;sky_ilk:ALLOCATOR-SPARK-A;" + holder},
		{"null chain", `SELECT position_key(NULL, 3, 'sky_ilk:ALLOCATOR-SPARK-A', $1)`, ";3;sky_ilk:ALLOCATOR-SPARK-A;" + holder},
		{"both null", `SELECT position_key(NULL, NULL, 'sky_ilk:ALLOCATOR-SPARK-A', $1)`, ";;sky_ilk:ALLOCATOR-SPARK-A;" + holder},
	} {
		if err := pool.QueryRow(ctx, c.sql, holder).Scan(&key); err != nil {
			t.Fatalf("position_key (%s): %v", c.name, err)
		}
		if key != c.want {
			t.Errorf("%s position_key = %q, want %q", c.name, key, c.want)
		}
	}

	// Guards must fail hard rather than emit a silently-wrong identity: NULL, empty, and a
	// delimiter in either identity field (the latter would collide two distinct positions).
	for _, g := range []struct{ name, sql string }{
		{"instrument_key null", `SELECT position_key(1,3,NULL,'` + holder + `')`},
		{"holder_id null", `SELECT position_key(1,3,'morpho_market:7',NULL)`},
		{"instrument_key empty", `SELECT position_key(1,3,'','` + holder + `')`},
		{"holder_id empty", `SELECT position_key(1,3,'morpho_market:7','')`},
		{"instrument_key has delimiter", `SELECT position_key(1,3,'a;b','` + holder + `')`},
		{"holder_id has delimiter", `SELECT position_key(1,3,'morpho_market:7','a;b')`},
	} {
		var s string
		if err := pool.QueryRow(ctx, g.sql).Scan(&s); err == nil {
			t.Errorf("%s: expected the helper to raise, got none (%q)", g.name, s)
		}
	}
}
