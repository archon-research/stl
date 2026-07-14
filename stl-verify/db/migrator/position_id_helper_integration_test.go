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

	// Chain + protocol present -> the canonical string per the spec.
	var key string
	if err := pool.QueryRow(ctx,
		`SELECT position_key(1, 3, 'morpho_market:7', 'em-000042')`).Scan(&key); err != nil {
		t.Fatalf("position_key: %v", err)
	}
	if want := "1;3;morpho_market:7;em-000042"; key != want {
		t.Errorf("position_key = %q, want %q", key, want)
	}

	// position_id is the 32-byte sha256 of exactly that string.
	var idIsSha256 bool
	var idLen int
	if err := pool.QueryRow(ctx, `
		SELECT position_id(1,3,'morpho_market:7','em-000042')
		         = sha256(convert_to('1;3;morpho_market:7;em-000042','UTF8')),
		       length(position_id(1,3,'morpho_market:7','em-000042'))`).Scan(&idIsSha256, &idLen); err != nil {
		t.Fatalf("position_id: %v", err)
	}
	if !idIsSha256 {
		t.Error("position_id is not the sha256 of position_key")
	}
	if idLen != 32 {
		t.Errorf("position_id length = %d bytes, want 32", idLen)
	}

	// Nullable structural fields render empty (Sky has no chain-specific protocol id).
	if err := pool.QueryRow(ctx,
		`SELECT position_key(1, NULL, 'sky_ilk:ALLOCATOR-SPARK-A', 'em-000005')`).Scan(&key); err != nil {
		t.Fatalf("position_key (null protocol): %v", err)
	}
	if want := "1;;sky_ilk:ALLOCATOR-SPARK-A;em-000005"; key != want {
		t.Errorf("null-protocol position_key = %q, want %q", key, want)
	}

	// Guards must fail hard rather than emit a silently-wrong identity.
	for _, g := range []struct{ name, sql string }{
		{"instrument_key required", `SELECT position_key(1,3,NULL,'em-000042')`},
		{"holder_id required", `SELECT position_key(1,3,'morpho_market:7',NULL)`},
	} {
		var s string
		if err := pool.QueryRow(ctx, g.sql).Scan(&s); err == nil {
			t.Errorf("%s: expected the helper to raise, got none (%q)", g.name, s)
		}
	}
}
