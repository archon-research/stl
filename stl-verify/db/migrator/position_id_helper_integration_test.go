//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestPositionIdHelper is the VEC-400 contract test: after migrations, public.position_key
// produces the canonical identity string, public.position_id is its 32-byte sha256, and the
// required-field / mutual-exclusion guards fail hard. Every per-protocol materializer depends
// on this helper, so the contract is pinned here.
func TestPositionIdHelper(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Wallet holder, protocol present, prime empty -> the canonical string per the spec.
	var key string
	if err := pool.QueryRow(ctx,
		`SELECT position_key(1, 3, 'morpho_market', '7', 42, NULL, 'LOAN')`).Scan(&key); err != nil {
		t.Fatalf("position_key: %v", err)
	}
	if want := "1;3;morpho_market:7;42;;LOAN"; key != want {
		t.Errorf("position_key = %q, want %q", key, want)
	}

	// position_id is the 32-byte sha256 of exactly that string.
	var idIsSha256 bool
	var idLen int
	if err := pool.QueryRow(ctx, `
		SELECT position_id(1,3,'morpho_market','7',42,NULL,'LOAN')
		         = sha256(convert_to('1;3;morpho_market:7;42;;LOAN','UTF8')),
		       length(position_id(1,3,'morpho_market','7',42,NULL,'LOAN'))`).Scan(&idIsSha256, &idLen); err != nil {
		t.Fatalf("position_id: %v", err)
	}
	if !idIsSha256 {
		t.Error("position_id is not the sha256 of position_key")
	}
	if idLen != 32 {
		t.Errorf("position_id length = %d bytes, want 32", idLen)
	}

	// Prime holder path renders user_id empty (and protocol empty for Sky).
	if err := pool.QueryRow(ctx,
		`SELECT position_key(1, NULL, 'ilk', 'ALLOCATOR-SPARK-A', NULL, 5, 'BORROW')`).Scan(&key); err != nil {
		t.Fatalf("position_key (prime): %v", err)
	}
	if want := "1;;ilk:ALLOCATOR-SPARK-A;;5;BORROW"; key != want {
		t.Errorf("prime position_key = %q, want %q", key, want)
	}

	// Guards must fail hard rather than emit a silently-wrong identity.
	for _, g := range []struct{ name, sql string }{
		{"user_id + prime_id both set", `SELECT position_key(1,3,'k','x',42,5,'LOAN')`},
		{"user_id + prime_id both null", `SELECT position_key(1,3,'k','x',NULL,NULL,'LOAN')`},
		{"deal_type_code required", `SELECT position_key(1,3,'k','x',42,NULL,NULL)`},
		{"kind/instrument_key required", `SELECT position_key(1,3,NULL,'x',42,NULL,'LOAN')`},
	} {
		var s string
		if err := pool.QueryRow(ctx, g.sql).Scan(&s); err == nil {
			t.Errorf("%s: expected the helper to raise, got none (%q)", g.name, s)
		}
	}
}
