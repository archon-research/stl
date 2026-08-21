//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestMaterializeSkyPrimeDebt is the VEC-406 contract test: after migrations,
// materialize_sky_prime_debt() projects raw prime_debt rows into position_state on the native
// per-instrument grain (VEC-400) — one position per (prime, ilk), keyed by the native ilk_name, held by
// the prime's vault address. Observations only: the spine writes no classification.
//
// Pins: native ilk_name key, prime vault address as holder, debt_wad as quantity, chain_id constant 1 /
// protocol_id NULL, prime_debt's own processing_version flowing into the spine, closure (VEC-409) — a
// repayment (positive->0) emits one closing zero-row, a prime x ilk never in debt emits nothing —
// multiple observations with one current classification, 32-byte ids, no collisions, idempotency.
func TestMaterializeSkyPrimeDebt(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Prime A (vault aa) borrows in ILK-A (two observations) and ILK-B; Prime B (vault bb) never carried
	// debt in ILK-A (single debt 0 row -> nothing emitted); Prime C (vault cc) borrows ILK-A then repays
	// to 0 (open + one closing zero-row).
	seed := `
DO $$
DECLARE paid bigint; pbid bigint; pcid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  -- Names are test-local: 20260305_120000 seeds the real 'spark', 'grove' and 'obex', and prime.name is
  -- UNIQUE, so reusing those names collides. The assertions key on the vault_address (the holder_id the
  -- projection emits), not the name, so a distinct name changes nothing under test.
  INSERT INTO prime (name, vault_address) VALUES ('itest-a', '\xaa') RETURNING id INTO paid;
  INSERT INTO prime (name, vault_address) VALUES ('itest-b', '\xbb') RETURNING id INTO pbid;
  INSERT INTO prime (name, vault_address) VALUES ('itest-c', '\xcc') RETURNING id INTO pcid;
  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
    (paid, 'ILK-A', 1000, 100, 0, '2026-01-01T00:00:00Z', 0, 0),
    (paid, 'ILK-A', 1500, 200, 0, '2026-01-02T00:00:00Z', 0, 0),
    -- Distinct synced_at from A/ILK-A above: prime_debt's UNIQUE key is
    -- (prime_id, block_number, block_version, processing_version, synced_at) with NO ilk_name, so two
    -- ilks of one prime at one block must carry different synced_at (as they do in prod) or they collide.
    (paid, 'ILK-B',  500, 100, 0, '2026-01-01T00:05:00Z', 0, 0),
    (pbid, 'ILK-A',    0, 100, 0, '2026-01-01T00:00:00Z', 0, 0),
    (pcid, 'ILK-A', 2000, 100, 0, '2026-01-01T00:00:00Z', 0, 0),
    (pcid, 'ILK-A',    0, 200, 0, '2026-01-02T00:00:00Z', 0, 0);
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt()`).Scan(&written); err != nil {
		t.Fatalf("materialize_sky_prime_debt: %v", err)
	}

	// A/ILK-A (2 obs) + A/ILK-B (1) + C/ILK-A (open + close = 2) = 5 rows; B/ILK-A never entered, skipped.
	// Distinct positions: A/ILK-A, A/ILK-B, C/ILK-A = 3.
	var rows, distinctPositions, collisions, badLen int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(DISTINCT position_id),
		       count(*) - count(DISTINCT (position_id, block_number, block_version, processing_version)),
		       count(*) FILTER (WHERE octet_length(position_id) <> 32)
		FROM position_state`).Scan(&rows, &distinctPositions, &collisions, &badLen); err != nil {
		t.Fatalf("position_state summary: %v", err)
	}
	if rows != 5 {
		t.Errorf("position_state rows = %d, want 5", rows)
	}
	if written != 5 {
		t.Errorf("materialize returned %d, want 5", written)
	}
	if distinctPositions != 3 {
		t.Errorf("distinct position_id = %d, want 3", distinctPositions)
	}
	if collisions != 0 {
		t.Errorf("PK collisions = %d, want 0", collisions)
	}
	if badLen != 0 {
		t.Errorf("%d position_id(s) not 32 bytes", badLen)
	}

	for _, c := range []struct {
		name       string
		instrument string
		holder     string
		wantQty    string
		wantRows   int
	}{
		{"A ILK-A latest of two observations", "ILK-A", "aa", "1500", 2},
		{"A ILK-B", "ILK-B", "aa", "500", 1},
		{"B ILK-A never entered (debt 0) emits nothing", "ILK-A", "bb", "", 0},
		{"C ILK-A repaid: borrow + one closing zero-row", "ILK-A", "cc", "0", 2},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var n int
			var latestQty *string
			if err := pool.QueryRow(ctx, `
				SELECT count(*),
				       (SELECT quantity::text FROM position_state
				        WHERE instrument_key = $1 AND holder_id = $2
				        ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1)
				FROM position_state WHERE instrument_key = $1 AND holder_id = $2`,
				c.instrument, c.holder).Scan(&n, &latestQty); err != nil {
				t.Fatalf("query: %v", err)
			}
			if n != c.wantRows {
				t.Errorf("rows = %d, want %d", n, c.wantRows)
			}
			if c.wantRows > 0 && (latestQty == nil || *latestQty != c.wantQty) {
				t.Errorf("latest quantity = %v, want %s", latestQty, c.wantQty)
			}
		})
	}

	// Idempotent: a second run re-derives the same observations and appends nothing.
	if _, err := pool.Exec(ctx, `SELECT materialize_sky_prime_debt()`); err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	var rows2 int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows2); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if rows2 != 5 {
		t.Errorf("after re-run: position_state=%d, want 5 (the rerun must append nothing)", rows2)
	}
}
