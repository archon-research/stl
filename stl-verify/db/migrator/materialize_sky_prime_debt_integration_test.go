//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-406 contract: materialize_sky_prime_debt() projects raw prime_debt rows into position_state on the
// native per-instrument grain (VEC-400) — one position per (prime, ilk), keyed by the native ilk_name,
// held by the prime's vault address. Observations only: the spine writes no classification, so nothing
// here asserts one.
//
// One behaviour per function, each seeding its own database, so a projection failure cannot cascade into
// unrelated assertions.

// skyPrimeDebtHolders are the vault addresses the seed creates, as the projection emits them.
const (
	skyPrimeA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	skyPrimeB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	skyPrimeC = "cccccccccccccccccccccccccccccccccccccccc"
)

// seedSkyPrimeDebt gives a test its own migrated database, seeds the fixture and runs the projection
// once, returning what it reported written.
//
// Prime A (vault aa) borrows in ILK-A (two observations) and ILK-B; Prime B (vault bb) never carried
// debt in ILK-A (single debt 0 row -> nothing emitted); Prime C (vault cc) borrows ILK-A then repays to
// 0 (open + one closing zero-row).
func seedSkyPrimeDebt(t *testing.T) (context.Context, *pgxpool.Pool, int64) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	t.Cleanup(cleanup)
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	seed := `
DO $$
DECLARE paid bigint; pbid bigint; pcid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  -- Names are test-local: 20260305_120000 seeds the real 'spark', 'grove' and 'obex', and prime.name is
  -- UNIQUE, so reusing those names collides. The assertions key on the vault_address (the holder_id the
  -- projection emits), not the name, so a distinct name changes nothing under test.
  INSERT INTO prime (name, vault_address) VALUES ('itest-a', '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO paid;
  INSERT INTO prime (name, vault_address) VALUES ('itest-b', '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') RETURNING id INTO pbid;
  INSERT INTO prime (name, vault_address) VALUES ('itest-c', '\xcccccccccccccccccccccccccccccccccccccccc') RETURNING id INTO pcid;
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
	return ctx, pool, written
}

// A/ILK-A (2 obs) + A/ILK-B (1) + C/ILK-A (open + close = 2) = 5 rows; B/ILK-A never entered, skipped.
// Distinct positions: A/ILK-A, A/ILK-B, C/ILK-A = 3.
func TestMaterializeSkyPrimeDebtProjectionShape(t *testing.T) {
	ctx, pool, written := seedSkyPrimeDebt(t)
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
}

// One case per seeded (prime, ilk): the native key, the holder, the latest quantity and the row count.
func TestMaterializeSkyPrimeDebtPerPosition(t *testing.T) {
	ctx, pool, _ := seedSkyPrimeDebt(t)
	for _, c := range []struct {
		name       string
		instrument string
		holder     string
		wantQty    string
		wantRows   int
	}{
		{"A ILK-A latest of two observations", "ILK-A", skyPrimeA, "1500", 2},
		{"A ILK-B", "ILK-B", skyPrimeA, "500", 1},
		{"B ILK-A never entered (debt 0) emits nothing", "ILK-A", skyPrimeB, "", 0},
		{"C ILK-A repaid: borrow + one closing zero-row", "ILK-A", skyPrimeC, "0", 2},
	} {
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
}

// A second run re-derives the same observations and appends nothing.
func TestMaterializeSkyPrimeDebtIsIdempotent(t *testing.T) {
	ctx, pool, _ := seedSkyPrimeDebt(t)
	var second int64
	if err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt()`).Scan(&second); err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	if second != 0 {
		t.Errorf("the second run reported %d rows appended, want 0", second)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if rows != 5 {
		t.Errorf("after re-run: position_state=%d, want 5 (the rerun must append nothing)", rows)
	}
}
