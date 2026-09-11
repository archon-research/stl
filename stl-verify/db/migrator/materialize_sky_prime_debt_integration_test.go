//go:build integration

package migrator_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
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
	// The Vat the migration seeds and backfills to; the indexer's own default is tied to it by
	// TestDefaultVatAddressMatchesTheSeededProtocolRow in the prime-debt-indexer package.
	skyVatAddress = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"
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
DECLARE paid bigint; pbid bigint; pcid bigint; vat bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  -- Names are test-local: 20260305_120000 seeds the real 'spark', 'grove' and 'obex', and prime.name is
  -- UNIQUE, so reusing those names collides. The assertions key on the vault_address (the holder_id the
  -- projection emits), not the name, so a distinct name changes nothing under test.
  SELECT id INTO STRICT vat FROM protocol WHERE chain_id = 1 AND address = '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b';
  INSERT INTO prime (name, vault_address) VALUES ('itest-a', '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO paid;
  INSERT INTO prime (name, vault_address) VALUES ('itest-b', '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') RETURNING id INTO pbid;
  INSERT INTO prime (name, vault_address) VALUES ('itest-c', '\xcccccccccccccccccccccccccccccccccccccccc') RETURNING id INTO pcid;
  INSERT INTO prime_debt (prime_id, protocol_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
    (paid, vat, 'ILK-A', 1000, 100, 0, '2026-01-01T00:00:00Z', 0, 0),
    (paid, vat, 'ILK-A', 1500, 200, 0, '2026-01-02T00:00:00Z', 0, 0),
    -- Distinct synced_at from A/ILK-A above: prime_debt's UNIQUE key is
    -- (prime_id, block_number, block_version, processing_version, synced_at) with NO ilk_name, so two
    -- ilks of one prime at one block must carry different synced_at (as they do in prod) or they collide.
    (paid, vat, 'ILK-B',  500, 100, 0, '2026-01-01T00:05:00Z', 0, 0),
    (pbid, vat, 'ILK-A',    0, 100, 0, '2026-01-01T00:00:00Z', 0, 0),
    (pcid, vat, 'ILK-A', 2000, 100, 0, '2026-01-01T00:00:00Z', 0, 0),
    (pcid, vat, 'ILK-A',    0, 200, 0, '2026-01-02T00:00:00Z', 0, 0);
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

// Sky's protocol_id is hashed into position_id, so it must be the MCD Vat's protocol row -- never NULL,
// which an earlier revision emitted on the reasoning that prime debt was "not protocol-scoped". The
// indexer reads exactly one contract, and that is the protocol.
func TestSkyPrimeDebtCarriesTheProtocolStampedOnTheRow(t *testing.T) {
	ctx, pool, _ := seedSkyPrimeDebt(t)
	// A second Vat: rows stamped with it must project under it, so no address is baked into the view.
	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; v2 bigint; BEGIN
	  INSERT INTO protocol (chain_id, address, name, protocol_type) VALUES (1, '\x02', 'sky-two', 'lending') RETURNING id INTO v2;
	  INSERT INTO prime (name, vault_address) VALUES ('itest-d', '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, protocol_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, v2, 'ILK-A', 42, 100, 0, '2026-01-01T00:00:00Z', 0, 0);
	END $s$`); err != nil {
		t.Fatalf("seed second Vat: %v", err)
	}
	var nulls, offVat, onSecond int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE protocol_id IS NULL),
		       count(*) FILTER (WHERE holder_id <> 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
		                          AND protocol_id IS DISTINCT FROM (SELECT id FROM protocol WHERE chain_id = 1 AND address = '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b')),
		       count(*) FILTER (WHERE holder_id = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
		                          AND protocol_id = (SELECT id FROM protocol WHERE chain_id = 1 AND address = '\x02'))
		FROM position_sky_prime_debt`).Scan(&nulls, &offVat, &onSecond); err != nil {
		t.Fatalf("read the projection: %v", err)
	}
	if nulls != 0 || offVat != 0 || onSecond != 1 {
		t.Errorf("NULL protocol=%d, Vat rows off the Vat=%d, second-Vat rows under it=%d (want 1)", nulls, offVat, onSecond)
	}
}

// A snapshot the projection cannot key refuses the whole run, by name, writing nothing. Three ways in:
// an unresolvable protocol_id (the Vat row is hashed into position_id); a padded ilk_name, which
// position_key() accepts and would silently fork into a second identity; and a vault_address that is not
// 20 bytes, which passes position_key() and fails the spine's hex CHECK with a 23514 naming no row.
func TestSkyPrimeDebtRefusesASnapshotItCannotKey(t *testing.T) {
	const goodVault = "ffffffffffffffffffffffffffffffffffffffff"
	for _, c := range []struct{ name, protocolExpr, ilk, vaultHex, want string }{
		{"NULL protocol_id", "NULL", "ILK-X", goodVault, "has protocol_id NULL"},
		{"dangling protocol_id", "999999", "ILK-X", goodVault, "has protocol_id 999999"},
		{"leading space in ilk_name", "(SELECT id FROM protocol WHERE chain_id = 1 AND address = decode('35d1b3f3d7966a1dfe207aa4514c12a259a0492b', 'hex'))", " ILK-X", goodVault, "ilk ' ILK-X'"},
		{"trailing space in ilk_name", "(SELECT id FROM protocol WHERE chain_id = 1 AND address = decode('35d1b3f3d7966a1dfe207aa4514c12a259a0492b', 'hex'))", "ILK-X ", goodVault, "ilk 'ILK-X '"},
		{"empty vault_address", "(SELECT id FROM protocol WHERE chain_id = 1 AND address = decode('35d1b3f3d7966a1dfe207aa4514c12a259a0492b', 'hex'))", "ILK-X", "", "vault_address ''"},
		{"19-byte vault_address", "(SELECT id FROM protocol WHERE chain_id = 1 AND address = decode('35d1b3f3d7966a1dfe207aa4514c12a259a0492b', 'hex'))", "ILK-X", goodVault[:38], "vault_address '" + goodVault[:38] + "'"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			pool, cleanup := setupMigratedPostgres(ctx, t)
			defer cleanup()
			if _, err := pool.Exec(ctx, `
			WITH p AS (INSERT INTO prime (name, vault_address) VALUES ('orphan', decode($2, 'hex')) RETURNING id)
			INSERT INTO prime_debt (prime_id, protocol_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id)
			SELECT p.id, `+c.protocolExpr+`, $1, 7, 100, 0, '2026-01-01T00:00:00Z', 0, 0 FROM p`, c.ilk, c.vaultHex); err != nil {
				t.Fatalf("seed: %v", err)
			}
			var written int64
			err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt()`).Scan(&written)
			if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "prime 'orphan'") {
				t.Fatalf("want a refusal naming prime 'orphan' and %q, got written=%d err=%v", c.want, written, err)
			}
			var rows int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows); err != nil {
				t.Fatalf("count position_state: %v", err)
			}
			if rows != 0 {
				t.Errorf("refused run left %d rows", rows)
			}
		})
	}
}

// Two prime_debt rows identical on (prime, ilk, block, version, processing_version) that differ only in
// synced_at are one observation to the projection, and the EARLIER synced_at wins: it is the stable pick,
// since a retry can only add a later row, whereas the latest would move and re-emit a stored key with a
// changed block_timestamp on every later run.
func TestSkyPrimeDebtSameKeyEarlierSyncedAtWins(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; vat bigint; BEGIN
	  SELECT id INTO STRICT vat FROM protocol WHERE chain_id = 1 AND address = '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b';
	  INSERT INTO prime (name, vault_address) VALUES ('tie', '\xdddddddddddddddddddddddddddddddddddddddd') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, protocol_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, vat, 'TIE-A', 100, 500, 0, '2026-06-01T10:00:00Z', 0, 0),
	    (pid, vat, 'TIE-A', 250, 500, 0, '2026-06-01T11:00:00Z', 0, 0);   -- same key, later sync, different debt
	END $s$`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var rows int
	var qty string
	var ts time.Time
	if err := pool.QueryRow(ctx, `
		SELECT count(*), min(quantity)::text, min(block_timestamp)
		  FROM position_sky_prime_debt WHERE instrument_key = 'TIE-A'`).Scan(&rows, &qty, &ts); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if rows != 1 {
		t.Fatalf("same-key pair projected %d rows, want 1", rows)
	}
	if qty != "100" || !ts.Equal(time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("projection kept quantity %s at %s; want 100 at 2026-06-01T10:00Z -- the earlier synced_at is the stable pick", qty, ts.UTC().Format(time.RFC3339))
	}
}

// Rows written before prime_debt.protocol_id existed are backfilled to the seeded Vat row by the same
// migration, including rows already sitting in a compressed chunk, which is where they live in prod.
func TestSkyPrimeDebtBackfillStampsLegacyRowsInCompressedChunks(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	// Apply every migration that sorts before the Sky one, from a staging copy of the migrations dir.
	const skyMigration = "20260819_140000_materialize_sky_prime_debt.sql"
	entries, err := os.ReadDir(getMigrationsPath())
	if err != nil {
		t.Fatal(err)
	}
	staged := t.TempDir()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") || e.Name() >= skyMigration {
			continue
		}
		src, err := os.ReadFile(filepath.Join(getMigrationsPath(), e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(staged, e.Name()), src, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := migrator.New(pool, staged).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations before %s: %v", skyMigration, err)
	}
	var hasColumn bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'prime_debt' AND column_name = 'protocol_id')`).Scan(&hasColumn); err != nil {
		t.Fatal(err)
	}
	if hasColumn {
		t.Fatal("prime_debt.protocol_id already exists before the Sky migration; the staging cut is wrong")
	}
	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; BEGIN
	  INSERT INTO prime (name, vault_address) VALUES ('legacy', '\x1111111111111111111111111111111111111111') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, 'ILK-L', 10, 100, 0, '2026-01-01T00:00:00Z', 0, 0),
	    (pid, 'ILK-L', 20, 200, 0, '2026-01-02T00:00:00Z', 0, 0),
	    (pid, 'ILK-L', 30, 300, 0, '2026-03-01T00:00:00Z', 0, 0);
	END $s$`); err != nil {
		t.Fatalf("seed legacy rows: %v", err)
	}
	var compressed int
	if err := pool.QueryRow(ctx, `SELECT count(compress_chunk(c)) FROM show_chunks('prime_debt') c`).Scan(&compressed); err != nil {
		t.Fatalf("compress prime_debt chunks: %v", err)
	}
	if compressed == 0 {
		t.Fatal("no prime_debt chunk was compressed; the test would not exercise the compressed path")
	}

	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("remaining migrations: %v", err)
	}
	var rows, stamped int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE protocol_id = (SELECT id FROM protocol WHERE chain_id = 1 AND address = '\x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'))
		FROM prime_debt`).Scan(&rows, &stamped); err != nil {
		t.Fatal(err)
	}
	if rows != 3 || stamped != 3 {
		t.Errorf("legacy rows=%d, stamped with the Vat row=%d; want 3/3 (chunks compressed: %d)", rows, stamped, compressed)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt()`).Scan(&written); err != nil {
		t.Fatalf("materialize after backfill: %v", err)
	}
	if written != 3 {
		t.Errorf("materialized %d rows from the backfilled legacy snapshots, want 3", written)
	}
}

// The column carries a constant DEFAULT, so stamping pre-existing rows is catalogue-only: no chunk is
// decompressed, which an ADD COLUMN plus a backfill UPDATE cannot manage. Pinned by lowering the
// decompression cap to 1 at database scope with every chunk compressed, so any decompression at all
// fails the migration, and by asserting no chunk takes the partial bit and the row-store heap is
// unchanged. Also covers the ArgoCD rollout window: a row written without naming the column, as the
// pre-rollout pod does, takes the DEFAULT rather than a NULL that would refuse every later run.
func TestSkyStampsLegacyRowsWithoutDecompressing(t *testing.T) {
	ctx := context.Background()
	dsn, dropDB := createTestDatabase(t)
	defer dropDB()

	admin := testutil.ConnectPool(t, dsn)
	var alter string
	if err := admin.QueryRow(ctx, `SELECT format('ALTER DATABASE %I SET timescaledb.max_tuples_decompressed_per_dml_transaction = 1', current_database())`).Scan(&alter); err != nil {
		t.Fatalf("build the ALTER DATABASE: %v", err)
	}
	if _, err := admin.Exec(ctx, alter); err != nil {
		t.Fatalf("lower the decompression cap: %v", err)
	}
	admin.Close()

	pool := testutil.ConnectPool(t, dsn)
	defer pool.Close()

	const skyMigration = "20260819_140000_materialize_sky_prime_debt.sql"
	entries, err := os.ReadDir(getMigrationsPath())
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	before := t.TempDir()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") || e.Name() >= skyMigration {
			continue
		}
		src, err := os.ReadFile(filepath.Join(getMigrationsPath(), e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(before, e.Name()), src, 0o644); err != nil {
			t.Fatalf("stage %s: %v", e.Name(), err)
		}
	}
	if err := migrator.New(pool, before).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations before %s: %v", skyMigration, err)
	}
	if err := testutil.DisableScheduledJobs(ctx, pool); err != nil {
		t.Fatalf("disable policy jobs: %v", err)
	}
	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; BEGIN
	  INSERT INTO prime (name, vault_address) VALUES ('legacy', '\x1111111111111111111111111111111111111111') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id)
	  SELECT pid, 'ILK-L', 10 + g, 100 + g, 0, TIMESTAMPTZ '2026-01-01' + (g * interval '1 day'), 0, 0
	  FROM generate_series(0, 11) g;
	END $s$`); err != nil {
		t.Fatalf("seed legacy rows: %v", err)
	}
	var compressed, legacyRows int
	if err := pool.QueryRow(ctx, `SELECT count(compress_chunk(c)) FROM show_chunks('prime_debt') c`).Scan(&compressed); err != nil {
		t.Fatalf("compress prime_debt chunks: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prime_debt`).Scan(&legacyRows); err != nil {
		t.Fatalf("count legacy rows: %v", err)
	}
	if compressed == 0 || legacyRows <= 1 {
		t.Fatalf("compressed=%d rows=%d; the stamp must exceed the cap of 1 inside compressed chunks", compressed, legacyRows)
	}
	const chunkStateSQL = `
		SELECT count(*) FILTER (WHERE (c.status & 8) <> 0), coalesce(sum(pg_relation_size(c.relid)), 0)
		FROM _timescaledb_catalog.chunk c
		JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
		WHERE h.table_name = 'prime_debt'`
	var partialBefore int
	var heapBefore int64
	if err := pool.QueryRow(ctx, chunkStateSQL).Scan(&partialBefore, &heapBefore); err != nil {
		t.Fatalf("chunk state before: %v", err)
	}

	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("the Sky migration must stamp without decompressing, got: %v", err)
	}

	var partialAfter int
	var heapAfter int64
	var stamped int
	if err := pool.QueryRow(ctx, chunkStateSQL).Scan(&partialAfter, &heapAfter); err != nil {
		t.Fatalf("chunk state after: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM prime_debt
		 WHERE protocol_id = (SELECT id FROM protocol WHERE chain_id = 1 AND address = decode($1, 'hex'))`,
		strings.TrimPrefix(skyVatAddress, "0x")).Scan(&stamped); err != nil {
		t.Fatalf("count stamped rows: %v", err)
	}
	if stamped != legacyRows {
		t.Errorf("stamped %d of %d legacy rows", stamped, legacyRows)
	}
	if partialAfter != partialBefore || heapAfter != heapBefore {
		t.Errorf("the migration decompressed: partial chunks %d -> %d, row-store heap %d -> %d bytes; the DEFAULT must be catalogue-only",
			partialBefore, partialAfter, heapBefore, heapAfter)
	}

	// The ArgoCD rollout window: the pre-rollout pod inserts without naming the column.
	var rolloutProtocol *int64
	if err := pool.QueryRow(ctx, `
		WITH ins AS (
		  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id)
		  SELECT (SELECT min(id) FROM prime), 'ILK-L', 999, 9999, 0, TIMESTAMPTZ '2026-02-01', 0, 0
		  RETURNING protocol_id)
		SELECT protocol_id FROM ins`).Scan(&rolloutProtocol); err != nil {
		t.Fatalf("insert a rollout-window row: %v", err)
	}
	if rolloutProtocol == nil {
		t.Error("a row written without naming protocol_id took a NULL; it would refuse every later run of the projection")
	}
}

// Sky prime debt is a liability: the prime owes the ilk's debt. deal_type must therefore be a SHORT
// row in ref_deal_type, asserted through the reference table rather than against a literal, so the
// direction is what is pinned -- a LONG value here would invert every downstream aggregate.
func TestMaterializeSkyPrimeDebtIsAShortLiability(t *testing.T) {
	ctx, pool, _ := seedSkyPrimeDebt(t)
	rows, err := pool.Query(ctx, `
		SELECT ps.deal_type, r.direction
		FROM position_state ps
		JOIN ref_deal_type r ON r.deal_type = ps.deal_type
		GROUP BY ps.deal_type, r.direction`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var seen int
	for rows.Next() {
		var dealType, direction string
		if err := rows.Scan(&dealType, &direction); err != nil {
			t.Fatal(err)
		}
		seen++
		if dealType != "BORROW" {
			t.Errorf("deal_type = %q; want BORROW", dealType)
		}
		if direction != "SHORT" {
			t.Errorf("deal_type %q has direction %q; a debt must be SHORT", dealType, direction)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Errorf("found %d distinct deal_types; want exactly one (BORROW)", seen)
	}
	// The join above silently passes if deal_type were NULL, so pin that separately.
	var nulls int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE deal_type IS NULL`).Scan(&nulls); err != nil {
		t.Fatal(err)
	}
	if nulls != 0 {
		t.Errorf("%d rows carry a NULL deal_type; every Sky observation is a BORROW", nulls)
	}
}

// The wrapper is the only path the runner calls, so it has to forward the writer run to the spine or
// every row this projection appends is provenance-free (ADR-0006 §2). The run record is the witness:
// its run_id can only have arrived through the wrapper's own parameter.
func TestMaterializeSkyPrimeDebtForwardsTheWriterRun(t *testing.T) {
	ctx, pool, _ := seedSkyPrimeDebt(t)
	if _, err := pool.Exec(ctx, `SELECT materialize_sky_prime_debt(7, 9182)`); err != nil {
		t.Fatalf("materialize_sky_prime_debt with a run: %v", err)
	}
	var runID *int64
	var buildID int
	if err := pool.QueryRow(ctx, `
		SELECT run_id, build_id FROM position_projection_run
		 WHERE projection = 'public.position_sky_prime_debt'
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
		SELECT proargnames::text[] FROM pg_proc WHERE proname = 'materialize_sky_prime_debt'`).Scan(&args); err != nil {
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
			t.Errorf("materialize_sky_prime_debt declares %v, missing %s -- the runner calls it by name", args, want)
		}
	}
}
