//go:build integration

package migrator_test

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-406 contract: materialize_sky_prime_debt() projects raw prime_debt rows into position_state on the
// native per-instrument grain (VEC-400) — one position per (prime, ilk), keyed by the native vat_address:ilk_name,
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
	// The Vat the projection keys on; the indexer's own default is tied to it by
	// TestDefaultVatAddressMatchesTheProjectionKey in the prime-debt-indexer package.
	skyVatAddress = "35d1b3f3d7966a1dfe207aa4514c12a259a0492b"
	skyVatKey     = skyVatAddress + ":"
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
  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'itest-a', '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO paid;
  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'itest-b', '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') RETURNING id INTO pbid;
  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'itest-c', '\xcccccccccccccccccccccccccccccccccccccccc') RETURNING id INTO pcid;
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
  -- Header times deliberately differ from synced_at, so a view reading the receipt clock is caught.
  INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES
    (1, 100, 0, '2025-12-31T23:00:00Z'),
    (1, 200, 0, '2025-12-31T23:00:12Z');
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
		{"A ILK-A latest of two observations", skyVatKey + "ILK-A", skyPrimeA, "1500", 2},
		{"A ILK-B", skyVatKey + "ILK-B", skyPrimeA, "500", 1},
		{"B ILK-A never entered (debt 0) emits nothing", skyVatKey + "ILK-A", skyPrimeB, "", 0},
		{"C ILK-A repaid: borrow + one closing zero-row", skyVatKey + "ILK-A", skyPrimeC, "0", 2},
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

// Sky is the issuer, not a protocol, so the key is the native Vat address and protocol_id is NULL.
// protocol.id is a BIGSERIAL: hashed into position_id it would give one Sky position a different id in
// each environment. The expected id is computed from constants alone, so it holds in any database.
func TestSkyPrimeDebtKeysOnTheVatAddressWithoutAProtocol(t *testing.T) {
	ctx, pool, _ := seedSkyPrimeDebt(t)
	var rows, off int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE chain_id IS DISTINCT FROM 1
		                           OR protocol_id IS NOT NULL
		                           OR position_id <> position_id(1, NULL, instrument_key, holder_id)
		                           OR left(instrument_key, $2) <> $1)
		FROM position_state`, skyVatKey, len(skyVatKey)).Scan(&rows, &off); err != nil {
		t.Fatalf("read position_state: %v", err)
	}
	if rows == 0 || off != 0 {
		t.Errorf("%d of %d rows are not chain 1, protocol NULL, keyed on the Vat address", off, rows)
	}
	// sha256 of position_key(1, NULL, key, holder) = "1;;<vat>:ILK-B;<holder>", computed outside the database
	// so a change to position_key() or position_id() cannot move both sides of the comparison.
	want, err := hex.DecodeString("73e3c3becebe457c0a1b3ccd16d5ba62b427eac18333c65f24645e4f05f697c8")
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	if err := pool.QueryRow(ctx, `
		SELECT position_id FROM position_state WHERE holder_id = $1 AND instrument_key = $2`,
		skyPrimeA, skyVatKey+"ILK-B").Scan(&got); err != nil {
		t.Fatalf("read A/ILK-B: %v", err)
	}
	if string(want) != string(got) {
		t.Errorf("A/ILK-B position_id = %x; want %x, the hash of chain 1, no protocol and the Vat key", got, want)
	}
}

// The projection reads prime_debt as it is: no protocol row is seeded and no column is added to a table
// the indexer writes and the API reads, so the migration takes no lock on it.
func TestSkyPrimeDebtMigrationLeavesPrimeDebtAndProtocolUntouched(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	var viewExists, hasColumn, hasVatProtocol bool
	if err := pool.QueryRow(ctx, `
		SELECT to_regclass('public.position_sky_prime_debt') IS NOT NULL,
		       EXISTS (SELECT 1 FROM information_schema.columns
		                WHERE table_schema = 'public' AND table_name = 'prime_debt' AND column_name = 'protocol_id'),
		       EXISTS (SELECT 1 FROM protocol WHERE address = decode($1, 'hex'))`, skyVatAddress).Scan(&viewExists, &hasColumn, &hasVatProtocol); err != nil {
		t.Fatalf("read the catalogue: %v", err)
	}
	if !viewExists {
		t.Fatal("position_sky_prime_debt does not exist; the Sky migration did not run, so the checks below prove nothing")
	}
	if hasColumn {
		t.Error("prime_debt has a protocol_id column; the Sky projection must not alter prime_debt")
	}
	if hasVatProtocol {
		t.Error("a protocol row exists for the MCD Vat; Sky is an entity, not a protocol")
	}
}

// A snapshot the projection cannot key refuses the whole run, by name, writing nothing: a vault_address
// that is not 20 bytes breaks position_id, and an ilk_name that is blank, whitespace-only, padded or
// carries ';' would silently fork one position into two.
func TestSkyPrimeDebtRefusesASnapshotItCannotKey(t *testing.T) {
	const goodVault = "ffffffffffffffffffffffffffffffffffffffff"
	for _, c := range []struct{ name, ilk, vaultHex, want string }{
		{"leading space in ilk_name", " ILK-X", goodVault, "ilk ' ILK-X'"},
		{"trailing space in ilk_name", "ILK-X ", goodVault, "ilk 'ILK-X '"},
		{"blank ilk_name", "", goodVault, "ilk ''"},
		{"tab-only ilk_name", "\t", goodVault, "ilk '\t'"},
		{"newline-only ilk_name", "\n", goodVault, "ilk '\n'"},
		{"ilk_name carrying the key delimiter", "ILK;X", goodVault, "ilk 'ILK;X'"},
		{"empty vault_address", "ILK-X", "", "vault_address ''"},
		{"19-byte vault_address", "ILK-X", goodVault[:38], "vault_address '" + goodVault[:38] + "'"},
		// Every other width case is short, so <> 20 weakened to < 20 would pass them all.
		{"21-byte vault_address", "ILK-X", goodVault + "ff", "vault_address '" + goodVault + "ff'"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			pool, cleanup := setupMigratedPostgres(ctx, t)
			defer cleanup()
			if _, err := pool.Exec(ctx, `
			WITH p AS (INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'orphan', decode($2, 'hex')) RETURNING id)
			INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id)
			SELECT p.id, $1, 7, 100, 0, '2026-01-01T00:00:00Z', 0, 0 FROM p`, c.ilk, c.vaultHex); err != nil {
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
// changed quantity on every later run.
func TestSkyPrimeDebtSameKeyEarlierSyncedAtWins(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; BEGIN
	  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'tie', '\xdddddddddddddddddddddddddddddddddddddddd') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, 'TIE-A', 250, 500, 0, '2026-06-01T11:00:00Z', 0, 0),   -- the LATER sync is inserted first, so
	    (pid, 'TIE-A', 100, 500, 0, '2026-06-01T10:00:00Z', 0, 0);   -- heap order cannot stand in for the ORDER BY
	  INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES (1, 500, 0, '2026-06-01T09:00:00Z');
	END $s$`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var rows int
	var qty string
	var ts time.Time
	if err := pool.QueryRow(ctx, `
		SELECT count(*), min(quantity)::text, min(block_timestamp)
		  FROM position_sky_prime_debt WHERE instrument_key = $1`, skyVatKey+"TIE-A").Scan(&rows, &qty, &ts); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if rows != 1 {
		t.Fatalf("same-key pair projected %d rows, want 1", rows)
	}
	if qty != "100" || !ts.Equal(time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("projection kept quantity %s at %s; want 100 (the earlier synced_at) at the block's header time 2026-06-01T09:00Z", qty, ts.UTC().Format(time.RFC3339))
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

// The wrapper is the only path the runner calls, so a window it cannot forward is a window this
// projection can never run with. The run record stamps what the spine actually received.
func TestSkyPrimeDebtForwardsTheWindow(t *testing.T) {
	ctx, pool, _ := seedSkyPrimeDebt(t)

	if _, err := pool.Exec(ctx, `SELECT materialize_sky_prime_debt(0, NULL, interval '36 hours')`); err != nil {
		t.Fatalf("calling with a window: %v", err)
	}

	var window *string
	if err := pool.QueryRow(ctx, `
		SELECT window_interval::text FROM position_projection_run
		 WHERE projection = 'public.position_sky_prime_debt'
		 ORDER BY created_at DESC LIMIT 1`).Scan(&window); err != nil {
		t.Fatalf("reading the run record: %v", err)
	}
	if window == nil {
		t.Fatal("the run recorded no window, so the wrapper dropped it")
	}
	if *window != "36:00:00" {
		t.Errorf("the run recorded window %q; want the 36 hours the wrapper was called with", *window)
	}
}

// block_timestamp is the block header time, never prime_debt.synced_at. On staging synced_at runs out of
// order against block_number (a backfill received early blocks months late), which made the spine
// withhold every Sky position; header times are ordered, so the same rows append.
func TestSkyPrimeDebtReadsHeaderTimeNotReceiptTime(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; BEGIN
	  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'late', '\x1111111111111111111111111111111111111111') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, 'LATE-A', 10, 1000, 0, '2026-05-21T08:10:14Z', 0, 0),  -- the lower block, received later
	    (pid, 'LATE-A', 20, 1016, 0, '2026-03-23T10:04:01Z', 0, 0);
	  INSERT INTO block_meta (chain_id, block_number, block_version, processing_version, block_timestamp) VALUES
	    (1, 1000, 0, 0, '2026-03-01T00:00:00Z'),
	    (1, 1016, 0, 0, '2026-03-01T00:03:12Z'),
	    (1, 1016, 0, 1, '2026-03-01T00:03:24Z');  -- a corrected header: the highest processing_version wins
	END $s$`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var appended int64
	if err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt()`).Scan(&appended); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if appended != 2 {
		t.Errorf("appended %d rows; want both observations, whose header times are in block order", appended)
	}
	var ts1000, ts1016 time.Time
	if err := pool.QueryRow(ctx, `
		SELECT max(block_timestamp) FILTER (WHERE block_number = 1000),
		       max(block_timestamp) FILTER (WHERE block_number = 1016)
		  FROM position_state`).Scan(&ts1000, &ts1016); err != nil {
		t.Fatalf("read position_state: %v", err)
	}
	if !ts1000.Equal(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)) || !ts1016.Equal(time.Date(2026, 3, 1, 0, 3, 24, 0, time.UTC)) {
		t.Errorf("block_timestamp = %s / %s; want the header times 00:00:00 and the corrected 00:03:24",
			ts1000.UTC().Format(time.RFC3339), ts1016.UTC().Format(time.RFC3339))
	}
}

// An observation whose block block_meta does not hold yet is not emitted, and a later run appends it once
// the loader reaches the block. The loader holds the newest blocks back, so refusing instead would stop
// every run.
func TestSkyPrimeDebtAppendsABlockOnceItsHeaderTimeArrives(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; BEGIN
	  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'wait', '\x2222222222222222222222222222222222222222') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, 'WAIT-A', 10, 2000, 0, '2026-04-01T00:00:00Z', 0, 0),
	    (pid, 'WAIT-A', 20, 2010, 0, '2026-04-01T00:05:00Z', 0, 0);
	  INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES (1, 2000, 0, '2026-04-01T00:00:00Z');
	END $s$`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var first, second int64
	if err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt()`).Scan(&first); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if first != 1 {
		t.Errorf("first run appended %d rows; want 1, block 2010 has no header time yet", first)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES (1, 2010, 0, '2026-04-01T00:02:00Z')`); err != nil {
		t.Fatalf("load block 2010: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT materialize_sky_prime_debt()`).Scan(&second); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second != 1 {
		t.Errorf("second run appended %d rows; want block 2010 now that its header time is loaded", second)
	}
}

// block_meta on another chain, or at another block_version, is not this block's header.
func TestSkyPrimeDebtIgnoresHeaderTimesForOtherChainsAndVersions(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; BEGIN
	  INSERT INTO chain (chain_id, name) VALUES (8453, 'base') ON CONFLICT DO NOTHING;
	  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'other', '\x3333333333333333333333333333333333333333') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, 'OTHER-A', 10, 3000, 0, '2026-04-01T00:00:00Z', 0, 0);
	  INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES
	    (8453, 3000, 0, '2026-04-01T00:00:00Z'),
	    (1, 3000, 1, '2026-04-01T00:00:00Z');
	END $s$`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_sky_prime_debt WHERE holder_id = '3333333333333333333333333333333333333333'`).Scan(&rows); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if rows != 0 {
		t.Errorf("projected %d rows from header times on another chain or block_version; want 0", rows)
	}
}

// Two processing_versions of one block are two observations, each emitted with its own block_number.
func TestSkyPrimeDebtEmitsEachProcessingVersionAtItsBlock(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	if _, err := pool.Exec(ctx, `
	DO $s$ DECLARE pid bigint; BEGIN
	  INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'pv', '\x4444444444444444444444444444444444444444') RETURNING id INTO pid;
	  INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id) VALUES
	    (pid, 'PV-A', 10, 4000, 0, '2026-04-01T00:00:00Z', 0, 0),
	    (pid, 'PV-A', 11, 4000, 0, '2026-04-01T00:00:00Z', 1, 1);
	  INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES (1, 4000, 0, '2026-04-01T00:00:00Z');
	END $s$`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var rows, atBlock int
	var qtys string
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE block_number = 4000), string_agg(quantity::text, ',' ORDER BY processing_version)
		  FROM position_sky_prime_debt WHERE holder_id = '4444444444444444444444444444444444444444'`).Scan(&rows, &atBlock, &qtys); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if rows != 2 || atBlock != 2 || qtys != "10,11" {
		t.Errorf("projected rows=%d at block 4000=%d quantities=%q; want 2 rows at block 4000 with quantities 10,11", rows, atBlock, qtys)
	}
}

// The wrapper pins tiered reads on, as the spine does, so its refusal check reads the same chunks as the run.
func TestSkyPrimeDebtWrapperPinsTieredReads(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	var config []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(proconfig, '{}') FROM pg_proc WHERE proname = 'materialize_sky_prime_debt'`).Scan(&config); err != nil {
		t.Fatalf("read the wrapper's settings: %v", err)
	}
	found := false
	for _, c := range config {
		if c == "timescaledb.enable_tiered_reads=on" {
			found = true
		}
	}
	if !found {
		t.Errorf("materialize_sky_prime_debt settings are %v; want timescaledb.enable_tiered_reads=on", config)
	}
}
