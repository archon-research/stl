//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
)

// vaultInstrument is the native instrument_key the seed produces: the vault contract address (abcd),
// lowercase hex, no 0x. A vault is a single native instrument, so there is no composite key.
const vaultInstrument = "abcd"

// VEC-403 contract: materialize_morpho_vault() projects raw morpho_vault_position rows into
// position_state on the native per-instrument grain (VEC-400). Observations only: the spine writes no
// classification, so nothing here asserts one.
//
// A vault is a single native instrument (no loan/collateral split, no netting), so what remains is: an
// event-time source observing one block twice collapsing to a single logical key (the view picks the
// latest timestamp), closure (VEC-409) -- an exit (positive->0) emits one closing zero-row and a deposit
// never entered emits nothing -- many observations per position, 32-byte ids, no PK collisions, and
// idempotency. One behaviour per function, each seeding its own database.

// The vault's holders, as the projection emits them.
const (
	vaultHolderA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	vaultHolderB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	vaultHolderC = "cccccccccccccccccccccccccccccccccccccccc"
	vaultHolderD = "dddddddddddddddddddddddddddddddddddddddd"
)

// seedMorphoVault gives a test its own migrated database, seeds the fixture and runs the projection
// once, returning what it reported written.
//
// One vault (address abcd) and four holders: A deposits (two observations), B has one block observed
// twice at different wall-clock timestamps, C never entered (single assets 0 row, nothing emitted), D
// deposits then exits (open + one closing zero-row).
func seedMorphoVault(t *testing.T) (context.Context, *pgxpool.Pool, int64) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	t.Cleanup(cleanup)
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	seed := `
DO $$
DECLARE pid bigint; atid bigint; uaid bigint; ubid bigint; ucid bigint; udid bigint; vid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO pid;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO atid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO uaid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') RETURNING id INTO ubid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xcccccccccccccccccccccccccccccccccccccccc') RETURNING id INTO ucid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xdddddddddddddddddddddddddddddddddddddddd') RETURNING id INTO udid;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, pid, '\xabcd', 'steakUSDC', atid, 1, 1) RETURNING id INTO vid;

  -- A: deposit with two observations (tests multiple observations per position_id -> one current class).
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (uaid, vid, 100, 0, '2026-01-01T00:00:00Z', 90, 100),
           (uaid, vid, 200, 0, '2026-01-02T00:00:00Z', 130, 150);
  -- B: one block observed twice at different wall-clock timestamps. assign_processing_version_morpho_vault_position
  -- keys its dedup on (user, vault, block, block_version, TIMESTAMP), so these are two separate groups and BOTH
  -- get pv=0 -- not pv=0 and pv=1. The projection's DISTINCT ON (..., processing_version) therefore collapses
  -- them to one row, taking the latest timestamp (assets 20), which is what the spine's contract requires:
  -- block_timestamp must be invariant per logical key, so an event-time source has to pick one stably.
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (ubid, vid, 100, 0, '2026-01-01T00:00:00Z', 9, 10),
           (ubid, vid, 100, 0, '2026-01-01T01:00:00Z', 18, 20);
  -- C: never entered (single assets 0 observation) -> no row emitted.
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (ucid, vid, 100, 0, '2026-01-01T00:00:00Z', 0, 0);
  -- D: deposit (50) then exit to 0 -> open + one closing zero-row.
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (udid, vid, 100, 0, '2026-01-01T00:00:00Z', 45, 50),
           (udid, vid, 200, 0, '2026-01-02T00:00:00Z', 0, 0);
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_vault()`).Scan(&written); err != nil {
		t.Fatalf("materialize_morpho_vault: %v", err)
	}
	return ctx, pool, written
}

// A (2 obs) + B (1: its two same-block rows collapse to one logical key) + D (open + close = 2) = 5
// rows; C never entered, skipped. Distinct positions: A, B, D = 3.
//
// B is the case worth stating: assign_processing_version_morpho_vault_position keys its dedup on
// (user, vault, block, block_version, TIMESTAMP), so B's two rows are separate groups and BOTH get
// pv=0 -- not pv=0 and pv=1. The projection's DISTINCT ON then collapses them to one row at the latest
// timestamp, which is what the spine requires: block_timestamp is invariant per logical key, so an
// event-time source has to pick one stably.
func TestMaterializeMorphoVaultProjectionShape(t *testing.T) {
	ctx, pool, written := seedMorphoVault(t)
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

func TestMaterializeMorphoVaultPerPosition(t *testing.T) {
	ctx, pool, _ := seedMorphoVault(t)
	for _, c := range []struct {
		name     string
		holder   string
		wantQty  string
		wantRows int
	}{
		{"A deposit, latest of two observations", vaultHolderA, "150", 2},
		{"B observed twice at one block: one logical key, latest timestamp wins", vaultHolderB, "20", 1},
		{"C never entered (assets 0) emits nothing", vaultHolderC, "", 0},
		{"D exit: deposit + one closing zero-row", vaultHolderD, "0", 2},
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
				vaultInstrument, c.holder).Scan(&n, &latestQty); err != nil {
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
func TestMaterializeMorphoVaultIsIdempotent(t *testing.T) {
	ctx, pool, _ := seedMorphoVault(t)
	var second int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_vault()`).Scan(&second); err != nil {
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
