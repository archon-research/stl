//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// vaultInstrument is the native instrument_key the seed produces: the vault contract address (abcd),
// lowercase hex, no 0x. A vault is a single native instrument, so there is no composite key.
const vaultInstrument = "abcd"

// TestMaterializeMorphoVault is the VEC-403 contract test: after migrations, materialize_morpho_vault()
// projects raw morpho_vault_position rows into position_state on the native per-instrument grain
// (VEC-400) — one position per vault deposit, keyed by the vault contract address — and writes the
// current deal_type into position_classification.
//
// A vault is a single native instrument (no loan/collateral split, no netting), so this pins the
// behaviours that remain: latest-timestamp dedup of same-(user,vault,block,version) rows, zero-asset
// observations skipped, many observations per position with one current classification, 32-byte ids,
// no PK collisions, and idempotency.
func TestMaterializeMorphoVault(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Seed one vault (address abcd) and three holders: A deposits (two observations), B is a same-block
	// dedup case, C has exited (assets 0, must be skipped).
	seed := `
DO $$
DECLARE pid bigint; atid bigint; uaid bigint; ubid bigint; ucid bigint; vid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO pid;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO atid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaa') RETURNING id INTO uaid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xbb') RETURNING id INTO ubid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xcc') RETURNING id INTO ucid;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, pid, '\xabcd', 'steakUSDC', atid, 1, 1) RETURNING id INTO vid;

  -- A: deposit with two observations (tests multiple observations per position_id -> one current class).
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (uaid, vid, 100, 0, '2026-01-01T00:00:00Z', 90, 100),
           (uaid, vid, 200, 0, '2026-01-02T00:00:00Z', 130, 150);
  -- B: same (user, vault, block, version), two timestamps -> dedup keeps the latest (assets 20).
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (ubid, vid, 100, 0, '2026-01-01T00:00:00Z', 9, 10),
           (ubid, vid, 100, 0, '2026-01-01T01:00:00Z', 18, 20);
  -- C: exited position (assets 0) -> skipped entirely.
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (ucid, vid, 100, 0, '2026-01-01T00:00:00Z', 0, 0);
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_vault()`).Scan(&written); err != nil {
		t.Fatalf("materialize_morpho_vault: %v", err)
	}

	// A (2 obs) + B (1, deduped) = 3 rows; C skipped. Distinct positions: A, B = 2.
	var rows, distinctPositions, collisions, badLen int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(DISTINCT position_id),
		       count(*) - count(DISTINCT (position_id, block_number, block_version, processing_version)),
		       count(*) FILTER (WHERE octet_length(position_id) <> 32)
		FROM position_state`).Scan(&rows, &distinctPositions, &collisions, &badLen); err != nil {
		t.Fatalf("position_state summary: %v", err)
	}
	if rows != 3 {
		t.Errorf("position_state rows = %d, want 3", rows)
	}
	if written != 3 {
		t.Errorf("materialize returned %d, want 3", written)
	}
	if distinctPositions != 2 {
		t.Errorf("distinct position_id = %d, want 2", distinctPositions)
	}
	if collisions != 0 {
		t.Errorf("PK collisions = %d, want 0", collisions)
	}
	if badLen != 0 {
		t.Errorf("%d position_id(s) not 32 bytes", badLen)
	}

	for _, c := range []struct {
		name     string
		holder   string
		wantQty  string
		wantRows int
	}{
		{"A deposit, latest of two observations", "aa", "150", 2},
		{"B same-block dedup keeps latest timestamp", "bb", "20", 1},
		{"C exited (assets 0) is skipped", "cc", "", 0},
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
				vaultInstrument, c.holder).Scan(&n, &latestQty); err != nil {
				t.Fatalf("query: %v", err)
			}
			if n != c.wantRows {
				t.Errorf("rows = %d, want %d", n, c.wantRows)
			}
			if c.wantRows > 0 {
				if latestQty == nil || *latestQty != c.wantQty {
					t.Errorf("latest quantity = %v, want %s", latestQty, c.wantQty)
				}
			}
		})
	}

	// One current classification per position (2), each LOAN/LONG (a vault deposit lends).
	var classRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_classification`).Scan(&classRows); err != nil {
		t.Fatalf("classification count: %v", err)
	}
	if classRows != 2 {
		t.Errorf("position_classification rows = %d, want 2", classRows)
	}
	for _, holder := range []string{"aa", "bb"} {
		holder := holder
		t.Run("classify "+holder, func(t *testing.T) {
			var dealType, direction string
			if err := pool.QueryRow(ctx, `
				SELECT pc.deal_type_code, pc.direction
				FROM position_classification pc
				JOIN (SELECT DISTINCT position_id, instrument_key, holder_id FROM position_state) ps
				  ON ps.position_id = pc.position_id
				WHERE ps.instrument_key = $1 AND ps.holder_id = $2`,
				vaultInstrument, holder).Scan(&dealType, &direction); err != nil {
				t.Fatalf("classification query: %v", err)
			}
			if dealType != "LOAN" || direction != "LONG" {
				t.Errorf("classification = %s/%s, want LOAN/LONG", dealType, direction)
			}
		})
	}

	// Idempotent: a second run upserts the same rows, not new ones.
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_vault()`); err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	var rows2, class2 int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM position_state), (SELECT count(*) FROM position_classification)`).Scan(&rows2, &class2); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if rows2 != 3 || class2 != 2 {
		t.Errorf("after re-run: position_state=%d (want 3), position_classification=%d (want 2)", rows2, class2)
	}
}
