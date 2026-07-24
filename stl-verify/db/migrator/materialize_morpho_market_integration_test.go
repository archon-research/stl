//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// Native instrument keys the seed produces (composite market_id ':' token_address, lowercase hex, no 0x):
// market 1234, loan token dead, collateral token beef; holders aa/bb/cc/dd.
const (
	loanInstrument = "1234:dead" // market : loan-token
	collInstrument = "1234:beef" // market : collateral-token
)

// TestMaterializeMorphoMarket is the VEC-402 contract test: after migrations,
// materialize_morpho_market() projects raw morpho_market_position rows into position_state on the
// native per-instrument grain (VEC-400) and writes the current deal_type into position_classification.
//
// It pins the behaviours the live data forced (verified over 866,833 rows, 2026-07-24):
//   - native per-instrument fan-out: one raw row -> its loan-token position and its collateral-token
//     position, keyed by the composite market_id:token native key (never a deal_type classifier);
//   - supply/borrow netting into one loan-token position (a single native instrument holds one position);
//   - latest-timestamp dedup of same-(user,market,block,version) rows;
//   - many observations per position_id, one CURRENT classification per position_id;
//   - 32-byte ids, no PK collisions, and idempotency.
func TestMaterializeMorphoMarket(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Seed one market (loan token dead, collateral token beef) and four holders, each exercising a
	// behaviour. All ids are resolved by natural key inside the DO block so no serial plumbing leaks out.
	seed := `
DO $$
DECLARE pid bigint; ltid bigint; ctid bigint;
        uaid bigint; ubid bigint; ucid bigint; udid bigint; mid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xff', 'morpho') RETURNING id INTO pid;
  INSERT INTO token ("chain_id", address, symbol, decimals) VALUES (1, '\xdead', 'USDC', 6) RETURNING id INTO ltid;
  INSERT INTO token ("chain_id", address, symbol, decimals) VALUES (1, '\xbeef', 'WETH', 18) RETURNING id INTO ctid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaa') RETURNING id INTO uaid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xbb') RETURNING id INTO ubid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xcc') RETURNING id INTO ucid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xdd') RETURNING id INTO udid;
  INSERT INTO morpho_market
    (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block)
    VALUES (1, pid, '\x1234', ltid, ctid, '\x00', '\x01', 0.86, 1) RETURNING id INTO mid;

  -- A: supplier, two observations (tests multiple observations per position_id -> one current class).
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (uaid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 100, 0),
           (uaid, mid, 200, 0, '2026-01-02T00:00:00Z', 0, 0, 0, 150, 0);
  -- B: borrower — borrow leg (loan token) + collateral leg (collateral token) -> two instruments.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (ubid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 5, 0, 30);
  -- C: supply-and-borrow loop of the loan token -> nets to one loan-token position (|100-40| = 60, LOAN).
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (ucid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 100, 40);
  -- D: same (user, market, block, version), two timestamps -> dedup keeps the latest (supply 20).
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (udid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 10, 0),
           (udid, mid, 100, 0, '2026-01-01T01:00:00Z', 0, 0, 0, 20, 0);
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Run the materializer.
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(&written); err != nil {
		t.Fatalf("materialize_morpho_market: %v", err)
	}

	// position_state row shape: A loan (2 obs) + B loan (1) + B coll (1) + C loan (1) + D loan (1) = 6.
	// Distinct positions: A, B-loan, B-coll, C, D = 5.
	var rows, distinctPositions, collisions, badLen int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(DISTINCT position_id),
		       count(*) - count(DISTINCT (position_id, block_number, block_version, processing_version)),
		       count(*) FILTER (WHERE octet_length(position_id) <> 32)
		FROM position_state`).Scan(&rows, &distinctPositions, &collisions, &badLen); err != nil {
		t.Fatalf("position_state summary: %v", err)
	}
	if rows != 6 {
		t.Errorf("position_state rows = %d, want 6", rows)
	}
	if written != 6 {
		t.Errorf("materialize returned %d, want 6", written)
	}
	if distinctPositions != 5 {
		t.Errorf("distinct position_id = %d, want 5", distinctPositions)
	}
	if collisions != 0 {
		t.Errorf("PK collisions = %d, want 0", collisions)
	}
	if badLen != 0 {
		t.Errorf("%d position_id(s) not 32 bytes", badLen)
	}

	// Per-holder quantity assertions, located by the native instrument_key + holder_id.
	for _, c := range []struct {
		name       string
		instrument string
		holder     string
		wantQty    string // numeric as text
		wantRows   int
	}{
		{"A supplier latest obs (multi-observation kept)", loanInstrument, "aa", "150", 2},
		{"B borrow leg (loan token)", loanInstrument, "bb", "30", 1},
		{"B collateral leg (collateral token)", collInstrument, "bb", "5", 1},
		{"C supply/borrow netted (|100-40|)", loanInstrument, "cc", "60", 1},
		{"D same-block dedup keeps latest timestamp", loanInstrument, "dd", "20", 1},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var n int
			var latestQty string
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
			if latestQty != c.wantQty {
				t.Errorf("latest quantity = %s, want %s", latestQty, c.wantQty)
			}
		})
	}

	// One current classification per position_id (5), each with the right deal_type and frozen direction.
	var classRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_classification`).Scan(&classRows); err != nil {
		t.Fatalf("classification count: %v", err)
	}
	if classRows != 5 {
		t.Errorf("position_classification rows = %d, want 5", classRows)
	}
	for _, c := range []struct {
		name          string
		instrument    string
		holder        string
		wantDealType  string
		wantDirection string
	}{
		{"A -> LOAN/LONG", loanInstrument, "aa", "LOAN", "LONG"},
		{"B loan -> BORROW/SHORT", loanInstrument, "bb", "BORROW", "SHORT"},
		{"B coll -> COLLATERAL/LONG", collInstrument, "bb", "COLLATERAL", "LONG"},
		{"C net-supply -> LOAN/LONG", loanInstrument, "cc", "LOAN", "LONG"},
	} {
		c := c
		t.Run("classify "+c.name, func(t *testing.T) {
			var dealType, direction string
			if err := pool.QueryRow(ctx, `
				SELECT pc.deal_type_code, pc.direction
				FROM position_classification pc
				JOIN (SELECT DISTINCT position_id, instrument_key, holder_id FROM position_state) ps
				  ON ps.position_id = pc.position_id
				WHERE ps.instrument_key = $1 AND ps.holder_id = $2`,
				c.instrument, c.holder).Scan(&dealType, &direction); err != nil {
				t.Fatalf("classification query: %v", err)
			}
			if dealType != c.wantDealType || direction != c.wantDirection {
				t.Errorf("classification = %s/%s, want %s/%s", dealType, direction, c.wantDealType, c.wantDirection)
			}
		})
	}

	// Idempotent: a second run upserts the same rows, not new ones.
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_market()`); err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	var rows2, class2 int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM position_state), (SELECT count(*) FROM position_classification)`).Scan(&rows2, &class2); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if rows2 != 6 || class2 != 5 {
		t.Errorf("after re-run: position_state=%d (want 6), position_classification=%d (want 5)", rows2, class2)
	}
}
