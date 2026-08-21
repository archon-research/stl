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
	loanInstrument   = "1234:dead" // market : loan-token
	collInstrument   = "1234:beef" // market : collateral-token
	m2LoanInstrument = "5678:dead" // degenerate market M2 (loan == collateral == dead) : loan-token
)

// TestMaterializeMorphoMarket is the VEC-402 contract test: after migrations,
// materialize_morpho_market() projects raw morpho_market_position rows into position_state on the
// native per-instrument grain (VEC-400). Observations only: the spine writes no classification.
//
// It pins the behaviours the live data forced (verified over 866,833 rows, 2026-07-24):
//   - native per-instrument fan-out: one raw row -> its loan-token position and its collateral-token
//     position, keyed by the composite market_id:token native key (never a deal_type classifier);
//   - supply/borrow netting into one loan-token position (a single native instrument holds one position);
//   - latest-timestamp dedup of same-(user,market,block,version) rows;
//   - many observations per position_id, one CURRENT classification per position_id;
//   - closure (VEC-409): a leg observed going from a positive quantity to 0 emits ONE closing
//     zero-row (so position_current shows it closed); a leg never entered emits nothing; leading and
//     repeated zeros are dropped; a re-open (positive after a close) survives; the classification keeps
//     the last NON-ZERO deal_type;
//   - the same-token guard: a market whose collateral token IS its loan token would key both legs on
//     one position_id; the collateral leg is dropped so the run cannot hit a duplicate-PK abort;
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
        ueid bigint; ufid bigint; ugid bigint; uhid bigint; uiid bigint; mid2 bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xff', 'morpho') RETURNING id INTO pid;
  INSERT INTO token ("chain_id", address, symbol, decimals) VALUES (1, '\xdead', 'USDC', 6) RETURNING id INTO ltid;
  INSERT INTO token ("chain_id", address, symbol, decimals) VALUES (1, '\xbeef', 'WETH', 18) RETURNING id INTO ctid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaa') RETURNING id INTO uaid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xbb') RETURNING id INTO ubid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xcc') RETURNING id INTO ucid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xdd') RETURNING id INTO udid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xee') RETURNING id INTO ueid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xef') RETURNING id INTO ufid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf0') RETURNING id INTO ugid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf1') RETURNING id INTO uiid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf2') RETURNING id INTO uhid;
  INSERT INTO morpho_market
    (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block)
    VALUES (1, pid, '\x1234', ltid, ctid, '\x00', '\x01', 0.86, 1) RETURNING id INTO mid;
  -- M2: a degenerate market whose collateral token IS its loan token (both dead). Both legs would key on
  -- market:dead -> same position_id; the collateral-leg guard (coll_addr <> loan_addr) must drop the
  -- collateral leg so the run does not hit a duplicate-PK ON CONFLICT abort.
  INSERT INTO morpho_market
    (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block)
    VALUES (1, pid, '\x5678', ltid, ltid, '\x00', '\x01', 0.86, 1) RETURNING id INTO mid2;

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
  -- D: one block observed twice at different wall-clock timestamps. assign_processing_version_morpho_market_position
  -- keys its dedup on (user, market, block, block_version, TIMESTAMP), so these are two separate groups and BOTH
  -- get pv=0 -- not pv=0 and pv=1. The projection's DISTINCT ON (..., processing_version) collapses them to one
  -- row at the latest timestamp (supply 20), which is what the spine requires: block_timestamp is invariant per
  -- logical key, so an event-time source must pick one stably. Retained for the record:
  -- position_current picks the latest pv (supply 20). (No explicit pv column: the trigger assigns it.)
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (udid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 10, 0),
           (udid, mid, 100, 0, '2026-01-01T01:00:00Z', 0, 0, 0, 20, 0);
  -- E: collateral opened (5) then withdrawn to 0 -> collateral leg emits open + one closing zero-row.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (ueid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 5, 0, 0),
           (ueid, mid, 200, 0, '2026-01-02T00:00:00Z', 0, 0, 0, 0, 0);
  -- F: loan-token borrowed (30) then repaid to 0 -> loan leg emits open (BORROW) + one closing zero-row.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (ufid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 0, 30),
           (ufid, mid, 200, 0, '2026-01-02T00:00:00Z', 0, 0, 0, 0, 0);
  -- G: collateral 0 (never entered) -> 5 (open) -> 0 (close) -> 0 (repeat). Leg emits only the open and
  -- the first close: leading and repeated zeros are dropped (LAG closure semantics).
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (ugid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 0, 0),
           (ugid, mid, 200, 0, '2026-01-02T00:00:00Z', 0, 0, 5, 0, 0),
           (ugid, mid, 300, 0, '2026-01-03T00:00:00Z', 0, 0, 0, 0, 0),
           (ugid, mid, 400, 0, '2026-01-04T00:00:00Z', 0, 0, 0, 0, 0);
  -- I: re-open — collateral 5 (open) -> 0 (close) -> 7 (re-open). All three survive: a positive after a
  -- close satisfies quantity > 0 regardless of prev; latest is the re-open (7).
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (uiid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 5, 0, 0),
           (uiid, mid, 200, 0, '2026-01-02T00:00:00Z', 0, 0, 0, 0, 0),
           (uiid, mid, 300, 0, '2026-01-03T00:00:00Z', 0, 0, 7, 0, 0);
  -- H: same-token market M2 (loan == collateral == dead). Supplies 100 AND posts 50 collateral. The loan
  -- leg emits (market2:dead, qty 100); the collateral leg (same key) is dropped by the guard, so there is
  -- no duplicate-PK abort and H has exactly one position.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (uhid, mid2, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 50, 100, 0);
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Run the materializer.
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(&written); err != nil {
		t.Fatalf("materialize_morpho_market: %v", err)
	}

	// position_state row shape:
	//   A loan (2 obs) + B loan (1) + B coll (1) + C loan (1) + D loan (2 pv: reprocessed) = 7
	//   E coll (open + close = 2) + F loan (open + close = 2) + G coll (open + close = 2) = 6
	//   I coll (open + close + reopen = 3) + H loan in M2 (1; collateral leg guarded out) = 4
	// Total 17. Distinct positions: A, B-loan, B-coll, C, D, E-coll, F-loan, G-coll, I-coll, H-loan-M2 = 10
	// (D is one position with two observations at the same block, different processing_version).
	var rows, distinctPositions, collisions, badLen int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(DISTINCT position_id),
		       count(*) - count(DISTINCT (position_id, block_number, block_version, processing_version)),
		       count(*) FILTER (WHERE octet_length(position_id) <> 32)
		FROM position_state`).Scan(&rows, &distinctPositions, &collisions, &badLen); err != nil {
		t.Fatalf("position_state summary: %v", err)
	}
	if rows != 16 {
		t.Errorf("position_state rows = %d, want 16", rows)
	}
	if written != 16 {
		t.Errorf("materialize returned %d, want 16", written)
	}
	if distinctPositions != 10 {
		t.Errorf("distinct position_id = %d, want 10", distinctPositions)
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
		{"D observed twice at one block: one logical key, latest timestamp wins", loanInstrument, "dd", "20", 1},
		{"E collateral closed: open + one closing zero-row", collInstrument, "ee", "0", 2},
		{"F loan closed: borrow + one closing zero-row", loanInstrument, "ef", "0", 2},
		{"G leading + repeated zeros dropped: open + one close", collInstrument, "f0", "0", 2},
		{"I re-open: open + close + reopen, latest is the reopen", collInstrument, "f1", "7", 3},
		{"H same-token market: loan leg only, collateral guarded out", m2LoanInstrument, "f2", "100", 1},
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

	// Idempotent: a second run re-derives the same observations and appends nothing.
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_market()`); err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	var rows2 int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows2); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if rows2 != 16 {
		t.Errorf("after re-run: position_state=%d, want 16 (the rerun must append nothing)", rows2)
	}
}
