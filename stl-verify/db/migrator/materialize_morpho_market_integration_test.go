//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Native instrument keys the seed produces (composite market_id ':' token_address, lowercase hex, no 0x):
// market 1234, loan token dead, collateral token beef; holders aa/bb/cc/dd.
const (
	loanInstrument   = "1234:dead" // market : loan-token
	collInstrument   = "1234:beef" // market : collateral-token
	m2LoanInstrument = "5678:dead" // degenerate market M2 (loan == collateral == dead) : loan-token
)

// The projection fans one raw row out into its loan-token leg (supply netted against borrow) and its
// collateral-token leg, keyed market_id:token, and emits one closing zero-row when a leg goes from a
// positive quantity to 0. A market whose collateral token is its loan token emits the loan leg only.
// One behaviour per function, each seeding its own database.

// seedMorphoMarket gives a test its own migrated database, seeds the fixture and runs the projection
// once, returning what it reported written.
func seedMorphoMarket(t *testing.T) (context.Context, *pgxpool.Pool, int64) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	t.Cleanup(cleanup)
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	seed := `
DO $$
DECLARE pid bigint; ltid bigint; ctid bigint;
        uaid bigint; ubid bigint; ucid bigint; udid bigint; mid bigint;
        ueid bigint; ufid bigint; ugid bigint; uhid bigint; uiid bigint; ujid bigint; mid2 bigint;
        ukid bigint; ulid bigint; umid bigint; unid bigint; uoid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xff', 'morpho') RETURNING id INTO pid;
  INSERT INTO token ("chain_id", address, symbol, decimals) VALUES (1, '\xdead', 'USDC', 6) RETURNING id INTO ltid;
  INSERT INTO token ("chain_id", address, symbol, decimals) VALUES (1, '\xbeef', 'WETH', 18) RETURNING id INTO ctid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO uaid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') RETURNING id INTO ubid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xcccccccccccccccccccccccccccccccccccccccc') RETURNING id INTO ucid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xdddddddddddddddddddddddddddddddddddddddd') RETURNING id INTO udid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') RETURNING id INTO ueid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xefefefefefefefefefefefefefefefefefefefef') RETURNING id INTO ufid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0') RETURNING id INTO ugid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1') RETURNING id INTO uiid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2') RETURNING id INTO uhid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3') RETURNING id INTO ujid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4') RETURNING id INTO ukid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5') RETURNING id INTO ulid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6') RETURNING id INTO umid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7') RETURNING id INTO unid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xf8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8') RETURNING id INTO uoid;
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
  -- D: one block observed twice at different wall-clock timestamps. Both rows take pv=0 (the source's
  -- dedup key includes timestamp), so the projection's DISTINCT ON collapses them to one row at the
  -- picked timestamp: block_timestamp must be invariant per logical key.
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
  -- J: a reprocess of one block: same key and timestamp, a different build_id, so the trigger assigns
  -- pv=1. Both are observations; the correction (supply 80) is the newer.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, build_id)
    VALUES (ujid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 70, 0, 0),
           (ujid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 80, 0, 1);
  -- H: same-token market M2 (loan == collateral == dead). Supplies 100 AND posts 50 collateral: one native
  -- instrument, so the loan leg alone is emitted with the collateral netted in (150).
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (uhid, mid2, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 50, 100, 0);
  -- M: same-token market with supply = borrow and collateral posted. Here the direction array really
  -- IS empty, so adding collateral to the magnitude stored quantity 100 with deal_type NULL -- and
  -- deal_type is nullable, so nothing objected. Netting first gives +100 LOAN.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (umid, mid2, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 100, 100, 100);
  -- N: loan leg opening LOAN then closing to zero: the close must keep the LOAN direction, which the
  -- fixture only covered for a BORROW. And O flips LOAN -> BORROW -> close, the case per-observation
  -- deal_type exists for, so the inherited direction must be the LAST one and not the first.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (unid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 500, 0),
           (unid, mid, 200, 0, '2026-01-02T00:00:00Z', 0, 0, 0, 0, 0);
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (uoid, mid, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 0, 400, 0),
           (uoid, mid, 200, 0, '2026-01-02T00:00:00Z', 0, 0, 0, 0, 300),
           (uoid, mid, 300, 0, '2026-01-03T00:00:00Z', 0, 0, 0, 0, 0);
  -- K: same-token market, borrow-dominant on the raw columns but net LONG once the collateral of the SAME
  -- token nets in: 0 supply - 100 borrow + 300 collateral = +200 LOAN. Adding collateral to a magnitude
  -- the direction never saw stored 400 SHORT here, a 600-unit signed error on one row.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (ukid, mid2, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 300, 0, 100);
  -- L: same-token market whose collateral exactly offsets the borrow, netting to 0. Adding collateral
  -- to the magnitude stored 200 BORROW here (the direction came from supply <> borrow, so it was set);
  -- netting first makes it a leading zero, which closure drops.
  INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
    VALUES (ulid, mid2, 100, 0, '2026-01-01T00:00:00Z', 0, 0, 100, 0, 100);
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(&written); err != nil {
		t.Fatalf("materialize_morpho_market: %v", err)
	}
	return ctx, pool, written
}

// Row shape:
//
//	A loan (2 obs) + B loan (1) + B coll (1) + C loan (1) + D loan (1) = 6
//	E coll (open + close = 2) + F loan (open + close = 2) + G coll (open + close = 2) = 6
//	I coll (open + close + reopen = 3) + H loan in M2 (1; collateral netted in) + J loan (pv 0 and 1 = 2) = 6
//	K loan in M2 (1) + L loan in M2 (0, a dropped leading zero) + M loan in M2 (1) = 2
//	N loan (open + close = 2) + O loan (open + flip + close = 3) = 5
//
// Total 25 over 15 distinct positions: the 12 above plus M-loan-M2, N-loan and O-loan. L nets to zero
// on its first observation, so it has none.
func TestMaterializeMorphoMarketProjectionShape(t *testing.T) {
	ctx, pool, written := seedMorphoMarket(t)
	var rows, distinctPositions, collisions, badLen int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(DISTINCT position_id),
		       count(*) - count(DISTINCT (position_id, block_number, block_version, processing_version)),
		       count(*) FILTER (WHERE octet_length(position_id) <> 32)
		FROM position_state`).Scan(&rows, &distinctPositions, &collisions, &badLen); err != nil {
		t.Fatalf("position_state summary: %v", err)
	}
	if rows != 25 {
		t.Errorf("position_state rows = %d, want 25", rows)
	}
	if written != 25 {
		t.Errorf("materialize returned %d, want 25", written)
	}
	if distinctPositions != 15 {
		t.Errorf("distinct position_id = %d, want 15", distinctPositions)
	}
	if collisions != 0 {
		t.Errorf("PK collisions = %d, want 0", collisions)
	}
	if badLen != 0 {
		t.Errorf("%d position_id(s) not 32 bytes", badLen)
	}
}

// Per-holder results, located by the native instrument_key + holder_id.
func TestMaterializeMorphoMarketPerPosition(t *testing.T) {
	ctx, pool, _ := seedMorphoMarket(t)
	for _, c := range []struct {
		name          string
		instrument    string
		holder        string
		wantQty       string // numeric as text
		wantRows      int
		wantDealTypes string // per observation in observation order, comma-joined
	}{
		{"A supplier latest obs (multi-observation kept)", loanInstrument, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "150", 2, "LOAN,LOAN"},
		{"B borrow leg (loan token)", loanInstrument, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "30", 1, "BORROW"},
		{"B collateral leg (collateral token)", collInstrument, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "5", 1, "COLLATERAL"},
		{"C supply/borrow netted (|100-40|)", loanInstrument, "cccccccccccccccccccccccccccccccccccccccc", "60", 1, "LOAN"},
		{"D observed twice at one block: one logical key, earliest timestamp is the stable pick", loanInstrument, "dddddddddddddddddddddddddddddddddddddddd", "10", 1, "LOAN"},
		{"E collateral closed: open + one closing zero-row", collInstrument, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "0", 2, "COLLATERAL,COLLATERAL"},
		{"F loan closed: borrow + one closing zero-row that keeps the BORROW direction", loanInstrument, "efefefefefefefefefefefefefefefefefefefef", "0", 2, "BORROW,BORROW"},
		{"G leading + repeated zeros dropped: open + one close", collInstrument, "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0", "0", 2, "COLLATERAL,COLLATERAL"},
		{"I re-open: open + close + reopen, latest is the reopen", collInstrument, "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1", "7", 3, "COLLATERAL,COLLATERAL,COLLATERAL"},
		{"H same-token market: loan leg only, collateral netted in", m2LoanInstrument, "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2", "150", 1, "LOAN"},
		{"J reprocessed block: both processing versions are observations, the correction is newest", loanInstrument, "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3", "80", 2, "LOAN,LOAN"},
		{"K same-token market: the collateral nets into the direction, not just the magnitude", m2LoanInstrument, "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4", "200", 1, "LOAN"},
		{"L same-token market netting to zero emits no leading zero, so no quantity without a direction", m2LoanInstrument, "f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5", "", 0, ""},
		{"M supply = borrow with collateral posted: the one case whose direction array is empty", m2LoanInstrument, "f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6", "100", 1, "LOAN"},
		{"N a LOAN closing to zero keeps the LOAN direction", loanInstrument, "f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7", "0", 2, "LOAN,LOAN"},
		{"O a direction flip inherits the LAST direction, not the first", loanInstrument, "f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8", "0", 3, "LOAN,BORROW,BORROW"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var n int
			var latestQty, dealTypes string
			if err := pool.QueryRow(ctx, `
				SELECT count(*),
				       coalesce((SELECT quantity::text FROM position_state
				        WHERE instrument_key = $1 AND holder_id = $2
				        ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1), ''),
				       coalesce(string_agg(deal_type, ',' ORDER BY block_number, block_version, processing_version), '')
				FROM position_state WHERE instrument_key = $1 AND holder_id = $2`,
				c.instrument, c.holder).Scan(&n, &latestQty, &dealTypes); err != nil {
				t.Fatalf("query: %v", err)
			}
			if n != c.wantRows {
				t.Errorf("rows = %d, want %d", n, c.wantRows)
			}
			if latestQty != c.wantQty {
				t.Errorf("latest quantity = %s, want %s", latestQty, c.wantQty)
			}
			if dealTypes != c.wantDealTypes {
				t.Errorf("deal_types = %s, want %s", dealTypes, c.wantDealTypes)
			}
		})
	}
}

// A second run re-derives the same observations and appends nothing.
func TestMaterializeMorphoMarketIsIdempotent(t *testing.T) {
	ctx, pool, _ := seedMorphoMarket(t)
	var second int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(&second); err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	if second != 0 {
		t.Errorf("the second run reported %d rows appended, want 0", second)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if rows != 25 {
		t.Errorf("after re-run: position_state=%d, want 25 (the rerun must append nothing)", rows)
	}
}

// None of supply_assets, borrow_assets or collateral carries a CHECK. Netting means abs() launders a
// negative one (collateral -50 stored as quantity 50, silently), and a negative borrow makes the sum
// MORE positive, so the spine's own check cannot see it either. The wrapper refuses it by name.
func TestMaterializeMorphoMarketNegativeSourceAmountAborts(t *testing.T) {
	ctx, pool, _ := seedMorphoMarket(t)
	for _, c := range []struct {
		name                            string
		supply, borrow, collateral, mkt string
	}{
		{"negative collateral in a same-token market, where it nets into the loan leg", "0", "0", "-50", `\x5678`},
		{"negative supply", "-70", "0", "0", `\x1234`},
		{"negative borrow", "0", "-70", "0", `\x1234`},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx2, pool2, _ := seedMorphoMarket(t)
			_ = ctx
			_ = pool
			if _, err := pool2.Exec(ctx2, `
				INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
				SELECT u.id, m.id, 900, 0, '2026-02-01T00:00:00Z', 0, 0, $1::numeric, $2::numeric, $3::numeric
				FROM "user" u, morpho_market m
				WHERE u.chain_id = 1 AND u.address = '\xf2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2'
				  AND m.market_id = $4`, c.collateral, c.supply, c.borrow, c.mkt); err != nil {
				t.Fatalf("seed: %v", err)
			}
			var n int64
			err := pool2.QueryRow(ctx2, `SELECT materialize_morpho_market()`).Scan(&n)
			if err == nil {
				t.Fatalf("the run appended %d rows; a negative source amount must abort", n)
			}
			if !strings.Contains(err.Error(), "negative source amount cannot be a position exposure") {
				t.Errorf("error %q does not name the negative source amount", err.Error())
			}
			var rows int
			if err := pool2.QueryRow(ctx2, `SELECT count(*) FROM position_state WHERE block_number = 900`).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 0 {
				t.Errorf("stored %d rows at the offending block; want none", rows)
			}
		})
	}
}
