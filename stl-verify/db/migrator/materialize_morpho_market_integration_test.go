//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

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
// Tests that leave the source rows unchanged share one database; a test that adds source rows seeds its own.

// materializeMorphoMarketFixture gives a test its own migrated database, seeds the fixture and runs the projection
// once, returning what it reported written. Scheduled jobs are off, so a policy run cannot take locks mid-test.
func materializeMorphoMarketFixture(t *testing.T) (context.Context, *pgxpool.Pool, int64) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
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
func morphoMarketProjectionShape(ctx context.Context, t *testing.T, pool *pgxpool.Pool, written int64) {
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
func morphoMarketPerPosition(ctx context.Context, t *testing.T, pool *pgxpool.Pool, _ int64) {
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
func morphoMarketIsIdempotent(ctx context.Context, t *testing.T, pool *pgxpool.Pool, _ int64) {
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
	for _, c := range []struct {
		name                            string
		supply, borrow, collateral, mkt string
	}{
		{"negative collateral in a same-token market, where it nets into the loan leg", "0", "0", "-50", `\x5678`},
		{"negative collateral in a different-token market, which reaches the collateral leg raw", "0", "0", "-50", `\x1234`},
		{"negative supply", "-70", "0", "0", `\x1234`},
		{"negative borrow", "0", "-70", "0", `\x1234`},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx2, pool2, _ := materializeMorphoMarketFixture(t)
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
			if !strings.Contains(err.Error(), "refusing to run") || !strings.Contains(err.Error(), "has a negative source amount") {
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

// The wrapper is the only path the runner calls, so it has to forward the writer run to the spine or
// every row this projection appends is provenance-free (ADR-0006 §2). The run record is the witness:
// its run_id can only have arrived through the wrapper's own parameter.
func morphoMarketForwardsTheWriterRun(ctx context.Context, t *testing.T, pool *pgxpool.Pool, _ int64) {
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_market(7, 9182)`); err != nil {
		t.Fatalf("materialize_morpho_market with a run: %v", err)
	}
	var runID *int64
	var buildID int
	if err := pool.QueryRow(ctx, `
		SELECT run_id, build_id FROM position_projection_run
		 WHERE projection = 'public.position_morpho_market'
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
		SELECT proargnames::text[] FROM pg_proc WHERE proname = 'materialize_morpho_market'`).Scan(&args); err != nil {
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
			t.Errorf("materialize_morpho_market declares %v, missing %s -- the runner calls it by name", args, want)
		}
	}
}

// chain_id and protocol_id come from the MARKET. Both feed the position_id hash, so a wrong constant
// forks every identity in a table that grants no UPDATE, and no count-based assertion would notice.
func morphoMarketTakesChainAndProtocolFromTheMarket(ctx context.Context, t *testing.T, pool *pgxpool.Pool, _ int64) {
	var mismatched int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM position_state s
		WHERE s.projection = 'public.position_morpho_market'
		  AND NOT EXISTS (
		      SELECT 1 FROM morpho_market m
		       WHERE m.chain_id = s.chain_id AND m.protocol_id = s.protocol_id
		         AND s.instrument_key LIKE encode(m.market_id, 'hex') || ':%')`).Scan(&mismatched); err != nil {
		t.Fatal(err)
	}
	var total int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM position_state WHERE projection = 'public.position_morpho_market'`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total == 0 {
		t.Fatal("the projection stored nothing, so the assertion below would pass vacuously")
	}
	if mismatched != 0 {
		t.Errorf("%d of %d rows carry a chain_id/protocol_id pair that is not their own market's", mismatched, total)
	}
}

// holder_id is the depositor's address alone while chain_id comes from the market, so two "user" rows
// sharing an address render one position_id and interleave two holders' histories under closure.
func TestMaterializeMorphoMarketRefusesOneAddressOnSeveralChains(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	if _, err := pool.Exec(ctx, `INSERT INTO chain (chain_id, name) VALUES (8453, 'base') ON CONFLICT (chain_id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO "user" (chain_id, address) VALUES (8453, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`); err != nil {
		t.Fatalf("seed the twin holder: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
		SELECT u.id, m.id, 950, 0, '2026-02-01T00:00:00Z', 0, 0, 0, 10, 0
		FROM "user" u, morpho_market m
		WHERE u.chain_id = 8453 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND m.market_id = '\x1234'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(&written)
	if err == nil {
		t.Fatalf("the run stored %d rows; two holders on one address must refuse", written)
	}
	if !strings.Contains(err.Error(), "refusing to run") || !strings.Contains(err.Error(), `"user" rows sharing address`) {
		t.Errorf("error %q does not name the shared holder address", err.Error())
	}
	if strings.Contains(err.Error(), "refusing to run: ;") {
		t.Errorf("error %q opens with an empty negative-amount list", err.Error())
	}
}

// A holder address that is not 20 bytes aborts on position_state's 40-hex CHECK naming no row.
func TestMaterializeMorphoMarketRefusesAMalformedHolder(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	if _, err := pool.Exec(ctx, `INSERT INTO "user" (chain_id, address) VALUES (1, '\xbeefcafe')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
		SELECT u.id, m.id, 960, 0, '2026-02-01T00:00:00Z', 0, 0, 0, 10, 0
		FROM "user" u, morpho_market m
		WHERE u.chain_id = 1 AND u.address = '\xbeefcafe' AND m.market_id = '\x1234'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(new(int64))
	if err == nil {
		t.Fatal("a 4-byte holder address must refuse by name")
	}
	if !strings.Contains(err.Error(), "4-byte address") {
		t.Errorf("error %q does not name the malformed holder", err.Error())
	}
}

// token is unique on (chain_id, address), so two token rows sharing an address are on different chains
// and are different tokens. Splitting the legs on the address would merge them into one quantity, so a
// market that takes a token from another chain is refused by name.
func TestMaterializeMorphoMarketRefusesATokenOnAnotherChain(t *testing.T) {
	for _, leg := range []string{"loan", "collateral"} {
		t.Run(leg, func(t *testing.T) {
			ctx, pool, _ := materializeMorphoMarketFixture(t)
			loan, coll := "1", "8453"
			if leg == "loan" {
				loan, coll = "8453", "1"
			}
			if _, err := pool.Exec(ctx, `
				INSERT INTO chain (chain_id, name) VALUES (8453, 'base') ON CONFLICT (chain_id) DO NOTHING;
				INSERT INTO token (chain_id, address, symbol, decimals) VALUES (8453, '\xdead', 'USDC', 6);
				INSERT INTO morpho_market (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id, lltv, oracle_address, irm_address, created_at_block)
				SELECT 1, p.id, '\x9abc', lt.id, ct.id, 0, '\x00', '\x00', 1
				FROM protocol p, token lt, token ct
				WHERE p.chain_id = 1 AND p.address = '\xff'
				  AND lt.chain_id = `+loan+` AND lt.address = '\xdead' AND ct.chain_id = `+coll+` AND ct.address = '\xdead';
				INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
				SELECT u.id, m.id, 970, 0, '2026-02-01T00:00:00Z', 0, 0, 40, 100, 0
				FROM "user" u, morpho_market m
				WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND m.market_id = '\x9abc'`); err != nil {
				t.Fatalf("seed: %v", err)
			}
			var written int64
			err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(&written)
			if err == nil {
				t.Fatalf("the run stored %d rows; a %s token from chain 8453 in a chain-1 market must refuse", written, leg)
			}
			if want := "market 9abc takes its " + leg + " token from chain 8453 but is on chain 1"; !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err.Error(), want)
			}
			var rows int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE block_number = 970`).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 0 {
				t.Errorf("stored %d rows at the offending block; want none", rows)
			}
		})
	}
}

// The wrapper is the only path the runner calls, so a window it cannot forward is a window this
// projection can never run with. The run record stamps what the spine actually received.
func morphoMarketForwardsTheWindow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, _ int64) {

	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_market(0, NULL, interval '36 hours')`); err != nil {
		t.Fatalf("calling with a window: %v", err)
	}

	var window *string
	if err := pool.QueryRow(ctx, `
		SELECT window_interval::text FROM position_projection_run
		 WHERE projection = 'public.position_morpho_market'
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

// Every other width case is SHORT, so <> 20 weakened to < 20 passes them all. 21 bytes is the case
// above the bound: it renders 42 hex characters and fails the same 40-hex CHECK.
func TestMaterializeMorphoMarketRefusesAnOversizeHolder(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	if _, err := pool.Exec(ctx,
		`INSERT INTO "user" (chain_id, address) VALUES (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
		SELECT u.id, m.id, 961, 0, '2026-02-01T00:00:00Z', 0, 0, 0, 10, 0
		FROM "user" u, morpho_market m
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff' AND m.market_id = '\x1234'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(new(int64))
	if err == nil {
		t.Fatal("a 21-byte holder address must refuse by name")
	}
	if !strings.Contains(err.Error(), "21-byte address") {
		t.Errorf("error %q does not name the oversize holder", err.Error())
	}
}

// These leave the source rows unchanged, so they share one database. Subtests that run the projection again
// come after the ones that count what the first run wrote.
func TestMaterializeMorphoMarketOnTheSeed(t *testing.T) {
	ctx, pool, written := materializeMorphoMarketFixture(t)
	for _, c := range []struct {
		name string
		run  func(context.Context, *testing.T, *pgxpool.Pool, int64)
	}{
		{"ProjectionShape", morphoMarketProjectionShape},
		{"PerPosition", morphoMarketPerPosition},
		{"TakesChainAndProtocolFromTheMarket", morphoMarketTakesChainAndProtocolFromTheMarket},
		{"IsIdempotent", morphoMarketIsIdempotent},
		{"ForwardsTheWriterRun", morphoMarketForwardsTheWriterRun},
		{"ForwardsTheWindow", morphoMarketForwardsTheWindow},
	} {
		t.Run(c.name, func(t *testing.T) { c.run(ctx, t, pool, written) })
	}
}

// seedMorphoMarketRow appends one raw row for holder f2 (H) in the named market at block bn, with the
// given timestamp, amounts and build_id. A second row differing only in build_id is a reprocess: the
// trigger gives it the next processing_version.
func seedMorphoMarketRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, market string, bn int, ts string, supply, borrow, collateral string, buildID int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, build_id)
		SELECT u.id, m.id, $1, 0, $2::timestamptz, 0, 0, $3::numeric, $4::numeric, $5::numeric, $6
		FROM "user" u, morpho_market m
		WHERE u.chain_id = 1 AND u.address = '\xf2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2' AND m.market_id = $7`,
		bn, ts, collateral, supply, borrow, buildID, market); err != nil {
		t.Fatalf("seed block %d: %v", bn, err)
	}
}

// A negative amount that a reprocess has already corrected is not an input any more. Refusing on it wedged
// every later run with no append-only way out; storing it would launder abs(-70) into a 70 that never held.
func TestMaterializeMorphoMarketRunsPastACorrectedNegativeAmount(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "0", "-70", "0", 0)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "0", "70", "0", 1)
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_market()`); err != nil {
		t.Fatalf("a negative amount superseded by its correction must not refuse the run: %v", err)
	}
	var got []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(processing_version || ':' || quantity || ':' || deal_type ORDER BY processing_version), '{}')
		FROM position_state WHERE block_number = 900 AND holder_id = 'f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "1:70:BORROW" {
		t.Errorf("stored %v at block 900; want only the correction 1:70:BORROW, never the superseded negative", got)
	}
}

// A negative amount that is still the newest version of its row refuses, correction or not elsewhere.
func TestMaterializeMorphoMarketRefusesANegativeAmountThatIsNewest(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "0", "70", "0", 0)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "0", "-70", "0", 1)
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(new(int64))
	if err == nil || !strings.Contains(err.Error(), "bn=900 bv=0 pv=1 has a negative source amount") {
		t.Fatalf("error %v; want a refusal naming the pv=1 negative row", err)
	}
}

// processing_version counts reprocesses of ONE observation, keyed by its timestamp. A reprocess of a
// second observation of the same block at a later timestamp does not correct a negative first one.
func TestMaterializeMorphoMarketRefusesANegativeAmountCorrectedOnlyAtAnotherTimestamp(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "0", "-70", "0", 0)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T01:00:00Z", "0", "70", "0", 0)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T01:00:00Z", "0", "70", "0", 1)
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(new(int64))
	if err == nil || !strings.Contains(err.Error(), "bn=900 bv=0 pv=0 has a negative source amount") {
		t.Fatalf("error %v; the uncorrected negative observation at 00:00 must refuse", err)
	}
	// The view must agree that it is uncorrected: dropping it there would hand a later run a block whose
	// first observation silently vanished.
	var ts string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(string_agg(DISTINCT block_timestamp::text, ','), '') FROM position_morpho_market
		WHERE block_number = 900 AND holder_id = 'f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2' AND processing_version = 0`).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	if ts != "2026-02-01 00:00:00+00" {
		t.Errorf("the view observes block 900 pv=0 at %q; want the uncorrected 00:00 observation", ts)
	}
}

// A correction is a higher processing_version of the SAME observation. A reprocess of another holder,
// market or block_version at the same block and timestamp corrects nothing here: the check must still
// refuse, and the view must still emit the negative observation rather than drop it.
func TestMaterializeMorphoMarketRefusesANegativeAmountCorrectedOnlyElsewhere(t *testing.T) {
	for _, c := range []struct {
		name, user, market string
		bv                 int
	}{
		{"another holder", `\xf3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3`, `\x1234`, 0},
		{"another market", `\xf2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2`, `\x5678`, 0},
		{"another block_version", `\xf2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2`, `\x1234`, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, pool, _ := materializeMorphoMarketFixture(t)
			seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "0", "-70", "0", 0)
			for _, build := range []int{0, 1} {
				if _, err := pool.Exec(ctx, `
					INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, build_id)
					SELECT u.id, m.id, 900, $1, '2026-02-01T00:00:00Z', 0, 0, 0, 70, 0, $2
					FROM "user" u, morpho_market m
					WHERE u.chain_id = 1 AND u.address = $3::bytea AND m.market_id = $4::bytea`, c.bv, build, c.user, c.market); err != nil {
					t.Fatalf("seed the other observation: %v", err)
				}
			}
			err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(new(int64))
			if err == nil || !strings.Contains(err.Error(), "user f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2 at bn=900 bv=0 pv=0 has a negative source amount") {
				t.Errorf("error %v; the uncorrected negative observation must refuse", err)
			}
			var n int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM position_morpho_market
				WHERE block_number = 900 AND block_version = 0 AND processing_version = 0
				  AND holder_id = 'f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2' AND instrument_key LIKE '1234:%'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Error("the view dropped the uncorrected negative observation")
			}
		})
	}
}

// A windowed run reads only the window's observations, so only a negative amount inside the window can
// reach it. One outside must not refuse that run; an unbounded run still sees and refuses it.
func TestMaterializeMorphoMarketJudgesNegativeAmountsInTheWindow(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "-70", "0", "0", 0)
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_market(0, NULL, interval '1 day')`); err != nil {
		t.Errorf("a negative amount months outside a 1-day window refused the windowed run: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(new(int64)); err == nil || !strings.Contains(err.Error(), "has a negative source amount") {
		t.Errorf("error %v; an unbounded run must still refuse the negative amount", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
		SELECT u.id, m.id, 990, 0, now() - interval '1 hour', 0, 0, 0, -5, 0
		FROM "user" u, morpho_market m
		WHERE u.chain_id = 1 AND u.address = '\xf3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3' AND m.market_id = '\x1234'`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_market(0, NULL, interval '1 day')`).Scan(new(int64)); err == nil || !strings.Contains(err.Error(), "bn=990") {
		t.Errorf("error %v; a negative amount inside the window must refuse the windowed run", err)
	}
}

// Each refusal class keeps its own five, so six negative amounts on a lower market id do not hide a shared
// holder address.
func TestMaterializeMorphoMarketNamesEveryRefusalClass(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	for bn := 901; bn <= 906; bn++ {
		seedMorphoMarketRow(ctx, t, pool, `\x1234`, bn, "2026-02-01T00:00:00Z", "-1", "0", "0", 0)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO chain (chain_id, name) VALUES (8453, 'base') ON CONFLICT (chain_id) DO NOTHING;
		INSERT INTO "user" (chain_id, address) VALUES (8453, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
		INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
		SELECT u.id, m.id, 950, 0, '2026-02-01T00:00:00Z', 0, 0, 0, 10, 0
		FROM "user" u, morpho_market m
		WHERE u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND m.market_id = '\x5678'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(new(int64))
	if err == nil {
		t.Fatal("the run succeeded; want a refusal")
	}
	if !strings.Contains(err.Error(), `market 5678 is held by 2 "user" rows sharing address`) {
		t.Errorf("error %q hides the shared holder address behind the negative amounts", err.Error())
	}
	if n := strings.Count(err.Error(), "has a negative source amount"); n != 5 {
		t.Errorf("error names %d negative amounts; want 5 of the 6", n)
	}
}

// An invalid window is refused before the refusal checks read anything.
func TestMaterializeMorphoMarketRejectsAnInvalidWindowFirst(t *testing.T) {
	ctx, pool, _ := materializeMorphoMarketFixture(t)
	seedMorphoMarketRow(ctx, t, pool, `\x1234`, 900, "2026-02-01T00:00:00Z", "-70", "0", "0", 0)
	for _, w := range []string{"-1 day", "0", "infinity"} {
		err := pool.QueryRow(ctx, `SELECT materialize_morpho_market(0, NULL, $1::interval)`, w).Scan(new(int64))
		if err == nil || !strings.Contains(err.Error(), "materialize_morpho_market: p_window must be a finite positive interval") {
			t.Errorf("window %s: error %v; want the wrapper to reject it before judging any input", w, err)
		}
	}
}
