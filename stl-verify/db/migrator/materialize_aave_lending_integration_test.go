//go:build integration

package migrator_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Native instrument keys the seed produces (token contract addresses, lowercase hex, no 0x).
// Protocol P1 has reserves USDC (underlying dead) and WETH (underlying beef); P2 has its own USDC reserve.
const (
	p1UsdcDebt   = "d1d1" // P1 variable-debt USDC
	p1UsdcAToken = "a1a1" // P1 aUSDC
	p1WethAToken = "a2a2" // P1 aWETH
	p2UsdcAToken = "a3a3" // P2 aUSDC
)

// VEC-404 contract: materialize_aave_lending() projects the borrower (debt) and borrower_collateral
// (supply) ledgers into position_state on the native per-instrument grain. The debt leg is keyed on the
// reserve's variable-debt token and is BORROW; the supply leg is keyed on the reserve's receipt token
// and is COLLATERAL while collateral_enabled, LOAN otherwise. block_timestamp is the ledger's
// created_at, which the tracker sets to the block header time. One behaviour per function, each
// seeding its own database.

// seedAaveLendingBase gives a test its own migrated database with the reference rows every case needs:
// two protocols, two reserves (three mapped receipt tokens), holders and the token mappings. debt_token
// is mapped for P1/USDC only, so a debt row on any other reserve is deliberately unmapped. Everything is
// looked up by (chain_id, address): the migrated database already carries real protocols and tokens.
func seedAaveLendingBase(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
	seed := `
DO $$
DECLARE p1 bigint; p2 bigint; usdc bigint; weth bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum'), (2, 'other') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\x01', 'p1') RETURNING id INTO p1;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\x02', 'p2') RETURNING id INTO p2;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xdead', 'USDC', 6)  RETURNING id INTO usdc;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xbeef', 'WETH', 18) RETURNING id INTO weth;
  INSERT INTO "user" (chain_id, address) VALUES
    (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'), (1, '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
    (1, '\xcccccccccccccccccccccccccccccccccccccccc'), (1, '\xdddddddddddddddddddddddddddddddddddddddd'),
    (1, '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'), (1, '\xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0'),
    (1, '\xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1'), (1, '\xf2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2'),
    (1, '\x1010101010101010101010101010101010101010'), (1, '\x2020202020202020202020202020202020202020'),
    (1, '\x3030303030303030303030303030303030303030'), (1, '\x4040404040404040404040404040404040404040'),
    (1, '\x5050505050505050505050505050505050505050'), (1, '\x6060606060606060606060606060606060606060'),
    (1, '\x7070707070707070707070707070707070707070'),
    (2, '\x9999999999999999999999999999999999999999');
  INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address) VALUES
    (1, p1, usdc, '\xa1a1'), (1, p1, weth, '\xa2a2'), (1, p2, usdc, '\xa3a3');
  INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address) VALUES (p1, usdc, '\xd1d1');
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed base: %v", err)
	}
	return ctx, pool
}

// aaveLedgerPrelude resolves the fixture ids inside a DO block. Every lookup is by (chain_id, address).
const aaveLedgerPrelude = `
  SELECT id INTO STRICT p1 FROM protocol WHERE chain_id = 1 AND address = '\x01';
  SELECT id INTO STRICT p2 FROM protocol WHERE chain_id = 1 AND address = '\x02';
  SELECT id INTO STRICT usdc FROM token WHERE chain_id = 1 AND address = '\xdead';
  SELECT id INTO STRICT weth FROM token WHERE chain_id = 1 AND address = '\xbeef';
  SELECT id INTO STRICT ua FROM "user" WHERE chain_id = 1 AND address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
  SELECT id INTO STRICT ub FROM "user" WHERE chain_id = 1 AND address = '\xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
  SELECT id INTO STRICT uc FROM "user" WHERE chain_id = 1 AND address = '\xcccccccccccccccccccccccccccccccccccccccc';
  SELECT id INTO STRICT ud FROM "user" WHERE chain_id = 1 AND address = '\xdddddddddddddddddddddddddddddddddddddddd';
  SELECT id INTO STRICT ue FROM "user" WHERE chain_id = 1 AND address = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee';
  SELECT id INTO STRICT uf FROM "user" WHERE chain_id = 1 AND address = '\xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0';
  SELECT id INTO STRICT ug FROM "user" WHERE chain_id = 1 AND address = '\xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1';
  SELECT id INTO STRICT uh FROM "user" WHERE chain_id = 1 AND address = '\xf2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2';
  SELECT id INTO STRICT ur FROM "user" WHERE chain_id = 1 AND address = '\x1010101010101010101010101010101010101010';
  SELECT id INTO STRICT uz FROM "user" WHERE chain_id = 1 AND address = '\x2020202020202020202020202020202020202020';
  SELECT id INTO STRICT ux FROM "user" WHERE chain_id = 1 AND address = '\x3030303030303030303030303030303030303030';
  SELECT id INTO STRICT uy FROM "user" WHERE chain_id = 1 AND address = '\x4040404040404040404040404040404040404040';
  SELECT id INTO STRICT us FROM "user" WHERE chain_id = 1 AND address = '\x5050505050505050505050505050505050505050';
  SELECT id INTO STRICT uq FROM "user" WHERE chain_id = 1 AND address = '\x6060606060606060606060606060606060606060';
  SELECT id INTO STRICT un FROM "user" WHERE chain_id = 1 AND address = '\x7070707070707070707070707070707070707070';
  SELECT id INTO STRICT u2 FROM "user" WHERE chain_id = 2 AND address = '\x9999999999999999999999999999999999999999';`

const aaveLedgerDeclare = `
DECLARE p1 bigint; p2 bigint; usdc bigint; weth bigint;
        ua bigint; ub bigint; uc bigint; ud bigint; ue bigint; uf bigint; ug bigint; uh bigint;
        ur bigint; uz bigint; ux bigint; uy bigint; us bigint; uq bigint; un bigint; u2 bigint;`

// Ledger fixture. created_at is the block header time; blocks 100..400 are one day apart.
//
//	A debt USDC@P1:    1000 -> 400 -> 0                BORROW x3, the last is the closing zero-row
//	B supply WETH@P1:  5 on -> 5 off -> 5 on           COLLATERAL, LOAN, COLLATERAL on ONE position
//	C USDC@P1 debt 70, supply 100; WETH@P1 supply 3    three instruments for one holder at one block
//	D debt USDC@P1:    snapshot 0 -> 50                leading zero dropped, one row
//	E supply WETH@P1:  5 -> 0 -> 0                     open + one closing zero-row, repeated zero dropped
//	F debt USDC@P1:    block 200 v0=10, v1=12          two observations, each with its own header time
//	G debt USDC@P1:    one key, two header times       two builds disagreed on block 100's time: earliest wins (5)
//	H supply USDC@P2:  9 off                           LOAN on P2's own aToken, protocol passes through
//	R debt USDC@P1:    5 -> 0 -> 7                     re-open survives: three rows, latest 7
//	Z debt USDC@P1:    block 100 pv0=10, pv1=12        reprocess: two observations (build_id 1 -> pv 1)
//	X USDC@P1:         supply 100@100; debt 0@200      the debt leg never opened: its snapshot emits nothing
//	Y USDC:            supply 100@P1; 0@P2 snapshot    P2 never opened: emits nothing (partition by protocol)
//	S supply WETH@P1:  5 -> 0 (v0) -> 0 (v1, reorg)    the reorged close is kept: three rows
//	Q debt USDC@P1:    1000 -> 0 (pv0) -> 0 (pv1)      the reprocessed close is kept: three rows
//	N debt USDC@P1:    0 (v0) -> 0 (v1) at block 100   never opened: a reorged leading zero emits nothing
const aaveLedger = `
DO $$` + aaveLedgerDeclare + `
BEGIN` + aaveLedgerPrelude + `
  INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, created_at, build_id) VALUES
    (ua, p1, usdc, 100, 0, 1000, 1000, 'Borrow', '\x01', '2026-01-01T00:00:00Z', 0),
    (ua, p1, usdc, 200, 0,  400,  600, 'Repay',  '\x02', '2026-01-02T00:00:00Z', 0),
    (ua, p1, usdc, 300, 0,    0,  400, 'Repay',  '\x03', '2026-01-03T00:00:00Z', 0),
    (uc, p1, usdc, 100, 0,   70,   70, 'Borrow', '\x04', '2026-01-01T00:00:00Z', 0),
    (ud, p1, usdc, 100, 0,    0,    0, 'internal:Snapshot', '\x05', '2026-01-01T00:00:00Z', 0),
    (ud, p1, usdc, 200, 0,   50,   50, 'Borrow', '\x06', '2026-01-02T00:00:00Z', 0),
    (uf, p1, usdc, 200, 0,   10,   10, 'Borrow', '\x07', '2026-01-02T00:00:00Z', 0),
    (uf, p1, usdc, 200, 1,   12,   12, 'Borrow', '\x08', '2026-01-02T00:00:12Z', 0),
    (ug, p1, usdc, 100, 0,    5,    5, 'Borrow', '\x09', '2026-01-01T00:00:00Z', 0),
    (ug, p1, usdc, 100, 0,    6,    6, 'Borrow', '\x09', '2026-01-01T01:00:00Z', 0),
    (ur, p1, usdc, 100, 0,    5,    5, 'Borrow', '\x0a', '2026-01-01T00:00:00Z', 0),
    (ur, p1, usdc, 200, 0,    0,    5, 'Repay',  '\x0b', '2026-01-02T00:00:00Z', 0),
    (ur, p1, usdc, 300, 0,    7,    7, 'Borrow', '\x0c', '2026-01-03T00:00:00Z', 0),
    (uz, p1, usdc, 100, 0,   10,   10, 'Borrow', '\x0d', '2026-01-01T00:00:00Z', 0),
    (uz, p1, usdc, 100, 0,   12,   12, 'Borrow', '\x0d', '2026-01-01T00:00:00Z', 1),
    (ux, p1, usdc, 200, 0,    0,    0, 'internal:Snapshot', '\x0e', '2026-01-02T00:00:00Z', 0),
    (uq, p1, usdc, 100, 0, 1000, 1000, 'Borrow', '\x0f', '2026-01-01T00:00:00Z', 0),
    (uq, p1, usdc, 200, 0,    0, 1000, 'Repay',  '\x10', '2026-01-02T00:00:00Z', 0),
    (uq, p1, usdc, 200, 0,    0, 1000, 'Repay',  '\x10', '2026-01-02T00:00:00Z', 1),
    (un, p1, usdc, 100, 0,    0,    0, 'internal:Snapshot', '\x20', '2026-01-01T00:00:00Z', 0),
    (un, p1, usdc, 100, 1,    0,    0, 'internal:Snapshot', '\x21', '2026-01-01T00:00:12Z', 0);

  INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled, created_at, build_id) VALUES
    (ub, p1, weth, 100, 0, 5, 5, 'Supply', '\x11', true,  '2026-01-01T00:00:00Z', 0),
    (ub, p1, weth, 200, 0, 5, 0, 'ReserveUsedAsCollateralDisabled', '\x12', false, '2026-01-02T00:00:00Z', 0),
    (ub, p1, weth, 300, 0, 5, 0, 'ReserveUsedAsCollateralEnabled',  '\x13', true,  '2026-01-03T00:00:00Z', 0),
    (uc, p1, usdc, 100, 0, 100, 100, 'Supply', '\x14', true, '2026-01-01T00:00:00Z', 0),
    (uc, p1, weth, 100, 0,   3,   3, 'Supply', '\x15', true, '2026-01-01T00:00:00Z', 0),
    (ue, p1, weth, 100, 0, 5, 5, 'Supply',   '\x16', true, '2026-01-01T00:00:00Z', 0),
    (ue, p1, weth, 200, 0, 0, 5, 'Withdraw', '\x17', true, '2026-01-02T00:00:00Z', 0),
    (ue, p1, weth, 300, 0, 0, 0, 'internal:Snapshot', '\x18', true, '2026-01-03T00:00:00Z', 0),
    (uh, p2, usdc, 100, 0, 9, 9, 'Supply', '\x19', false, '2026-01-01T00:00:00Z', 0),
    (ux, p1, usdc, 100, 0, 100, 100, 'Supply', '\x1a', true, '2026-01-01T00:00:00Z', 0),
    (uy, p1, usdc, 100, 0, 100, 100, 'Supply', '\x1b', true, '2026-01-01T00:00:00Z', 0),
    (uy, p2, usdc, 200, 0,   0,   0, 'internal:Snapshot', '\x1c', true, '2026-01-02T00:00:00Z', 0),
    (us, p1, weth, 100, 0, 5, 5, 'Supply',   '\x1d', true, '2026-01-01T00:00:00Z', 0),
    (us, p1, weth, 200, 0, 0, 5, 'Withdraw', '\x1e', true, '2026-01-02T00:00:00Z', 0),
    (us, p1, weth, 200, 1, 0, 5, 'Withdraw', '\x1f', true, '2026-01-02T00:00:12Z', 0);
END $$;`

// Row shape: A3 B3 C3 D1 E2 F2 G1 H1 R3 Z2 X1 Y1 S3 Q3 = 29 rows over 16 positions
// (A, B, C-debt, C-usdc, C-weth, D, E, F, G, H, R, Z, X-supply, Y-P1, S, Q).
const (
	aaveWantRows      = 29
	aaveWantPositions = 16
)

// seedAaveLendingLedger seeds the base and the ledger fixture without running the projection.
func seedAaveLendingLedger(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, pool := seedAaveLendingBase(t)
	if _, err := pool.Exec(ctx, aaveLedger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	return ctx, pool
}

// seedAaveLending seeds the fixture and runs the projection once, returning what it reported written.
func seedAaveLending(t *testing.T) (context.Context, *pgxpool.Pool, int64) {
	t.Helper()
	ctx, pool := seedAaveLendingLedger(t)
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending()`).Scan(&written); err != nil {
		t.Fatalf("materialize_aave_lending: %v", err)
	}
	return ctx, pool, written
}

func TestMaterializeAaveLendingProjectionShape(t *testing.T) {
	ctx, pool, written := seedAaveLending(t)
	if written != aaveWantRows {
		t.Errorf("materialize_aave_lending() reported %d rows, want %d", written, aaveWantRows)
	}
	var rows, positions, badIDs, nullProtocol int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT position_id),
		       count(*) FILTER (WHERE octet_length(position_id) <> 32),
		       count(*) FILTER (WHERE protocol_id IS NULL OR chain_id IS NULL)
		FROM position_state`).Scan(&rows, &positions, &badIDs, &nullProtocol); err != nil {
		t.Fatalf("shape query: %v", err)
	}
	if rows != aaveWantRows || positions != aaveWantPositions || badIDs != 0 || nullProtocol != 0 {
		t.Errorf("position_state rows=%d positions=%d non-32-byte ids=%d null chain/protocol=%d, want %d/%d/0/0",
			rows, positions, badIDs, nullProtocol, aaveWantRows, aaveWantPositions)
	}
}

func TestMaterializeAaveLendingPerPosition(t *testing.T) {
	ctx, pool, _ := seedAaveLending(t)
	for _, c := range []struct {
		name          string
		instrument    string
		holder        string
		wantProtocol  string
		wantRows      int
		wantQty       string   // latest observation's quantity, numeric as text; "" when no rows
		wantDealTypes []string // per observation, in observation order
	}{
		{"A debt: three observations, the last a closing zero-row", p1UsdcDebt, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "p1", 3, "0", []string{"BORROW", "BORROW", "BORROW"}},
		{"B supply: collateral toggled off and on flips the deal type on one position", p1WethAToken, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "p1", 3, "5", []string{"COLLATERAL", "LOAN", "COLLATERAL"}},
		{"C debt leg of a same-reserve supply+borrow", p1UsdcDebt, "cccccccccccccccccccccccccccccccccccccccc", "p1", 1, "70", []string{"BORROW"}},
		{"C supply leg of a same-reserve supply+borrow", p1UsdcAToken, "cccccccccccccccccccccccccccccccccccccccc", "p1", 1, "100", []string{"COLLATERAL"}},
		{"C second reserve at the same block is its own position", p1WethAToken, "cccccccccccccccccccccccccccccccccccccccc", "p1", 1, "3", []string{"COLLATERAL"}},
		{"D leading zero snapshot dropped", p1UsdcDebt, "dddddddddddddddddddddddddddddddddddddddd", "p1", 1, "50", []string{"BORROW"}},
		{"E supply closed: open + one closing zero-row, repeated zero dropped", p1WethAToken, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "p1", 2, "0", []string{"COLLATERAL", "COLLATERAL"}},
		{"F reorged block: both versions are observations", p1UsdcDebt, "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0", "p1", 2, "12", []string{"BORROW", "BORROW"}},
		{"G one key with two header times: the earliest is the stable pick", p1UsdcDebt, "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1", "p1", 1, "5", []string{"BORROW"}},
		{"H second protocol: its own aToken, LOAN while collateral is disabled", p2UsdcAToken, "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2", "p2", 1, "9", []string{"LOAN"}},
		{"R re-open: open, close, re-open all survive", p1UsdcDebt, "1010101010101010101010101010101010101010", "p1", 3, "7", []string{"BORROW", "BORROW", "BORROW"}},
		{"Z reprocessed block: both processing versions are observations", p1UsdcDebt, "2020202020202020202020202020202020202020", "p1", 2, "12", []string{"BORROW", "BORROW"}},
		{"X supply leg present", p1UsdcAToken, "3030303030303030303030303030303030303030", "p1", 1, "100", []string{"COLLATERAL"}},
		{"X debt leg never opened: the supply leg does not leak into its closure", p1UsdcDebt, "3030303030303030303030303030303030303030", "", 0, "", nil},
		{"Y P1 supply present", p1UsdcAToken, "4040404040404040404040404040404040404040", "p1", 1, "100", []string{"COLLATERAL"}},
		{"Y P2 never opened: P1 does not leak into its closure", p2UsdcAToken, "4040404040404040404040404040404040404040", "", 0, "", nil},
		{"S reorged close: the sibling zero of the closing block is kept", p1WethAToken, "5050505050505050505050505050505050505050", "p1", 3, "0", []string{"COLLATERAL", "COLLATERAL", "COLLATERAL"}},
		{"Q reprocessed close: the sibling zero of the closing block is kept", p1UsdcDebt, "6060606060606060606060606060606060606060", "p1", 3, "0", []string{"BORROW", "BORROW", "BORROW"}},
		{"N never opened: a reorged leading zero is not a close", p1UsdcDebt, "7070707070707070707070707070707070707070", "", 0, "", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			var n int
			var latestQty, protocolName string
			if err := pool.QueryRow(ctx, `
				SELECT count(*),
				       coalesce((SELECT quantity::text FROM position_state
				                 WHERE instrument_key = $1 AND holder_id = $2
				                 ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1), ''),
				       coalesce((SELECT p.name FROM position_state ps JOIN protocol p ON p.id = ps.protocol_id
				                 WHERE ps.instrument_key = $1 AND ps.holder_id = $2 LIMIT 1), '')
				FROM position_state WHERE instrument_key = $1 AND holder_id = $2`,
				c.instrument, c.holder).Scan(&n, &latestQty, &protocolName); err != nil {
				t.Fatalf("query: %v", err)
			}
			if n != c.wantRows {
				t.Errorf("rows = %d, want %d", n, c.wantRows)
			}
			if latestQty != c.wantQty {
				t.Errorf("latest quantity = %q, want %q", latestQty, c.wantQty)
			}
			if protocolName != c.wantProtocol {
				t.Errorf("protocol = %q, want %q", protocolName, c.wantProtocol)
			}
			var dealTypes []string
			if err := pool.QueryRow(ctx, `
				SELECT coalesce(array_agg(deal_type ORDER BY block_number, block_version, processing_version), '{}')
				FROM position_state WHERE instrument_key = $1 AND holder_id = $2`,
				c.instrument, c.holder).Scan(&dealTypes); err != nil {
				t.Fatalf("deal_type query: %v", err)
			}
			if !slices.Equal(dealTypes, c.wantDealTypes) && !(len(dealTypes) == 0 && len(c.wantDealTypes) == 0) {
				t.Errorf("deal_types = %v, want %v", dealTypes, c.wantDealTypes)
			}
		})
	}
}

// A reorged block is a different block: each version's observation carries that version's header time.
// A reprocessed block is the same block: both processing versions share one header time.
func TestMaterializeAaveLendingVersionsCarryTheirOwnHeaderTime(t *testing.T) {
	ctx, pool, _ := seedAaveLending(t)
	for _, c := range []struct {
		name              string
		holder            string
		block             int
		wantDistinctTimes int
		wantGapSeconds    float64
	}{
		{"F reorg: two header times 12s apart", "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0", 200, 2, 12},
		{"Z reprocess: one header time", "2020202020202020202020202020202020202020", 100, 1, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			var distinctTimes int
			var gapSeconds float64
			if err := pool.QueryRow(ctx, `
				SELECT count(DISTINCT block_timestamp), extract(epoch FROM max(block_timestamp) - min(block_timestamp))
				FROM position_state WHERE instrument_key = $1 AND holder_id = $2 AND block_number = $3`,
				p1UsdcDebt, c.holder, c.block).Scan(&distinctTimes, &gapSeconds); err != nil {
				t.Fatalf("query: %v", err)
			}
			if distinctTimes != c.wantDistinctTimes || gapSeconds != c.wantGapSeconds {
				t.Errorf("%d distinct header times %.0fs apart, want %d and %.0fs", distinctTimes, gapSeconds, c.wantDistinctTimes, c.wantGapSeconds)
			}
		})
	}
}

// A second run re-derives the same observations and appends nothing, and every stored key still matches
// what the projection emits for it (a re-emitted key with different content would also report 0 appended).
func TestMaterializeAaveLendingIsIdempotent(t *testing.T) {
	ctx, pool, _ := seedAaveLending(t)
	var second int64
	if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending()`).Scan(&second); err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	if second != 0 {
		t.Errorf("the second run reported %d rows appended, want 0", second)
	}
	var rows, drift int
	if err := pool.QueryRow(ctx, `
		WITH s AS (SELECT chain_id, protocol_id, instrument_key, holder_id, quantity,
		                  block_number, block_version, processing_version, block_timestamp FROM position_state),
		     v AS (SELECT v.chain_id, v.protocol_id, v.instrument_key, v.holder_id, v.quantity,
		                  v.block_number, v.block_version, v.processing_version, v.block_timestamp
		             FROM position_aave_lending v
		             JOIN position_state p ON (p.instrument_key, p.holder_id, p.block_number, p.block_version, p.processing_version)
		                                    = (v.instrument_key, v.holder_id, v.block_number, v.block_version, v.processing_version))
		SELECT (SELECT count(*) FROM position_state),
		       (SELECT count(*) FROM ((TABLE v EXCEPT TABLE s) UNION ALL (TABLE s EXCEPT TABLE v)) d)`).Scan(&rows, &drift); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if rows != aaveWantRows || drift != 0 {
		t.Errorf("after re-run: position_state=%d (want %d), rows differing between view and spine=%d (want 0)", rows, aaveWantRows, drift)
	}
}

func TestMaterializeAaveLendingStampsTheBuildID(t *testing.T) {
	ctx, pool := seedAaveLendingLedger(t)
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending(7)`).Scan(&written); err != nil {
		t.Fatalf("materialize_aave_lending(7): %v", err)
	}
	var stamped int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE build_id = 7) FROM position_state`).Scan(&stamped); err != nil {
		t.Fatalf("count: %v", err)
	}
	if written != aaveWantRows || stamped != aaveWantRows {
		t.Errorf("written=%d, rows stamped build_id 7=%d, want %d/%d", written, stamped, aaveWantRows, aaveWantRows)
	}
}

// A reserve the projection cannot key is recorded with its tag and skipped; the rest of the run
// proceeds. Minting a surrogate key instead would change the position_id when the real mapping
// arrives, and the spine has no delete channel to undo it.
func TestMaterializeAaveLendingRecordsUnmappedReservesAndProjectsTheRest(t *testing.T) {
	ctx, pool := seedAaveLendingBase(t)
	// P1/WETH has a receipt token but no debt_token row, so its debt leg cannot be keyed; P2/WETH has
	// neither, so its supply leg cannot either. P1/USDC is fully mapped and must still be projected.
	if _, err := pool.Exec(ctx, `
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 10, 10, 'Borrow', '\x01'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';
		INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
		SELECT u.id, p.id, t.id, 100, 0, 5, 5, 'Supply', '\x02', true
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x02' AND t.chain_id = 1 AND t.address = '\xbeef';
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 70, 70, 'Borrow', '\x03'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xdead'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending(9)`).Scan(&written); err != nil {
		t.Fatalf("the run must proceed past an unmapped reserve: %v", err)
	}
	if written != 1 {
		t.Errorf("projected %d rows, want 1 (the mapped P1/USDC debt row)", written)
	}
	var gaps []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(t.symbol || '=' || g.reason || 'x' || g.observations || '@' || g.build_id
		                          ORDER BY t.symbol, g.reason), '{}')
		FROM aave_unmapped_reserve g JOIN token t ON t.id = g.token_id`).Scan(&gaps); err != nil {
		t.Fatalf("read the gap table: %v", err)
	}
	want := "WETH=no_receipt_tokenx1@9,WETH=no_variable_debt_tokenx1@9"
	if strings.Join(gaps, ",") != want {
		t.Errorf("recorded %v; want %s", gaps, want)
	}
	// The skipped exposure is real and absent from the spine, which is the point of recording it.
	var spine int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&spine); err != nil {
		t.Fatal(err)
	}
	if spine != 1 {
		t.Errorf("position_state holds %d rows, want 1", spine)
	}
}

// Each run appends its own view of the gap, so a closed mapping shows as its disappearance from later
// runs rather than as a mutation of an existing row.
// TWO reserves of one protocol with a NULL variable_debt_address are two gaps, not one token mapped
// twice. SQL groups every NULL into a single group, so a count(*) ambiguity check saw "one token, two
// reserves" and refused the whole run -- taking the fully mapped reserves down with it.
func TestMaterializeAaveLendingTwoUnmappedDebtReservesAreGapsNotAnAmbiguity(t *testing.T) {
	ctx, pool := seedAaveLendingBase(t)
	if _, err := pool.Exec(ctx, `
		-- Two debt_token rows for protocol 1 carrying only a stable-debt address, an Aave V2 stable-only
		-- reserve: both have variable_debt_address NULL.
		INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address, stable_debt_address)
		SELECT p.id, t.id, NULL, '\xd1d9' FROM protocol p, token t
		WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 11, 11, 'Borrow', '\x01'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 200, 0, 13, 2, 'Borrow', '\x02'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';
		-- A second unmapped reserve of the SAME protocol, and it too needs a debt_token row carrying a
		-- NULL variable_debt_address -- that is what makes the two NULLs group together and look repeated.
		INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xfeed', 'DAI', 18);
		INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address, stable_debt_address)
		SELECT p.id, t.id, NULL, '\xd2da' FROM protocol p, token t
		WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xfeed';
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 12, 12, 'Borrow', '\x01'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xfeed';
		-- A fully mapped reserve with real exposure, which the refusal also dropped.
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 70, 70, 'Borrow', '\x03'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xdead'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending(9)`).Scan(&written); err != nil {
		t.Fatalf("two unmapped reserves are two gaps; the run must proceed: %v", err)
	}
	if written != 1 {
		t.Errorf("projected %d rows, want 1: the mapped reserve must land", written)
	}
	var gaps string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(string_agg(t.symbol || '=' || g.reason || 'x' || g.observations,
		                           ',' ORDER BY t.symbol), '')
		FROM aave_unmapped_reserve g JOIN token t ON t.id = g.token_id
		WHERE g.reason = 'no_variable_debt_token'`).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	// DAI carries one ledger row and WETH two, so this also discriminates the observation COUNT from
	// the constant 1 -- every gap reserve in the other fixtures has exactly one.
	if want := "DAI=no_variable_debt_tokenx1,WETH=no_variable_debt_tokenx2"; gaps != want {
		t.Errorf("recorded %q; want %q", gaps, want)
	}
}

func TestMaterializeAaveLendingGapTableIsAppendOnlyPerRun(t *testing.T) {
	ctx, pool := seedAaveLendingBase(t)
	if _, err := pool.Exec(ctx, `
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 10, 10, 'Borrow', '\x01'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for range 2 {
		if _, err := pool.Exec(ctx, `SELECT materialize_aave_lending()`); err != nil {
			t.Fatalf("run: %v", err)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aave_unmapped_reserve`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Errorf("two runs recorded %d gap rows, want 2 (one per run)", rows)
	}
	// Close the mapping; the next run records nothing new and projects the row.
	if _, err := pool.Exec(ctx, `
		INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address)
		SELECT p.id, t.id, '\xd2d2' FROM protocol p, token t
		WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef'`); err != nil {
		t.Fatalf("close the mapping: %v", err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending()`).Scan(&written); err != nil {
		t.Fatalf("run after closing the mapping: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aave_unmapped_reserve`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if written != 1 || rows != 2 {
		t.Errorf("after closing the mapping: projected %d rows (want 1) and the gap table holds %d rows (want the same 2)", written, rows)
	}
}

// Inputs that would key WRONGLY still refuse the run: a colliding key cannot be undone in the spine.
func TestMaterializeAaveLendingRefusesInputsThatWouldKeyWrongly(t *testing.T) {
	const holderA = `\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`
	debt := func(protocolAddr, tokenAddr string) string {
		return `INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		        SELECT u.id, p.id, t.id, 100, 0, 10, 10, 'Borrow', '\x01'
		        FROM "user" u, protocol p, token t
		        WHERE u.chain_id = 1 AND u.address = '` + holderA + `'
		          AND p.chain_id = 1 AND p.address = '` + protocolAddr + `'
		          AND t.chain_id = 1 AND t.address = '` + tokenAddr + `';`
	}
	for _, c := range []struct {
		name, setup, wantInError string
	}{
		{"a supply reserve mapped to two receipt tokens",
			`INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address)
			 SELECT 1, p.id, t.id, '\xa2a3' FROM protocol p, token t WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';
			 INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
			 SELECT u.id, p.id, t.id, 100, 0, 5, 5, 'Supply', '\x01', true
			 FROM "user" u, protocol p, token t
			 WHERE u.chain_id = 1 AND u.address = '` + holderA + `' AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';`,
			"maps to 2 receipt tokens"},
		{"one variable-debt token shared across two reserves",
			`INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address)
			 SELECT p.id, t.id, '\xd1d1' FROM protocol p, token t WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';` +
				debt(`\x01`, `\xdead`) + debt(`\x01`, `\xbeef`),
			"is mapped to 2 reserves"},
		{"an address registered as both a variable-debt and a receipt token",
			`INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address)
			 SELECT p.id, t.id, '\xa1a1' FROM protocol p, token t WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';` +
				debt(`\x01`, `\xbeef`),
			"both a variable-debt and a receipt token"},
		{"a token on another chain than the protocol",
			`INSERT INTO token (chain_id, address, symbol, decimals) VALUES (2, '\xdead', 'USDC', 6);
			 INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
			 SELECT u.id, p.id, t.id, 100, 0, 10, 10, 'Borrow', '\x01'
			 FROM "user" u, protocol p, token t
			 WHERE u.chain_id = 1 AND u.address = '` + holderA + `'
			   AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 2 AND t.address = '\xdead';`,
			"mixes chains"},
		{"a holder whose address is not 20 bytes",
			`INSERT INTO "user" (chain_id, address) VALUES (1, '\xbeefcafe');
			 INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
			 SELECT u.id, p.id, t.id, 100, 0, 10, 10, 'Borrow', '\x01'
			 FROM "user" u, protocol p, token t
			 WHERE u.chain_id = 1 AND u.address = '\xbeefcafe' AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xdead';`,
			"4-byte address"},
		{"a holder on another chain than the protocol",
			`INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
			 SELECT u.id, p.id, t.id, 100, 0, 10, 10, 'Borrow', '\x01'
			 FROM "user" u, protocol p, token t
			 WHERE u.chain_id = 2 AND u.address = '\x9999999999999999999999999999999999999999'
			   AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xdead';`,
			"mixes chains"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, pool := seedAaveLendingBase(t)
			if _, err := pool.Exec(ctx, c.setup); err != nil {
				t.Fatalf("setup: %v", err)
			}
			var written int64
			err := pool.QueryRow(ctx, `SELECT materialize_aave_lending()`).Scan(&written)
			if err == nil {
				t.Fatalf("materialize_aave_lending() succeeded writing %d rows; want a refusal naming %q", written, c.wantInError)
			}
			if !strings.Contains(err.Error(), "would key wrongly") || !strings.Contains(err.Error(), c.wantInError) {
				t.Errorf("error %q does not refuse naming %q", err.Error(), c.wantInError)
			}
			var rows, gaps int
			if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM position_state), (SELECT count(*) FROM aave_unmapped_reserve)`).Scan(&rows, &gaps); err != nil {
				t.Fatal(err)
			}
			if rows != 0 || gaps != 0 {
				t.Errorf("a refused run left %d spine rows and %d gap rows, want 0 and 0", rows, gaps)
			}
		})
	}
}

// The view emits nothing for a reserve it cannot key, rather than a row with a NULL key that the
// spine would reject: the gap is recorded by the wrapper instead.
func TestMaterializeAaveLendingViewSkipsUnkeyableReserves(t *testing.T) {
	ctx, pool := seedAaveLendingBase(t)
	if _, err := pool.Exec(ctx, `
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 10, 10, 'Borrow', '\x02'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var rows, nullKeys int
	if err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE instrument_key IS NULL) FROM position_aave_lending`).Scan(&rows, &nullKeys); err != nil {
		t.Fatalf("view: %v", err)
	}
	if rows != 0 || nullKeys != 0 {
		t.Errorf("view emitted %d rows (%d with a NULL key) for an unmapped reserve, want 0 and 0", rows, nullKeys)
	}
}

// A cross-PROTOCOL address collision must be allowed: protocol_id feeds position_id, so P2's debt
// token sharing an address with P1's receipt token keys two disjoint positions. The collision branch
// is deliberately scoped to one protocol, and nothing pinned that it stays that way.
func TestMaterializeAaveLendingAllowsACrossProtocolAddressCollision(t *testing.T) {
	ctx, pool := seedAaveLendingBase(t)
	if _, err := pool.Exec(ctx, `
		-- P2's variable-debt token for USDC is the SAME address as P1's aUSDC receipt token.
		INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address)
		SELECT p.id, t.id, (SELECT rt.receipt_token_address FROM receipt_token rt
		                    JOIN protocol pp ON pp.id = rt.protocol_id
		                    WHERE pp.address = '\x01' LIMIT 1)
		FROM protocol p, token t
		WHERE p.chain_id = 1 AND p.address = '\x02' AND t.chain_id = 1 AND t.address = '\xdead';
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		SELECT u.id, p.id, t.id, 100, 0, 60, 60, 'Borrow', '\x01'
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x02' AND t.chain_id = 1 AND t.address = '\xdead';
		INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
		SELECT u.id, p.id, t.id, 100, 0, 90, 90, 'Supply', '\x02', true
		FROM "user" u, protocol p, token t
		WHERE u.chain_id = 1 AND u.address = '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
		  AND p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xdead'`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending()`).Scan(&written); err != nil {
		t.Fatalf("a cross-protocol collision keys two disjoint positions and must run: %v", err)
	}
	var ids int
	if err := pool.QueryRow(ctx, `SELECT count(DISTINCT position_id) FROM position_state`).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if written != 2 || ids != 2 {
		t.Errorf("appended %d rows over %d position_ids; want 2 and 2, the ledgers staying disjoint", written, ids)
	}
}

// Every pre-check branch is scoped to reserves a ledger actually keys through. A reference-data
// problem on a protocol nobody holds a position in keys nothing, so it must not abort the run --
// the collision branch shipped without that guard and refused exactly this.
func TestMaterializeAaveLendingIgnoresReferenceDataNoLedgerKeysThrough(t *testing.T) {
	for _, c := range []struct{ name, setup string }{
		{"an address registered as both a debt and a receipt token, with no ledger rows",
			`INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address)
			 SELECT p.id, t.id, (SELECT rt.receipt_token_address FROM receipt_token rt
			                     JOIN protocol pp ON pp.id = rt.protocol_id
			                     WHERE pp.address = '\x01' LIMIT 1)
			 FROM protocol p, token t
			 WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';`},
		{"one variable-debt token shared across two reserves, with no ledger rows",
			`INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address)
			 SELECT p.id, t.id, '\xd1d1' FROM protocol p, token t
			 WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xbeef';
			 INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xfeed', 'DAI', 18);
			 INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address)
			 SELECT p.id, t.id, '\xd1d1' FROM protocol p, token t
			 WHERE p.chain_id = 1 AND p.address = '\x01' AND t.chain_id = 1 AND t.address = '\xfeed';`},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, pool := seedAaveLendingBase(t)
			if _, err := pool.Exec(ctx, c.setup); err != nil {
				t.Fatalf("setup: %v", err)
			}
			var written int64
			if err := pool.QueryRow(ctx, `SELECT materialize_aave_lending()`).Scan(&written); err != nil {
				t.Fatalf("no ledger keys through this reserve, so the run must proceed: %v", err)
			}
			if written != 0 {
				t.Errorf("appended %d rows from a fixture with no ledger rows; want 0", written)
			}
		})
	}
}

// A reserve losing its token mapping is not a gap in new data, it strands exposure already stored:
// the view stops emitting that instrument, the stored rows keep their last quantity, and nothing
// closes them. Reference data regressing is not a data conflict, so the run refuses by name.
func TestMaterializeAaveLendingRefusesWhenAMappingStrandsStoredExposure(t *testing.T) {
	ctx, pool, written := seedAaveLending(t)
	if written == 0 {
		t.Fatal("the base fixture appended nothing, so there is no stored exposure to strand")
	}

	// A debt-leg instrument specifically: variable_debt_address is the nullable mapping, so
	// clearing it is the resync this guard is about. receipt_token_address is NOT NULL and a
	// receipt mapping disappears by deletion instead, which is the same signal to the view.
	var instrument string
	if err := pool.QueryRow(ctx, `
		SELECT DISTINCT ON (position_id) instrument_key FROM position_state
		 WHERE projection = 'public.position_aave_lending' AND quantity > 0 AND deal_type = 'BORROW'
		 ORDER BY position_id, block_number DESC, block_version DESC,
		          processing_version DESC, block_timestamp DESC
		 LIMIT 1`).Scan(&instrument); err != nil {
		t.Fatalf("finding a stored live debt position: %v", err)
	}

	res, err := pool.Exec(ctx, `
		UPDATE debt_token SET variable_debt_address = NULL
		 WHERE encode(variable_debt_address, 'hex') = $1`, instrument)
	if err != nil {
		t.Fatalf("clearing the debt mapping: %v", err)
	}
	if res.RowsAffected() != 1 {
		t.Fatalf("cleared %d debt mappings for %s; want exactly the one it was keyed from", res.RowsAffected(), instrument)
	}

	_, err = pool.Exec(ctx, `SELECT materialize_aave_lending()`)
	if err == nil {
		t.Fatal("the run succeeded while its stored exposure had no instrument left in the view")
	}
	if !strings.Contains(err.Error(), "no longer emits") {
		t.Errorf("refused with %v; want the stranded-exposure refusal", err)
	}
	if !strings.Contains(err.Error(), instrument) {
		t.Errorf("the refusal does not name %s: %v", instrument, err)
	}
}

// The control: a closed position carries no live exposure, so losing its mapping strands nothing
// and must not stop the run. Without this the guard could refuse on every historical mapping change.
func TestMaterializeAaveLendingAClosedPositionLosingItsMappingDoesNotRefuse(t *testing.T) {
	ctx, pool, _ := seedAaveLending(t)

	// Zero every stored position, so nothing is live regardless of which mapping goes.
	if _, err := pool.Exec(ctx, `
		INSERT INTO position_state (position_id, chain_id, protocol_id, instrument_key, holder_id,
		                            quantity, deal_type, block_number, block_version, processing_version,
		                            block_timestamp, projection, build_id)
		SELECT DISTINCT ON (position_id) position_id, chain_id, protocol_id, instrument_key, holder_id,
		       0, deal_type, block_number + 1000, block_version, processing_version,
		       block_timestamp + interval '1 day', projection, build_id
		  FROM position_state WHERE projection = 'public.position_aave_lending'
		 ORDER BY position_id, block_number DESC`); err != nil {
		t.Fatalf("closing the stored positions: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE debt_token SET variable_debt_address = NULL`); err != nil {
		t.Fatalf("clearing every debt mapping: %v", err)
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_aave_lending()`); err != nil {
		t.Fatalf("the run refused although nothing live was stranded: %v", err)
	}
}
