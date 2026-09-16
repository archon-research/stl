//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"strings"
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
// earliest timestamp), closure (VEC-409) -- an exit (positive->0) emits one closing zero-row and a deposit
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
  -- B: one block observed twice at different wall-clock timestamps. Both rows take pv=0 (the source's
  -- dedup key includes timestamp), so the projection's DISTINCT ON collapses them to one row at the
  -- earliest timestamp (assets 10), the stable pick: block_timestamp must be invariant per logical key.
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
		{"B observed twice at one block: one logical key, earliest timestamp is the stable pick", vaultHolderB, "10", 1},
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

// holder_id is the depositor's address alone while chain_id comes from the vault, and
// morpho_vault_position constrains neither against the other: two "user" rows sharing an address on
// different chains render one position_id. At one block that is a double-emit; at different blocks
// their histories silently interleave under closure (verified: 2 rows, 1 position_id, no error).
func TestMaterializeMorphoVaultRefusesOneAddressOnSeveralChains(t *testing.T) {
	seedTwoChainDepositor := func(t *testing.T, blockB int, tsB string) (context.Context, *pgxpool.Pool) {
		t.Helper()
		ctx := context.Background()
		pool, cleanup := setupPostgres(ctx, t)
		t.Cleanup(cleanup)
		if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
			t.Fatalf("migrations: %v", err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(`
DO $$
DECLARE pid bigint; atid bigint; u1 bigint; u2 bigint; vid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT DO NOTHING;
  INSERT INTO chain (chain_id, name) VALUES (8453, 'base') ON CONFLICT DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO pid;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO atid;
  INSERT INTO "user" (chain_id, address) VALUES (1,    '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') RETURNING id INTO u1;
  INSERT INTO "user" (chain_id, address) VALUES (8453, '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') RETURNING id INTO u2;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, pid, '\xabcd', 'steakUSDC', atid, 1, 1) RETURNING id INTO vid;
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (u1, vid, 100, 0, '2026-01-01T00:00:00Z', 0, 10),
           (u2, vid, %d, 0, '%s'::timestamptz, 0, 20);
END $$;`, blockB, tsB)); err != nil {
			t.Fatalf("seed: %v", err)
		}
		return ctx, pool
	}
	// Both orderings refuse: the same block, which the shared materializer would catch as a
	// double-emit, and different blocks, which it would not catch at all.
	for _, c := range []struct {
		name, ts string
		block    int
	}{
		{"at one block", "2026-01-01T00:00:00Z", 100},
		{"at different blocks", "2026-01-02T00:00:00Z", 200},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, pool := seedTwoChainDepositor(t, c.block, c.ts)
			var written int64
			err := pool.QueryRow(ctx, `SELECT materialize_morpho_vault()`).Scan(&written)
			if err == nil {
				t.Fatalf("the run succeeded writing %d rows; want a refusal", written)
			}
			if !strings.Contains(err.Error(), "several chains would collapse into one position") {
				t.Errorf("error %q does not name the collision", err.Error())
			}
			var rows int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 0 {
				t.Errorf("a refused run stored %d rows; want none", rows)
			}
		})
	}
}

// Negative control: one address on ONE chain is the normal case and must still project, or the guard
// above would refuse every real vault.
func TestMaterializeMorphoVaultProjectsOneAddressOnOneChain(t *testing.T) {
	ctx, pool, written := seedMorphoVault(t)
	if written == 0 {
		t.Fatal("the fixture projected nothing")
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows == 0 {
		t.Error("no rows stored; the cross-chain guard must not refuse single-chain depositors")
	}
}

// The wrapper is the only path the runner calls, so it has to forward the writer run to the spine or
// every row this projection appends is provenance-free (ADR-0006 §2). The run record is the witness:
// its run_id can only have arrived through the wrapper's own parameter.
func TestMaterializeMorphoVaultForwardsTheWriterRun(t *testing.T) {
	ctx, pool, _ := seedMorphoVault(t)
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_vault(7, 9182)`); err != nil {
		t.Fatalf("materialize_morpho_vault with a run: %v", err)
	}
	var runID *int64
	var buildID int
	if err := pool.QueryRow(ctx, `
		SELECT run_id, build_id FROM position_projection_run
		 WHERE projection = 'public.position_morpho_vault'
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
		SELECT proargnames::text[] FROM pg_proc WHERE proname = 'materialize_morpho_vault'`).Scan(&args); err != nil {
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
			t.Errorf("materialize_morpho_vault declares %v, missing %s -- the runner calls it by name", args, want)
		}
	}
}

// freshMorphoVaultDB gives a test its own migrated database with no fixture, for the cases that need
// their own chains, vaults or malformed addresses.
func freshMorphoVaultDB(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	t.Cleanup(cleanup)
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	return ctx, pool
}

// deal_type is the projection's whole contribution to the column: the spine copies it through, so
// nothing else would catch a wrong literal.
func TestMaterializeMorphoVaultIsAlwaysLoan(t *testing.T) {
	ctx, pool, _ := seedMorphoVault(t)
	var deals []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(DISTINCT deal_type), '{}') FROM position_state
		WHERE projection = 'public.position_morpho_vault'`).Scan(&deals); err != nil {
		t.Fatal(err)
	}
	if len(deals) != 1 || deals[0] != "LOAN" {
		t.Errorf("deal types = %v; want only LOAN — a vault deposit is a loan to the vault", deals)
	}
}

// block_timestamp is part of the stored key and is otherwise unasserted. B's two readings of block 100
// collapse to the EARLIEST, so this also pins the DISTINCT ON tie-break.
func TestMaterializeMorphoVaultObservesAtTheEarliestTimestamp(t *testing.T) {
	ctx, pool, _ := seedMorphoVault(t)
	for _, c := range []struct{ holder, block, wantTS string }{
		{vaultHolderA, "100", "2026-01-01 00:00:00+00"},
		{vaultHolderA, "200", "2026-01-02 00:00:00+00"},
		{vaultHolderB, "100", "2026-01-01 00:00:00+00"},
	} {
		var got string
		if err := pool.QueryRow(ctx, `
			SELECT block_timestamp::text FROM position_state
			WHERE projection = 'public.position_morpho_vault' AND holder_id = $1 AND block_number = $2::bigint`,
			c.holder, c.block).Scan(&got); err != nil {
			t.Fatalf("holder %s block %s: %v", c.holder[:4], c.block, err)
		}
		if got != c.wantTS {
			t.Errorf("holder %s block %s observed at %s; want %s", c.holder[:4], c.block, got, c.wantTS)
		}
	}
}

// chain_id and protocol_id come from the VAULT, never the depositor. Both feed the position_id hash, so
// taking them from the user would fork every identity, permanently, in an append-only table.
func TestMaterializeMorphoVaultTakesChainAndProtocolFromTheVault(t *testing.T) {
	ctx, pool := freshMorphoVaultDB(t)
	if _, err := pool.Exec(ctx, `
DO $$
DECLARE pid bigint; atid bigint; uid bigint; vid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum'), (8453, 'base') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO pid;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO atid;
  -- The depositor is registered on a DIFFERENT chain from the vault it deposits into.
  INSERT INTO "user" (chain_id, address) VALUES (8453, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO uid;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, pid, '\xabcd', 'steakUSDC', atid, 1, 1) RETURNING id INTO vid;
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (uid, vid, 100, 0, '2026-01-01T00:00:00Z', 90, 100);
END $$;`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_vault()`); err != nil {
		t.Fatalf("a depositor registered on another chain must still project: %v", err)
	}
	var chainID int
	var protocolMatches bool
	if err := pool.QueryRow(ctx, `
		SELECT s.chain_id,
		       s.protocol_id = (SELECT v.protocol_id FROM morpho_vault v WHERE v.address = '\xabcd'::bytea)
		FROM position_state s
		WHERE s.projection = 'public.position_morpho_vault'`).Scan(&chainID, &protocolMatches); err != nil {
		t.Fatal(err)
	}
	if chainID != 1 || !protocolMatches {
		t.Errorf("chain_id=%d protocol matches vault=%v; want chain 1 and the vault's protocol, not the depositor's 8453",
			chainID, protocolMatches)
	}
}

// A reorg correction and a reprocess correction are their own observations: neither block_version nor
// processing_version may be dropped from the dedup key, or the correction is silently discarded.
func TestMaterializeMorphoVaultKeepsCorrections(t *testing.T) {
	for _, c := range []struct {
		name, col string
	}{
		{"a reorged block keeps both block_versions", "block_version"},
		{"a reprocess keeps both processing_versions", "processing_version"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, pool := freshMorphoVaultDB(t)
			if _, err := pool.Exec(ctx, `
DO $$
DECLARE pid bigint; atid bigint; uid bigint; vid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO pid;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO atid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO uid;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, pid, '\xabcd', 'steakUSDC', atid, 1, 1) RETURNING id INTO vid;
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (uid, vid, 100, 0, '2026-01-01T00:00:00Z', 90, 100);
END $$;`); err != nil {
				t.Fatalf("seed: %v", err)
			}
			// The correction differs from the row above in exactly one version column.
			ins := `INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
			        SELECT p.user_id, p.morpho_vault_id, 100, ` + map[string]string{"block_version": "1", "processing_version": "0"}[c.col] +
				`, '2026-01-01T00:00:00Z', 180, 200 FROM morpho_vault_position p LIMIT 1`
			if c.col == "processing_version" {
				// processing_version is trigger-assigned from build_id, so drive it through build_id.
				ins = `INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets, build_id)
				       SELECT p.user_id, p.morpho_vault_id, 100, 0, '2026-01-01T00:00:00Z', 180, 200, 1 FROM morpho_vault_position p LIMIT 1`
			}
			if _, err := pool.Exec(ctx, ins); err != nil {
				t.Fatalf("seed the correction: %v", err)
			}
			var srcRows int
			if err := pool.QueryRow(ctx, `SELECT count(DISTINCT `+c.col+`) FROM morpho_vault_position`).Scan(&srcRows); err != nil {
				t.Fatal(err)
			}
			if srcRows != 2 {
				t.Fatalf("the source holds %d distinct %s; the fixture must produce 2 or this tests nothing", srcRows, c.col)
			}
			if _, err := pool.Exec(ctx, `SELECT materialize_morpho_vault()`); err != nil {
				t.Fatalf("materialize: %v", err)
			}
			var got []string
			if err := pool.QueryRow(ctx, `
				SELECT coalesce(array_agg(quantity::text ORDER BY quantity), '{}') FROM position_state
				WHERE projection = 'public.position_morpho_vault'`).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, ",") != "100,200" {
				t.Errorf("stored %v; want both observations 100 and 200 — dropping %s from the dedup key discards the correction", got, c.col)
			}
		})
	}
}

// Two vaults sharing an address on different chains are two instruments, not a collision: the refusal
// must key on the vault, or a legitimate multi-chain deployment is refused.
func TestMaterializeMorphoVaultAllowsOneVaultAddressOnTwoChains(t *testing.T) {
	ctx, pool := freshMorphoVaultDB(t)
	if _, err := pool.Exec(ctx, `
DO $$
DECLARE p1 bigint; p2 bigint; t1 bigint; t2 bigint; u1 bigint; u2 bigint; v1 bigint; v2 bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum'), (8453, 'base') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO p1;
  INSERT INTO protocol (chain_id, address, name) VALUES (8453, '\xfe', 'morpho-base') RETURNING id INTO p2;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO t1;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (8453, '\xda', 'USDC', 6) RETURNING id INTO t2;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO u1;
  -- The SAME holder address on both chains: only grouping the refusal by vault keeps these apart.
  INSERT INTO "user" (chain_id, address) VALUES (8453, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO u2;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, p1, '\xabcd', 'steakUSDC', t1, 1, 1) RETURNING id INTO v1;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (8453, p2, '\xabcd', 'steakUSDC', t2, 1, 1) RETURNING id INTO v2;
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (u1, v1, 100, 0, '2026-01-01T00:00:00Z', 90, 100),
           (u2, v2, 100, 0, '2026-01-01T00:00:00Z', 90, 300);
END $$;`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_morpho_vault()`).Scan(&written); err != nil {
		t.Fatalf("one vault address deployed on two chains must project: %v", err)
	}
	var positions int
	var chains []string
	if err := pool.QueryRow(ctx, `
		SELECT count(DISTINCT position_id), coalesce(array_agg(DISTINCT chain_id::text ORDER BY chain_id::text), '{}')
		FROM position_state WHERE projection = 'public.position_morpho_vault'`).Scan(&positions, &chains); err != nil {
		t.Fatal(err)
	}
	if written != 2 || positions != 2 || strings.Join(chains, ",") != "1,8453" {
		t.Errorf("written=%d positions=%d chains=%v; want 2 separate positions on chains 1 and 8453", written, positions, chains)
	}
}

// An address that is not 20 bytes cannot render the 40-hex identity position_state requires. Without a
// named refusal it aborts inside position_key() or on the column CHECK, naming no vault and no holder.
func TestMaterializeMorphoVaultRefusesAMalformedAddress(t *testing.T) {
	ctx, pool := freshMorphoVaultDB(t)
	if _, err := pool.Exec(ctx, `
DO $$
DECLARE pid bigint; atid bigint; uid bigint; vid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO pid;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO atid;
  -- 19 bytes, one short of an address.
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO uid;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, pid, '\xabcd', 'steakUSDC', atid, 1, 1) RETURNING id INTO vid;
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (uid, vid, 100, 0, '2026-01-01T00:00:00Z', 90, 100);
END $$;`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var written int64
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_vault()`).Scan(&written)
	if err == nil {
		t.Fatalf("the run succeeded writing %d rows; want a refusal naming the malformed address", written)
	}
	if !strings.Contains(err.Error(), "refusing to run") || !strings.Contains(err.Error(), "cannot render the 40-hex holder_id") {
		t.Errorf("error %q does not name the malformed address; it aborted somewhere that cannot identify the row", err.Error())
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Errorf("a refused run wrote %d rows, want 0", rows)
	}
}

// The refusal must name the vault whose deposits collide, not just that a collision happened.
func TestMaterializeMorphoVaultRefusalNamesTheVault(t *testing.T) {
	ctx, pool := freshMorphoVaultDB(t)
	if _, err := pool.Exec(ctx, `
DO $$
DECLARE pid bigint; atid bigint; u1 bigint; u2 bigint; vid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum'), (8453, 'base') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO protocol (chain_id, address, name) VALUES (1, '\xfe', 'morpho') RETURNING id INTO pid;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xda', 'USDC', 6) RETURNING id INTO atid;
  INSERT INTO "user" (chain_id, address) VALUES (1, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO u1;
  INSERT INTO "user" (chain_id, address) VALUES (8453, '\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') RETURNING id INTO u2;
  INSERT INTO morpho_vault (chain_id, protocol_id, address, symbol, asset_token_id, vault_version, created_at_block)
    VALUES (1, pid, '\xabcd', 'steakUSDC', atid, 1, 1) RETURNING id INTO vid;
  INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
    VALUES (u1, vid, 100, 0, '2026-01-01T00:00:00Z', 90, 100),
           (u2, vid, 200, 0, '2026-01-02T00:00:00Z', 90, 300);
END $$;`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := pool.QueryRow(ctx, `SELECT materialize_morpho_vault()`).Scan(new(int64))
	if err == nil {
		t.Fatal("expected the cross-chain collision to refuse the run")
	}
	if !strings.Contains(err.Error(), "vault "+vaultInstrument+" ") {
		t.Errorf("error %q does not name vault %s, so an operator cannot tell which contract to inspect", err.Error(), vaultInstrument)
	}
}

// The wrapper is the only path the runner calls, so a window it cannot forward is a window this
// projection can never run with. The run record stamps what the spine actually received.
func TestMorphoVaultForwardsTheWindow(t *testing.T) {
	ctx, pool, _ := seedMorphoVault(t)

	if _, err := pool.Exec(ctx, `SELECT materialize_morpho_vault(0, NULL, interval '36 hours')`); err != nil {
		t.Fatalf("calling with a window: %v", err)
	}

	var window *string
	if err := pool.QueryRow(ctx, `
		SELECT window_interval::text FROM position_projection_run
		 WHERE projection = 'public.position_morpho_vault'
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
