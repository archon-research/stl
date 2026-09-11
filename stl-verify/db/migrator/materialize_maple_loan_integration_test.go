//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-405: position_maple_loan places each maple_loan_state cron cycle at the surviving block_meta
// block at or before its synced_at, collapses cycles sharing a block, and closes a loan from its
// absence. Every mutation named in a subtest's comment was run against that subtest; the mutation
// table lives in the PR, because a subtest can only claim what its own assertions pin.

const mapleTolerance = "10 minutes"

type mapleFixture struct {
	pool  *pgxpool.Pool
	ctx   context.Context
	loans map[string]int64
}

// seedMaple builds the registry: two chains, and the named loans on the chain given for each. Loan
// addresses are derived from the name, so every lookup below is by address and never by chain alone.
func seedMaple(ctx context.Context, t *testing.T, pool *pgxpool.Pool, loans map[string]int) *mapleFixture {
	t.Helper()
	f := &mapleFixture{pool: pool, ctx: ctx, loans: map[string]int64{}}
	f.exec(t, `INSERT INTO protocol (chain_id, address, name, protocol_type)
	           SELECT c, decode(md5('maple-protocol' || c) || 'a1b2c3d4', 'hex'), 'Maple', 'lending'
	           FROM (VALUES (1), (8453)) v(c) ON CONFLICT DO NOTHING`)
	f.exec(t, `INSERT INTO token (chain_id, address, symbol, decimals)
	           SELECT c, decode(md5('maple-asset' || c) || 'a1b2c3d4', 'hex'), 'MPLUSDC', 6
	           FROM (VALUES (1), (8453)) v(c) ON CONFLICT DO NOTHING`)
	f.exec(t, `INSERT INTO maple_pool (chain_id, protocol_id, address, asset_token_id)
	           SELECT p.chain_id, p.id, decode(md5('maple-pool' || p.chain_id) || 'a1b2c3d4', 'hex'), tk.id
	           FROM protocol p JOIN token tk ON tk.chain_id = p.chain_id AND tk.symbol = 'MPLUSDC'
	           WHERE p.name = 'Maple' ON CONFLICT DO NOTHING`)
	for name, chain := range loans {
		f.exec(t, `INSERT INTO "user" (chain_id, address) VALUES ($1, decode(md5($2) || 'a1b2c3d4', 'hex'))
		           ON CONFLICT DO NOTHING`, chain, "borrower-"+name)
		f.exec(t, `INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
		           SELECT $1, p.id, decode(md5($2) || 'a1b2c3d4', 'hex'), mp.id, u.id
		           FROM protocol p
		           JOIN maple_pool mp ON mp.chain_id = p.chain_id
		           JOIN "user" u ON u.chain_id = $1 AND u.address = decode(md5($3) || 'a1b2c3d4', 'hex')
		           WHERE p.chain_id = $1 AND p.name = 'Maple' ON CONFLICT DO NOTHING`,
			chain, "loan-"+name, "borrower-"+name)
		var id int64
		if err := pool.QueryRow(ctx,
			`SELECT id FROM maple_loan WHERE loan_address = decode(md5($1) || 'a1b2c3d4', 'hex')`, "loan-"+name).Scan(&id); err != nil {
			t.Fatalf("seeding loan %s: %v", name, err)
		}
		f.loans[name] = id
	}
	return f
}

func (f *mapleFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", strings.Join(strings.Fields(sql), " "), err)
	}
}

// blocks seeds a dense series: count blocks from startBN at stepSec apart. Density is what keeps a
// placement honest, so the fixtures below stay inside the wrapper's skew tolerance on purpose.
func (f *mapleFixture) blocks(t *testing.T, chain, startBN int, startTS string, count, stepSec int) {
	t.Helper()
	f.exec(t, `INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
	           SELECT $1, $2 + g, 0, $3::timestamptz + (g * $4 || ' seconds')::interval
	           FROM generate_series(0, $5 - 1) g ON CONFLICT DO NOTHING`,
		chain, startBN, startTS, stepSec, count)
}

func (f *mapleFixture) block(t *testing.T, chain, bn, bv int, ts string) {
	t.Helper()
	f.exec(t, `INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
	           VALUES ($1, $2, $3, $4::timestamptz) ON CONFLICT DO NOTHING`, chain, bn, bv, ts)
}

func (f *mapleFixture) cycle(t *testing.T, loan, ts, principal string, build int) {
	t.Helper()
	f.exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, build_id)
	           VALUES ($1, $2::timestamptz, 'Active', $3::numeric, $4)`, f.loans[loan], ts, principal, build)
}

// cycleState seeds a non-Active row, which the indexer never writes but stl_readwrite can.
func (f *mapleFixture) cycleState(t *testing.T, loan, ts, state, principal string) {
	t.Helper()
	f.exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, build_id)
	           VALUES ($1, $2::timestamptz, $3, $4::numeric, 0)`, f.loans[loan], ts, state, principal)
}

func (f *mapleFixture) runWith(t *testing.T, tolerance string) (int64, error) {
	t.Helper()
	var n int64
	err := f.pool.QueryRow(f.ctx, `SELECT materialize_maple_loan(0, $1::interval)`, tolerance).Scan(&n)
	return n, err
}

func (f *mapleFixture) run(t *testing.T) (int64, error) {
	t.Helper()
	var n int64
	err := f.pool.QueryRow(f.ctx, `SELECT materialize_maple_loan(0, $1::interval)`, mapleTolerance).Scan(&n)
	return n, err
}

func (f *mapleFixture) mustRun(t *testing.T) int64 {
	t.Helper()
	n, err := f.run(t)
	if err != nil {
		t.Fatalf("materialize_maple_loan: %v", err)
	}
	return n
}

type mapleRow struct {
	chain      int
	protocolID int64
	instrument string
	holder     string
	dealType   string
	qty        string
	ts         time.Time
	bn         int64
	bv, pv     int
}

// rows reads the spine for one named loan, so every assertion is over real materialized output.
func (f *mapleFixture) rows(t *testing.T, loan string) []mapleRow {
	t.Helper()
	rs, err := f.pool.Query(f.ctx,
		`SELECT ps.chain_id, ps.protocol_id, ps.instrument_key, ps.holder_id, ps.deal_type,
		        ps.quantity::text, ps.block_timestamp, ps.block_number, ps.block_version, ps.processing_version
		 FROM position_state ps
		 JOIN maple_loan l ON ps.instrument_key = encode(l.loan_address, 'hex')
		 WHERE l.id = $1
		 ORDER BY ps.block_number, ps.block_version, ps.processing_version`, f.loans[loan])
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []mapleRow
	for rs.Next() {
		var r mapleRow
		if err := rs.Scan(&r.chain, &r.protocolID, &r.instrument, &r.holder, &r.dealType,
			&r.qty, &r.ts, &r.bn, &r.bv, &r.pv); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// viewQty reads the VIEW rather than the spine, which is the only way to see the two diverge.
func (f *mapleFixture) viewQty(t *testing.T, loan string, bn int64) []string {
	t.Helper()
	rs, err := f.pool.Query(f.ctx,
		`SELECT v.quantity::text FROM position_maple_loan v
		 JOIN maple_loan l ON v.instrument_key = encode(l.loan_address, 'hex')
		 WHERE l.id = $1 AND v.block_number = $2 ORDER BY v.processing_version`, f.loans[loan], bn)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var q string
		if err := rs.Scan(&q); err != nil {
			t.Fatal(err)
		}
		out = append(out, q)
	}
	return out
}

func TestMapleLoanPlacement(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"a": 1, "b": 8453, "c": 1})

	// Blocks every 2 minutes from 08:00, cycles every 10, so nothing is ever stale beyond tolerance.
	f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
	f.blocks(t, 8453, 500, "2026-06-16T08:01:00Z", 60, 120)

	t.Run("a cycle is placed at the last block at or before it and takes that block's timestamp", func(t *testing.T) {
		// 08:35 falls between block 1017 (08:34) and 1018 (08:36). Kills nearest-block, next-block
		// and synced_at-as-block_timestamp: all three place it elsewhere or stamp 08:35.
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		if n := f.mustRun(t); n != 1 {
			t.Fatalf("appended %d rows, want 1", n)
		}
		rs := f.rows(t, "a")
		if len(rs) != 1 {
			t.Fatalf("want one observation, got %d", len(rs))
		}
		if rs[0].bn != 1017 || !rs[0].ts.Equal(time.Date(2026, 6, 16, 8, 34, 0, 0, time.UTC)) {
			t.Errorf("placed at block %d stamped %s; want block 1017 stamped its own 08:34", rs[0].bn, rs[0].ts)
		}
		if rs[0].qty != "500" || rs[0].dealType != "BORROW" || rs[0].chain != 1 {
			t.Errorf("qty=%s deal_type=%s chain=%d; want 500 / BORROW / 1", rs[0].qty, rs[0].dealType, rs[0].chain)
		}
	})

	t.Run("identity carries the loan's protocol, its chain-qualified address and the borrower", func(t *testing.T) {
		// protocol_id and instrument_key both feed position_id, so a wrong value silently re-keys the
		// position. Nothing else in the suite reads protocol_id, and a min(protocol.id) mutation
		// survives without this.
		var wantProto int64
		var wantAddr, wantHolder string
		if err := pool.QueryRow(ctx,
			`SELECT l.protocol_id, encode(l.loan_address,'hex'), encode(u.address,'hex')
			 FROM maple_loan l JOIN "user" u ON u.id = l.borrower_user_id WHERE l.id = $1`,
			f.loans["a"]).Scan(&wantProto, &wantAddr, &wantHolder); err != nil {
			t.Fatal(err)
		}
		rs := f.rows(t, "a")
		if rs[0].protocolID != wantProto {
			t.Errorf("protocol_id=%d; want the loan's own %d", rs[0].protocolID, wantProto)
		}
		if rs[0].instrument != wantAddr {
			t.Errorf("instrument_key=%s; want the bare loan address %s", rs[0].instrument, wantAddr)
		}
		if rs[0].holder != wantHolder {
			t.Errorf("holder_id=%s; want the borrower's address %s", rs[0].holder, wantHolder)
		}
	})

	t.Run("a cycle at a block's exact header instant belongs to that block", func(t *testing.T) {
		// The `<=` boundary. A `<` mutation places this at 1019 instead.
		f.cycle(t, "a", "2026-06-16T08:40:00Z", "600", 1)
		f.mustRun(t)
		rs := f.rows(t, "a")
		last := rs[len(rs)-1]
		if last.bn != 1020 || last.qty != "600" {
			t.Errorf("a cycle exactly at 08:40 placed at block %d qty %s; want block 1020 qty 600", last.bn, last.qty)
		}
	})

	t.Run("cycles sharing a block collapse to one observation, the earliest synced_at winning", func(t *testing.T) {
		// Three cycles inside block 1030's two-minute window, seeded out of order and all present
		// before loan c's FIRST run, so the pick itself is under test rather than the spine's dedupe.
		// Asserts the winner IS min(synced_at) read from the source, so the oracle is independent of
		// plan order: dropping the ORDER BY pick key entirely then fails, not just reversing it.
		f.cycle(t, "c", "2026-06-16T09:01:30Z", "700", 1)
		f.cycle(t, "c", "2026-06-16T09:00:10Z", "710", 1)
		f.cycle(t, "c", "2026-06-16T09:00:50Z", "720", 1)
		f.mustRun(t)
		rs := f.rows(t, "c")
		if len(rs) != 1 {
			t.Fatalf("three cycles in one block must be one observation, got %d: %+v", len(rs), rs)
		}
		var wantQty string
		if err := pool.QueryRow(ctx,
			`SELECT principal_owed::text FROM maple_loan_state
			 WHERE maple_loan_id = $1 ORDER BY synced_at LIMIT 1`, f.loans["c"]).Scan(&wantQty); err != nil {
			t.Fatal(err)
		}
		if rs[0].qty != wantQty {
			t.Errorf("quantity=%s; want %s, the principal of min(synced_at) in that block", rs[0].qty, wantQty)
		}
	})

	t.Run("blocks sharing a header timestamp resolve to the highest height", func(t *testing.T) {
		// Two blocks at one instant: the tie-break. Dropping `block_number DESC` from the LATERAL's
		// ORDER BY survives every other subtest, because no other fixture has an equal-timestamp pair.
		f.block(t, 1, 5000, 0, "2026-06-16T12:00:00Z")
		f.block(t, 1, 5001, 0, "2026-06-16T12:00:00Z")
		f.cycle(t, "a", "2026-06-16T12:00:30Z", "800", 1)
		f.mustRun(t)
		rs := f.rows(t, "a")
		last := rs[len(rs)-1]
		if last.bn != 5001 {
			t.Errorf("placed at block %d; want 5001, the highest height sharing that header instant", last.bn)
		}
	})

	t.Run("a reorg keeps only the surviving version, so height and header time cannot invert", func(t *testing.T) {
		// The wedge case. A depth-2 reorg from height 6000: orphans 6000/v0 and 6001/v0 carry EARLIER
		// header times than the replacements. Resolving over a mixed timeline places a later cycle at
		// a LOWER height with a HIGHER timestamp, which trips the spine's monotonic gate forever.
		// Dense enough to stay inside the skew tolerance, so this subtest tests the timeline and not
		// the coverage guard: 5950 is the canonical block the first cycle must land on.
		f.block(t, 1, 5950, 0, "2026-06-16T13:00:00Z")
		f.block(t, 1, 6000, 0, "2026-06-16T13:00:10Z")
		f.block(t, 1, 6001, 0, "2026-06-16T13:00:20Z")
		f.block(t, 1, 6000, 1, "2026-06-16T13:00:30Z")
		f.block(t, 1, 6001, 1, "2026-06-16T13:00:40Z")
		// 13:00:25 sits after orphan 6001/v0 (13:00:20) but before replacement 6000/v1 (13:00:30), so
		// on a mixed timeline it picks height 6001 and the next cycle picks the LOWER height 6000 at a
		// LATER time. That is the inversion; both cycles after 13:00:30 would never expose it.
		f.cycle(t, "a", "2026-06-16T13:00:25Z", "900", 1)
		f.cycle(t, "a", "2026-06-16T13:00:35Z", "910", 1)
		if _, err := f.run(t); err != nil {
			t.Fatalf("a depth-2 reorg must not wedge the run: %v", err)
		}
		rs := f.rows(t, "a")
		var prevBN int64
		var prevTS time.Time
		for _, r := range rs {
			if r.bn < prevBN && r.ts.After(prevTS) {
				t.Errorf("block %d at %s follows block %d at %s: height and header time invert, which wedges the spine's gate",
					r.bn, r.ts, prevBN, prevTS)
			}
			prevBN, prevTS = r.bn, r.ts
		}
		last := rs[len(rs)-1]
		if last.bn != 6000 || last.bv != 1 {
			t.Errorf("last placement %d/%d; want 6000/1, the surviving block at or before 13:00:35", last.bn, last.bv)
		}
	})

	t.Run("the block lookup is scoped to the loan's own chain", func(t *testing.T) {
		// Chain 1's series is offset a minute earlier, so at 08:36 chain 1 has a block AT 08:36 while
		// chain 8453's latest is 08:35. An unscoped lookup therefore picks chain 1's, and this is the
		// only fixture where the two chains' candidates differ -- an equal one would pass either way.
		f.cycle(t, "b", "2026-06-16T08:36:00Z", "42", 1)
		f.mustRun(t)
		rs := f.rows(t, "b")
		if len(rs) != 1 {
			t.Fatalf("want one observation for loan b, got %d", len(rs))
		}
		if rs[0].chain != 8453 || rs[0].bn != 517 || !rs[0].ts.Equal(time.Date(2026, 6, 16, 8, 35, 0, 0, time.UTC)) {
			t.Errorf("loan b placed at chain %d block %d (%s); want chain 8453 block 517 at 08:35",
				rs[0].chain, rs[0].bn, rs[0].ts.UTC())
		}
	})

	t.Run("two loans on one chain in one run do not cross-contaminate", func(t *testing.T) {
		aRows, cRows := f.rows(t, "a"), f.rows(t, "c")
		if len(aRows) == 0 || len(cRows) == 0 {
			t.Fatalf("both loans must have observations, got %d and %d", len(aRows), len(cRows))
		}
		if aRows[0].instrument == cRows[0].instrument || aRows[0].holder == cRows[0].holder {
			t.Errorf("loans a and c share an instrument_key or holder: %s / %s", aRows[0].instrument, cRows[0].instrument)
		}
	})

	t.Run("a distinct processing_version at one block is a distinct observation", func(t *testing.T) {
		// The source's trigger versions a re-synced cycle by build_id; both are real observations.
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "550", 2)
		f.mustRun(t)
		var at []mapleRow
		for _, r := range f.rows(t, "a") {
			if r.bn == 1017 {
				at = append(at, r)
			}
		}
		if len(at) != 2 || at[0].pv == at[1].pv {
			t.Fatalf("want two processing_versions at block 1017, got %+v", at)
		}
		if at[1].qty != "550" {
			t.Errorf("the reprocessed version carries %s; want 550", at[1].qty)
		}
	})

	t.Run("re-running with no new source rows appends nothing", func(t *testing.T) {
		// Guarded against passing vacuously: the spine must be non-empty first, so this cannot be
		// satisfied by a run that never materialized anything.
		var before int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		if before == 0 {
			t.Fatal("nothing was materialized, so idempotence is not under test")
		}
		if n := f.mustRun(t); n != 0 {
			t.Errorf("re-running appended %d rows; want 0 with %d already stored", n, before)
		}
	})

	t.Run("the run records the batch's own latest block_timestamp and build id", func(t *testing.T) {
		// count(*) > 0 was satisfied by any prior success, including runs that emitted nothing.
		var runTS, spineTS time.Time
		var buildID int
		if err := pool.QueryRow(ctx,
			`SELECT block_timestamp, build_id FROM position_projection_run
			 WHERE projection = 'public.position_maple_loan' ORDER BY created_at DESC LIMIT 1`).Scan(&runTS, &buildID); err != nil {
			t.Fatalf("no run recorded under the view's qualified name: %v", err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT max(ps.block_timestamp) FROM position_state ps
			 JOIN maple_loan l ON ps.instrument_key = encode(l.loan_address, 'hex')`).Scan(&spineTS); err != nil {
			t.Fatal(err)
		}
		if !runTS.Equal(spineTS) {
			t.Errorf("run recorded block_timestamp %s; want the projection's own max %s", runTS, spineTS)
		}
		if buildID != 0 {
			t.Errorf("build_id=%d; want the 0 passed to the wrapper", buildID)
		}
	})
}

func TestMapleLoanRefusals(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"a": 1, "b": 8453})

	t.Run("a cycle no block precedes is refused by name, and nothing is appended", func(t *testing.T) {
		f.blocks(t, 1, 1000, "2026-06-16T10:00:00Z", 10, 120)
		f.cycle(t, "a", "2026-06-16T09:00:00Z", "500", 1)
		_, err := f.run(t)
		if err == nil || !strings.Contains(err.Error(), "no block_meta block precedes") {
			t.Fatalf("want the unplaceable-cycle refusal, got %v", err)
		}
		if !strings.Contains(err.Error(), "chain 1") || !strings.Contains(err.Error(), "2026-06-16 09:00:00") {
			t.Errorf("the refusal must name the chain and the earliest offending cycle: %v", err)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("a refused run appended %d rows; want none", n)
		}
	})

	t.Run("the unplaceable check is scoped per chain and names only the offending one", func(t *testing.T) {
		// Chain 8453's cycle precedes all of ITS blocks while chain 1 has an earlier block. Dropping
		// the chain predicate from the pre-check lets chain 1's coverage vouch for chain 8453.
		f.blocks(t, 8453, 500, "2026-06-16T11:00:00Z", 10, 120)
		f.cycle(t, "b", "2026-06-16T10:30:00Z", "42", 1)
		_, err := f.run(t)
		if err == nil || !strings.Contains(err.Error(), "chain 8453") {
			t.Fatalf("want a refusal naming chain 8453, got %v", err)
		}
		f.exec(t, `DELETE FROM maple_loan_state WHERE synced_at = '2026-06-16T10:30:00Z'::timestamptz`)
	})

	t.Run("a cycle at the earliest block's exact instant is placeable, not refused", func(t *testing.T) {
		// The pre-check's own `<=`. Tightening it to `<` refuses a cycle the view places fine, and no
		// other fixture catches that because every other cycle has a strictly earlier block.
		f.exec(t, `DELETE FROM maple_loan_state`)
		f.exec(t, `DELETE FROM block_meta`)
		f.block(t, 1, 1900, 0, "2026-06-16T08:00:00Z")
		f.blocks(t, 1, 2000, "2026-06-16T10:00:00Z", 5, 120)
		f.cycle(t, "a", "2026-06-16T10:00:00Z", "500", 1)
		if _, err := f.run(t); err != nil {
			t.Fatalf("a cycle exactly at the earliest block's instant must be placeable: %v", err)
		}
	})

	t.Run("coverage too sparse to place a cycle closely is refused, naming the gap", func(t *testing.T) {
		// The silent back-dating case: one block far in the past absorbs a whole history without it.
		f.exec(t, `DELETE FROM maple_loan_state`)
		f.exec(t, `DELETE FROM position_state`)
		f.exec(t, `DELETE FROM block_meta`)
		f.block(t, 1, 100, 0, "2026-01-01T00:00:00Z")
		f.cycle(t, "a", "2026-06-16T08:00:00Z", "500", 1)
		_, err := f.run(t)
		if err == nil || !strings.Contains(err.Error(), "too sparse") {
			t.Fatalf("want the sparse-coverage refusal, got %v", err)
		}
		if !strings.Contains(err.Error(), "166 days") {
			t.Errorf("the refusal must quantify the gap it is refusing: %v", err)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("a refused run appended %d rows; want none", n)
		}
	})

	t.Run("a widened tolerance is the only way to accept that gap", func(t *testing.T) {
		var n int64
		if err := pool.QueryRow(ctx,
			`SELECT materialize_maple_loan(0, INTERVAL '200 days')`).Scan(&n); err != nil {
			t.Fatalf("an explicitly widened tolerance must accept it: %v", err)
		}
		if n != 1 {
			t.Errorf("appended %d rows, want 1", n)
		}
	})

	t.Run("block_meta header times that invert against height are refused", func(t *testing.T) {
		// A mis-parsed header time. Unrefused, it silently wins the placement when it is the only
		// candidate, and wedges the spine's monotonic gate when it is not.
		f.exec(t, `DELETE FROM maple_loan_state`)
		f.exec(t, `DELETE FROM position_state`)
		f.exec(t, `DELETE FROM block_meta`)
		f.blocks(t, 1, 3000, "2026-06-16T08:00:00Z", 10, 120)
		f.block(t, 1, 2500, 0, "2026-06-16T08:19:00Z")
		f.cycle(t, "a", "2026-06-16T08:18:30Z", "500", 1)
		_, err := f.run(t)
		if err == nil || !strings.Contains(err.Error(), "invert against height") {
			t.Fatalf("want the inverted-header refusal, got %v", err)
		}
		if !strings.Contains(err.Error(), "2500") {
			t.Errorf("the refusal must name the offending block: %v", err)
		}
	})

	t.Run("an orphaned reorg version does not count as an inversion", func(t *testing.T) {
		// Only surviving versions form the timeline, so a superseded row with an odd time is ignored
		// rather than blocking every future run. Dropping the version guard from the check refuses here.
		f.exec(t, `DELETE FROM block_meta WHERE block_number = 2500`)
		f.block(t, 1, 2500, 0, "2026-06-16T08:19:00Z")
		f.block(t, 1, 2500, 1, "2026-06-16T07:50:00Z")
		if _, err := f.run(t); err != nil {
			t.Fatalf("a superseded version must not be read as an inversion: %v", err)
		}
	})

	t.Run("a non-Active source row is refused rather than projected as an open position", func(t *testing.T) {
		// The indexer only writes Active, but stl_readwrite can INSERT directly and the sibling FTL
		// query already fetches four states. Projected silently, a Repaid loan reads as still owing.
		f.exec(t, `DELETE FROM maple_loan_state`)
		f.exec(t, `DELETE FROM position_state`)
		f.cycleState(t, "a", "2026-06-16T08:10:00Z", "Repaid", "777")
		_, err := f.run(t)
		if err == nil || !strings.Contains(err.Error(), "Repaid") {
			t.Fatalf("want a refusal naming the unexpected state, got %v", err)
		}
	})
}

func TestMapleLoanAbsenceClose(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"gone": 1, "peer1": 1, "peer2": 1, "lonely": 8453})
	f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
	f.blocks(t, 8453, 500, "2026-06-16T08:00:00Z", 60, 120)

	// Four cycle instants ten minutes apart. `gone` stops after the first; its peers continue, so the
	// absence is attributable to the loan and not to a truncated cycle.
	instants := []string{"2026-06-16T08:05:00Z", "2026-06-16T08:15:00Z", "2026-06-16T08:25:00Z", "2026-06-16T08:35:00Z"}

	t.Run("no close is emitted while too few cycles have passed since the loan vanished", func(t *testing.T) {
		// Persistence guard: one missed cycle is not yet a repayment.
		f.cycle(t, "gone", instants[0], "500", 1)
		for _, ts := range instants[:2] {
			f.cycle(t, "peer1", ts, "100", 1)
			f.cycle(t, "peer2", ts, "200", 1)
		}
		f.mustRun(t)
		rs := f.rows(t, "gone")
		if len(rs) != 1 || rs[0].qty != "500" {
			t.Fatalf("want the single open observation, got %+v", rs)
		}
	})

	t.Run("absence closes the position once peers keep reporting and the absence persists", func(t *testing.T) {
		// maple_loan_state's COMMENT makes absence the close signal; this is that signal acted on.
		for _, ts := range instants[2:] {
			f.cycle(t, "peer1", ts, "100", 1)
			f.cycle(t, "peer2", ts, "200", 1)
		}
		f.mustRun(t)
		rs := f.rows(t, "gone")
		if len(rs) != 2 {
			t.Fatalf("want the open observation and one close, got %d: %+v", len(rs), rs)
		}
		if rs[1].qty != "0" {
			t.Errorf("the closing observation carries %s; want 0", rs[1].qty)
		}
		if rs[1].bn <= rs[0].bn {
			t.Errorf("the close is at block %d, at or before the open at %d", rs[1].bn, rs[0].bn)
		}
	})

	t.Run("exactly one close is emitted, and re-running adds no more", func(t *testing.T) {
		// Closure keeps the first zero after a positive and drops repeats, so a second close would be
		// a silent duplicate rather than a visible error.
		if n := f.mustRun(t); n != 0 {
			t.Errorf("re-running appended %d rows; want 0", n)
		}
		var zeros int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM position_state ps
			 JOIN maple_loan l ON ps.instrument_key = encode(l.loan_address,'hex')
			 WHERE l.id = $1 AND ps.quantity = 0`, f.loans["gone"]).Scan(&zeros); err != nil {
			t.Fatal(err)
		}
		if zeros != 1 {
			t.Errorf("got %d closing observations; want exactly 1", zeros)
		}
	})

	t.Run("a loan whose cycles never had peers present is never closed", func(t *testing.T) {
		// The peer guard. `lonely` is the only loan on its chain, so its absence is indistinguishable
		// from a cycle that failed, and an unguarded close would zero it permanently.
		f.cycle(t, "lonely", instants[0], "999", 1)
		f.exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, build_id)
		           SELECT $1, $2::timestamptz, 'Active', 1, 0`, f.loans["lonely"], instants[1])
		f.exec(t, `DELETE FROM maple_loan_state WHERE maple_loan_id = $1 AND synced_at = $2::timestamptz`,
			f.loans["lonely"], instants[1])
		f.mustRun(t)
		var zeros int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM position_state ps
			 JOIN maple_loan l ON ps.instrument_key = encode(l.loan_address,'hex')
			 WHERE l.id = $1 AND ps.quantity = 0`, f.loans["lonely"]).Scan(&zeros); err != nil {
			t.Fatal(err)
		}
		if zeros != 0 {
			t.Errorf("closed a loan with no peer observations (%d zeros); the guard must hold", zeros)
		}
	})

	t.Run("a close needs peers at the closing cycle, not merely cycles that passed", func(t *testing.T) {
		// The peer guard alone. On chain 8453 `lonely` vanishes after t0 and three further cycles pass,
		// so the persistence guard is satisfied -- but each of those cycles observed only `solo`, so a
		// truncated response is indistinguishable from a repayment and the close must not fire.
		f.exec(t, `INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
		           SELECT 8453, p.id, decode(md5('loan-solo') || 'a1b2c3d4', 'hex'), mp.id, u.id
		           FROM protocol p JOIN maple_pool mp ON mp.chain_id = p.chain_id
		           JOIN "user" u ON u.chain_id = 8453
		           WHERE p.chain_id = 8453 AND p.name = 'Maple' LIMIT 1 ON CONFLICT DO NOTHING`)
		if err := pool.QueryRow(ctx, `SELECT id FROM maple_loan WHERE loan_address = decode(md5('loan-solo') || 'a1b2c3d4', 'hex')`).Scan(new(int64)); err != nil {
			t.Fatal(err)
		}
		var soloID int64
		if err := pool.QueryRow(ctx, `SELECT id FROM maple_loan WHERE loan_address = decode(md5('loan-solo') || 'a1b2c3d4', 'hex')`).Scan(&soloID); err != nil {
			t.Fatal(err)
		}
		f.loans["solo"] = soloID
		for _, ts := range instants[1:] {
			f.cycle(t, "solo", ts, "7", 1)
		}
		f.mustRun(t)
		var zeros int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM position_state ps
			 JOIN maple_loan l ON ps.instrument_key = encode(l.loan_address,'hex')
			 WHERE l.id = $1 AND ps.quantity = 0`, f.loans["lonely"]).Scan(&zeros); err != nil {
			t.Fatal(err)
		}
		if zeros != 0 {
			t.Errorf("closed a loan whose absence no peer cycle corroborates (%d zeros)", zeros)
		}
	})

	t.Run("a live loan reporting at the newest cycle is never closed", func(t *testing.T) {
		for _, loan := range []string{"peer1", "peer2"} {
			rs := f.rows(t, loan)
			for _, r := range rs {
				if r.qty == "0" {
					t.Errorf("loan %s is still reporting but was closed at block %d", loan, r.bn)
				}
			}
		}
	})
}

func TestMapleLoanViewContract(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"a": 1})
	f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 30, 120)

	t.Run("block_meta carries the four-column index the placement's ORDER BY needs", func(t *testing.T) {
		var def string
		if err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE tablename = 'block_meta'
			   AND indexname = 'block_meta_chain_time_idx'`).Scan(&def); err != nil {
			t.Fatalf("block_meta_chain_time_idx is missing: %v", err)
		}
		for _, want := range []string{"chain_id", "block_timestamp DESC", "block_number DESC", "block_version DESC"} {
			if !strings.Contains(def, want) {
				t.Errorf("indexdef %s is missing %s; the pick then needs a sort node", def, want)
			}
		}
	})

	t.Run("the view is not writable, by its shape rather than by a privilege", func(t *testing.T) {
		// ALTER DEFAULT PRIVILEGES grants stl_readwrite INSERT and UPDATE on every new public view, so
		// a privilege assertion here cannot fail and would not be evidence. The shape is what holds.
		var insertable, updatable string
		if err := pool.QueryRow(ctx,
			`SELECT t.is_insertable_into, v.is_updatable FROM information_schema.tables t
			 JOIN information_schema.views v USING (table_schema, table_name)
			 WHERE t.table_name = 'position_maple_loan'`).Scan(&insertable, &updatable); err != nil {
			t.Fatal(err)
		}
		if insertable != "NO" || updatable != "NO" {
			t.Errorf("is_insertable_into=%s is_updatable=%s; want NO/NO", insertable, updatable)
		}
		_, err := pool.Exec(ctx, `INSERT INTO position_maple_loan (chain_id) VALUES (1)`)
		if err == nil || !strings.Contains(err.Error(), "cannot insert into view") {
			t.Errorf("an INSERT through the view must be refused by Postgres, got %v", err)
		}
	})

	t.Run("a borrower address that is not 20 bytes is refused by name", func(t *testing.T) {
		// Without the guard this surfaces only as position_state_holder_hex_chk on a chunk, naming no
		// loan, chain or user. "user" is written by every indexer, so one bad row poisons every run.
		f.exec(t, `INSERT INTO "user" (chain_id, address) VALUES (1, '\x0badc0de')
		           ON CONFLICT DO NOTHING`)
		f.exec(t, `INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
		           SELECT 1, p.id, decode(md5('loan-short') || 'a1b2c3d4', 'hex'), mp.id, u.id
		           FROM protocol p JOIN maple_pool mp ON mp.chain_id = p.chain_id
		           JOIN "user" u ON u.chain_id = 1 AND u.address = '\x0badc0de'
		           WHERE p.chain_id = 1 AND p.name = 'Maple' ON CONFLICT DO NOTHING`)
		var shortID int64
		if err := pool.QueryRow(ctx, `SELECT id FROM maple_loan WHERE loan_address = decode(md5('loan-short') || 'a1b2c3d4', 'hex')`).Scan(&shortID); err != nil {
			t.Fatal(err)
		}
		f.loans["short"] = shortID
		f.cycle(t, "short", "2026-06-16T08:15:00Z", "5", 1)
		_, err := f.run(t)
		if err == nil || !strings.Contains(err.Error(), "not a 20-byte EVM address") {
			t.Fatalf("want the borrower-address refusal, got %v", err)
		}
		if !strings.Contains(err.Error(), "4-byte") {
			t.Errorf("the refusal must name the actual width: %v", err)
		}
		f.exec(t, `DELETE FROM maple_loan_state WHERE maple_loan_id = $1`, shortID)
	})

	t.Run("a backfilled earlier cycle diverges the view from the spine, and the run says so", func(t *testing.T) {
		// The residual of the earliest-synced_at pick: the spine cannot revise an emitted observation,
		// so a replayed earlier cycle changes the view alone. The materializer warns and keeps the
		// stored row; this pins that the divergence is real and reported, not silently reconciled.
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		f.mustRun(t)
		stored := f.rows(t, "a")
		if len(stored) != 1 || stored[0].qty != "500" {
			t.Fatalf("want one stored observation of 500, got %+v", stored)
		}
		f.cycle(t, "a", "2026-06-16T08:34:10Z", "999", 1)
		if n := f.mustRun(t); n != 0 {
			t.Errorf("a replayed earlier cycle appended %d rows; the observation key is unchanged", n)
		}
		if got := f.viewQty(t, "a", stored[0].bn); len(got) != 1 || got[0] != "999" {
			t.Errorf("view now reports %v at block %d; want the replayed 999", got, stored[0].bn)
		}
		after := f.rows(t, "a")
		if len(after) != 1 || after[0].qty != "500" {
			t.Errorf("spine reports %+v; want the original 500 kept, since it cannot be revised", after)
		}
	})
}

// A partial fetch and a repayment look identical to a count floor: a truncated cycle holding
// any two loans clears a floor of two. Five loans, then three cycles carrying only two of them,
// must close nothing -- a false zero cannot be retracted on an append-only spine.
func TestMapleLoanTruncatedCycleDoesNotClose(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"l1": 1, "l2": 1, "l3": 1, "l4": 1, "l5": 1})
	f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)

	instants := []string{
		"2026-06-16T08:05:00Z", "2026-06-16T08:15:00Z",
		"2026-06-16T08:25:00Z", "2026-06-16T08:35:00Z",
	}
	for _, l := range []string{"l1", "l2", "l3", "l4", "l5"} {
		f.cycle(t, l, instants[0], "100", 1)
	}
	// The fetch degrades and returns two of the five for every later cycle.
	for _, ts := range instants[1:] {
		f.cycle(t, "l1", ts, "100", 1)
		f.cycle(t, "l2", ts, "100", 1)
	}
	f.mustRun(t)

	for _, l := range []string{"l3", "l4", "l5"} {
		rs := f.rows(t, l)
		if len(rs) != 1 {
			t.Errorf("%s: want only its open observation, got %d: %+v", l, len(rs), rs)
			continue
		}
		if rs[0].qty != "100" {
			t.Errorf("%s: stored %s, want the open 100", l, rs[0].qty)
		}
	}
}

// The peer count still has to fall for a close to be inferred at all, so a single repayment
// out of five closes normally. Without this the truncation guard could pass by never closing.
func TestMapleLoanSingleRepaymentAmongManyStillCloses(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"r1": 1, "r2": 1, "r3": 1, "r4": 1, "repaid": 1})
	f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)

	instants := []string{
		"2026-06-16T08:05:00Z", "2026-06-16T08:15:00Z",
		"2026-06-16T08:25:00Z", "2026-06-16T08:35:00Z",
	}
	for _, l := range []string{"r1", "r2", "r3", "r4", "repaid"} {
		f.cycle(t, l, instants[0], "100", 1)
	}
	for _, ts := range instants[1:] {
		for _, l := range []string{"r1", "r2", "r3", "r4"} {
			f.cycle(t, l, ts, "100", 1)
		}
	}
	f.mustRun(t)

	rs := f.rows(t, "repaid")
	if len(rs) != 2 {
		t.Fatalf("want the open observation and one close, got %d: %+v", len(rs), rs)
	}
	if rs[1].qty != "0" {
		t.Errorf("the closing observation carries %s; want 0", rs[1].qty)
	}
}

// A count of peers cannot tell "still there" from "replaced": a fetch that loses two loans while
// two others are originated reports the same number, so a count guard zeroes the two it lost.
// The rule is peer retention, so the loans that were there must still be there.
func TestMapleLoanOriginationDoesNotMaskATruncation(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{
		"x1": 1, "x2": 1, "x3": 1, "x4": 1, "x5": 1, "new6": 1, "new7": 1,
	})
	f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)

	instants := []string{
		"2026-06-16T08:05:00Z", "2026-06-16T08:15:00Z",
		"2026-06-16T08:25:00Z", "2026-06-16T08:35:00Z",
	}
	for _, l := range []string{"x1", "x2", "x3", "x4", "x5"} {
		f.cycle(t, l, instants[0], "100", 1)
	}
	// The fetch loses x4 and x5 while new6 and new7 are originated, so the count stays at five.
	for _, ts := range instants[1:] {
		for _, l := range []string{"x1", "x2", "x3", "new6", "new7"} {
			f.cycle(t, l, ts, "100", 1)
		}
	}
	f.mustRun(t)

	for _, l := range []string{"x4", "x5"} {
		rs := f.rows(t, l)
		if len(rs) != 1 {
			t.Errorf("%s: want only its open observation, got %d: %+v", l, len(rs), rs)
			continue
		}
		if rs[0].qty != "100" {
			t.Errorf("%s: stored %s, want the open 100", l, rs[0].qty)
		}
	}
}

// The retention rule must not stop a genuine repayment closing while other loans are originated
// alongside it, or it would trade a false zero for a position that never closes.
func TestMapleLoanARepaymentClosesAlongsideNewOriginations(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"k1": 1, "k2": 1, "gone": 1, "fresh": 1})
	f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)

	instants := []string{
		"2026-06-16T08:05:00Z", "2026-06-16T08:15:00Z",
		"2026-06-16T08:25:00Z", "2026-06-16T08:35:00Z",
	}
	for _, l := range []string{"k1", "k2", "gone"} {
		f.cycle(t, l, instants[0], "100", 1)
	}
	// gone repays; k1 and k2 keep reporting and fresh is originated.
	for _, ts := range instants[1:] {
		for _, l := range []string{"k1", "k2", "fresh"} {
			f.cycle(t, l, ts, "100", 1)
		}
	}
	f.mustRun(t)

	rs := f.rows(t, "gone")
	if len(rs) != 2 {
		t.Fatalf("want the open observation and one close, got %d: %+v", len(rs), rs)
	}
	if rs[1].qty != "0" {
		t.Errorf("the closing observation carries %s; want 0", rs[1].qty)
	}
}

// The wrapper is the only path the runner calls, so it has to forward the writer run to the spine or
// every row this projection appends is provenance-free (ADR-0006 §2). The run record is the witness:
// its run_id can only have arrived through the wrapper's own parameter.
func TestMapleLoanForwardsTheWriterRun(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]int{"run-fwd": 1})
	f.blocks(t, 1, 100, "2026-04-01T00:00:00Z", 40, 3600)
	f.cycle(t, "run-fwd", "2026-04-01T05:00:00Z", "1000", 0)
	var n int64
	if err := pool.QueryRow(ctx, `SELECT materialize_maple_loan(7, $1::interval, 9182)`, mapleTolerance).Scan(&n); err != nil {
		t.Fatalf("materialize_maple_loan with a run: %v", err)
	}
	if n != 1 {
		t.Fatalf("appended %d rows, want 1", n)
	}
	var stamped, unstamped int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE run_id = 9182 AND build_id = 7),
		       count(*) FILTER (WHERE run_id IS DISTINCT FROM 9182)
		  FROM position_state`).Scan(&stamped, &unstamped); err != nil {
		t.Fatalf("read the appended rows: %v", err)
	}
	if stamped != 1 || unstamped != 0 {
		t.Errorf("appended rows: %d carry run 9182 at build 7, %d do not, want 1 and 0", stamped, unstamped)
	}
	// The run record is the wrapper's other witness, and it is the only one the other six assert.
	var runRecord *int64
	if err := pool.QueryRow(ctx, `
		SELECT run_id FROM position_projection_run
		 WHERE projection = 'public.position_maple_loan'
		 ORDER BY created_at DESC LIMIT 1`).Scan(&runRecord); err != nil {
		t.Fatalf("read the run record: %v", err)
	}
	if runRecord == nil || *runRecord != 9182 {
		t.Errorf("run record = %v, want 9182", runRecord)
	}

	// The runner passes the two provenance arguments BY NAME, so these parameter names are the
	// contract: renaming one here leaves this migration valid and breaks that projection only.
	var args []string
	if err := pool.QueryRow(ctx, `
		SELECT proargnames::text[] FROM pg_proc WHERE proname = 'materialize_maple_loan'`).Scan(&args); err != nil {
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
			t.Errorf("materialize_maple_loan declares %v, missing %s -- the runner calls it by name", args, want)
		}
	}
}
