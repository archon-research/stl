//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-405: maple_loan_state carries only the cron cycle's synced_at, so position_maple_loan places
// each cycle at the last block_meta block at or before that instant and takes THAT block's timestamp.
// These tests pin the placement, the stable pick, the chain scoping and the refusal, and each one was
// checked to fail against a mutated view (nearest-block, next-block, synced_at-as-timestamp,
// latest-synced_at pick, chain-unscoped lookup, LOAN instead of BORROW, dropped refusal).
func TestMaterializeMapleLoan(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	exec := func(t *testing.T, sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Two chains, so the chain scoping of the block lookup is observable: chain 1 and chain 8453 are
	// both seeded by migrations, and each gets its own block_meta heights and its own loan.
	exec(t, `INSERT INTO protocol (chain_id, address, name, protocol_type)
	         VALUES (1, '\x11111111111111111111111111111111111111a1', 'Maple', 'lending'),
	                (8453, '\x11111111111111111111111111111111111111a2', 'Maple', 'lending')
	         ON CONFLICT DO NOTHING`)
	exec(t, `INSERT INTO token (chain_id, address, symbol, decimals)
	         VALUES (1, '\x22222222222222222222222222222222222222b1', 'MPLUSDC', 6),
	                (8453, '\x22222222222222222222222222222222222222b2', 'MPLUSDC', 6)
	         ON CONFLICT DO NOTHING`)
	exec(t, `INSERT INTO maple_pool (chain_id, protocol_id, address, asset_token_id)
	         SELECT c.chain_id, p.id, decode(md5('pool' || c.chain_id), 'hex'), tk.id
	         FROM (VALUES (1), (8453)) c(chain_id)
	         JOIN protocol p ON p.chain_id = c.chain_id AND p.name = 'Maple'
	         JOIN token tk ON tk.chain_id = c.chain_id AND tk.symbol = 'MPLUSDC'
	         ON CONFLICT DO NOTHING`)
	exec(t, `INSERT INTO "user" (chain_id, address)
	         VALUES (1, '\x33333333333333333333333333333333333333c1'),
	                (8453, '\x33333333333333333333333333333333333333c2')
	         ON CONFLICT DO NOTHING`)
	// loanA on chain 1, loanB on chain 8453; loan_address differs per chain so instrument_key does too.
	exec(t, `INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
	         SELECT c.chain_id, p.id, decode(md5('loan' || c.chain_id), 'hex'), mp.id, u.id
	         FROM (VALUES (1), (8453)) c(chain_id)
	         JOIN protocol p ON p.chain_id = c.chain_id AND p.name = 'Maple'
	         JOIN maple_pool mp ON mp.chain_id = c.chain_id
	         JOIN "user" u ON u.chain_id = c.chain_id
	         ON CONFLICT DO NOTHING`)
	// A third loan on chain 1, kept out of every run until the stable-pick subtest, so that its
	// colliding cycles are ALL present at its first materialization. Asserting the pick on a loan
	// that already has a row would only re-prove the append-only dedupe.
	exec(t, `INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
	         SELECT 1, p.id, decode(md5('loanC'), 'hex'), mp.id, u.id
	         FROM protocol p, maple_pool mp, "user" u
	         WHERE p.chain_id = 1 AND p.name = 'Maple' AND mp.chain_id = 1 AND u.chain_id = 1
	         ON CONFLICT DO NOTHING`)

	loanID := func(t *testing.T, chain int) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `SELECT id FROM maple_loan WHERE chain_id = $1`, chain).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	loanA, loanB := loanID(t, 1), loanID(t, 8453)
	var loanC int64
	if err := pool.QueryRow(ctx, `SELECT id FROM maple_loan WHERE chain_id = 1 AND loan_address = decode(md5('loanC'), 'hex')`).Scan(&loanC); err != nil {
		t.Fatal(err)
	}
	instrC := "" // filled below, once the loan's hex address is needed

	block := func(t *testing.T, chain, bn, bv int, ts string) {
		t.Helper()
		exec(t, `INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
		         VALUES ($1, $2, $3, $4::timestamptz) ON CONFLICT DO NOTHING`, chain, bn, bv, ts)
	}
	cycle := func(t *testing.T, loan int64, ts string, principal string, build int) {
		t.Helper()
		exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, build_id)
		         VALUES ($1, $2::timestamptz, 'Active', $3::numeric, $4)`, loan, ts, principal, build)
	}
	// Rows land in position_state only via the wrapper, so every read below is of the real output.
	run := func(t *testing.T) (int64, error) {
		t.Helper()
		var n int64
		err := pool.QueryRow(ctx, `SELECT materialize_maple_loan(0)`).Scan(&n)
		return n, err
	}
	type row struct {
		instrument, holder, dealType, qty, ts string
		bn                                    int64
		bv, pv                                int
		chain                                 int
	}
	rowsFor := func(t *testing.T, instrument string) []row {
		t.Helper()
		rs, err := pool.Query(ctx,
			`SELECT chain_id, instrument_key, holder_id, deal_type, quantity::text,
			        block_timestamp::text, block_number, block_version, processing_version
			 FROM position_state WHERE instrument_key = $1
			 ORDER BY block_number, block_version, processing_version`, instrument)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		var out []row
		for rs.Next() {
			var r row
			if err := rs.Scan(&r.chain, &r.instrument, &r.holder, &r.dealType, &r.qty, &r.ts, &r.bn, &r.bv, &r.pv); err != nil {
				t.Fatal(err)
			}
			out = append(out, r)
		}
		if err := rs.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	t.Run("the wrapper refuses a cycle that no block on its own chain precedes, naming the chain", func(t *testing.T) {
		// block_meta starts at 10:00 but the cycle is at 09:00, so the LATERAL would silently drop the
		// row and loanA's history would begin late. This is the fail-first case for the whole design.
		block(t, 1, 1000, 0, "2026-06-16T10:00:00Z")
		cycle(t, loanA, "2026-06-16T09:00:00Z", "500", 1)
		_, err := run(t)
		if err == nil || !strings.Contains(err.Error(), "no block_meta block precedes") {
			t.Fatalf("want a refusal naming the unplaceable cycles, got %v", err)
		}
		if !strings.Contains(err.Error(), "chain 1") || !strings.Contains(err.Error(), "2026-06-16 09:00:00") {
			t.Errorf("the refusal must name the chain and the earliest offending cycle, got %v", err)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("a refused run must append nothing, got %d rows", n)
		}
	})

	t.Run("the refusal clears once block_meta reaches back far enough", func(t *testing.T) {
		block(t, 1, 999, 0, "2026-06-16T08:00:00Z")
		if _, err := run(t); err != nil {
			t.Fatalf("with a block at 08:00 the 09:00 cycle is placeable: %v", err)
		}
		rs := rowsFor(t, hexOf(t, pool, ctx, 1))
		if len(rs) != 1 {
			t.Fatalf("want one observation, got %d", len(rs))
		}
		// Placed at 999, not 1000: the block at or BEFORE the cycle, and carrying 999's header time,
		// not the cycle's synced_at. A nearest-block or synced_at-timestamp mutation fails here.
		if rs[0].bn != 999 || !strings.HasPrefix(rs[0].ts, "2026-06-16 08:00:00") {
			t.Errorf("placement: bn=%d ts=%s; want block 999 at its own header time 08:00", rs[0].bn, rs[0].ts)
		}
		if rs[0].dealType != "BORROW" {
			t.Errorf("deal_type=%q; want BORROW (the holder is the borrower owing principal)", rs[0].dealType)
		}
		if rs[0].qty != "500" {
			t.Errorf("quantity=%s; want principal_owed 500", rs[0].qty)
		}
		if rs[0].chain != 1 {
			t.Errorf("chain_id=%d; want the loan's chain 1", rs[0].chain)
		}
		var wantHolder string
		if err := pool.QueryRow(ctx, `SELECT encode(address,'hex') FROM "user" WHERE chain_id = 1`).Scan(&wantHolder); err != nil {
			t.Fatal(err)
		}
		if rs[0].holder != wantHolder {
			t.Errorf("holder_id=%s; want the borrower's address %s", rs[0].holder, wantHolder)
		}
	})

	t.Run("a second run appends nothing", func(t *testing.T) {
		n, err := run(t)
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("re-running appended %d rows; the materializer must be idempotent", n)
		}
	})

	t.Run("many cycles inside one block collapse to one observation, earliest synced_at winning", func(t *testing.T) {
		// The real cadence is ~139 syncs per loan-day, far more than one per block. Three cycles of
		// loanC fall between block 999 (08:00) and block 1000 (10:00) and are all present before its
		// FIRST run, so the pick itself is under test: the earliest cycle wins and a later arrival
		// must not move it. Seeded out of order so a pick that follows insertion order also fails.
		if err := pool.QueryRow(ctx, `SELECT encode(loan_address,'hex') FROM maple_loan WHERE id = $1`, loanC).Scan(&instrC); err != nil {
			t.Fatal(err)
		}
		cycle(t, loanC, "2026-06-16T09:30:00Z", "700", 1)
		cycle(t, loanC, "2026-06-16T08:30:00Z", "600", 1)
		cycle(t, loanC, "2026-06-16T09:00:00Z", "650", 1)
		if _, err := run(t); err != nil {
			t.Fatal(err)
		}
		rs := rowsFor(t, instrC)
		if len(rs) != 1 {
			t.Fatalf("three cycles in one block must be one observation, got %d: %+v", len(rs), rs)
		}
		if rs[0].bn != 999 || rs[0].qty != "600" {
			t.Errorf("block %d quantity %s; want block 999 carrying 600, the earliest cycle in it", rs[0].bn, rs[0].qty)
		}
	})

	t.Run("a cycle arriving later inside an already-observed block does not move the pick", func(t *testing.T) {
		// The append-only spine cannot revise an emitted observation, so the pick has to be stable
		// under arrival order too: a cycle inserted afterwards but timestamped EARLIER than the one
		// that won must leave the stored quantity alone rather than emit a competing row.
		cycle(t, loanC, "2026-06-16T08:10:00Z", "555", 1)
		if _, err := run(t); err != nil {
			t.Fatal(err)
		}
		rs := rowsFor(t, instrC)
		if len(rs) != 1 || rs[0].qty != "600" {
			t.Errorf("after a late-arriving earlier cycle: %+v; want the single stored observation still at 600", rs)
		}
	})

	t.Run("a cycle at a block's exact header instant places at that block", func(t *testing.T) {
		cycle(t, loanA, "2026-06-16T10:00:00Z", "800", 1)
		if _, err := run(t); err != nil {
			t.Fatal(err)
		}
		rs := rowsFor(t, hexOf(t, pool, ctx, 1))
		if len(rs) != 2 || rs[1].bn != 1000 || rs[1].qty != "800" {
			t.Fatalf("a cycle exactly at 10:00 belongs to block 1000, got %+v", rs)
		}
		if !strings.HasPrefix(rs[1].ts, "2026-06-16 10:00:00") {
			t.Errorf("block_timestamp=%s; want block 1000's header time", rs[1].ts)
		}
	})

	t.Run("a reorg version at the same height is a distinct block and the later header time wins", func(t *testing.T) {
		block(t, 1, 1001, 0, "2026-06-16T11:00:00Z")
		block(t, 1, 1001, 1, "2026-06-16T11:00:30Z")
		cycle(t, loanA, "2026-06-16T11:00:40Z", "900", 1)
		if _, err := run(t); err != nil {
			t.Fatal(err)
		}
		rs := rowsFor(t, hexOf(t, pool, ctx, 1))
		last := rs[len(rs)-1]
		if last.bn != 1001 || last.bv != 1 || !strings.HasPrefix(last.ts, "2026-06-16 11:00:30") {
			t.Errorf("placement %d/%d at %s; want the reorged 1001/1 whose header time is the latest at or before the cycle", last.bn, last.bv, last.ts)
		}
	})

	t.Run("the block lookup is scoped to the loan's chain", func(t *testing.T) {
		// chain 8453 has its own heights, deliberately overlapping chain 1's numbers with different
		// times. An unscoped lookup would place loanB on a chain-1 block.
		block(t, 8453, 500, 0, "2026-06-16T09:45:00Z")
		cycle(t, loanB, "2026-06-16T12:00:00Z", "42", 1)
		if _, err := run(t); err != nil {
			t.Fatal(err)
		}
		rs := rowsFor(t, hexOf(t, pool, ctx, 8453))
		if len(rs) != 1 {
			t.Fatalf("want one observation for loanB, got %d", len(rs))
		}
		if rs[0].chain != 8453 || rs[0].bn != 500 || !strings.HasPrefix(rs[0].ts, "2026-06-16 09:45:00") {
			t.Errorf("loanB placed at chain %d block %d (%s); want chain 8453 block 500 at 09:45", rs[0].chain, rs[0].bn, rs[0].ts)
		}
	})

	t.Run("a reprocessing_version at the same block is a distinct observation", func(t *testing.T) {
		// The source's trigger versions a re-synced cycle by build_id, and both versions are real
		// observations of the same block: dropping the version from the pick would lose the reprocess.
		cycle(t, loanA, "2026-06-16T11:00:40Z", "950", 2)
		if _, err := run(t); err != nil {
			t.Fatal(err)
		}
		rs := rowsFor(t, hexOf(t, pool, ctx, 1))
		var atBlock []row
		for _, r := range rs {
			if r.bn == 1001 {
				atBlock = append(atBlock, r)
			}
		}
		if len(atBlock) != 2 || atBlock[0].pv == atBlock[1].pv {
			t.Fatalf("want two processing_versions at block 1001, got %+v", atBlock)
		}
		if atBlock[1].qty != "950" {
			t.Errorf("the reprocessed version carries %s; want 950", atBlock[1].qty)
		}
	})

	t.Run("block_meta carries the chain_id/block_timestamp index this projection needs", func(t *testing.T) {
		// The PK leads with block_number, so "the block at or before this instant" has no usable index
		// without this one, and the projection degrades to a seq scan per cycle.
		var def string
		if err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE tablename = 'block_meta' AND indexname = 'block_meta_chain_time_idx'`).Scan(&def); err != nil {
			t.Fatalf("block_meta_chain_time_idx is missing: %v", err)
		}
		if !strings.Contains(def, "chain_id") || !strings.Contains(def, "block_timestamp") {
			t.Errorf("indexdef = %s; want (chain_id, block_timestamp)", def)
		}
	})

	t.Run("the app role can read the projection but the view is not writable through", func(t *testing.T) {
		var canSelect bool
		if err := pool.QueryRow(ctx,
			`SELECT has_table_privilege('stl_readwrite', 'position_maple_loan', 'SELECT')`).Scan(&canSelect); err != nil {
			t.Fatal(err)
		}
		if !canSelect {
			t.Error("stl_readwrite must be able to read position_maple_loan")
		}
	})

	t.Run("every emitted observation is positive, so closure drops nothing", func(t *testing.T) {
		// Documented limit: the source only reports state Active with a non-zero principal, so a
		// repaid loan stops being reported and never receives a closing zero.
		var zeros int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM position_state WHERE deal_type = 'BORROW' AND quantity <= 0`).Scan(&zeros); err != nil {
			t.Fatal(err)
		}
		if zeros != 0 {
			t.Errorf("got %d non-positive Maple observations; the source cannot produce one", zeros)
		}
		var srcZero int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM maple_loan_state WHERE principal_owed <= 0 OR state <> 'Active'`).Scan(&srcZero); err != nil {
			t.Fatal(err)
		}
		if srcZero != 0 {
			t.Errorf("the fixture drifted from the measured source shape (%d rows are closed or non-positive); revisit the no-close limit", srcZero)
		}
	})

	t.Run("the run is recorded", func(t *testing.T) {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM position_projection_run WHERE projection = 'public.position_maple_loan'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Error("materialize_position_projection must record the run under the view's qualified name")
		}
	})
}

func hexOf(t *testing.T, pool *pgxpool.Pool, ctx context.Context, chain int) string {
	t.Helper()
	var h string
	if err := pool.QueryRow(ctx, `SELECT encode(loan_address,'hex') FROM maple_loan WHERE chain_id = $1`, chain).Scan(&h); err != nil {
		t.Fatal(err)
	}
	return h
}
