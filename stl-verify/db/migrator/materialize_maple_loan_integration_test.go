//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// VEC-405: position_maple_loan places each loan cycle at the surviving block at or before it, and closes a
// loan from its absence at a cycle whose pool's principal_out equals the loans that cycle returned.

const mapleTolerance = "10 minutes"

type mapleLoanSpec struct {
	chain int
	pool  string
}

type mapleFixture struct {
	pool  *pgxpool.Pool
	ctx   context.Context
	loans map[string]int64
	// loanPool maps a loan to its pool tag, so a fetch can write every pool's principal_out.
	loanPool map[string]string
	pools    map[string]int64
}

// seedMaple builds the registry once per test function: two chains, the named pools on the chain their
// tag starts with, and the named loans in them. Every subtest then resets the time-series tables.
func seedMaple(ctx context.Context, t *testing.T, pool *pgxpool.Pool, loans map[string]mapleLoanSpec) *mapleFixture {
	t.Helper()
	f := &mapleFixture{pool: pool, ctx: ctx, loans: map[string]int64{}, loanPool: map[string]string{}, pools: map[string]int64{}}
	f.exec(t, `INSERT INTO protocol (chain_id, address, name, protocol_type)
	           SELECT c, decode(md5('maple-protocol' || c) || 'a1b2c3d4', 'hex'), 'Maple', 'lending'
	           FROM (VALUES (1), (8453)) v(c) ON CONFLICT DO NOTHING`)
	f.exec(t, `INSERT INTO token (chain_id, address, symbol, decimals)
	           SELECT c, decode(md5('maple-asset' || c) || 'a1b2c3d4', 'hex'), 'MPLUSDC', 6
	           FROM (VALUES (1), (8453)) v(c) ON CONFLICT DO NOTHING`)
	names := make([]string, 0, len(loans))
	for name := range loans {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		spec := loans[name]
		f.ensurePool(t, spec.chain, spec.pool)
		f.exec(t, `INSERT INTO "user" (chain_id, address) VALUES ($1, decode(md5($2) || 'a1b2c3d4', 'hex'))
		           ON CONFLICT DO NOTHING`, spec.chain, "borrower-"+name)
		f.exec(t, `INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
		           SELECT $1, p.id, decode(md5($2) || 'a1b2c3d4', 'hex'), $4, u.id
		           FROM protocol p
		           JOIN "user" u ON u.chain_id = $1 AND u.address = decode(md5($3) || 'a1b2c3d4', 'hex')
		           WHERE p.chain_id = $1 AND p.name = 'Maple'`,
			spec.chain, "loan-"+name, "borrower-"+name, f.pools[spec.pool])
		var id int64
		if err := pool.QueryRow(ctx,
			`SELECT id FROM maple_loan WHERE loan_address = decode(md5($1) || 'a1b2c3d4', 'hex')`, "loan-"+name).Scan(&id); err != nil {
			t.Fatalf("seeding loan %s: %v", name, err)
		}
		f.loans[name] = id
		f.loanPool[name] = spec.pool
	}
	return f
}

func (f *mapleFixture) ensurePool(t *testing.T, chain int, tag string) {
	t.Helper()
	if _, ok := f.pools[tag]; ok {
		return
	}
	var id int64
	if err := f.pool.QueryRow(f.ctx,
		`INSERT INTO maple_pool (chain_id, protocol_id, address, asset_token_id)
		 SELECT p.chain_id, p.id, decode(md5('maple-pool-' || $2) || 'a1b2c3d4', 'hex'), tk.id
		 FROM protocol p JOIN token tk ON tk.chain_id = p.chain_id AND tk.symbol = 'MPLUSDC'
		 WHERE p.chain_id = $1 AND p.name = 'Maple'
		 RETURNING id`, chain, tag).Scan(&id); err != nil {
		t.Fatalf("seeding pool %s: %v", tag, err)
	}
	f.pools[tag] = id
}

func (f *mapleFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", strings.Join(strings.Fields(sql), " "), err)
	}
}

// reset empties every table a run reads or writes, so each subtest stands alone and can run by itself.
func (f *mapleFixture) reset(t *testing.T) {
	t.Helper()
	for _, table := range []string{
		"position_current", "position_state", "position_projection_refusal", "position_projection_run",
		"maple_loan_state", "maple_pool_state", "block_meta",
	} {
		f.exec(t, "DELETE FROM "+table)
	}
}

// blocks seeds count blocks from startBN, stepSec apart.
func (f *mapleFixture) blocks(t *testing.T, chain, startBN int, startTS string, count, stepSec int) {
	t.Helper()
	f.exec(t, `INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
	           SELECT $1, $2 + g, 0, $3::timestamptz + (g * $4 || ' seconds')::interval
	           FROM generate_series(0, $5 - 1) g ON CONFLICT DO NOTHING`,
		chain, startBN, startTS, stepSec, count)
}

func (f *mapleFixture) block(t *testing.T, chain, bn, bv int, ts string) {
	t.Helper()
	f.blockPV(t, chain, bn, bv, 0, ts)
}

func (f *mapleFixture) blockPV(t *testing.T, chain, bn, bv, pv int, ts string) {
	t.Helper()
	f.exec(t, `INSERT INTO block_meta (chain_id, block_number, block_version, processing_version, block_timestamp)
	           VALUES ($1, $2, $3, $4, $5::timestamptz)`, chain, bn, bv, pv, ts)
}

// cycle writes one loan row. build distinguishes a reprocessed cycle, which the source versions.
func (f *mapleFixture) cycle(t *testing.T, loan, ts, principal string, build int) {
	t.Helper()
	f.exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, build_id)
	           VALUES ($1, $2::timestamptz, 'Active', $3::numeric, $4)`, f.loans[loan], ts, principal, build)
}

func (f *mapleFixture) cycleState(t *testing.T, loan, ts, state, principal string) {
	t.Helper()
	f.exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, build_id)
	           VALUES ($1, $2::timestamptz, $3, $4::numeric, 0)`, f.loans[loan], ts, state, principal)
}

func (f *mapleFixture) poolCycle(t *testing.T, tag, ts string, principalOut int64, build int) {
	t.Helper()
	f.exec(t, `INSERT INTO maple_pool_state (maple_pool_id, synced_at, liquid_assets, principal_out, utilization, build_id)
	           VALUES ($1, $2::timestamptz, 0, $3, 0, $4)`, f.pools[tag], ts, principalOut, build)
}

// fetch writes one sync cycle as the indexer does: the loans the loan query returned, and for every pool
// a principal_out that also counts the loans the query missed. An empty missed map is a complete fetch.
func (f *mapleFixture) fetch(t *testing.T, ts string, reported, missed map[string]int64) {
	t.Helper()
	out := map[string]int64{}
	for tag := range f.pools {
		out[tag] = 0
	}
	for loan, owed := range reported {
		f.cycle(t, loan, ts, fmt.Sprint(owed), 1)
		out[f.loanPool[loan]] += owed
	}
	for loan, owed := range missed {
		out[f.loanPool[loan]] += owed
	}
	for tag, total := range out {
		f.poolCycle(t, tag, ts, total, 1)
	}
}

func (f *mapleFixture) runWith(t *testing.T, tolerance string) (int64, error) {
	t.Helper()
	var n int64
	err := f.pool.QueryRow(f.ctx, `SELECT materialize_maple_loan(p_build_id => 0, p_max_skew => $1::interval)`, tolerance).Scan(&n)
	return n, err
}

func (f *mapleFixture) run(t *testing.T) (int64, error) {
	t.Helper()
	return f.runWith(t, mapleTolerance)
}

func (f *mapleFixture) mustRun(t *testing.T) int64 {
	t.Helper()
	n, err := f.run(t)
	if err != nil {
		t.Fatalf("materialize_maple_loan: %v", err)
	}
	return n
}

// runCollectingWarnings runs on its own connection and returns the WARNINGs the run raised.
func (f *mapleFixture) runCollectingWarnings(t *testing.T) []string {
	t.Helper()
	cfg := f.pool.Config().ConnConfig.Copy()
	var warnings []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if n.Severity == "WARNING" && strings.HasPrefix(n.Message, "materialize_maple_loan:") {
			warnings = append(warnings, n.Message)
		}
	}
	conn, err := pgx.ConnectConfig(f.ctx, cfg)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer func() {
		if err := conn.Close(f.ctx); err != nil {
			t.Error(err)
		}
	}()
	if _, err := conn.Exec(f.ctx, `SELECT materialize_maple_loan(p_build_id => 0, p_max_skew => $1::interval)`, mapleTolerance); err != nil {
		t.Fatalf("materialize_maple_loan: %v", err)
	}
	return warnings
}

// mustRefuse runs and returns the refusal, failing unless it contains want.
func (f *mapleFixture) mustRefuse(t *testing.T, tolerance, want string) string {
	t.Helper()
	_, err := f.runWith(t, tolerance)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("want a refusal containing %q, got %v", want, err)
	}
	return err.Error()
}

// text renders a SQL expression in the session's own formatting, so an expected message does not
// depend on IntervalStyle, DateStyle or TimeZone.
func (f *mapleFixture) text(t *testing.T, expr string) string {
	t.Helper()
	var s string
	if err := f.pool.QueryRow(f.ctx, `SELECT (`+expr+`)::text`).Scan(&s); err != nil {
		t.Fatalf("rendering %s: %v", expr, err)
	}
	return s
}

func (f *mapleFixture) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
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

// rows reads the spine for one named loan.
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

// mustRows reads the spine for a loan and fails unless it holds exactly want rows.
func (f *mapleFixture) mustRows(t *testing.T, loan string, want int) []mapleRow {
	t.Helper()
	rs := f.rows(t, loan)
	if len(rs) != want {
		t.Fatalf("loan %s stored %d rows, want %d: %+v", loan, len(rs), want, rs)
	}
	return rs
}

func (f *mapleFixture) zeros(t *testing.T, loan string) []mapleRow {
	t.Helper()
	var out []mapleRow
	for _, r := range f.rows(t, loan) {
		if r.qty == "0" {
			out = append(out, r)
		}
	}
	return out
}

// blockAt is the oracle for a placement: the surviving block at or before ts, computed independently.
func (f *mapleFixture) blockAt(t *testing.T, chain int, ts string) int64 {
	t.Helper()
	var bn int64
	if err := f.pool.QueryRow(f.ctx, `
		SELECT block_number FROM (
		    SELECT DISTINCT ON (block_number) block_number, block_timestamp
		    FROM block_meta WHERE chain_id = $1
		    ORDER BY block_number, block_version DESC, processing_version DESC) s
		WHERE block_timestamp <= $2::timestamptz
		ORDER BY block_timestamp DESC, block_number DESC LIMIT 1`, chain, ts).Scan(&bn); err != nil {
		t.Fatalf("no block at or before %s on chain %d: %v", ts, chain, err)
	}
	return bn
}

func TestMapleLoanPlacement(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]mapleLoanSpec{"a": {1, "p1"}, "b": {8453, "p8453"}, "c": {1, "p1"}})

	// Blocks every 2 minutes from 08:00 on chain 1, offset a minute on 8453; cycles sit inside tolerance.
	seedBlocks := func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
		f.blocks(t, 8453, 500, "2026-06-16T08:01:00Z", 60, 120)
	}

	t.Run("a cycle is placed at the last block at or before it, with that block's timestamp and the loan's identity", func(t *testing.T) {
		seedBlocks(t)
		// 08:35 falls between block 1017 (08:34) and 1018 (08:36).
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		if n := f.mustRun(t); n != 1 {
			t.Fatalf("appended %d rows, want 1", n)
		}
		r := f.mustRows(t, "a", 1)[0]
		if r.bn != 1017 || !r.ts.Equal(time.Date(2026, 6, 16, 8, 34, 0, 0, time.UTC)) {
			t.Errorf("placed at block %d stamped %s; want block 1017 stamped its own 08:34", r.bn, r.ts)
		}
		if r.qty != "500" || r.dealType != "BORROW" || r.chain != 1 {
			t.Errorf("qty=%s deal_type=%s chain=%d; want 500 / BORROW / 1", r.qty, r.dealType, r.chain)
		}
		var wantProto int64
		var wantAddr, wantHolder string
		if err := pool.QueryRow(ctx,
			`SELECT l.protocol_id, encode(l.loan_address,'hex'), encode(u.address,'hex')
			 FROM maple_loan l JOIN "user" u ON u.id = l.borrower_user_id WHERE l.id = $1`,
			f.loans["a"]).Scan(&wantProto, &wantAddr, &wantHolder); err != nil {
			t.Fatal(err)
		}
		if r.protocolID != wantProto || r.instrument != wantAddr || r.holder != wantHolder {
			t.Errorf("identity %d/%s/%s; want the loan's protocol %d, address %s and borrower %s",
				r.protocolID, r.instrument, r.holder, wantProto, wantAddr, wantHolder)
		}
	})

	t.Run("a cycle at a block's exact header instant belongs to that block", func(t *testing.T) {
		seedBlocks(t)
		f.cycle(t, "a", "2026-06-16T08:40:00Z", "600", 1)
		f.mustRun(t)
		if r := f.mustRows(t, "a", 1)[0]; r.bn != 1020 {
			t.Errorf("a cycle exactly at 08:40 placed at block %d; want 1020", r.bn)
		}
	})

	t.Run("cycles sharing a block collapse to one observation, the earliest synced_at winning", func(t *testing.T) {
		seedBlocks(t)
		// Seeded out of order and all present before the first run, so the pick is under test.
		f.cycle(t, "c", "2026-06-16T09:01:30Z", "700", 1)
		f.cycle(t, "c", "2026-06-16T09:00:10Z", "710", 1)
		f.cycle(t, "c", "2026-06-16T09:00:50Z", "720", 1)
		f.mustRun(t)
		if r := f.mustRows(t, "c", 1)[0]; r.qty != "710" {
			t.Errorf("quantity=%s; want 710, the principal of the earliest cycle in that block", r.qty)
		}
	})

	t.Run("blocks sharing a header timestamp resolve to the highest height", func(t *testing.T) {
		seedBlocks(t)
		f.block(t, 1, 5000, 0, "2026-06-16T12:00:00Z")
		f.block(t, 1, 5001, 0, "2026-06-16T12:00:00Z")
		f.cycle(t, "a", "2026-06-16T12:00:30Z", "800", 1)
		f.mustRun(t)
		if r := f.mustRows(t, "a", 1)[0]; r.bn != 5001 {
			t.Errorf("placed at block %d; want 5001, the highest height sharing that header instant", r.bn)
		}
	})

	t.Run("a reorg keeps only the surviving block_version, so height and header time cannot invert", func(t *testing.T) {
		seedBlocks(t)
		f.block(t, 1, 5950, 0, "2026-06-16T13:00:00Z")
		f.block(t, 1, 6000, 0, "2026-06-16T13:00:10Z")
		f.block(t, 1, 6001, 0, "2026-06-16T13:00:20Z")
		f.block(t, 1, 6000, 1, "2026-06-16T13:00:30Z")
		f.block(t, 1, 6001, 1, "2026-06-16T13:00:40Z")
		// 13:00:25 sits after orphan 6001/v0 but before replacement 6000/v1.
		f.cycle(t, "a", "2026-06-16T13:00:25Z", "900", 1)
		f.cycle(t, "a", "2026-06-16T13:00:35Z", "910", 1)
		if _, err := f.run(t); err != nil {
			t.Fatalf("a depth-2 reorg must not wedge the run: %v", err)
		}
		rs := f.mustRows(t, "a", 2)
		if rs[0].bn != 5950 || rs[1].bn != 6000 || rs[1].bv != 1 {
			t.Errorf("placed at %d/%d then %d/%d; want 5950/0 then 6000/1", rs[0].bn, rs[0].bv, rs[1].bn, rs[1].bv)
		}
	})

	t.Run("a block_meta reprocess keeps only the highest processing_version of a height", func(t *testing.T) {
		seedBlocks(t)
		// 7000 is reprocessed with a later header time, past the cycle, so only the pv=0 row precedes it.
		f.block(t, 1, 6999, 0, "2026-06-16T14:00:00Z")
		f.blockPV(t, 1, 7000, 0, 0, "2026-06-16T14:00:10Z")
		f.blockPV(t, 1, 7000, 0, 1, "2026-06-16T14:00:40Z")
		f.cycle(t, "a", "2026-06-16T14:00:20Z", "950", 1)
		f.mustRun(t)
		if r := f.mustRows(t, "a", 1)[0]; r.bn != 6999 {
			t.Errorf("placed at block %d; want 6999, since 7000's surviving version is after the cycle", r.bn)
		}
	})

	t.Run("the block lookup is scoped to the loan's own chain", func(t *testing.T) {
		seedBlocks(t)
		// At 08:36 chain 1 has a block AT 08:36 while chain 8453's latest is 08:35.
		f.cycle(t, "b", "2026-06-16T08:36:00Z", "42", 1)
		f.mustRun(t)
		r := f.mustRows(t, "b", 1)[0]
		if r.chain != 8453 || r.bn != 517 || !r.ts.Equal(time.Date(2026, 6, 16, 8, 35, 0, 0, time.UTC)) {
			t.Errorf("loan b placed at chain %d block %d (%s); want chain 8453 block 517 at 08:35", r.chain, r.bn, r.ts.UTC())
		}
	})

	t.Run("two loans on one chain in one run keep their own identities", func(t *testing.T) {
		seedBlocks(t)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		f.cycle(t, "c", "2026-06-16T08:35:00Z", "501", 1)
		f.mustRun(t)
		a, c := f.mustRows(t, "a", 1)[0], f.mustRows(t, "c", 1)[0]
		if a.instrument == c.instrument || a.holder == c.holder || a.qty != "500" || c.qty != "501" {
			t.Errorf("loans a and c cross-contaminate: %+v / %+v", a, c)
		}
	})

	t.Run("a reprocess of the winning cycle is a new observation at its block carrying the reprocessed value", func(t *testing.T) {
		seedBlocks(t)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		f.mustRun(t)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "550", 2)
		f.mustRun(t)
		rs := f.mustRows(t, "a", 2)
		if rs[0].bn != 1017 || rs[1].bn != 1017 || rs[0].pv != 0 || rs[1].pv != 1 || rs[1].qty != "550" {
			t.Errorf("want 500@pv0 then 550@pv1 at block 1017, got %+v", rs)
		}
	})

	t.Run("a reprocess of a later cycle in the same block does not move that block's observation", func(t *testing.T) {
		seedBlocks(t)
		// Cycles at 08:34:10 and 08:35:00 both resolve to block 1017; only the later one is reprocessed.
		f.cycle(t, "a", "2026-06-16T08:34:10Z", "100", 1)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "200", 1)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "205", 2)
		f.mustRun(t)
		if r := f.mustRows(t, "a", 1)[0]; r.qty != "100" || r.pv != 0 {
			t.Errorf("block 1017 reads %s at pv %d; want the earliest cycle's 100 at pv 0", r.qty, r.pv)
		}
	})

	t.Run("reprocessing the earlier cycle after a later one raises no refusal", func(t *testing.T) {
		seedBlocks(t)
		f.cycle(t, "a", "2026-06-16T08:34:10Z", "100", 1)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "200", 1)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "205", 2)
		f.mustRun(t)
		f.cycle(t, "a", "2026-06-16T08:34:10Z", "105", 2)
		f.mustRun(t)
		f.mustRun(t)
		if n := f.count(t, `SELECT count(*) FROM position_projection_refusal`); n != 0 {
			t.Errorf("%d refusals recorded; a reprocess must not drift a stored key", n)
		}
		rs := f.mustRows(t, "a", 2)
		if rs[1].qty != "105" || rs[1].pv != 1 {
			t.Errorf("want the reprocessed 105 at pv 1, got %+v", rs[1])
		}
	})

	t.Run("re-running with no new source rows appends nothing", func(t *testing.T) {
		seedBlocks(t)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		if n := f.mustRun(t); n != 1 {
			t.Fatalf("first run appended %d rows, want 1", n)
		}
		if n := f.mustRun(t); n != 0 {
			t.Errorf("re-running appended %d rows; want 0", n)
		}
	})

	t.Run("the run records the batch's own latest block_timestamp and build id", func(t *testing.T) {
		seedBlocks(t)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		f.cycle(t, "c", "2026-06-16T09:05:00Z", "501", 1)
		f.mustRun(t)
		var runTS time.Time
		var buildID int
		if err := pool.QueryRow(ctx,
			`SELECT block_timestamp, build_id FROM position_projection_run
			 WHERE projection = 'public.position_maple_loan' ORDER BY created_at DESC LIMIT 1`).Scan(&runTS, &buildID); err != nil {
			t.Fatalf("no run recorded under the view's qualified name: %v", err)
		}
		if !runTS.Equal(time.Date(2026, 6, 16, 9, 4, 0, 0, time.UTC)) || buildID != 0 {
			t.Errorf("run recorded %s build %d; want block 1032's 09:04 and build 0", runTS, buildID)
		}
	})

	t.Run("a block added later closer to a cycle re-places it as a second observation", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 100, 0, "2026-06-16T08:30:00Z")
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		f.mustRun(t)
		f.block(t, 1, 101, 0, "2026-06-16T08:34:00Z")
		if n := f.mustRun(t); n != 1 {
			t.Errorf("appended %d rows; want 1, the cycle re-placed at block 101", n)
		}
		rs := f.mustRows(t, "a", 2)
		if rs[0].bn != 100 || rs[1].bn != 101 {
			t.Errorf("want observations at 100 and 101, got %+v", rs)
		}
	})
}

func TestMapleLoanView(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]mapleLoanSpec{"a": {1, "p1"}})

	t.Run("the view emits an unplaceable cycle with a NULL block rather than dropping it", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 1000, 0, "2026-06-16T10:00:00Z")
		f.cycle(t, "a", "2026-06-16T09:00:00Z", "500", 1)
		n := f.count(t, `SELECT count(*) FROM position_maple_loan WHERE block_number IS NULL AND quantity = 500`)
		if n != 1 {
			t.Errorf("the view emits %d NULL-block rows for a cycle no block precedes; want 1, so the spine refuses it", n)
		}
	})

	t.Run("block_meta carries the index the placement's ORDER BY needs, in that column order", func(t *testing.T) {
		var def string
		if err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'block_meta'
			   AND indexname = 'block_meta_chain_time_idx'`).Scan(&def); err != nil {
			t.Fatalf("block_meta_chain_time_idx is missing: %v", err)
		}
		want := "(chain_id, block_timestamp DESC, block_number DESC, block_version DESC, processing_version DESC)"
		if !strings.Contains(def, want) {
			t.Errorf("indexdef %s; want columns %s", def, want)
		}
	})

	t.Run("the view is not writable, by its shape rather than by a privilege", func(t *testing.T) {
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
	})
}

func TestMapleLoanClose(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]mapleLoanSpec{
		"gone": {1, "p1"}, "peer1": {1, "p1"}, "peer2": {1, "p1"},
		"other": {1, "p2"}, "solo": {8453, "p8453"},
	})
	inst := []string{
		"2026-06-16T08:05:00Z", "2026-06-16T08:15:00Z", "2026-06-16T08:25:00Z",
		"2026-06-16T08:35:00Z", "2026-06-16T08:45:00Z",
	}
	seed := func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
		f.blocks(t, 8453, 500, "2026-06-16T08:00:00Z", 60, 120)
	}
	all := func() map[string]int64 {
		return map[string]int64{"gone": 500, "peer1": 100, "peer2": 200, "other": 300, "solo": 700}
	}
	without := func(names ...string) map[string]int64 {
		m := all()
		for _, n := range names {
			delete(m, n)
		}
		return m
	}

	t.Run("a loan absent from a complete cycle closes there, at pv 0, after every sighting", func(t *testing.T) {
		seed(t)
		for _, ts := range inst[:3] {
			f.fetch(t, ts, all(), nil)
		}
		for _, ts := range inst[3:] {
			f.fetch(t, ts, without("gone"), nil)
		}
		f.mustRun(t)
		rs := f.mustRows(t, "gone", 4)
		last := rs[3]
		if last.qty != "0" || last.pv != 0 {
			t.Fatalf("the final row is %s at pv %d; want the close, 0 at pv 0", last.qty, last.pv)
		}
		if want := f.blockAt(t, 1, inst[3]); last.bn != want {
			t.Errorf("the close landed at block %d; want %d, behind the first cycle the loan is missing from", last.bn, want)
		}
		if rs[2].bn >= last.bn || rs[2].qty != "500" {
			t.Errorf("the last positive is %+v; the close must follow the last sighting", rs[2])
		}
	})

	t.Run("a truncated fetch that drops only this loan closes nothing, and healing leaves no false zero", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		f.fetch(t, inst[1], all(), nil)
		// The loan query loses `gone` for two cycles while every peer is returned; the pool still counts it.
		f.fetch(t, inst[2], without("gone"), map[string]int64{"gone": 500})
		f.fetch(t, inst[3], without("gone"), map[string]int64{"gone": 500})
		f.mustRun(t)
		f.fetch(t, inst[4], all(), nil)
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 0 {
			t.Errorf("stored %d false closes: %+v", len(z), z)
		}
	})

	t.Run("every loan repaid in one cycle closes", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		f.fetch(t, inst[1], without("gone", "peer1"), nil)
		f.fetch(t, inst[2], without("gone", "peer1"), nil)
		f.mustRun(t)
		for _, loan := range []string{"gone", "peer1"} {
			if z := f.zeros(t, loan); len(z) != 1 {
				t.Errorf("%s: %d closes; want 1", loan, len(z))
			}
		}
		if z := f.zeros(t, "peer2"); len(z) != 0 {
			t.Errorf("peer2 still reports but was closed: %+v", z)
		}
	})

	t.Run("a pool's only loan closes when the pool reports nothing outstanding", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		f.fetch(t, inst[1], without("solo"), nil)
		f.mustRun(t)
		if z := f.zeros(t, "solo"); len(z) != 1 {
			t.Errorf("%d closes; want 1, since p8453 reported 0 outstanding with no loans", len(z))
		}
	})

	t.Run("a cycle where the loan query returned nothing for a pool that still has principal out closes nothing", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		// The loan indexer is down: pools keep reporting, no loan rows are written at all.
		for _, ts := range inst[1:] {
			f.fetch(t, ts, nil, all())
		}
		f.mustRun(t)
		for loan := range all() {
			if z := f.zeros(t, loan); len(z) != 0 {
				t.Errorf("%s closed during a loan-fetch outage: %+v", loan, z)
			}
		}
	})

	t.Run("a cycle with no pool row closes nothing", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		for _, ts := range inst[1:] {
			for loan, owed := range without("gone") {
				f.cycle(t, loan, ts, fmt.Sprint(owed), 1)
			}
		}
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 0 {
			t.Errorf("closed with no completeness evidence: %+v", z)
		}
	})

	t.Run("completeness is per pool, so a loss in one pool is not offset by a gain in another", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		// p1 drops `gone` (500) by truncation while p2's loan grows by exactly 500: the all-pool sum still agrees.
		for _, ts := range inst[1:] {
			reported := without("gone")
			reported["other"] += 500
			for loan, owed := range reported {
				f.cycle(t, loan, ts, fmt.Sprint(owed), 1)
			}
			f.poolCycle(t, "p1", ts, 500+100+200, 1)
			f.poolCycle(t, "p2", ts, 800, 1)
			f.poolCycle(t, "p8453", ts, 700, 1)
		}
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 0 {
			t.Errorf("closed although p1's loans fall 500 short of its principal_out: %+v", z)
		}
	})

	t.Run("completeness reads the pool's highest processing_version", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		f.fetch(t, inst[1], without("gone"), nil)
		// Reprocessed pool row at inst[1] now counts `gone` again, so that cycle is no longer complete.
		f.poolCycle(t, "p1", inst[1], 800, 2)
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 0 {
			t.Errorf("closed on a superseded pool row: %+v", z)
		}
		f.poolCycle(t, "p1", inst[1], 300, 3)
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 1 {
			t.Errorf("%d closes; want 1 once the highest version agrees again", len(z))
		}
	})

	t.Run("a pool reprocess at another instant does not hide a complete cycle", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		f.fetch(t, inst[1], without("gone"), nil)
		f.poolCycle(t, "p1", inst[0], 800, 2)
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 1 {
			t.Errorf("%d closes; want 1, since inst[1] is still complete", len(z))
		}
	})

	t.Run("a loan left open past a day with no complete pool cycle is warned about by name", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 300, 600)
		f.blocks(t, 8453, 500, "2026-06-16T08:00:00Z", 300, 600)
		f.fetch(t, "2026-06-16T08:05:00Z", all(), nil)
		// principal_out also counts 1000 the loan query never returns, so no later cycle is complete.
		for _, ts := range []string{"2026-06-17T09:05:00Z", "2026-06-17T09:15:00Z"} {
			for loan, owed := range without("gone") {
				f.cycle(t, loan, ts, fmt.Sprint(owed), 1)
			}
			f.poolCycle(t, "p1", ts, 300+1000, 1)
			f.poolCycle(t, "p2", ts, 300, 1)
			f.poolCycle(t, "p8453", ts, 700, 1)
		}
		warnings := f.runCollectingWarnings(t)
		if len(warnings) != 1 || !strings.Contains(warnings[0], fmt.Sprintf("loan %d (chain 1) open at 500", f.loans["gone"])) {
			t.Errorf("warnings %q; want one naming loan %d open at 500", warnings, f.loans["gone"])
		}
		if z := f.zeros(t, "gone"); len(z) != 0 {
			t.Errorf("closed without a complete cycle: %+v", z)
		}
	})

	t.Run("a closed loan raises no stall warning", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 300, 600)
		f.blocks(t, 8453, 500, "2026-06-16T08:00:00Z", 300, 600)
		f.fetch(t, "2026-06-16T08:05:00Z", all(), nil)
		f.fetch(t, "2026-06-17T09:05:00Z", without("gone"), nil)
		if warnings := f.runCollectingWarnings(t); len(warnings) != 0 {
			t.Errorf("warnings %q; want none once the loan closed", warnings)
		}
	})

	t.Run("completeness reads each loan's highest processing_version", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		for loan, owed := range without("gone") {
			f.cycle(t, loan, inst[1], fmt.Sprint(owed), 1)
		}
		f.poolCycle(t, "p1", inst[1], 350, 1)
		f.poolCycle(t, "p2", inst[1], 300, 1)
		f.poolCycle(t, "p8453", inst[1], 700, 1)
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 0 {
			t.Fatalf("closed while p1's loans sum to 300 against 350: %+v", z)
		}
		// peer1 is reprocessed to 150, so p1's latest loans now sum to 350.
		f.cycle(t, "peer1", inst[1], "150", 2)
		f.mustRun(t)
		if z := f.zeros(t, "gone"); len(z) != 1 {
			t.Errorf("%d closes; want 1 once the reprocessed loan completes the pool", len(z))
		}
	})

	t.Run("a close whose first complete cycle shares the last sighting's block moves to a later block", func(t *testing.T) {
		f.reset(t)
		// Blocks every 30 minutes: 08:00, 08:30, 09:00.
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 3, 1800)
		f.blocks(t, 8453, 500, "2026-06-16T08:00:00Z", 3, 1800)
		f.fetch(t, "2026-06-16T08:05:00Z", all(), nil)
		for _, ts := range []string{"2026-06-16T08:15:00Z", "2026-06-16T08:25:00Z", "2026-06-16T08:35:00Z"} {
			f.fetch(t, ts, without("gone"), nil)
		}
		if _, err := f.runWith(t, "30 minutes"); err != nil {
			t.Fatal(err)
		}
		z := f.zeros(t, "gone")
		if len(z) != 1 || z[0].bn != 1001 {
			t.Errorf("closes %+v; want one at block 1001, the first block after the sighting's block 1000", z)
		}
	})

	t.Run("a block at the last sighting's exact instant does not count as the next block", func(t *testing.T) {
		f.reset(t)
		// The sighting at 08:30 places at block 1001 (08:30); block 1002 is the first block after it.
		f.block(t, 1, 1000, 0, "2026-06-16T08:00:00Z")
		f.block(t, 1, 1001, 0, "2026-06-16T08:30:00Z")
		f.block(t, 1, 1002, 0, "2026-06-16T08:40:00Z")
		f.block(t, 8453, 500, 0, "2026-06-16T08:00:00Z")
		f.fetch(t, "2026-06-16T08:30:00Z", all(), nil)
		f.fetch(t, "2026-06-16T08:35:00Z", without("gone"), nil)
		f.fetch(t, "2026-06-16T08:45:00Z", without("gone"), nil)
		if _, err := f.runWith(t, "1 hour"); err != nil {
			t.Fatal(err)
		}
		if z := f.zeros(t, "gone"); len(z) != 1 || z[0].bn != 1002 {
			t.Errorf("closes %+v; want one at block 1002", z)
		}
	})

	t.Run("no close is emitted until a block follows the last sighting, then it is", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 1000, 0, "2026-06-16T08:00:00Z")
		f.block(t, 8453, 500, 0, "2026-06-16T08:00:00Z")
		f.fetch(t, "2026-06-16T08:05:00Z", all(), nil)
		f.fetch(t, "2026-06-16T08:06:00Z", without("gone"), nil)
		if _, err := f.runWith(t, "1 hour"); err != nil {
			t.Fatal(err)
		}
		if z := f.zeros(t, "gone"); len(z) != 0 {
			t.Fatalf("closed in the sighting's own block: %+v", z)
		}
		f.block(t, 1, 1001, 0, "2026-06-16T08:06:00Z")
		if _, err := f.runWith(t, "1 hour"); err != nil {
			t.Fatal(err)
		}
		if z := f.zeros(t, "gone"); len(z) != 1 || z[0].bn != 1001 {
			t.Errorf("closes %+v; want one at block 1001", z)
		}
	})

	t.Run("the next block after a sighting is looked up on the loan's own chain", func(t *testing.T) {
		f.reset(t)
		// Chain 8453 has a block at 08:07, before chain 1's next block at 08:20.
		f.block(t, 1, 1000, 0, "2026-06-16T08:00:00Z")
		f.block(t, 1, 1001, 0, "2026-06-16T08:20:00Z")
		f.block(t, 8453, 500, 0, "2026-06-16T08:00:00Z")
		f.block(t, 8453, 501, 0, "2026-06-16T08:07:00Z")
		f.fetch(t, "2026-06-16T08:05:00Z", all(), nil)
		f.fetch(t, "2026-06-16T08:08:00Z", without("gone"), nil)
		f.fetch(t, "2026-06-16T08:25:00Z", without("gone"), nil)
		if _, err := f.runWith(t, "1 hour"); err != nil {
			t.Fatal(err)
		}
		if z := f.zeros(t, "gone"); len(z) != 1 || z[0].bn != 1001 {
			t.Errorf("closes %+v; want one at chain 1's next block 1001", z)
		}
	})

	t.Run("loans still reporting are never closed, and a re-run adds no second close", func(t *testing.T) {
		seed(t)
		f.fetch(t, inst[0], all(), nil)
		for _, ts := range inst[1:] {
			f.fetch(t, ts, without("gone"), nil)
		}
		f.mustRun(t)
		if n := f.mustRun(t); n != 0 {
			t.Errorf("re-running appended %d rows; want 0", n)
		}
		if z := f.zeros(t, "gone"); len(z) != 1 {
			t.Errorf("%d closes; want exactly 1", len(z))
		}
		for loan := range without("gone") {
			if z := f.zeros(t, loan); len(z) != 0 {
				t.Errorf("%s is still reporting but was closed: %+v", loan, z)
			}
		}
	})
}

func TestMapleLoanRefusals(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]mapleLoanSpec{"a": {1, "p1"}, "b": {8453, "p8453"}})

	appended := func(t *testing.T) {
		t.Helper()
		if n := f.count(t, `SELECT count(*) FROM position_state`); n != 0 {
			t.Errorf("a refused run appended %d rows; want none", n)
		}
	}

	t.Run("a cycle no block precedes is refused naming its chain and earliest cycle", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T10:00:00Z", 10, 120)
		f.cycle(t, "a", "2026-06-16T09:00:00Z", "500", 1)
		msg := f.mustRefuse(t, mapleTolerance, "no surviving block precedes")
		want := fmt.Sprintf("chain 1: 1 cycle(s) that no surviving block precedes, earliest at %s",
			f.text(t, `'2026-06-16T09:00:00Z'::timestamptz`))
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q; want it to contain %q", msg, want)
		}
		appended(t)
	})

	t.Run("the placement check names only the offending chain", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
		f.blocks(t, 8453, 500, "2026-06-16T11:00:00Z", 10, 120)
		f.cycle(t, "a", "2026-06-16T08:35:00Z", "500", 1)
		// Chain 1's last block (09:58) is within tolerance of this cycle, so only a chain-scoped lookup refuses it.
		f.cycle(t, "b", "2026-06-16T10:05:00Z", "42", 1)
		msg := f.mustRefuse(t, mapleTolerance, "chain 8453")
		if strings.Contains(msg, "chain 1:") {
			t.Errorf("chain 1 places fine but is named: %s", msg)
		}
	})

	t.Run("a chain with no blocks at all is refused as unloaded, not as late history", func(t *testing.T) {
		f.reset(t)
		f.cycle(t, "a", "2026-06-16T09:00:00Z", "500", 1)
		f.mustRefuse(t, mapleTolerance, "chain 1: block_meta holds no blocks for this chain")
		appended(t)
	})

	t.Run("a cycle preceded only by an orphaned version is refused by name", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 100, 0, "2026-06-16T10:00:00Z")
		f.block(t, 1, 100, 1, "2026-06-16T12:00:00Z")
		f.cycle(t, "a", "2026-06-16T11:00:00Z", "500", 1)
		f.mustRefuse(t, "1 day", "chain 1: 1 cycle(s) that no surviving block precedes")
		appended(t)
	})

	t.Run("a cycle at the earliest block's exact instant is placeable", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 2000, "2026-06-16T10:00:00Z", 5, 120)
		f.cycle(t, "a", "2026-06-16T10:00:00Z", "500", 1)
		if _, err := f.run(t); err != nil {
			t.Fatalf("a cycle exactly at the earliest block's instant must be placeable: %v", err)
		}
	})

	t.Run("a cycle further than the tolerance from its block is refused, quantifying the gap", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 100, 0, "2026-01-01T00:00:00Z")
		f.cycle(t, "a", "2026-06-16T08:00:00Z", "500", 1)
		msg := f.mustRefuse(t, mapleTolerance, "stale by up to")
		gap := f.text(t, `'2026-06-16T08:00:00Z'::timestamptz - '2026-01-01T00:00:00Z'::timestamptz`)
		if !strings.Contains(msg, "stale by up to "+gap+",") {
			t.Errorf("refusal %q; want the gap %s", msg, gap)
		}
		appended(t)
	})

	t.Run("the tolerance is inclusive to the microsecond", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 100, 0, "2026-06-16T08:00:00Z")
		f.cycle(t, "a", "2026-06-16T08:10:00Z", "500", 1)
		if _, err := f.runWith(t, "10 minutes"); err != nil {
			t.Errorf("a gap equal to the tolerance must be accepted: %v", err)
		}
		f.reset(t)
		f.block(t, 1, 100, 0, "2026-06-16T08:00:00Z")
		f.cycle(t, "a", "2026-06-16T08:10:00Z", "500", 1)
		f.mustRefuse(t, "9 minutes 59.999999 seconds", "stale by up to")
	})

	t.Run("a widened tolerance accepts the gap", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 100, 0, "2026-01-01T00:00:00Z")
		f.cycle(t, "a", "2026-06-16T08:00:00Z", "500", 1)
		if n, err := f.runWith(t, "200 days"); err != nil || n != 1 {
			t.Errorf("an explicitly widened tolerance appended %d rows, err %v; want 1", n, err)
		}
	})

	t.Run("a pool cycle too far from its block is refused, since it can place a close", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 10, 120)
		f.cycle(t, "a", "2026-06-16T08:05:00Z", "500", 1)
		f.poolCycle(t, "p1", "2026-06-16T08:05:00Z", 500, 1)
		f.poolCycle(t, "p1", "2026-06-16T12:00:00Z", 0, 1)
		f.mustRefuse(t, mapleTolerance, "chain 1: 1 cycle(s) stale by up to")
		appended(t)
	})

	t.Run("a pool cycle before any of its loans was seen places no close and is not checked", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 10, 120)
		f.poolCycle(t, "p1", "2026-06-01T00:00:00Z", 0, 1)
		f.cycle(t, "a", "2026-06-16T08:05:00Z", "500", 1)
		f.poolCycle(t, "p1", "2026-06-16T08:05:00Z", 500, 1)
		if _, err := f.run(t); err != nil {
			t.Errorf("a pool row older than block_meta, before the pool's first loan cycle, refused the run: %v", err)
		}
	})

	seedOldOffenders := func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 1, 0, "2020-01-01T00:00:00Z")
		f.cycle(t, "a", "2020-01-01T00:05:00Z", "500", 1)
		f.poolCycle(t, "p1", "2020-06-01T00:00:00Z", 0, 1)
		f.cycleState(t, "a", "2020-06-01T00:10:00Z", "Repaid", "1")
	}

	t.Run("a bounded run ignores stale pool cycles and non-Active states older than its window", func(t *testing.T) {
		seedOldOffenders(t)
		if _, err := pool.Exec(ctx, `SELECT materialize_maple_loan(0, NULL, INTERVAL '1 hour')`); err != nil {
			t.Errorf("a one-hour window refused on six-year-old rows: %v", err)
		}
	})

	t.Run("an unbounded run refuses the same old offenders", func(t *testing.T) {
		seedOldOffenders(t)
		f.mustRefuse(t, mapleTolerance, "stale by up to")
		f.exec(t, `DELETE FROM maple_pool_state`)
		f.exec(t, `DELETE FROM maple_loan_state WHERE state = 'Active'`)
		f.mustRefuse(t, "10 years", "Repaid x1")
	})

	t.Run("a chain with both unplaceable and stale cycles names both", func(t *testing.T) {
		f.reset(t)
		f.block(t, 1, 100, 0, "2026-06-16T08:00:00Z")
		f.cycle(t, "a", "2026-06-16T07:00:00Z", "500", 1)
		f.cycle(t, "a", "2026-06-16T13:00:00Z", "500", 1)
		msg := f.mustRefuse(t, mapleTolerance, "1 cycle(s) that no surviving block precedes")
		if !strings.Contains(msg, "1 cycle(s) stale by up to") {
			t.Errorf("refusal %q names the unplaceable cycle but hides the stale one", msg)
		}
	})

	t.Run("a pool cycle is checked against the blocks of its loan's chain, not the pool's", func(t *testing.T) {
		f.reset(t)
		f.exec(t, `UPDATE maple_loan SET maple_pool_id = $1 WHERE id = $2`, f.pools["p8453"], f.loans["a"])
		t.Cleanup(func() {
			f.exec(t, `UPDATE maple_loan SET maple_pool_id = $1 WHERE id = $2`, f.pools["p1"], f.loans["a"])
		})
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 21, 60)
		f.blocks(t, 8453, 500, "2026-06-16T08:00:00Z", 400, 120)
		f.cycle(t, "a", "2026-06-16T08:10:00Z", "500", 1)
		f.poolCycle(t, "p8453", "2026-06-16T13:00:00Z", 0, 1)
		f.mustRefuse(t, mapleTolerance, "chain 1: 1 cycle(s) stale by up to")
		appended(t)
	})

	t.Run("an inversion superseded by a block_meta reprocess is not refused", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 3000, "2026-06-16T08:00:00Z", 10, 120)
		f.blockPV(t, 1, 2500, 0, 0, "2026-06-16T08:19:00Z")
		f.blockPV(t, 1, 2500, 0, 1, "2026-06-16T07:50:00Z")
		f.cycle(t, "a", "2026-06-16T08:18:30Z", "500", 1)
		if _, err := f.run(t); err != nil {
			t.Fatalf("a reprocessed header time must replace the inverted one: %v", err)
		}
	})

	t.Run("the wrapper takes the materializer's lock before its checks", func(t *testing.T) {
		// Refusable data: under a different key the checks would run and refuse instead of waiting.
		f.reset(t)
		f.cycle(t, "a", "2026-06-16T09:00:00Z", "500", 1)
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := holder.Rollback(ctx); err != nil {
				t.Error(err)
			}
		}()
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('materialize_position_projection.public.position_maple_loan', 0))`); err != nil {
			t.Fatal(err)
		}
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, `SET lock_timeout = '200ms'`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := conn.Exec(ctx, `RESET lock_timeout`); err != nil {
				t.Error(err)
			}
		}()
		_, err = conn.Exec(ctx, `SELECT materialize_maple_loan()`)
		if err == nil || !strings.Contains(err.Error(), "lock timeout") {
			t.Errorf("want the wrapper to block on the held materializer lock, got %v", err)
		}
	})

	t.Run("a non-Active state inside the window is refused, listing states in order", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
		for i, s := range []string{"S7", "S2", "S9", "S1", "S5", "S3", "S8"} {
			f.cycleState(t, "a", fmt.Sprintf("2026-06-16T08:%02d:00Z", 10+i), s, "1")
		}
		msg := f.mustRefuse(t, mapleTolerance, "cannot classify as an open BORROW")
		var got []string
		for _, m := range regexp.MustCompile(`S\d x`).FindAllString(msg, -1) {
			got = append(got, strings.TrimSuffix(m, " x"))
		}
		if strings.Join(got, ",") != "S1,S2,S3,S5,S7" {
			t.Errorf("reported states %v; want the first five in order, S1,S2,S3,S5,S7", got)
		}
	})

	t.Run("block_meta header times that invert against height are refused naming the block", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 3000, "2026-06-16T08:00:00Z", 10, 120)
		f.block(t, 1, 2500, 0, "2026-06-16T08:19:00Z")
		f.cycle(t, "a", "2026-06-16T08:18:30Z", "500", 1)
		f.mustRefuse(t, mapleTolerance, "precedes block 2500 at")
	})

	t.Run("an orphaned reorg version does not count as an inversion", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 3000, "2026-06-16T08:00:00Z", 10, 120)
		f.block(t, 1, 2500, 0, "2026-06-16T08:19:00Z")
		f.block(t, 1, 2500, 1, "2026-06-16T07:50:00Z")
		f.cycle(t, "a", "2026-06-16T08:18:30Z", "500", 1)
		if _, err := f.run(t); err != nil {
			t.Fatalf("a superseded version must not be read as an inversion: %v", err)
		}
	})

	t.Run("an inversion on a chain maple does not lend on is ignored, and on its own chain is not", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 30, 120)
		f.exec(t, `INSERT INTO chain (chain_id, name) VALUES (99, 'other') ON CONFLICT (chain_id) DO NOTHING`)
		f.block(t, 99, 10, 0, "2026-06-16T10:00:00Z")
		f.block(t, 99, 11, 0, "2026-06-16T09:00:00Z")
		if _, err := f.run(t); err != nil {
			t.Fatalf("an inversion on a chain with no maple loans must not abort: %v", err)
		}
		f.block(t, 1, 100000, 0, "2026-01-01T00:00:00Z")
		f.mustRefuse(t, mapleTolerance, "invert against height")
	})

	t.Run("the inversion check stays linear in the number of blocks", func(t *testing.T) {
		f.reset(t)
		f.exec(t, `INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
		           SELECT 1, 1000000 + g, 0, '2026-07-01T00:00:00Z'::timestamptz + (g * interval '12 seconds')
		           FROM generate_series(1, 40000) g`)
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, `SET statement_timeout = '30s'`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := conn.Exec(ctx, `RESET statement_timeout`); err != nil {
				t.Error(err)
			}
		}()
		if _, err := conn.Exec(ctx, `SELECT materialize_maple_loan(p_build_id => 0)`); err != nil {
			t.Fatalf("the pre-checks must finish well inside 30s on 40,000 blocks: %v", err)
		}
	})

	for _, c := range []struct {
		name, column string
		address      string
		want         string
	}{
		{"a short loan address", "loan", `\x0badc0de`, "4-byte loan address"},
		{"an oversize loan address", "loan", `\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff`, "21-byte loan address"},
		{"a short borrower address", "borrower", `\x0badc0de`, "4-byte borrower address"},
		{"an oversize borrower address", "borrower", `\xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff`, "21-byte borrower address"},
	} {
		t.Run(c.name+" is refused by name", func(t *testing.T) {
			f.reset(t)
			loanAddr, borrower := `decode(md5('loan-bad') || 'a1b2c3d4', 'hex')`, `decode(md5('borrower-bad') || 'a1b2c3d4', 'hex')`
			if c.column == "loan" {
				loanAddr = fmt.Sprintf(`'%s'::bytea`, c.address)
			} else {
				borrower = fmt.Sprintf(`'%s'::bytea`, c.address)
			}
			f.exec(t, `INSERT INTO "user" (chain_id, address) VALUES (1, `+borrower+`)`)
			var badID int64
			if err := pool.QueryRow(ctx, `
				INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
				SELECT 1, p.id, `+loanAddr+`, $1, u.id
				FROM protocol p JOIN "user" u ON u.chain_id = 1 AND u.address = `+borrower+`
				WHERE p.chain_id = 1 AND p.name = 'Maple' RETURNING id`, f.pools["p1"]).Scan(&badID); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				f.exec(t, `DELETE FROM maple_loan_state WHERE maple_loan_id = $1`, badID)
				f.exec(t, `DELETE FROM maple_loan WHERE id = $1`, badID)
				f.exec(t, `DELETE FROM "user" WHERE chain_id = 1 AND address = `+borrower)
			})
			// The check reads maple_loan alone, so a loan with no cycle yet is refused too.
			f.mustRefuse(t, mapleTolerance, c.want)
			f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 10, 120)
			f.exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed)
			           VALUES ($1, '2026-06-16T08:05:00Z', 'Active', 5)`, badID)
			f.mustRefuse(t, mapleTolerance, c.want)
			appended(t)
		})
	}
}

func TestMapleLoanWrapper(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]mapleLoanSpec{"a": {1, "p1"}})

	t.Run("the writer run and build are forwarded, under the parameter names the runner uses", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 100, "2026-04-01T00:00:00Z", 40, 3600)
		f.cycle(t, "a", "2026-04-01T05:00:00Z", "1000", 0)
		var n int64
		if err := pool.QueryRow(ctx, `SELECT materialize_maple_loan(p_build_id => 7, p_max_skew => $1::interval, p_run_id => 9182)`, mapleTolerance).Scan(&n); err != nil {
			t.Fatalf("materialize_maple_loan with a run: %v", err)
		}
		if n != 1 {
			t.Fatalf("appended %d rows, want 1", n)
		}
		stamped := f.count(t, `SELECT count(*) FROM position_state WHERE run_id = 9182 AND build_id = 7`)
		unstamped := f.count(t, `SELECT count(*) FROM position_state WHERE run_id IS DISTINCT FROM 9182`)
		if stamped != 1 || unstamped != 0 {
			t.Errorf("appended rows: %d carry run 9182 at build 7, %d do not, want 1 and 0", stamped, unstamped)
		}
		if runs := f.count(t, `SELECT count(*) FROM position_projection_run
			 WHERE projection = 'public.position_maple_loan' AND run_id = 9182`); runs != 1 {
			t.Errorf("%d run records carry run 9182, want 1", runs)
		}
	})

	t.Run("the window is forwarded to the materializer", func(t *testing.T) {
		f.reset(t)
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
		f.cycle(t, "a", "2026-06-16T08:05:00Z", "500", 1)
		if _, err := pool.Exec(ctx, `SELECT materialize_maple_loan(p_build_id => 0, p_window => interval '36 hours')`); err != nil {
			t.Fatalf("calling with a window: %v", err)
		}
		var window *string
		if err := pool.QueryRow(ctx, `
			SELECT window_interval::text FROM position_projection_run
			 WHERE projection = 'public.position_maple_loan'
			 ORDER BY created_at DESC LIMIT 1`).Scan(&window); err != nil {
			t.Fatalf("reading the run record: %v", err)
		}
		if window == nil || *window != f.text(t, `interval '36 hours'`) {
			t.Errorf("the run recorded window %v; want the 36 hours the wrapper was called with", window)
		}
	})

	t.Run("the search_path is empty, so a role-named schema cannot shadow a table it checks", func(t *testing.T) {
		f.reset(t)
		var cfg []string
		if err := pool.QueryRow(ctx, `
			SELECT coalesce(proconfig, ARRAY[]::text[]) FROM pg_proc
			 WHERE oid = 'public.materialize_maple_loan(integer, bigint, interval, interval)'::regprocedure`).Scan(&cfg); err != nil {
			t.Fatal(err)
		}
		empty := false
		for _, c := range cfg {
			if c == `search_path=""` || c == "search_path=" {
				empty = true
			}
		}
		if !empty {
			t.Errorf("proconfig %v; want an empty search_path", cfg)
		}
		f.exec(t, `INSERT INTO "user" (chain_id, address) VALUES (1, '\x0badc0de')`)
		var badID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
			SELECT 1, p.id, decode(md5('loan-shadowed') || 'a1b2c3d4', 'hex'), $1, u.id
			FROM protocol p JOIN "user" u ON u.chain_id = 1 AND u.address = '\x0badc0de'
			WHERE p.chain_id = 1 AND p.name = 'Maple' RETURNING id`, f.pools["p1"]).Scan(&badID); err != nil {
			t.Fatal(err)
		}
		var role string
		if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
			t.Fatal(err)
		}
		shadow := pgx.Identifier{role}.Sanitize()
		f.exec(t, `CREATE SCHEMA `+shadow)
		f.exec(t, `CREATE TABLE `+shadow+`."user" AS SELECT id, decode(repeat('ab', 20), 'hex') AS address FROM public."user"`)
		t.Cleanup(func() {
			f.exec(t, `DROP SCHEMA `+shadow+` CASCADE`)
			f.exec(t, `DELETE FROM maple_loan WHERE id = $1`, badID)
			f.exec(t, `DELETE FROM "user" WHERE chain_id = 1 AND address = '\x0badc0de'`)
		})
		f.mustRefuse(t, mapleTolerance, "4-byte borrower address")
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, `SET search_path = pg_catalog`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := conn.Exec(ctx, `RESET search_path`); err != nil {
				t.Error(err)
			}
		}()
		if _, err := conn.Exec(ctx, `SELECT public.materialize_maple_loan()`); err == nil || !strings.Contains(err.Error(), "4-byte borrower address") {
			t.Errorf("under a caller search_path without public, want the address refusal, got %v", err)
		}
	})

	t.Run("re-applying the migration over a one-argument signature leaves one callable function", func(t *testing.T) {
		f.reset(t)
		f.exec(t, `CREATE FUNCTION public.materialize_maple_loan(p_build_id integer DEFAULT 0) RETURNS bigint
		           LANGUAGE sql AS 'SELECT 0::bigint'`)
		f.exec(t, `DELETE FROM migrations WHERE filename = '20260909_140000_materialize_maple_loan.sql'`)
		if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
			t.Fatalf("re-applying: %v", err)
		}
		if n := f.count(t, `SELECT count(*) FROM pg_proc WHERE proname = 'materialize_maple_loan'`); n != 1 {
			t.Errorf("%d signatures of materialize_maple_loan exist; want 1", n)
		}
		f.blocks(t, 1, 1000, "2026-06-16T08:00:00Z", 60, 120)
		if _, err := pool.Exec(ctx, `SELECT materialize_maple_loan()`); err != nil {
			t.Errorf("a call with no arguments is ambiguous or fails: %v", err)
		}
	})
}

// seedPlacementVolume gives the planner enough rows to choose the shapes it chooses at staging volume:
// a dense block series with reorged heights, loan cycles across it, and a pool cycle beside each.
func seedPlacementVolume(t *testing.T, f *mapleFixture) {
	t.Helper()
	f.blocks(t, 1, 1000, "2026-06-16T00:00:00Z", 15000, 12)
	f.exec(t, `INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
	           SELECT chain_id, block_number, 1, block_timestamp + interval '1 second'
	           FROM block_meta WHERE chain_id = 1 AND block_number % 97 = 0`)
	f.exec(t, `INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, build_id)
	           SELECT l.id, '2026-06-16T00:05:00Z'::timestamptz + k * interval '10 minutes', 'Active', 100, 0
	           FROM maple_loan l CROSS JOIN generate_series(0, 280) k
	           WHERE l.chain_id = 1 AND (k < 200 OR l.id % 2 = 0)`)
	f.exec(t, `INSERT INTO maple_pool_state (maple_pool_id, synced_at, liquid_assets, principal_out, utilization)
	           SELECT s.maple_pool_id, s.synced_at, 0, sum(s.principal_owed), 0
	           FROM (SELECT l.maple_pool_id, st.synced_at, st.principal_owed
	                 FROM maple_loan_state st JOIN maple_loan l ON l.id = st.maple_loan_id) s
	           GROUP BY 1, 2`)
	f.exec(t, `ANALYZE block_meta`)
	f.exec(t, `ANALYZE maple_loan_state`)
	f.exec(t, `ANALYZE maple_pool_state`)
}

// explainLines returns the plan of query, one line per element.
func explainLines(ctx context.Context, t *testing.T, pool *pgxpool.Pool, query string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, "EXPLAIN (COSTS OFF) "+query)
	if err != nil {
		t.Fatalf("explaining %s: %v", query, err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scanning a plan line: %v", err)
		}
		plan = append(plan, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading the plan: %v", err)
	}
	return plan
}

// The placement's time bound must be an index condition on block_meta_chain_time_idx, or each cycle
// sorts the chain's blocks.
func TestMapleLoanPlacementReadsTheTimeIndex(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]mapleLoanSpec{"a": {1, "p1"}, "b": {1, "p1"}, "c": {1, "p2"}})
	seedPlacementVolume(t, f)

	plan := explainLines(ctx, t, pool, `SELECT * FROM position_maple_loan`)
	bounded := false
	for i, line := range plan {
		if !strings.Contains(line, "block_meta_chain_time_idx") {
			continue
		}
		for _, next := range plan[i+1:] {
			if strings.Contains(next, "->") {
				break
			}
			if strings.Contains(next, "Index Cond:") && strings.Contains(next, "block_timestamp <=") {
				bounded = true
			}
		}
	}
	if !bounded {
		t.Errorf("placement does not bound block_meta_chain_time_idx by block_timestamp:\n%s", strings.Join(plan, "\n"))
	}
}

// pairwiseFilter is a join or scan filter comparing a block or pool instant with a cycle instant, which
// is evaluated for every pair instead of as an index condition.
var pairwiseFilter = regexp.MustCompile(`Filter: .*(block_timestamp (<=|>) \S*synced_at|synced_at (>=|<) \S*block_timestamp|synced_at >= \S*nb\.block_timestamp)`)

// Every plan a run executes is captured, since the checks live inside the function where EXPLAIN cannot reach.
func TestMapleLoanRunComparesNoCycleWithEveryBlock(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	f := seedMaple(ctx, t, pool, map[string]mapleLoanSpec{"a": {1, "p1"}, "b": {1, "p1"}, "c": {1, "p2"}})
	seedPlacementVolume(t, f)

	cfg := pool.Config().ConnConfig.Copy()
	var plans []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if strings.Contains(n.Message, "plan:") {
			plans = append(plans, n.Message)
		}
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer conn.Close(ctx)
	for _, stmt := range []string{
		`LOAD 'auto_explain'`,
		`SET auto_explain.log_min_duration = 0`,
		`SET auto_explain.log_nested_statements = on`,
		`SET auto_explain.log_level = notice`,
		`SET client_min_messages = notice`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var appended int64
	if err := conn.QueryRow(ctx, `SELECT materialize_maple_loan(p_build_id => 0)`).Scan(&appended); err != nil {
		t.Fatalf("materialize_maple_loan: %v", err)
	}
	if closes := f.count(t, `SELECT count(*) FROM position_state WHERE quantity = 0`); closes == 0 {
		t.Fatal("the volume fixture closed nothing, so the close path's plan was not exercised")
	}
	if len(plans) == 0 {
		t.Fatal("auto_explain captured no plans, so nothing was checked")
	}
	for _, plan := range plans {
		for _, line := range strings.Split(plan, "\n") {
			if pairwiseFilter.MatchString(line) {
				t.Errorf("a statement compares instants outside an index condition:\n%s", plan)
			}
		}
	}
}
