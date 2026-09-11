//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// VEC-491 work list. The arms are populated one partition-column window at a time, into a committed
// table, so these pin the three things that shape can get wrong: a window that loses blocks at its
// boundary, a run that still holds a transaction open, and the Sky arm's chain-1 constant.

// seedWorkListSources gives the loader something to enumerate. sparklend_reserve_data is the one arm
// partitioned on block_number (interval 100000), and the blocks straddle 1,000,000 deliberately: its
// chunk ranges then have differing digit counts, which orders wrongly as text and right as a number.
func seedWorkListSources(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum'), (8453, 'base') ON CONFLICT DO NOTHING;
		INSERT INTO protocol (chain_id, address, name) VALUES (1, '\x9001', 'wl-eth') ON CONFLICT DO NOTHING;
		INSERT INTO token (chain_id, address) VALUES (1, '\x9002') ON CONFLICT DO NOTHING;
		INSERT INTO prime (name, vault_address) VALUES ('wl-prime', '\x9003') ON CONFLICT DO NOTHING;
		-- 9 blocks spanning 900,000 to 1,100,000: eight chunks at interval 100000.
		INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version)
		SELECT (SELECT id FROM protocol WHERE address='\x9001'),
		       (SELECT id FROM token WHERE address='\x9002'),
		       900000 + g * 25000, 0
		  FROM generate_series(0, 8) g;
		-- Sky contributes only on chain 1, and carries no chain column of its own.
		INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at)
		SELECT (SELECT id FROM prime WHERE name='wl-prime'), 'WL-A', 1, 7000000 + g, 0,
		       TIMESTAMPTZ '2026-01-01' + (g * interval '3 days')
		  FROM generate_series(0, 5) g;`); err != nil {
		t.Fatalf("seed work-list sources: %v", err)
	}
}

func openList(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) []int64 {
	t.Helper()
	_, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, chainID)
	if err != nil {
		t.Fatalf("open the work list for chain %d: %v", chainID, err)
	}
	defer list.Close(ctx)
	var blocks []int64
	for {
		refs, err := list.Next(ctx, 3) // small, so paging is exercised across several calls
		if err != nil {
			t.Fatalf("page the work list: %v", err)
		}
		if len(refs) == 0 {
			return blocks
		}
		for _, r := range refs {
			blocks = append(blocks, r.Number)
		}
	}
}

// The windows must partition the source exactly: every referenced block appears once, none is lost at
// a window boundary. Asserted against the set the arms' own sources hold, not against a second copy of
// the windowing logic.
func TestWorkListWindowsCoverEveryReferencedBlock(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	var want int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
		  SELECT sr.block_number FROM sparklend_reserve_data sr
		    JOIN protocol p ON p.id = sr.protocol_id WHERE p.chain_id = 1
		  UNION
		  SELECT pd.block_number FROM prime_debt pd) s`).Scan(&want); err != nil {
		t.Fatalf("count referenced blocks: %v", err)
	}
	got := openList(t, ctx, pool, 1)
	if len(got) != want {
		t.Errorf("work list holds %d blocks, want %d; a window boundary is losing or duplicating rows", len(got), want)
	}
	seen := map[int64]bool{}
	for _, b := range got {
		if seen[b] {
			t.Errorf("block %d appears more than once in the work list", b)
		}
		seen[b] = true
	}
	// The straddle is the point: without numeric ordering of integer chunk ranges these are the ones lost.
	for _, b := range []int64{900000, 1000000, 1100000} {
		if !seen[b] {
			t.Errorf("block %d is missing; integer chunk ranges must order numerically, not as text", b)
		}
	}
}

// Blocks already in block_meta are not work.
func TestWorkListExcludesBlocksAlreadyLoaded(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	before := len(openList(t, ctx, pool, 1))
	if _, err := pool.Exec(ctx, `
		INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
		VALUES (1, 900000, 0, TIMESTAMPTZ '2026-01-01')`); err != nil {
		t.Fatalf("load one block: %v", err)
	}
	after := openList(t, ctx, pool, 1)
	if len(after) != before-1 {
		t.Errorf("work list holds %d blocks after loading one, want %d", len(after), before-1)
	}
	for _, b := range after {
		if b == 900000 {
			t.Error("block 900000 is loaded and must not be work")
		}
	}
}

// prime_debt has no chain column, so its arm asserts chain 1. On any other chain it must contribute
// nothing at all -- asserted against the work-list TABLE rather than the cursor, because the cursor
// filters by chain and would hide rows the Sky arm wrongly wrote under chain 1 during another chain's run.
func TestWorkListSkipsPrimeDebtOffChainOne(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	var before int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta_worklist`).Scan(&before); err != nil {
		t.Fatalf("count the work list before: %v", err)
	}
	if got := openList(t, ctx, pool, 8453); len(got) != 0 {
		t.Errorf("chain 8453 enumerated %d blocks; nothing in the fixture belongs to it", len(got))
	}
	var sky int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM block_meta_worklist
		 WHERE block_number BETWEEN 7000000 AND 7000005`).Scan(&sky); err != nil {
		t.Fatalf("count Sky rows in the work list: %v", err)
	}
	if sky != 0 {
		t.Errorf("a chain 8453 run wrote %d prime_debt block(s) into the work list; the Sky arm must not run off chain 1", sky)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta_worklist`).Scan(&after); err != nil {
		t.Fatalf("count the work list after: %v", err)
	}
	if after != before {
		t.Errorf("a chain 8453 run changed the work list from %d rows to %d; it must write nothing", before, after)
	}
}

// The reason the work list is a committed table rather than a temp one: a transaction open across the
// run pins VACUUM's removable cutoff database-wide. Paging must leave no transaction open at all.
func TestWorkListHoldsNoOpenTransactionWhilePaging(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	_, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, 1)
	if err != nil {
		t.Fatalf("open the work list: %v", err)
	}
	defer list.Close(ctx)
	if _, err := list.Next(ctx, 2); err != nil {
		t.Fatalf("page the work list: %v", err)
	}
	var pinned int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		 WHERE datname = current_database() AND pid <> pg_backend_pid()
		   AND state = 'idle in transaction' AND backend_xid IS NOT NULL`).Scan(&pinned); err != nil {
		t.Fatalf("read pg_stat_activity: %v", err)
	}
	if pinned != 0 {
		t.Errorf("%d backend(s) sit idle in transaction holding an xid while the work list pages; that pins VACUUM database-wide", pinned)
	}
}

// A run killed by its deadline must leave the enumeration behind, so the next one resumes instead of
// spending hours rebuilding it.
func TestWorkListSurvivesClose(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	n := len(openList(t, ctx, pool, 1)) // opens, pages to exhaustion, closes
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1`).Scan(&rows); err != nil {
		t.Fatalf("count surviving rows: %v", err)
	}
	if rows != n {
		t.Errorf("%d rows survive the close, want %d; a killed run must be able to resume", rows, n)
	}
}
