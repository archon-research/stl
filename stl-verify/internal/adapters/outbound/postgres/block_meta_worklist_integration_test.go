//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, chainID, 0)
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

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, 1, 0)
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

// Resume, both halves. A pass that reached the end clears its chain, so the next run enumerates
// fresh. A pass that was interrupted leaves its rows, and the next Open must USE them rather than
// re-enumerate — the enumeration is the expensive half and the whole reason the table is committed.
func TestWorkListResumesAnInterruptedPassAndClearsACompletedOne(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}

	// An interrupted pass: one page read, then closed without reaching the end.
	list, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := list.Next(ctx, 2); err != nil {
		t.Fatalf("page: %v", err)
	}
	list.Close(ctx)

	var survived int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1`).Scan(&survived); err != nil {
		t.Fatal(err)
	}
	if survived == 0 {
		t.Fatal("an interrupted pass cleared its work list; the next run would re-enumerate the chain")
	}

	// The next Open must REUSE those rows rather than run the arms again. A brand-new source block is
	// what makes that observable: re-enumeration would pick it up, resuming cannot. Counting rows
	// cannot tell the two apart, because a re-enumeration re-inserts exactly what is already there.
	const newBlock = 7009999
	if _, err := pool.Exec(ctx, `
		INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at)
		SELECT (SELECT id FROM prime WHERE name='wl-prime'), 'WL-A', 1, $1, 0, TIMESTAMPTZ '2026-02-01'`,
		newBlock); err != nil {
		t.Fatalf("add a source row: %v", err)
	}
	resumed, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	var sawNew int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1 AND block_number = $1`, newBlock).Scan(&sawNew); err != nil {
		t.Fatal(err)
	}
	if sawNew != 0 {
		t.Errorf("resuming picked up a block added after the interrupted pass; it re-enumerated instead of resuming")
	}

	// Page it to the end; a completed pass clears the chain.
	for {
		refs, err := resumed.Next(ctx, 50)
		if err != nil {
			t.Fatalf("page the resumed list: %v", err)
		}
		if len(refs) == 0 {
			break
		}
	}
	resumed.Close(ctx)
	var afterComplete int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1`).Scan(&afterComplete); err != nil {
		t.Fatal(err)
	}
	if afterComplete != 0 {
		t.Errorf("a completed pass left %d rows; the next run would resume a list with nothing left to do", afterComplete)
	}
}

// build_id is what ADR-0006 reads to tell a tracked write from pre-tracking data, and the column
// COMMENT promises the loader's build. It defaulted to 0 on every row until the repository carried it.
func TestBlockMetaUpsertStampsBuildAndRun(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seed chain: %v", err)
	}
	if _, err := repo.Upsert(ctx, []outbound.BlockMetaRow{{
		ChainID: 1, BlockNumber: 4242, BlockVersion: 0, BlockTimestamp: time.Unix(1_700_000_000, 0).UTC(),
	}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	var gotBuild, gotRun int64
	if err := pool.QueryRow(ctx, `
		SELECT build_id, run_id FROM block_meta WHERE chain_id = 1 AND block_number = 4242`).Scan(&gotBuild, &gotRun); err != nil {
		t.Fatalf("read the row back: %v", err)
	}
	if gotBuild != int64(buildID) || gotBuild == 0 {
		t.Errorf("build_id = %d, want %d (0 means the column default, which ADR-0006 reads as pre-tracking)", gotBuild, buildID)
	}
	if gotRun != int64(runID) {
		t.Errorf("run_id = %d, want %d", gotRun, runID)
	}
}

// Four of the six source tables carry a one-year tiering policy. A tiered chunk is still listed in
// the chunk catalogue, so a window gets built for it; read with enable_tiered_reads at its default
// of off, that window silently returns nothing — losing exactly the deep-tail blocks this loader
// exists to cover. Nothing is a year old yet, so no local fixture can tier a chunk; what is
// assertable is that the setting is on for the statement, which is the thing that was missing.
//
// The arm list is swapped for a probe that records the setting it actually observes, because the
// value has to be read from inside the arm's own transaction — SET LOCAL is invisible outside it.
func TestWorkListEnumeratesWithTieredReadsOn(t *testing.T) {
	ctx := context.Background()
	pool, dsn, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	if _, err := pool.Exec(ctx, `CREATE TABLE tiered_probe (setting text NOT NULL)`); err != nil {
		t.Fatalf("create the probe table: %v", err)
	}

	// The default is already on in the versions in use, so asserting the arm observes "on" proves
	// nothing by itself — it passes with the SET LOCAL deleted. Lower the database default to off
	// first, so "on" can only come from the statement's own setting.
	var alter string
	if err := pool.QueryRow(ctx,
		`SELECT format('ALTER DATABASE %I SET timescaledb.enable_tiered_reads = off', current_database())`).Scan(&alter); err != nil {
		t.Fatalf("build the ALTER DATABASE: %v", err)
	}
	if _, err := pool.Exec(ctx, alter); err != nil {
		t.Fatalf("lower the default: %v", err)
	}

	// A fresh pool, so sessions pick the lowered default up.
	lowered := testutil.ConnectPool(t, dsn)
	defer lowered.Close()
	var def string
	if err := lowered.QueryRow(ctx, `SELECT current_setting('timescaledb.enable_tiered_reads')`).Scan(&def); err != nil {
		t.Fatalf("read the lowered default: %v", err)
	}
	if def != "off" {
		t.Fatalf("database default is %q, want off; the test would pass on the default alone", def)
	}

	// One arm over sparklend_reserve_data, which the fixture gives chunks, so windows exist and the
	// statement runs. It writes the observed setting instead of work-list rows.
	original := workListArms
	workListArms = []workListArm{{
		table:   "sparklend_reserve_data",
		partCol: "sr.block_number",
		sql: `INSERT INTO tiered_probe (setting)
		      SELECT current_setting('timescaledb.enable_tiered_reads')
		        FROM sparklend_reserve_data sr
		       WHERE $1::bigint > 0 AND %s
		       LIMIT 1`,
	}}
	t.Cleanup(func() { workListArms = original })

	buildID, runID := testutil.OpenTestRun(t, ctx, lowered)
	repo, err := NewBlockMetaRepository(lowered, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	if _, err := repo.OpenWorkList(ctx, 1, 0); err != nil {
		t.Fatalf("open the work list: %v", err)
	}

	var observed []string
	rows, err := lowered.Query(ctx, `SELECT setting FROM tiered_probe`)
	if err != nil {
		t.Fatalf("read the probe: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var got string
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("scan the probe: %v", err)
		}
		observed = append(observed, got)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate the probe: %v", err)
	}

	if len(observed) == 0 {
		t.Fatal("the probe arm never ran; the test proves nothing about the setting")
	}
	for _, got := range observed {
		if got != "on" {
			t.Errorf("an arm statement ran with enable_tiered_reads=%q against an off default; a tiered chunk would return nothing", got)
		}
	}
}

// The provenance invariants, as a battery rather than one case.
//
// The behaviour was already correct; what was missing was anything holding it there. These name
// what must stay true: the constructor refuses a build it cannot attribute, the writer's build and
// run reach the row, and a later run by a different build does not rewrite what an earlier one
// wrote — build_id is audit metadata, and ON CONFLICT DO NOTHING is what keeps the first writer's.
func TestBlockMetaProvenanceInvariants(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	if _, err := pool.Exec(ctx, `INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	t.Run("a zero build is refused, as a zero run already is", func(t *testing.T) {
		if _, err := NewBlockMetaRepository(pool, nil, 0, runID); err == nil {
			t.Error("a zero build id was accepted; 0 is the column default that ADR-0006 reads as pre-tracking")
		}
	})

	t.Run("a zero run is refused", func(t *testing.T) {
		if _, err := NewBlockMetaRepository(pool, nil, buildID, 0); err == nil {
			t.Error("a zero run id was accepted")
		}
	})

	t.Run("the writer's build and run reach the row", func(t *testing.T) {
		repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
		if err != nil {
			t.Fatalf("build the repository: %v", err)
		}
		if _, err := repo.Upsert(ctx, []outbound.BlockMetaRow{{
			ChainID: 1, BlockNumber: 9100, BlockVersion: 0, BlockTimestamp: time.Unix(1_700_000_000, 0).UTC(),
		}}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		var gotBuild, gotRun int64
		if err := pool.QueryRow(ctx,
			`SELECT build_id, run_id FROM block_meta WHERE chain_id = 1 AND block_number = 9100`).Scan(&gotBuild, &gotRun); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if gotBuild != int64(buildID) || gotRun != int64(runID) {
			t.Errorf("row carries build %d run %d, want %d and %d", gotBuild, gotRun, buildID, runID)
		}
	})

	t.Run("a later run does not rewrite an earlier one's provenance", func(t *testing.T) {
		// A second writer run, as a redeploy produces, re-offering the same block.
		build2, run2 := testutil.OpenTestRun(t, ctx, pool)
		if run2 == runID {
			t.Skip("the harness reused the run; this case needs two")
		}
		repo2, err := NewBlockMetaRepository(pool, nil, build2, run2)
		if err != nil {
			t.Fatalf("build the second repository: %v", err)
		}
		n, err := repo2.Upsert(ctx, []outbound.BlockMetaRow{{
			ChainID: 1, BlockNumber: 9100, BlockVersion: 0, BlockTimestamp: time.Unix(1_700_000_999, 0).UTC(),
		}})
		if err != nil {
			t.Fatalf("second upsert: %v", err)
		}
		if n != 0 {
			t.Errorf("the second run reported %d rows written for a block already present, want 0", n)
		}
		var gotBuild, gotRun int64
		var gotTS time.Time
		if err := pool.QueryRow(ctx,
			`SELECT build_id, run_id, block_timestamp FROM block_meta WHERE chain_id = 1 AND block_number = 9100`).Scan(&gotBuild, &gotRun, &gotTS); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if gotBuild != int64(buildID) || gotRun != int64(runID) {
			t.Errorf("the stored row now carries build %d run %d; the first writer's must survive", gotBuild, gotRun)
		}
		if gotTS.Unix() != 1_700_000_000 {
			t.Errorf("the stored timestamp changed to %d; a correction belongs at a higher processing_version", gotTS.Unix())
		}
	})
}
