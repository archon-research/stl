//go:build integration

package postgres

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
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
		INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), 'wl-prime', '\x9003') ON CONFLICT DO NOTHING;
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
		    JOIN protocol p ON p.id = sr.protocol_id WHERE p.chain_id = 1) s`).Scan(&want); err != nil {
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

// Every open re-enumerates. The work list is scratch, not state: rows surviving a previous run say
// nothing about whether that run's enumeration finished, because windows commit one at a time. Resuming
// the expensive half -- the S3 reads -- is the anti-join against block_meta, which works whether or not
// this table survived. So the chain is cleared at the start of every run and the arms all run again.
func TestWorkListClearsAndReEnumeratesOnEveryOpen(t *testing.T) {
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
		t.Fatalf("open: %v", err)
	}
	if _, err := list.Next(ctx, 2); err != nil {
		t.Fatalf("page: %v", err)
	}
	list.Close(ctx)

	// A source block added between the two opens is the probe: only a re-enumeration can see it.
	const addedBetweenRuns = 1150000
	if _, err := pool.Exec(ctx, `
		INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version)
		SELECT (SELECT id FROM protocol WHERE address='\x9001'),
		       (SELECT id FROM token WHERE address='\x9002'), $1, 0`, addedBetweenRuns); err != nil {
		t.Fatalf("add a source row between runs: %v", err)
	}
	if _, err := repo.OpenWorkList(ctx, 1, 0); err != nil {
		t.Fatalf("re-open: %v", err)
	}
	var sawNew int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1 AND block_number = $1`, addedBetweenRuns).Scan(&sawNew); err != nil {
		t.Fatal(err)
	}
	if sawNew != 1 {
		t.Errorf("a block added between runs is absent from the second run's work list; the open did not re-enumerate")
	}
}
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
	// statement runs. It writes the observed setting instead of work-list rows. Driven through
	// enumerateWindow directly: the arms are derived from the register, so there is no list to swap.
	probe := workListArm{
		table:   "sparklend_reserve_data",
		partCol: "sr.block_number",
		sql: `INSERT INTO tiered_probe (setting)
		      SELECT current_setting('timescaledb.enable_tiered_reads')
		        FROM sparklend_reserve_data sr
		       WHERE $1::bigint > 0 AND $2::bigint > 0 AND %s
		       LIMIT 1`,
	}

	buildID, runID := testutil.OpenTestRun(t, ctx, lowered)
	repo, err := NewBlockMetaRepository(lowered, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	windows, err := repo.windowPredicates(ctx, probe.table, probe.partCol)
	if err != nil {
		t.Fatalf("build the probe's windows: %v", err)
	}
	if len(windows) == 0 {
		t.Fatal("no windows over the fixture's chunks; the probe would never run")
	}
	for _, where := range windows {
		if err := repo.enumerateWindow(ctx, probe, where, 1); err != nil {
			t.Fatalf("run the probe window: %v", err)
		}
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
		// A second writer run from a DIFFERENT artefact, as a redeploy produces, re-offering the same
		// block. The build has to differ too: OpenTestRun registers one identity, so reusing it would
		// leave build2 == buildID and the build half of the assertion below could not fail.
		reg2, err := buildregistry.NewWithIdentity(ctx, pool, testutil.TestIdentity("test-redeploy"))
		if err != nil {
			t.Fatalf("register the second build: %v", err)
		}
		run2, err := reg2.OpenRun(ctx, time.Now().UTC(), nil)
		if err != nil {
			t.Fatalf("open the second writer run: %v", err)
		}
		build2 := reg2.BuildID()
		if build2 == buildID || run2 == runID {
			t.Fatalf("the second writer reused build %d run %d; this case needs two of each", build2, run2)
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

// The head margin exists to leave the newest blocks alone while the archive catches up, so it has
// to measure from the chain's head. Measured instead from the highest block still pending, a gap
// repaired deep in the chain's history sits entirely within its own margin and is deleted whole:
// the run loads nothing, reports success, and every later run repeats it.
func TestHeadMarginMeasuresFromTheChainHeadNotThePendingSet(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	// A gap of eleven blocks spanning ten, four million blocks below the head.
	if _, err := pool.Exec(ctx, `
		INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT DO NOTHING;
		INSERT INTO protocol (chain_id, address, name) VALUES (1, '\x9101', 'margin-eth') ON CONFLICT DO NOTHING;
		INSERT INTO token (chain_id, address) VALUES (1, '\x9102') ON CONFLICT DO NOTHING;
		INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version)
		SELECT (SELECT id FROM protocol WHERE address='\x9101'),
		       (SELECT id FROM token WHERE address='\x9102'),
		       1000000 + g, 0
		  FROM generate_series(0, 10) g;`); err != nil {
		t.Fatalf("seed the gap: %v", err)
	}

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	// block_meta already holds the chain up to 5,000,000, so the head is nowhere near the gap.
	if _, err := pool.Exec(ctx, `
		INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id, run_id)
		VALUES (1, 5000000, 0, TIMESTAMPTZ '2026-01-01', $1, $2)`, int(buildID), int64(runID)); err != nil {
		t.Fatalf("seed the chain head: %v", err)
	}

	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, 1, 300)
	if err != nil {
		t.Fatalf("open the work list: %v", err)
	}
	defer list.Close(ctx)

	refs, err := list.Next(ctx, 100)
	if err != nil {
		t.Fatalf("page the work list: %v", err)
	}
	if len(refs) != 11 {
		t.Fatalf("the work list holds %d of the 11 blocks in the gap; a margin measured from the pending set deletes a gap narrower than itself", len(refs))
	}
}

// The arms are derived from the register, so this pins the derivation: exactly the tables declaring a
// block_meta fill become arms, and nothing else. A filter that drops one would enumerate fewer tables
// than the register says need block_meta, and every value on the missing one would resolve NULL.
//
// Matched on the fill's TABLE, not its column: block_meta is the block dimension, and block_timestamp
// is only the column it carries today, so a fill for a column added later is covered unchanged.
func TestWorkListArmsAreExactlyTheRegistersBlockMetaFills(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}

	register, err := schemamaster.Load()
	if err != nil {
		t.Fatalf("load schema_master.json: %v", err)
	}
	var want []string
	for _, f := range register.Fills {
		if f.BlockMeta {
			want = append(want, f.Table)
		}
	}
	if len(want) == 0 {
		t.Fatal("no table declares a block_meta fill; the register did not load as expected")
	}
	slices.Sort(want)
	want = slices.Compact(want)

	arms, err := repo.workListArms(ctx)
	if err != nil {
		t.Fatalf("derive the arms: %v", err)
	}
	var got []string
	for _, a := range arms {
		got = append(got, a.table)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("the arms are %v, the register's block_meta fills are %v; the derivation is not the register", got, want)
	}
}

// Enumeration commits per window, so a run killed PART WAY through it leaves rows for the arms that
// finished and none for the arms that never ran. "Rows survive" then reads as a complete work list:
// the next Open skips every arm, pages the partial list to the end, clears the chain and reports
// success, and the blocks the unrun arms would have found are never loaded. Nothing says so.
//
// Simulated by leaving rows for the chain with no completion marker, which is exactly the state a
// killed enumeration leaves. The new source row is the probe: a resume cannot see it, a re-enumeration
// must, and here re-enumerating is the only correct behaviour because the list was never finished.
func TestWorkListReEnumeratesAfterAnInterruptedEnumeration(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}

	// The state a killed enumeration leaves: some rows, no marker, owned by the run that died.
	killed := backdatedRun(t, ctx, pool, buildID, "1 hour")
	if _, err := pool.Exec(ctx, `
		INSERT INTO block_meta_worklist (chain_id, run_id, block_number, block_version)
		VALUES (1, $1, 7000001, 0) ON CONFLICT DO NOTHING`, killed); err != nil {
		t.Fatalf("leave a partial work list: %v", err)
	}
	var partial int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1`).Scan(&partial); err != nil {
		t.Fatal(err)
	}
	if partial == 0 {
		t.Fatal("the partial work list did not survive; this case is not being exercised")
	}

	const unrunArmBlock = 1175000
	if _, err := pool.Exec(ctx, `
		INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version)
		SELECT (SELECT id FROM protocol WHERE address='\x9001'),
		       (SELECT id FROM token WHERE address='\x9002'), $1, 0`, unrunArmBlock); err != nil {
		t.Fatalf("add the block an unrun arm would find: %v", err)
	}

	if _, err := repo.OpenWorkList(ctx, 1, 0); err != nil {
		t.Fatalf("open over a partial work list: %v", err)
	}
	var found int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1 AND block_number = $1`, unrunArmBlock).Scan(&found); err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		t.Error("a work list left by an interrupted ENUMERATION was resumed as if it were complete: the arms " +
			"that never ran were skipped, so their blocks are absent and the run will report success without them")
	}
}

// The deep tail this loader exists to cover is exactly what tiers first, and a tiered chunk LEAVES
// timescaledb_information.chunks. Reading windows from that view alone, the oldest visible bound jumps
// forward to the oldest untiered chunk the day tiering starts, no window is built below it, and the
// blocks down there drop out of the work list silently — enable_tiered_reads cannot help a range no
// statement scans.
//
// Asserted on the predicates rather than end to end: a tiered chunk cannot be created locally, and
// simply inserting a row far below the live range does not reproduce it, because the insert CREATES a
// local chunk there and the bound becomes visible the ordinary way. The local harness has no tiering
// extension, so the OSM catalogue is stood up here with the four range columns the real one carries,
// which is what the code probes for.
func TestWindowPredicatesCoverTieredChunkRanges(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}

	// Control: with no OSM catalogue the loader still builds windows, which is every environment
	// without tiering, the local harness included.
	before, err := repo.windowPredicates(ctx, "sparklend_reserve_data", "sr.block_number")
	if err != nil {
		t.Fatalf("windows with no tiered-chunk catalogue present: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("no windows at all over the live chunks; the comparison below would prove nothing")
	}

	// A tiered range far below every live chunk: the fixture seeds 900,000 upwards, so nothing local
	// covers this and only the OSM catalogue can put it in range.
	const tieredLo, tieredHi = 100000, 200000
	for _, ddl := range []string{
		`CREATE SCHEMA IF NOT EXISTS timescaledb_osm`,
		`CREATE TABLE IF NOT EXISTS timescaledb_osm.tiered_chunks (
		    hypertable_name      text,
		    range_start_integer  bigint,
		    range_end_integer    bigint,
		    range_start          timestamptz,
		    range_end            timestamptz)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("stand up the tiered-chunk catalogue: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO timescaledb_osm.tiered_chunks
		    (hypertable_name, range_start_integer, range_end_integer)
		VALUES ('sparklend_reserve_data', $1, $2)`, tieredLo, tieredHi); err != nil {
		t.Fatalf("seed the tiered chunk range: %v", err)
	}

	after, err := repo.windowPredicates(ctx, "sparklend_reserve_data", "sr.block_number")
	if err != nil {
		t.Fatalf("windows with a tiered chunk present: %v", err)
	}
	covered := false
	for _, w := range after {
		if strings.Contains(w, strconv.Itoa(tieredLo)) {
			covered = true
			break
		}
	}
	if !covered {
		t.Errorf("the tiered range [%d, %d) is in no window (%d windows: %v); the loader would never scan "+
			"the tiered tail and would report success having skipped it", tieredLo, tieredHi, len(after), after)
	}
}

// Two runs may cover one chain at once: an operator's on-demand pass and the scheduled top-up. Each
// enumerates its own slice and deletes only that, so an overlap costs duplicated archive reads.
//
// The second run's pending set is deliberately made SMALLER than the first's: the blocks the first has
// not reached are loaded before the second opens, so the second legitimately enumerates none of them.
// Sharing one slice, the second run's open would take those blocks off the first run's list -- and the
// first run's cursor would read the end of the list and report success having skipped them.
func TestAnOverlappingRunDoesNotTruncateTheOtherRunsWorkList(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	first, firstRun := openRunList(t, ctx, pool, 1)
	defer first.Close(ctx)
	want := sliceBlocks(t, ctx, pool, 1, firstRun)
	const page = 2
	if len(want) <= page {
		t.Fatalf("the first run enumerated %d blocks, too few to page and still have a tail", len(want))
	}
	head, err := first.Next(ctx, page)
	if err != nil {
		t.Fatalf("page the first run: %v", err)
	}
	if len(head) != page {
		t.Fatalf("the first run read %d blocks, want %d", len(head), page)
	}

	// Everything the first run has not reached is loaded by the time the second run enumerates, so the
	// second legitimately enumerates none of it.
	for _, b := range want[page:] {
		if _, err := pool.Exec(ctx, `
			INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
			VALUES (1, $1, 0, TIMESTAMPTZ '2026-01-01') ON CONFLICT DO NOTHING`, b); err != nil {
			t.Fatalf("load block %d: %v", b, err)
		}
	}
	second, _ := openRunList(t, ctx, pool, 1)
	defer second.Close(ctx)
	// Only the blocks the first run already read are still pending, so that is the second run's whole
	// slice: it is strictly smaller than the first's, which is what makes a shared list visible.
	if left := blockNumbers(drain(t, ctx, second)); !slices.Equal(left, want[:page]) {
		t.Fatalf("the second run enumerated %v, want %v", left, want[:page])
	}

	got := append(blockNumbers(head), blockNumbers(drain(t, ctx, first))...)
	if !slices.Equal(got, want) {
		t.Errorf("the first run read %v, want %v: the second run's open took blocks off a list it does not own",
			got, want)
	}
}

func blockNumbers(refs []outbound.BlockRef) []int64 {
	out := make([]int64, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Number)
	}
	return out
}

// A run killed before it closes leaves its slice behind; nothing else may delete another run's rows,
// so without a sweep the table only grows. The sweep is bounded by age, not by liveness, because
// writer_run records no end: a slice is another run's until that run is older than any run can be.
func TestAbandonedSlicesAreSweptOnceTheirRunIsOldEnough(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	stale := backdatedRun(t, ctx, pool, buildID, "49 hours")
	fresh := backdatedRun(t, ctx, pool, buildID, "47 hours")
	for _, owner := range []int64{stale, fresh} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO block_meta_worklist (chain_id, run_id, block_number, block_version)
			VALUES (1, $1, 8000000, 0)`, owner); err != nil {
			t.Fatalf("seed an abandoned slice: %v", err)
		}
	}

	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer list.Close(ctx)

	var staleRows, freshRows int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE run_id = $1), count(*) FILTER (WHERE run_id = $2)
		  FROM block_meta_worklist WHERE chain_id = 1`, stale, fresh).Scan(&staleRows, &freshRows); err != nil {
		t.Fatal(err)
	}
	if staleRows != 0 {
		t.Errorf("%d row(s) of a run past the sweep age survive; abandoned slices accumulate forever", staleRows)
	}
	if freshRows != 1 {
		t.Errorf("the row count of a run inside the sweep age is %d, want 1; a run still in flight had its list deleted", freshRows)
	}
}

// openRunList opens a work list under its own writer run, the way a separate process would, and
// returns that run's id so a test can read the slice it owns.
func openRunList(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64) (outbound.BlockWorkList, buildregistry.RunID) {
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
	return list, runID
}

// sliceBlocks reads the rows a run owns straight from the table, so a test's expectation comes from
// the enumeration rather than from the cursor it is checking.
func sliceBlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, runID buildregistry.RunID) []int64 {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT block_number FROM block_meta_worklist
		 WHERE chain_id = $1 AND run_id = $2 ORDER BY block_number, block_version`, chainID, int64(runID))
	if err != nil {
		t.Fatalf("read the run's slice: %v", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var b int64
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan a slice row: %v", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate the run's slice: %v", err)
	}
	return out
}

// drain reads a list to its end.
func drain(t *testing.T, ctx context.Context, list outbound.BlockWorkList) []outbound.BlockRef {
	t.Helper()
	var out []outbound.BlockRef
	for {
		refs, err := list.Next(ctx, 3)
		if err != nil {
			t.Fatalf("page the work list: %v", err)
		}
		if len(refs) == 0 {
			return out
		}
		out = append(out, refs...)
	}
}

// backdatedRun inserts a writer_run that started age ago. writer_run is insert-only, so the age is
// written at insert rather than updated afterwards.
func backdatedRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, buildID buildregistry.BuildID, age string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO writer_run (build_id, started_at, reference_snapshot, reference_effective_at)
		VALUES ($1, now() - $2::interval, 'test', now())
		RETURNING id`, int64(buildID), age).Scan(&id); err != nil {
		t.Fatalf("insert a %s-old writer run: %v", age, err)
	}
	return id
}

// A slice deleted under a live run must stop that run, not end it. Every cause is the same shape --
// the sweep reaching a run that outlived its margin, a migration clearing the table, an operator's
// DELETE -- and all of them leave a cursor reading an empty page it cannot distinguish from the end of
// its list. Before the cursor checked, that page was the run's success: it reported the blocks it
// never reached as nothing left to do.
func TestACursorRefusesToEndOnASliceDeletedUnderIt(t *testing.T) {
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
		t.Fatalf("open: %v", err)
	}
	defer list.Close(ctx)

	page, err := list.Next(ctx, 2)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("read %d blocks, want 2; the seed is not exercising this", len(page))
	}

	tag, err := pool.Exec(ctx, `DELETE FROM block_meta_worklist WHERE chain_id = 1 AND run_id = $1`, int64(runID))
	if err != nil {
		t.Fatalf("delete the slice under the run: %v", err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatal("nothing was deleted; the run held no slice and this case is not being exercised")
	}

	rest, err := list.Next(ctx, 2)
	if err == nil {
		t.Errorf("the cursor returned %d blocks and no error after its slice was deleted; the run treats a "+
			"list that vanished as a list it finished", len(rest))
	}
}

// The counterpart: a run that pages to the end of an intact slice ends cleanly. Without this the check
// above is satisfied by a cursor that simply always fails.
func TestACursorEndsCleanlyOnAnIntactSlice(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	list, _ := openRunList(t, ctx, pool, 1)
	defer list.Close(ctx)
	if blocks := drain(t, ctx, list); len(blocks) == 0 {
		t.Fatal("the seed produced no pending blocks; this case is not being exercised")
	}
}

// Close is the ordinary release, and SIGTERM is the ordinary stop: it arrives with the run's context
// already cancelled, so a Close that ran on it would always fail and leave the slice for the sweep.
func TestCloseReleasesTheSliceOnACancelledContext(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	list, err := repo.OpenWorkList(runCtx, 1, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var held int64
	if err := pool.QueryRow(ctx, sliceSizeSQL, int64(1), int64(runID)).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held == 0 {
		t.Fatal("the run holds no rows; this case is not being exercised")
	}

	cancel() // SIGTERM
	list.Close(runCtx)

	var left int64
	if err := pool.QueryRow(ctx, sliceSizeSQL, int64(1), int64(runID)).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Errorf("%d of %d rows survive a Close on a cancelled context; every ordinary shutdown orphans "+
			"its slice until the sweep reclaims it", left, held)
	}
}

// The sweep covers every chain, not the one being opened: a chain whose deployment is retired opens no
// further lists, so a slice abandoned there would have no other sweeper.
func TestTheSweepReclaimsSlicesOnChainsOtherThanTheOneOpened(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	retired := backdatedRun(t, ctx, pool, buildID, "4 days")
	if _, err := pool.Exec(ctx, `
		INSERT INTO block_meta_worklist (chain_id, run_id, block_number, block_version)
		VALUES (8453, $1, 9000000, 0)`, retired); err != nil {
		t.Fatalf("seed the retired chain's slice: %v", err)
	}

	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("open on chain 1: %v", err)
	}
	defer list.Close(ctx)

	var left int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM block_meta_worklist WHERE chain_id = 8453`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Errorf("%d row(s) abandoned on a chain nobody opens survive; nothing else will ever reclaim them", left)
	}
}

// Closing one run releases that run's rows and no others, and the other run's cursor carries on from
// where it stopped.
func TestClosingOneRunLeavesTheOtherRunsListIntact(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	first, firstRun := openRunList(t, ctx, pool, 1)
	defer first.Close(ctx)
	want := sliceBlocks(t, ctx, pool, 1, firstRun)
	head, err := first.Next(ctx, 2)
	if err != nil {
		t.Fatalf("page the first run: %v", err)
	}

	second, _ := openRunList(t, ctx, pool, 1)
	second.Close(ctx)

	got := append(blockNumbers(head), blockNumbers(drain(t, ctx, first))...)
	if !slices.Equal(got, want) {
		t.Errorf("the first run read %v after the second closed, want %v: Close released rows it does not own",
			got, want)
	}
}

// The head margin trims the run applying it. A second run opening with a margin must not trim the
// first run's list, which was opened without one.
func TestTheHeadMarginTrimsOnlyTheRunApplyingIt(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	first, firstRun := openRunList(t, ctx, pool, 1) // no margin: holds the chain head
	defer first.Close(ctx)
	want := sliceBlocks(t, ctx, pool, 1, firstRun)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	// A margin wide enough to take everything above the lowest block.
	second, err := repo.OpenWorkList(ctx, 1, 100000)
	if err != nil {
		t.Fatalf("open the second run's list: %v", err)
	}
	defer second.Close(ctx)
	if trimmed := drain(t, ctx, second); len(trimmed) >= len(want) {
		t.Fatalf("the margin trimmed the second run to %d of %d blocks; it is not wide enough to exercise this",
			len(trimmed), len(want))
	}

	if got := blockNumbers(drain(t, ctx, first)); !slices.Equal(got, want) {
		t.Errorf("the first run read %v, want %v: the second run's head margin trimmed a list it does not own",
			got, want)
	}
}

// The sweep must not reach its own run. Ordering hides it on the chain being opened -- that slice is
// cleared first -- so this run's rows sit on another chain, where only the sweep can touch them.
func TestTheSweepSpareItsOwnRunsRows(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, _ := testutil.OpenTestRun(t, ctx, pool)
	old := backdatedRun(t, ctx, pool, buildID, "72 hours") // older than the sweep age
	if _, err := pool.Exec(ctx, `
		INSERT INTO block_meta_worklist (chain_id, run_id, block_number, block_version)
		VALUES (8453, $1, 9100000, 0)`, old); err != nil {
		t.Fatalf("seed the run's own rows on another chain: %v", err)
	}

	repo, err := NewBlockMetaRepository(pool, nil, buildID, buildregistry.RunID(old))
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("open on chain 1: %v", err)
	}
	defer list.Close(ctx)

	var mine int64
	if err := pool.QueryRow(ctx, sliceSizeSQL, int64(8453), old).Scan(&mine); err != nil {
		t.Fatal(err)
	}
	if mine != 1 {
		t.Errorf("the run swept %d of its own rows; a long-lived run deletes the list it is paging", 1-mine)
	}
}

// The sweep age is a threshold, not a range: a run at it is swept, a run inside it is not.
func TestTheSweepAgeIsTheThreshold(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	at := backdatedRun(t, ctx, pool, buildID, "48 hours")
	inside := backdatedRun(t, ctx, pool, buildID, "47 hours 59 minutes")
	for _, owner := range []int64{at, inside} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO block_meta_worklist (chain_id, run_id, block_number, block_version)
			VALUES (1, $1, 8100000, 0)`, owner); err != nil {
			t.Fatalf("seed a slice: %v", err)
		}
	}

	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	list, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer list.Close(ctx)

	var atRows, insideRows int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE run_id = $1), count(*) FILTER (WHERE run_id = $2)
		  FROM block_meta_worklist WHERE chain_id = 1`, at, inside).Scan(&atRows, &insideRows); err != nil {
		t.Fatal(err)
	}
	if atRows != 0 {
		t.Errorf("a run at the sweep age kept its rows; the threshold is not where it is documented")
	}
	if insideRows != 1 {
		t.Errorf("a run a minute inside the sweep age lost its rows; the threshold is not where it is documented")
	}
}

// A chain with nothing pending ends the list cleanly. The cursor's short-slice check must read an
// empty slice as empty, not as one that went missing.
func TestAnEmptyPendingSetEndsWithoutError(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	list, _ := openRunList(t, ctx, pool, 8453) // seeded as a chain, but nothing references it
	defer list.Close(ctx)
	refs, err := list.Next(ctx, 10)
	if err != nil {
		t.Fatalf("an empty pending set reported an error: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("chain 8453 enumerated %d blocks, want 0", len(refs))
	}
}

// Close is idempotent, and a second call must not delete a slice the run has since reopened.
func TestCloseIsIdempotentAndDoesNotTouchALaterSlice(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewBlockMetaRepository(pool, nil, buildID, runID)
	if err != nil {
		t.Fatalf("build the repository: %v", err)
	}
	first, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first.Close(ctx)

	second, err := repo.OpenWorkList(ctx, 1, 0)
	if err != nil {
		t.Fatalf("re-open under the same run: %v", err)
	}
	defer second.Close(ctx)
	first.Close(ctx) // the stale handle, closed again

	if blocks := drain(t, ctx, second); len(blocks) == 0 {
		t.Error("a second Close on the old handle emptied the list the run had just reopened")
	}
}

// Two runs paging the same chain at the same time. The second opens while the first holds an open
// list -- the order that matters, since an open is what clears and enumerates -- and then both page in
// parallel. Neither may fail, and both must read the whole pending set.
func TestTwoRunsPageOneChainConcurrently(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	first, firstRun := openRunList(t, ctx, pool, 1)
	defer first.Close(ctx)
	want := sliceBlocks(t, ctx, pool, 1, firstRun)
	if len(want) == 0 {
		t.Fatal("the first run enumerated nothing; the seed is not exercising this")
	}

	// The second run opens against the first's live list, then both page together.
	second, _ := openRunList(t, ctx, pool, 1)
	defer second.Close(ctx)

	type result struct {
		blocks []outbound.BlockRef
		err    error
	}
	results := make(chan result, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, list := range []outbound.BlockWorkList{first, second} {
		go func() {
			start.Wait()
			var blocks []outbound.BlockRef
			for {
				refs, err := list.Next(ctx, 2)
				if err != nil {
					results <- result{err: err}
					return
				}
				if len(refs) == 0 {
					results <- result{blocks: blocks}
					return
				}
				blocks = append(blocks, refs...)
			}
		}()
	}
	start.Done()

	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatalf("a concurrent run failed: %v", r.err)
		}
		if got := blockNumbers(r.blocks); !slices.Equal(got, want) {
			t.Errorf("a concurrent run read %v, want %v", got, want)
		}
	}
}
