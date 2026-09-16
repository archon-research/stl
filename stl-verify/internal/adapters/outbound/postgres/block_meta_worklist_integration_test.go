//go:build integration

package postgres

import (
	"context"
	"slices"
	"strconv"
	"strings"
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
		-- One row on another chain, so a run for 8453 has work of its own and a test asserting what it
		-- does NOT enumerate cannot pass on an empty list.
		INSERT INTO protocol (chain_id, address, name) VALUES (8453, '\x9004', 'wl-base') ON CONFLICT DO NOTHING;
		INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version)
		VALUES ((SELECT id FROM protocol WHERE address='\x9004'),
		        (SELECT id FROM token WHERE address='\x9002'), 1250000, 0);
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

// A table whose chain is a register constant is work like any other. prime_debt carries no chain
// column and no config parent -- schema_master gives it chain 1 as a literal -- so its arm has to take
// the chain from that constant. Before the const arm exists its blocks are simply absent from the list.
func TestWorkListEnumeratesAConstChainArm(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	got := openList(t, ctx, pool, 1)
	for _, b := range []int64{7000000, 7000005} {
		if !slices.Contains(got, b) {
			t.Errorf("block %d is referenced by prime_debt and is not in the work list; the const-chain arm is missing", b)
		}
	}
}

// The constant is a filter, not a label. A run for another chain must not write Sky's blocks into the
// work list at all -- asserted on the table rather than on that run's cursor, because a row inserted
// under chain 1 is invisible to a chain-8453 cursor and survives the next run's DELETE, which is scoped
// to its own chain. Block 1250000 is that run's own work, so an empty list cannot pass this.
func TestWorkListConstChainArmIsScopedToItsChain(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	seedWorkListSources(t, ctx, pool)

	got := openList(t, ctx, pool, 8453)
	if !slices.Contains(got, int64(1250000)) {
		t.Fatalf("chain 8453 enumerated %v, which does not include its own block 1250000; the run found nothing and the assertion below would be vacuous", got)
	}

	var sky int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM block_meta_worklist WHERE block_number BETWEEN 7000000 AND 7000005`).Scan(&sky); err != nil {
		t.Fatalf("count Sky rows in the work list: %v", err)
	}
	if sky != 0 {
		t.Errorf("a chain-8453 run left %d of Sky's chain-1 blocks in the work list; the constant must filter the run, not label the rows", sky)
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
		       WHERE $1::bigint > 0 AND %s
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

	// The state a killed enumeration leaves: some rows, no marker.
	if _, err := pool.Exec(ctx, `
		INSERT INTO block_meta_worklist (chain_id, block_number, block_version)
		VALUES (1, 7000001, 0) ON CONFLICT DO NOTHING`); err != nil {
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
