//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const cacheRowsDBName = "test_position_cache_rows"

var cacheRowsPool *pgxpool.Pool

func init() {
	registerTestFileSetup(func() {
		cacheRowsPool = testutil.SetupDBForMain(sharedDSN, cacheRowsDBName)
	}, func() {
		testutil.CleanupDBForMain(sharedDSN, cacheRowsPool, cacheRowsDBName)
	})
}

// CacheRowEstimates feeds VectorPositionMaterializerCacheTableGrowthHigh, the tripwire
// db/migrations/AGENTS.md charges for keeping these caches plain. A mock cannot tell us the query
// names a real relation or that reltuples reads back as a row count, so this runs it for real.
func TestCacheRowEstimates_ReadsTheLevelFromPlannerStatistics(t *testing.T) {
	ctx := context.Background()
	repo := NewPositionMaterializerRepository(cacheRowsPool, nil)

	// reltuples is -1 on a table never analyzed since creation. The migrations analyze as they go, so
	// that state is set here rather than waited for -- otherwise this asserts 0 against a table that
	// genuinely holds 0 rows and the clamp is never exercised. It must read as 0: "not yet measured"
	// is not evidence a table has grown, and a negative level would read as shrinking below empty.
	if _, err := cacheRowsPool.Exec(ctx,
		`UPDATE pg_class SET reltuples = -1 WHERE oid = 'public.position_current'::regclass`); err != nil {
		t.Fatalf("put position_current back into its never-analyzed state: %v", err)
	}
	var raw float32
	if err := cacheRowsPool.QueryRow(ctx,
		`SELECT reltuples FROM pg_class WHERE oid = 'public.position_current'::regclass`).Scan(&raw); err != nil {
		t.Fatalf("read reltuples back: %v", err)
	}
	if raw != -1 {
		t.Fatalf("reltuples is %v, not -1; the clamp below would not be exercised", raw)
	}

	before, err := repo.CacheRowEstimates(ctx)
	if err != nil {
		t.Fatalf("CacheRowEstimates on a never-analyzed table: %v", err)
	}
	got, ok := before["position_current"]
	if !ok {
		t.Fatalf("position_current is missing; the query names no such relation. Present: %v", before)
	}
	if got != 0 {
		t.Errorf("a never-analyzed position_current estimates %d rows; want 0, not reltuples' -1", got)
	}

	// Rows in as the owner, then ANALYZE so the planner's estimate is populated. The spine's trigger
	// is not involved: this asserts the read, not how the rows got there.
	if _, err := cacheRowsPool.Exec(ctx, `
		INSERT INTO position_current
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type)
		SELECT sha256(g::text::bytea), 1, 1, 'inst-' || g,
		       substr(md5(g::text) || md5(g::text), 1, 40), 5, 100, 0, 0,
		       '2026-03-02T00:00:00Z'::timestamptz, 'public.proj-0', 0, 'LOAN'
		  FROM generate_series(1, 500) g`); err != nil {
		t.Fatalf("seed position_current: %v", err)
	}
	if _, err := cacheRowsPool.Exec(ctx, `ANALYZE position_current`); err != nil {
		t.Fatalf("analyze position_current: %v", err)
	}

	after, err := repo.CacheRowEstimates(ctx)
	if err != nil {
		t.Fatalf("CacheRowEstimates after seeding: %v", err)
	}
	if got := after["position_current"]; got != 500 {
		t.Errorf("position_current estimates %d rows after ANALYZE over 500; want 500", got)
	}
}

// position_daily (VEC-636) is named in the cache list but its migration has not landed yet. A cache
// whose table does not exist must be absent from the map rather than an error, or this read starts
// failing every run the moment the list runs ahead of the schema -- and it takes the level for the
// caches that DO exist down with it.
func TestCacheRowEstimates_SkipsACacheWhoseTableDoesNotExistYet(t *testing.T) {
	ctx := context.Background()
	repo := NewPositionMaterializerRepository(cacheRowsPool, nil)

	var exists bool
	if err := cacheRowsPool.QueryRow(ctx,
		`SELECT to_regclass('public.position_daily') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("check whether position_daily exists: %v", err)
	}

	estimates, err := repo.CacheRowEstimates(ctx)
	if err != nil {
		t.Fatalf("CacheRowEstimates: %v", err)
	}
	if _, reported := estimates["position_daily"]; reported != exists {
		t.Errorf("position_daily exists=%t but reported=%t; a cache is reported exactly when its table is there", exists, reported)
	}
	// Whatever the answer, the cache that does exist still reports: one missing table must not empty
	// the map, which is what would silence the tripwire for every cache at once.
	if _, ok := estimates["position_current"]; !ok {
		t.Errorf("position_current dropped out of the estimates: %v", estimates)
	}
}
