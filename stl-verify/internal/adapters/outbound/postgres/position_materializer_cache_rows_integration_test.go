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

	// pg_class.reltuples is -1 on a table never analyzed since creation. The migrations analyze as they
	// go, so that state is set here rather than waited for -- otherwise this asserts 0 against a table
	// that genuinely holds 0 rows and nothing is exercised. It must read as 0: "not yet measured" is
	// not evidence a table has grown, and a negative level would read as shrinking below empty.
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

// The tripwire tells an operator to convert the table to a hypertable, and the measurement has to
// survive that or the alert goes quiet exactly when it succeeded. pg_class.reltuples does not: a
// hypertable's rows live in its chunks, so the root relation reads 0 while the data is all still there.
//
// position_daily's own migration (VEC-636) has not landed on this branch, so the table is built here
// with the shape the read cares about -- it is looked up by name, so this exercises the real query --
// and dropped again. position_current cannot stand in: its PK is position_id alone, and
// create_hypertable refuses a unique index that omits the partition column.
func TestCacheRowEstimates_SurvivesAHypertableConversion(t *testing.T) {
	ctx := context.Background()
	repo := NewPositionMaterializerRepository(cacheRowsPool, nil)

	if _, err := cacheRowsPool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS position_daily (
		    position_id bytea NOT NULL,
		    as_of_date  date  NOT NULL,
		    quantity    numeric NOT NULL,
		    CONSTRAINT position_daily_probe_pkey PRIMARY KEY (position_id, as_of_date))`); err != nil {
		t.Fatalf("build the table under test: %v", err)
	}
	t.Cleanup(func() { _, _ = cacheRowsPool.Exec(context.Background(), `DROP TABLE IF EXISTS position_daily CASCADE`) })
	if _, err := cacheRowsPool.Exec(ctx, `
		INSERT INTO position_daily
		SELECT sha256(g::text::bytea), DATE '2026-03-01' + (g % 40), 1 FROM generate_series(1, 4000) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := cacheRowsPool.Exec(ctx, `ANALYZE position_daily`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	before, err := repo.CacheRowEstimates(ctx)
	if err != nil {
		t.Fatalf("CacheRowEstimates before conversion: %v", err)
	}
	if before["position_daily"] != 4000 {
		t.Fatalf("the plain table reports %d rows, want 4000; the conversion comparison below would prove nothing",
			before["position_daily"])
	}

	if _, err := cacheRowsPool.Exec(ctx,
		`SELECT create_hypertable('position_daily', by_range('as_of_date', INTERVAL '7 days'), migrate_data => true)`); err != nil {
		t.Fatalf("convert position_daily: %v", err)
	}
	if _, err := cacheRowsPool.Exec(ctx, `ANALYZE position_daily`); err != nil {
		t.Fatalf("analyze after conversion: %v", err)
	}
	// Control: this is the reading that breaks, and it must really break, or the assertion below
	// passes for a table that was never actually converted.
	var rootReltuples int64
	if err := cacheRowsPool.QueryRow(ctx,
		`SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = 'public.position_daily'::regclass`).
		Scan(&rootReltuples); err != nil {
		t.Fatal(err)
	}
	if rootReltuples != 0 {
		t.Fatalf("pg_class.reltuples still reads %d on the converted table; the hazard this guards did not occur", rootReltuples)
	}

	after, err := repo.CacheRowEstimates(ctx)
	if err != nil {
		t.Fatalf("CacheRowEstimates after conversion: %v", err)
	}
	if after["position_daily"] != before["position_daily"] {
		t.Errorf("position_daily reports %d rows after the conversion and %d before; the level must survive the "+
			"very remedy the alert prescribes, or the tripwire reads empty forever",
			after["position_daily"], before["position_daily"])
	}
}
