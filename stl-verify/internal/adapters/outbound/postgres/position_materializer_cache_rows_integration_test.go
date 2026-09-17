//go:build integration

package postgres

import (
	"context"
	"regexp"
	"testing"
)

// CacheRowEstimates feeds VectorPositionMaterializerCacheTableGrowthHigh, the tripwire
// db/migrations/AGENTS.md charges for keeping these caches plain. A mock cannot tell us the query
// names a real relation or that reltuples reads back as a row count, so this runs it for real.
func TestCacheRowEstimates_ReadsTheLevelFromPlannerStatistics(t *testing.T) {
	ctx := context.Background()
	repo := NewPositionMaterializerRepository(positionMaterializerPool, nil)

	// pg_class.reltuples is -1 on a table never analyzed since creation. The migrations analyze as they
	// go, so that state is set here rather than waited for -- otherwise this asserts 0 against a table
	// that genuinely holds 0 rows and nothing is exercised. It must read as 0: "not yet measured" is
	// not evidence a table has grown, and a negative level would read as shrinking below empty.
	if _, err := positionMaterializerPool.Exec(ctx,
		`UPDATE pg_class SET reltuples = -1 WHERE oid = 'public.position_current'::regclass`); err != nil {
		t.Fatalf("put position_current back into its never-analyzed state: %v", err)
	}
	var raw float32
	if err := positionMaterializerPool.QueryRow(ctx,
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
	if _, err := positionMaterializerPool.Exec(ctx, `
		INSERT INTO position_current
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id, deal_type)
		SELECT sha256(g::text::bytea), 1, 1, 'inst-' || g,
		       substr(md5(g::text) || md5(g::text), 1, 40), 5, 100, 0, 0,
		       '2026-03-02T00:00:00Z'::timestamptz, 'public.proj-0', 0, 'LOAN'
		  FROM generate_series(1, 500) g`); err != nil {
		t.Fatalf("seed position_current: %v", err)
	}
	if _, err := positionMaterializerPool.Exec(ctx, `ANALYZE position_current`); err != nil {
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

// The tripwire tells an operator to convert a cache to a hypertable where its key allows, and the
// measurement has to survive that or the alert goes quiet exactly when it succeeded. pg_class.reltuples
// does not: a hypertable's rows live in its chunks, so the root relation reads 0 while the data is all
// still there. position_current cannot be converted (its PK omits the time column), so this runs the
// adapter's read over a throwaway table that can be.
func TestCacheRowEstimates_SurvivesAHypertableConversion(t *testing.T) {
	ctx := context.Background()
	repo := NewPositionMaterializerRepository(positionMaterializerPool, nil)
	const table = "cache_rows_probe"

	if _, err := positionMaterializerPool.Exec(ctx, `
		CREATE TABLE `+table+` (k bytea NOT NULL, d date NOT NULL, v numeric NOT NULL, PRIMARY KEY (k, d))`); err != nil {
		t.Fatalf("build the table under test: %v", err)
	}
	t.Cleanup(func() {
		_, _ = positionMaterializerPool.Exec(context.Background(), `DROP TABLE IF EXISTS `+table+` CASCADE`)
	})
	if _, err := positionMaterializerPool.Exec(ctx, `
		INSERT INTO `+table+` SELECT sha256(g::text::bytea), DATE '2026-03-01' + (g % 40), 1 FROM generate_series(1, 4000) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := positionMaterializerPool.Exec(ctx, `ANALYZE `+table); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	before, err := repo.rowEstimates(ctx, []string{table})
	if err != nil {
		t.Fatalf("rowEstimates before conversion: %v", err)
	}
	if before[table] != 4000 {
		t.Fatalf("the plain table reports %d rows, want 4000; the comparison below would prove nothing", before[table])
	}

	if _, err := positionMaterializerPool.Exec(ctx,
		`SELECT create_hypertable('`+table+`', by_range('d', INTERVAL '7 days'), migrate_data => true)`); err != nil {
		t.Fatalf("convert: %v", err)
	}
	if _, err := positionMaterializerPool.Exec(ctx, `ANALYZE `+table); err != nil {
		t.Fatalf("analyze after conversion: %v", err)
	}
	// Control: the reading that breaks must really break, or the assertion below passes for a table
	// that was never actually converted.
	var rootReltuples int64
	if err := positionMaterializerPool.QueryRow(ctx,
		`SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = 'public.`+table+`'::regclass`).Scan(&rootReltuples); err != nil {
		t.Fatal(err)
	}
	if rootReltuples != 0 {
		t.Fatalf("pg_class.reltuples still reads %d on the converted table; the hazard this guards did not occur", rootReltuples)
	}

	after, err := repo.rowEstimates(ctx, []string{table})
	if err != nil {
		t.Fatalf("rowEstimates after conversion: %v", err)
	}
	if after[table] != before[table] {
		t.Errorf("%s reports %d rows after the conversion and %d before; the level must survive the very remedy "+
			"the alert prescribes, or the tripwire reads empty forever", table, after[table], before[table])
	}
}

// A name with no relation is absent, not an error: one missing table must not silence the level for
// every cache at once.
func TestCacheRowEstimates_ANameWithNoRelationIsAbsentNotAnError(t *testing.T) {
	repo := NewPositionMaterializerRepository(positionMaterializerPool, nil)
	got, err := repo.rowEstimates(context.Background(), []string{"position_current", "no_such_cache"})
	if err != nil {
		t.Fatalf("a missing name made the read fail: %v", err)
	}
	if _, ok := got["no_such_cache"]; ok {
		t.Error("a name with no relation was reported")
	}
	if _, ok := got["position_current"]; !ok {
		t.Errorf("the cache that exists dropped out alongside the missing name: %v", got)
	}
}

// Every trigger on position_state feeds a cache the tripwire must watch. A trigger's target is only in
// its function body, so this reads the body for a name from positionStateCaches.
func TestPositionStateCaches_NamesEveryTriggerFedCache(t *testing.T) {
	rows, err := positionMaterializerPool.Query(context.Background(), `
		SELECT t.tgname, p.prosrc
		  FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid
		 WHERE t.tgrelid = 'public.position_state'::regclass AND NOT t.tgisinternal`)
	if err != nil {
		t.Fatalf("reading triggers on position_state: %v", err)
	}
	defer rows.Close()
	covered := map[string]bool{}
	triggers := 0
	for rows.Next() {
		var name, body string
		if err := rows.Scan(&name, &body); err != nil {
			t.Fatalf("scanning trigger: %v", err)
		}
		triggers++
		named := false
		for _, cache := range positionStateCaches {
			if regexp.MustCompile(`\b` + cache + `\b`).MatchString(body) {
				covered[cache], named = true, true
			}
		}
		if !named {
			t.Errorf("trigger %s on position_state writes no table in positionStateCaches; add its cache", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating triggers: %v", err)
	}
	if triggers == 0 {
		t.Fatal("position_state has no triggers; the check read nothing")
	}
	for _, cache := range positionStateCaches {
		if !covered[cache] {
			t.Errorf("positionStateCaches names %s, which no trigger on position_state writes", cache)
		}
	}
}
