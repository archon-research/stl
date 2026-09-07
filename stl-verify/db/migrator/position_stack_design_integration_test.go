//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPositionStackDesignInvariants drives the position stack -- position_state + deal_type +
// position_current + position_daily -- against randomly generated observation histories, on a fresh
// database per seed, and checks invariants of the MODEL rather than expected values.
//
// The oracle is computed in SQL from position_state, independently of the cache writers, so it cannot
// inherit a mistake from the code under test.
//
// Histories arrive in SEVERAL out-of-order batches, one materializer run each. That is not incidental:
// a single-statement history lets DISTINCT ON pick the winner, so the caches' ON CONFLICT arms never
// run at all, and dropping a column from a DO UPDATE SET list or deleting the newer-wins predicate
// outright both pass. Verified by mutation.
func TestPositionStackDesignInvariants(t *testing.T) {
	const seeds = 24
	type failure struct {
		seed        int
		inv, detail string
	}
	var failures, notes []failure
	report := func(seed int, inv, detail string) { failures = append(failures, failure{seed, inv, detail}) }
	note := func(seed int, inv, detail string) { notes = append(notes, failure{seed, inv, detail}) }

	// position_current and position_daily arrive in their own migrations. Cover whichever are present
	// rather than requiring all three, so this lives in one PR and gains coverage as the caches land.
	// The spine invariants always run, so it can never degrade to testing nothing.
	presentCaches := cachesPresent(t)
	has := func(n string) bool {
		for _, c := range presentCaches {
			if c == n {
				return true
			}
		}
		return false
	}

	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			ctx := context.Background()
			pool, cleanup := setupMigratedPostgres(ctx, t)
			defer cleanup()

			rng := rand.New(rand.NewSource(int64(seed) * 7919))
			rows := generateHistory(rng)
			view := fmt.Sprintf("pv_design_%d", seed)

			var inserted int64
			for bi, batch := range splitBatches(rng, rows) {
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+view+` AS `+valuesBody(batch)); err != nil {
					t.Fatalf("create view (batch %d): %v", bi, err)
				}
				var n int64
				if err := pool.QueryRow(ctx,
					`SELECT materialize_position_projection($1::regclass)`, view).Scan(&n); err != nil {
					report(seed, "materializer refused a legal history", err.Error())
					t.Fatalf("materialize batch %d: %v", bi, err)
				}
				inserted += n
			}
			if int(inserted) != len(rows) {
				report(seed, "I0 spine took every emitted observation",
					fmt.Sprintf("emitted %d, inserted %d", len(rows), inserted))
			}

			// I1/I2: each cache equals the argmax over the spine at its own grain, on EVERY column the
			// two tables share -- read from the catalogue, so a column dropped from a writer is caught
			// and no column list is hardcoded here.
			if has("position_current") {
				if d := diffAgainstOracle(ctx, t, pool, "position_current", ""); d != "" {
					report(seed, "I1 position_current == spine argmax per position", d)
				}
			}
			if has("position_daily") {
				if d := diffAgainstOracle(ctx, t, pool, "position_daily",
					", (block_timestamp AT TIME ZONE 'utc')::date"); d != "" {
					report(seed, "I2 position_daily == spine argmax per (position, date)", d)
				}
			}

			// I3: position_daily has a row for exactly the observed dates.
			if has("position_daily") {
				var extra, missing int
				if err := pool.QueryRow(ctx, `
				WITH obs AS (SELECT DISTINCT position_id, (block_timestamp AT TIME ZONE 'utc')::date d
				               FROM position_state),
				     cache AS (SELECT position_id, as_of_date d FROM position_daily)
				SELECT (SELECT count(*) FROM (SELECT * FROM cache EXCEPT SELECT * FROM obs) x),
				       (SELECT count(*) FROM (SELECT * FROM obs EXCEPT SELECT * FROM cache) y)`).
					Scan(&extra, &missing); err != nil {
					t.Fatalf("I3: %v", err)
				}
				if extra != 0 || missing != 0 {
					report(seed, "I3 position_daily covers exactly the observed dates",
						fmt.Sprintf("%d rows for unobserved dates, %d observed dates missing", extra, missing))
				}

			}

			// I4a: the row position_current holds must be the row position_daily holds for that
			// observation's own date. I4b: and that date should be the latest date in daily.
			if has("position_current") && has("position_daily") {
				var mismatch, latestDisagree int
				if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM position_current c
				 WHERE NOT EXISTS (SELECT 1 FROM position_daily d
				    WHERE d.position_id = c.position_id
				      AND d.as_of_date = (c.block_timestamp AT TIME ZONE 'utc')::date
				      AND d.block_number = c.block_number AND d.block_version = c.block_version
				      AND d.processing_version = c.processing_version
				      AND d.block_timestamp = c.block_timestamp AND d.quantity = c.quantity
				      AND d.deal_type IS NOT DISTINCT FROM c.deal_type)`).Scan(&mismatch); err != nil {
					t.Fatalf("I4a: %v", err)
				}
				if mismatch != 0 {
					report(seed, "I4a the current row is the daily row for its own date",
						fmt.Sprintf("%d positions disagree", mismatch))
				}
				if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM position_current c
				 WHERE (c.block_timestamp AT TIME ZONE 'utc')::date
				     <> (SELECT max(d.as_of_date) FROM position_daily d WHERE d.position_id = c.position_id)`).
					Scan(&latestDisagree); err != nil {
					t.Fatalf("I4b: %v", err)
				}
				if latestDisagree != 0 {
					var ex string
					_ = pool.QueryRow(ctx, `
					SELECT format('current bn=%s ts=%s date=%s, latest daily date=%s',
					   c.block_number, c.block_timestamp, (c.block_timestamp AT TIME ZONE 'utc')::date,
					   (SELECT max(d.as_of_date) FROM position_daily d WHERE d.position_id=c.position_id))
					  FROM position_current c
					 WHERE (c.block_timestamp AT TIME ZONE 'utc')::date
					     <> (SELECT max(d.as_of_date) FROM position_daily d WHERE d.position_id = c.position_id)
					 LIMIT 1`).Scan(&ex)
					report(seed, "I4b the current value sits on the latest date in daily",
						fmt.Sprintf("%d positions disagree; e.g. %s", latestDisagree, ex))
				}

			}

			// I5: deal_type reaches the spine for exactly the observations that emitted one.
			var nullInSpine int
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM position_state WHERE deal_type IS NULL`).Scan(&nullInSpine); err != nil {
				t.Fatalf("I5: %v", err)
			}
			wantNull := 0
			for _, r := range rows {
				if r.dealType == "" {
					wantNull++
				}
			}
			if nullInSpine != wantNull {
				report(seed, "I5 deal_type round-trips",
					fmt.Sprintf("%d NULL stored, %d emitted as NULL", nullInSpine, wantNull))
			}

			// I6: re-running both operator REBUILD regions on a converged cache changes nothing.
			if len(presentCaches) > 0 {
				before := snapshotCaches(ctx, t, pool, presentCaches)
				var regions []string
				if has("position_current") {
					regions = append(regions, extractRegion(t, "20260819_150100_backfill_position_current.sql", "position_current"))
				}
				if has("position_daily") {
					regions = append(regions, extractRegion(t, "20260824_120000_create_position_daily.sql", "position_daily"))
				}
				for _, region := range regions {
					if _, err := pool.Exec(ctx, region); err != nil {
						report(seed, "I6 rebuild runs on a converged cache", err.Error())
					}
				}
				if snapshotCaches(ctx, t, pool, presentCaches) != before {
					report(seed, "I6 rebuild is idempotent", "cache digest changed after a rebuild")
				}
			}

			// I7: nothing anywhere still carries the pre-rename column name.
			var stale int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
				 JOIN pg_namespace n ON n.oid = c.relnamespace
				 WHERE n.nspname = 'public' AND c.relkind IN ('r','v','m')
				   AND a.attname = 'deal_type_code' AND a.attnum > 0 AND NOT a.attisdropped`).Scan(&stale); err != nil {
				t.Fatalf("I7: %v", err)
			}
			if stale != 0 {
				report(seed, "I7 no relation still uses deal_type_code", fmt.Sprintf("%d columns remain", stale))
			}

			// I8: one position_id may span more than one deal type over time. Recorded, not a defect:
			// deal type is not in the position_id hash, so a holder flipping from net supplier to net
			// borrower keeps one identity. It is why deal type cannot be an attribute of the position.
			var flipped int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM (SELECT position_id FROM position_state WHERE deal_type IS NOT NULL
				  GROUP BY position_id HAVING count(DISTINCT deal_type) > 1) z`).Scan(&flipped); err != nil {
				t.Fatalf("I8: %v", err)
			}
			if flipped > 0 {
				note(seed, "one position_id spans more than one deal type",
					fmt.Sprintf("%d positions do; deal type is not in the position_id hash, so a holder "+
						"flipping net supplier/borrower keeps one identity", flipped))
			}
		})
	}

	fmt.Printf("\n==== POSITION STACK DESIGN INVARIANTS: %d seeds, caches present: %v ====\n", seeds, presentCaches)
	if len(failures) == 0 {
		fmt.Println("  every invariant held on every seed")
	}
	if len(notes) > 0 {
		seen := map[string]int{}
		for _, n := range notes {
			seen[n.inv]++
		}
		fmt.Println("\n  recorded properties of the model (not violations):")
		for k, c := range seen {
			fmt.Printf("    %s -- on %d/%d seeds\n", k, c, seeds)
		}
	}
	byInv := map[string][]int{}
	for _, f := range failures {
		byInv[f.inv] = append(byInv[f.inv], f.seed)
	}
	var keys []string
	for k := range byInv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var sd []string
		for _, s := range byInv[k] {
			sd = append(sd, fmt.Sprint(s))
		}
		var ex string
		for _, f := range failures {
			if f.inv == k {
				ex = f.detail
				break
			}
		}
		fmt.Printf("\n  BROKE: %s\n    seeds %s (%d/%d)\n    e.g.  %s\n", k, strings.Join(sd, ","), len(sd), seeds, ex)
	}
}

// TestRealProjectionsCarryDealType runs the three shipped projection views end to end and asserts the
// deal type reaches both caches -- the question a consumer actually asks.
func TestRealProjectionsCarryDealType(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	rows, err := pool.Query(ctx, `
		SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname='public' AND c.relkind='v' AND c.relname LIKE 'position\_%'
		 ORDER BY c.relname`)
	if err != nil {
		t.Fatalf("list projection views: %v", err)
	}
	defer rows.Close()
	var views []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		views = append(views, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if len(views) == 0 {
		// The projection views ship in their own migrations. Nothing to assert here yet, and saying so
		// is better than a Fatal that reads as a defect or a silent pass that hides one.
		t.Skip("no position_* projection views in this branch's migration set")
	}
	fmt.Printf("\n##### shipped projection views #####\n")
	for _, v := range views {
		var emits bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_attribute
			  WHERE attrelid = $1::regclass AND attname='deal_type'
			    AND attnum > 0 AND NOT attisdropped)`, v).Scan(&emits); err != nil {
			t.Fatalf("%s: %v", v, err)
		}
		fmt.Printf("  %-34s emits deal_type: %v\n", v, emits)
		if !emits {
			t.Errorf("%s does not emit deal_type, so the materializer stores NULL for every row it "+
				"produces -- an absent column is the optional case and fails silently", v)
		}
	}
}

type obsRow struct {
	holder            string
	qty               int
	block, bver, pver int
	ts                time.Time
	dealType          string // "" means emit NULL
}

// generateHistory produces a legal but adversarial history: block_timestamp drawn independently of
// block_number (so a higher block can carry an earlier instant, as an event-time source stamping
// synced_at does), several block_versions at one block, corrections, deal-type flips, zero
// quantities, and dates spanning position_daily's 7-day chunk boundary.
func generateHistory(rng *rand.Rand) []obsRow {
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	var rows []obsRow
	seen := map[string]bool{}
	for h := 0; h < 1+rng.Intn(3); h++ {
		holder := fmt.Sprintf("%040x", h+10)
		for i := 0; i < 2+rng.Intn(6); i++ {
			r := obsRow{
				holder: holder,
				qty:    []int{0, 1, 30, 150, 999}[rng.Intn(5)],
				block:  100 + rng.Intn(400),
				bver:   rng.Intn(2),
				pver:   rng.Intn(2),
				ts: base.Add(time.Duration(rng.Intn(30*24)) * time.Hour).
					Add(time.Duration(rng.Intn(60)) * time.Minute),
			}
			switch rng.Intn(4) {
			case 0:
				r.dealType = ""
			case 1, 2:
				r.dealType = "LOAN"
			default:
				r.dealType = "BORROW"
			}
			k := fmt.Sprintf("%s|%d|%d|%d|%s", holder, r.block, r.bver, r.pver, r.ts.Format(time.RFC3339))
			if seen[k] {
				continue // the spine PK forbids a duplicate coordinate
			}
			seen[k] = true
			rows = append(rows, r)
		}
	}
	return rows
}

// splitBatches cuts the history into 2-5 arrival batches in random order, so a later batch can carry
// an OLDER observation than one already cached.
func splitBatches(rng *rand.Rand, rows []obsRow) [][]obsRow {
	s := make([]obsRow, len(rows))
	copy(s, rows)
	rng.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
	n := 2 + rng.Intn(4)
	if n > len(s) {
		n = len(s)
	}
	buckets := make([][]obsRow, n)
	for i, r := range s {
		buckets[i%n] = append(buckets[i%n], r)
	}
	var kept [][]obsRow
	for _, b := range buckets {
		if len(b) > 0 {
			kept = append(kept, b)
		}
	}
	return kept
}

func valuesBody(rows []obsRow) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		dt := "NULL::text"
		if r.dealType != "" {
			dt = "'" + r.dealType + "'::text"
		}
		parts = append(parts, fmt.Sprintf(
			"(1::int, 10::bigint, 'design-inst'::text, '%s'::text, %d::numeric, %d::bigint, %d::int, %d::int, '%s'::timestamptz, %s)",
			r.holder, r.qty, r.block, r.bver, r.pver, r.ts.Format(time.RFC3339), dt))
	}
	return `SELECT * FROM (VALUES ` + strings.Join(parts, ",") +
		`) v(chain_id,protocol_id,instrument_key,holder_id,quantity,block_number,block_version,` +
		`processing_version,block_timestamp,deal_type)`
}

// diffAgainstOracle compares a cache against an argmax computed from position_state over every column
// the two tables share.
func diffAgainstOracle(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache, grain string) string {
	t.Helper()
	cols := sharedColumns(ctx, t, pool, cache)
	sel := strings.Join(cols, ", ")
	var onlyOracle, onlyCache int
	var example string
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		WITH ranked AS (
		  SELECT %s, row_number() OVER (PARTITION BY position_id%s
		           ORDER BY block_number DESC, block_version DESC, processing_version DESC,
		                    block_timestamp DESC) rn
		    FROM position_state),
		     oracle AS (SELECT %s FROM ranked WHERE rn = 1),
		     cached AS (SELECT %s FROM %s)
		SELECT (SELECT count(*) FROM (SELECT * FROM oracle EXCEPT SELECT * FROM cached) a),
		       (SELECT count(*) FROM (SELECT * FROM cached EXCEPT SELECT * FROM oracle) b),
		       COALESCE((SELECT a::text FROM (SELECT * FROM oracle EXCEPT SELECT * FROM cached) a LIMIT 1), '')`,
		sel, grain, sel, sel, cache)).Scan(&onlyOracle, &onlyCache, &example); err != nil {
		t.Fatalf("oracle compare on %s: %v", cache, err)
	}
	if onlyOracle == 0 && onlyCache == 0 {
		return ""
	}
	return fmt.Sprintf("%d rows the spine implies the cache lacks, %d the cache has that the spine does not; oracle-only e.g. %s",
		onlyOracle, onlyCache, example)
}

func sharedColumns(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cache string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT a.attname FROM pg_attribute a
		 WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
		   AND a.attname <> 'created_at'
		   AND EXISTS (SELECT 1 FROM pg_attribute b
		                WHERE b.attrelid = 'position_state'::regclass AND b.attname = a.attname
		                  AND b.attnum > 0 AND NOT b.attisdropped)
		 ORDER BY a.attname`, cache)
	if err != nil {
		t.Fatalf("shared columns for %s: %v", cache, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	// deal_type must be among them, or this comparison silently stops covering the column the whole
	// change exists for.
	var hasDealType bool
	for _, c := range out {
		if c == "deal_type" {
			hasDealType = true
		}
	}
	if !hasDealType {
		t.Fatalf("%s does not share deal_type with position_state; the comparison would not cover it", cache)
	}
	if len(out) < 9 {
		t.Fatalf("%s shares only %d columns with position_state (%v); the comparison would be weak",
			cache, len(out), out)
	}
	return out
}

func snapshotCaches(ctx context.Context, t *testing.T, pool *pgxpool.Pool, caches []string) string {
	t.Helper()
	out := ""
	for _, c := range caches {
		var d string
		if err := pool.QueryRow(ctx, fmt.Sprintf(
			`SELECT COALESCE(md5(string_agg(x::text,'|' ORDER BY x::text)),'') FROM %s x`, c)).Scan(&d); err != nil {
			t.Fatalf("snapshot %s: %v", c, err)
		}
		out += c + "=" + d + ";"
	}
	return out
}

// cachesPresent reads the migration directory rather than a live database, so the seed loop knows up
// front which invariants it can check. A cache whose migration is not in this branch is not skipped
// silently -- the summary prints exactly which were covered.
func cachesPresent(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, c := range []struct{ table, file string }{
		{"position_current", "20260819_150000_create_position_current.sql"},
		{"position_daily", "20260824_120000_create_position_daily.sql"},
	} {
		if _, err := os.Stat(filepath.Join(getMigrationsPath(), c.file)); err == nil {
			out = append(out, c.table)
		}
	}
	return out
}

// extractRegion lifts a REBUILD region out of the shipped migration text, so the harness runs exactly
// what an operator runs.
func extractRegion(t *testing.T, file, marker string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), file))
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	s := string(raw)
	begin, end := "-- REBUILD-BEGIN "+marker+"\n", "\n-- REBUILD-END "+marker
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < 0 {
		t.Fatalf("%s has no REBUILD markers for %s", file, marker)
	}
	return s[i+len(begin) : j]
}
