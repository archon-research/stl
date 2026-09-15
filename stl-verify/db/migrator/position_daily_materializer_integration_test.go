//go:build integration

package migrator_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The rest of the position_daily_observation suite writes to the spine directly, which exercises the trigger but
// not the write path production actually uses. These drive the table through
// materialize_position_projection, where the batch is an INSERT ... WHERE NOT EXISTS ... ON CONFLICT
// DO NOTHING whose transition table is what the statement trigger reads. Reads go through
// position_daily, the newest appended row per (position, date).

// mppRow is one contract-shaped projection row with its own block_timestamp, which the package's
// shared row() helper pins to a single date.
func mppRow(ik string, qty, bn, bv, pv int, ts, dealType string) string {
	return "(1::int,10::bigint,'" + ik + "'::text,'" + strings.Repeat("b", 40) + "'::text," +
		strconv.Itoa(qty) + "::numeric,'" + dealType + "'::text," +
		strconv.Itoa(bn) + "::bigint," + strconv.Itoa(bv) + "::int," + strconv.Itoa(pv) + "::int,'" + ts + "'::timestamptz)"
}

// cacheDivergence reports every (position, date) where position_daily is not the spine's winning
// observation -- missing, extra, or holding a losing row -- as one description per divergence. The
// whole-row comparison is what a writer that forgets a column fails.
func cacheDivergence(t *testing.T, f *psFixture) []string {
	t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		WITH winner AS (
		    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
		           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date AS as_of_date,
		           p.instrument_key, p.quantity, p.deal_type, p.block_number, p.block_version,
		           p.processing_version, p.block_timestamp, p.projection, p.build_id, p.run_id
		      FROM position_state p
		     ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
		              p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
		)
		SELECT format('ik=%s date=%s cache=%s spine=%s',
		              coalesce(w.instrument_key, d.instrument_key),
		              coalesce(w.as_of_date, d.as_of_date)::text,
		              coalesce(to_jsonb(d) - 'position_id' - 'created_at', 'null'::jsonb)::text,
		              coalesce(to_jsonb(w) - 'position_id', 'null'::jsonb)::text)
		  FROM winner w
		  FULL OUTER JOIN position_daily d ON d.position_id = w.position_id AND d.as_of_date = w.as_of_date
		 WHERE w.position_id IS NULL OR d.position_id IS NULL
		    OR (d.instrument_key, d.quantity, d.block_number, d.block_version, d.processing_version,
		        d.block_timestamp, d.projection, d.build_id)
		       IS DISTINCT FROM
		       (w.instrument_key, w.quantity, w.block_number, w.block_version, w.processing_version,
		        w.block_timestamp, w.projection, w.build_id)
		    OR d.deal_type IS DISTINCT FROM w.deal_type
		    OR d.run_id IS DISTINCT FROM w.run_id
		 ORDER BY 1`)
	if err != nil {
		t.Fatalf("read the cache divergence: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("cache divergence iteration: %v", err)
	}
	return out
}

// cachedDays returns one position's series through the latest view as (date, quantity) pairs, keyed by
// instrument_key because the materializer derives position_id itself.
func cachedDays(t *testing.T, f *psFixture, ik string) []string {
	t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		SELECT as_of_date::text || '=' || quantity::text FROM position_daily
		 WHERE instrument_key = $1 ORDER BY as_of_date`, ik)
	if err != nil {
		t.Fatalf("cachedDays(%s): %v", ik, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("cachedDays(%s) iteration: %v", ik, err)
	}
	return out
}

// cacheDisagreement reports every position where position_current is not the position_daily row on
// that position's latest observed date. The two caches read the same spine through two triggers on the same
// statement, and their agreement is what the block_time_inverts_height refusal exists to protect: they
// order by block and date by timestamp, so an inverted pair makes them name different winners.
func cacheDisagreement(t *testing.T, f *psFixture) []string {
	t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		WITH latest_day AS (
		    SELECT DISTINCT ON (position_id) position_id, as_of_date, instrument_key, quantity, deal_type,
		           block_number, block_version, processing_version, block_timestamp, projection, build_id, run_id
		      FROM position_daily ORDER BY position_id, as_of_date DESC
		)
		SELECT format('ik=%s daily(%s)=%s current=%s',
		              coalesce(d.instrument_key, c.instrument_key), d.as_of_date::text,
		              coalesce(to_jsonb(d) - 'position_id' - 'instrument_key', 'null'::jsonb)::text,
		              coalesce(to_jsonb(c) - 'position_id' - 'created_at' - 'instrument_key', 'null'::jsonb)::text)
		  FROM latest_day d
		  FULL OUTER JOIN position_current c ON c.position_id = d.position_id
		 WHERE d.position_id IS NULL OR c.position_id IS NULL
		    OR (d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
		        d.projection, d.build_id)
		       IS DISTINCT FROM
		       (c.quantity, c.block_number, c.block_version, c.processing_version, c.block_timestamp,
		        c.projection, c.build_id)
		    OR d.deal_type IS DISTINCT FROM c.deal_type
		    OR d.run_id IS DISTINCT FROM c.run_id
		 ORDER BY 1`)
	if err != nil {
		t.Fatalf("read the cross-cache disagreement: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("cross-cache disagreement iteration: %v", err)
	}
	return out
}

// TestPositionDailyThroughTheMaterializer shares one migrated schema across its cases, per
// stl-verify/AGENTS.md ("Share setup, don't repeat it"); each uses its own instrument_keys.
func TestPositionDailyThroughTheMaterializer(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()

	// One batch, several positions and dates, corrections and a reorg version: the reading must equal
	// the spine's argmax per (position, UTC date) on every column, not merely hold a row.
	t.Run("a materialized batch lands at the spine argmax per date", func(t *testing.T) {
		body := valuesOf(
			mppRow("mpp-a", 100, 500, 0, 0, "2026-04-01T01:00:00Z", "LOAN"),
			mppRow("mpp-a", 150, 600, 0, 0, "2026-04-01T20:00:00Z", "LOAN"),
			mppRow("mpp-a", 175, 700, 0, 0, "2026-04-02T05:00:00Z", "BORROW"),
			mppRow("mpp-b", 20, 550, 0, 0, "2026-04-01T02:00:00Z", "LOAN"),
			// A reorg re-observation of one height, and a correction on top of it.
			mppRow("mpp-b", 25, 550, 1, 0, "2026-04-01T02:00:00Z", "LOAN"),
			mppRow("mpp-b", 30, 550, 1, 1, "2026-04-01T02:00:00Z", "BORROW"),
		)
		if n := f.mppN(t, "pv_daily_argmax", body, "a multi-position batch"); n != 6 {
			t.Fatalf("the materializer appended %d observations, want 6", n)
		}
		if got := cachedDays(t, f, "mpp-a"); len(got) != 2 || got[0] != "2026-04-01=150" || got[1] != "2026-04-02=175" {
			t.Errorf("mpp-a cached %v; want the day's winner on each observed date, 2026-04-01=150 and 2026-04-02=175", got)
		}
		if got := cachedDays(t, f, "mpp-b"); len(got) != 1 || got[0] != "2026-04-01=30" {
			t.Errorf("mpp-b cached %v; want 2026-04-01=30, the highest (block_version, processing_version) for the day", got)
		}
		if d := cacheDivergence(t, f); len(d) != 0 {
			t.Errorf("the cache diverges from the spine argmax on %d (position, date)(s): %s", len(d), strings.Join(d, " | "))
		}
	})

	// The block_time_inverts_height refusal exists BECAUSE the caches order by block and date by
	// timestamp. The withheld position must reach neither the spine nor the cache, and its peer must
	// still land -- a refusal that took the whole batch down would be a different bug.
	t.Run("a withheld position reaches neither spine nor cache while its peer lands", func(t *testing.T) {
		body := valuesOf(
			mppRow("mpp-inv", 10, 800, 0, 0, "2026-04-10T00:00:00Z", "LOAN"),
			mppRow("mpp-inv", 11, 900, 0, 0, "2026-04-09T00:00:00Z", "LOAN"),
			mppRow("mpp-peer", 44, 850, 0, 0, "2026-04-10T12:00:00Z", "LOAN"),
		)
		if n := f.mppN(t, "pv_daily_inverted", body, "an inverted pair plus a peer"); n != 1 {
			t.Errorf("the materializer appended %d observations, want 1: the peer lands, the inverted position is withheld", n)
		}
		var refusals int
		if err := f.pool.QueryRow(f.ctx, `
			SELECT count(*) FROM position_projection_refusal
			 WHERE reason = 'block_time_inverts_height' AND detail LIKE 'ik=mpp-inv %'`).Scan(&refusals); err != nil {
			t.Fatal(err)
		}
		if refusals == 0 {
			t.Error("the inverted position was not recorded as refused, so this case is not exercising the withholding path at all")
		}
		if got := cachedDays(t, f, "mpp-inv"); len(got) != 0 {
			t.Errorf("the withheld position is cached as %v; nothing the spine refused may reach the cache", got)
		}
		if got := cachedDays(t, f, "mpp-peer"); len(got) != 1 || got[0] != "2026-04-10=44" {
			t.Errorf("the peer cached %v; want 2026-04-10=44 -- one position's refusal must not withhold another", got)
		}
		if d := cacheDivergence(t, f); len(d) != 0 {
			t.Errorf("the cache diverges from the spine argmax: %s", strings.Join(d, " | "))
		}
	})

	// A projection re-emitting a stored coordinate with a changed payload is drift: the spine keeps the
	// stored row and records the drift, inserting nothing. The cache must not take the emitted value --
	// if it did, it would hold a number that exists in no observation of history.
	t.Run("a drift re-emission does not move the cache", func(t *testing.T) {
		const ik = "mpp-drift"
		if n := f.mppN(t, "pv_daily_drift", valuesOf(mppRow(ik, 70, 1000, 0, 0, "2026-04-20T00:00:00Z", "LOAN")),
			"the stored observation"); n != 1 {
			t.Fatalf("seeding appended %d, want 1", n)
		}
		// Same coordinate, different quantity and deal_type.
		if n := f.mppN(t, "pv_daily_drift", valuesOf(mppRow(ik, 99, 1000, 0, 0, "2026-04-20T00:00:00Z", "BORROW")),
			"the drifted re-emission"); n != 0 {
			t.Errorf("the drifted re-emission appended %d observations, want 0: the spine keeps the stored row", n)
		}
		var drifts int
		if err := f.pool.QueryRow(f.ctx, `
			SELECT count(*) FROM position_projection_refusal
			 WHERE reason IN ('observation_drift', 'deal_type_drift') AND detail LIKE 'ik=' || $1 || ' %'`, ik).
			Scan(&drifts); err != nil {
			t.Fatal(err)
		}
		if drifts == 0 {
			t.Error("no drift was recorded, so the re-emission did not take the drift path and this proves nothing")
		}
		if got := cachedDays(t, f, ik); len(got) != 1 || got[0] != "2026-04-20=70" {
			t.Errorf("the cache holds %v after a drifted re-emission; want 2026-04-20=70, the value the spine kept", got)
		}
		if got := f.dealTypeOf(t, ik, "2026-04-20"); got != "LOAN" {
			t.Errorf("the cached deal_type is %q after a drifted re-emission; want LOAN, the stored one", got)
		}
		if d := cacheDivergence(t, f); len(d) != 0 {
			t.Errorf("the cache diverges from the spine argmax: %s", strings.Join(d, " | "))
		}
	})

	// A re-run over unchanged history appends nothing to the spine, so the statement trigger fires on an
	// empty transition table and must append nothing here either.
	t.Run("a re-run appends nothing to the table", func(t *testing.T) {
		const ik = "mpp-rerun"
		body := valuesOf(mppRow(ik, 60, 1100, 0, 0, "2026-04-25T00:00:00Z", "LOAN"))
		if n := f.mppN(t, "pv_daily_rerun", body, "the first run"); n != 1 {
			t.Fatalf("the first run appended %d, want 1", n)
		}
		before := f.dailyImagesFor(t, ik)
		if len(before) != 1 {
			t.Fatalf("the first run left %d row(s) for %s, want 1", len(before), ik)
		}
		if n := f.mppN(t, "pv_daily_rerun", body, "the re-run"); n != 0 {
			t.Errorf("the re-run appended %d observations, want 0", n)
		}
		// Row images, not a count: a count is unchanged by a rewrite in place, which is the thing an
		// append-only table must never do and the thing a re-run is most likely to do.
		if after := f.dailyImagesFor(t, ik); !slices.Equal(before, after) {
			t.Errorf("the re-run changed the stored rows for %s while appending nothing to the spine:\n  before %v\n  after  %v", ik, before, after)
		}
		if d := cacheDivergence(t, f); len(d) != 0 {
			t.Errorf("the reading diverges from the spine argmax: %s", strings.Join(d, " | "))
		}
	})

	// build_id and run_id are the audit trail for which run last advanced a day. They are the
	// materializer's own arguments, so only a run through it can prove the cache carries them.
	t.Run("the materializer's build_id and run_id reach the cache", func(t *testing.T) {
		const ik = "mpp-audit"
		if _, err := f.pool.Exec(f.ctx, `CREATE OR REPLACE VIEW pv_daily_audit AS `+
			valuesOf(mppRow(ik, 12, 1200, 0, 0, "2026-04-28T00:00:00Z", "LOAN"))); err != nil {
			t.Fatalf("create the view: %v", err)
		}
		var buildID int
		var runID int64
		if err := f.pool.QueryRow(f.ctx, `
			INSERT INTO build_registry (git_hash, service) VALUES ('mpp-audit-hash', 'migrator-test') RETURNING id`).Scan(&buildID); err != nil {
			t.Fatalf("register a build: %v", err)
		}
		if err := f.pool.QueryRow(f.ctx, `
			INSERT INTO writer_run (build_id, reference_snapshot, reference_effective_at)
			VALUES ($1, 'mpp-audit', now()) RETURNING id`, buildID).Scan(&runID); err != nil {
			t.Fatalf("open a writer run: %v", err)
		}
		var inserted int64
		if err := f.pool.QueryRow(f.ctx,
			`SELECT materialize_position_projection('pv_daily_audit'::regclass, $1, $2)`, buildID, runID).Scan(&inserted); err != nil {
			t.Fatalf("materialize with a build and run: %v", err)
		}
		if inserted != 1 {
			t.Fatalf("appended %d, want 1", inserted)
		}
		var gotBuild int
		var gotRun int64
		if err := f.pool.QueryRow(f.ctx, `
			SELECT build_id, run_id FROM position_daily WHERE instrument_key = $1`, ik).Scan(&gotBuild, &gotRun); err != nil {
			t.Fatalf("read the cached audit columns: %v", err)
		}
		if gotBuild != buildID || gotRun != runID {
			t.Errorf("the cache carries build_id=%d run_id=%d; want %d and %d from the run that wrote the observation",
				gotBuild, gotRun, buildID, runID)
		}
		if d := cacheDivergence(t, f); len(d) != 0 {
			t.Errorf("the cache diverges from the spine argmax: %s", strings.Join(d, " | "))
		}
	})
}

// The two caches are fed by two triggers on the same INSERT, from the same spine. position_current holds
// the newest observation outright; position_daily_observation's latest date must be that same observation, or one of
// them is telling a consumer something the other denies.
//
// This holds while a position's newest observation also falls on its latest observed date, which the
// block_time_inverts_height refusal secures across DIFFERENT blocks. It does NOT hold when a correction
// at the SAME block carries an instant on an earlier date -- see
// TestPositionDailyKeepsADayACorrectionMovedAway, which pins that gap.
func TestPositionCurrentAndPositionDailyNameTheSameWinner(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()

	// Multi-day history per position, with a reorg re-observation and a correction on the newest day --
	// the coordinates where the two orderings could pick differently.
	body := valuesOf(
		mppRow("agree-a", 10, 400, 0, 0, "2026-05-01T02:00:00Z", "LOAN"),
		mppRow("agree-a", 20, 500, 0, 0, "2026-05-02T02:00:00Z", "LOAN"),
		mppRow("agree-a", 30, 600, 0, 0, "2026-05-03T02:00:00Z", "BORROW"),
		mppRow("agree-a", 35, 600, 1, 0, "2026-05-03T02:00:00Z", "BORROW"),
		mppRow("agree-a", 40, 600, 1, 1, "2026-05-03T02:00:00Z", "LOAN"),
		// A position whose newest day carries several observations, so the within-day pick matters.
		mppRow("agree-b", 5, 410, 0, 0, "2026-05-01T03:00:00Z", "LOAN"),
		mppRow("agree-b", 6, 610, 0, 0, "2026-05-03T01:00:00Z", "LOAN"),
		mppRow("agree-b", 7, 620, 0, 0, "2026-05-03T23:00:00Z", "LOAN"),
		// A single-observation position: its only day is also its newest.
		mppRow("agree-c", 99, 700, 0, 0, "2026-05-04T00:00:00Z", "BORROW"),
	)
	if n := f.mppN(t, "pv_agree", body, "a multi-day history"); n != 9 {
		t.Fatalf("the materializer appended %d observations, want 9", n)
	}

	// Positive control: both caches are actually populated, so an empty-vs-empty comparison cannot pass.
	var daily, current int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (SELECT count(*) FROM position_daily_observation), (SELECT count(*) FROM position_current)`).
		Scan(&daily, &current); err != nil {
		t.Fatal(err)
	}
	if daily != 6 || current != 3 {
		t.Fatalf("position_daily_observation holds %d rows and position_current %d; want 6 observed days across 3 positions", daily, current)
	}
	if d := cacheDisagreement(t, f); len(d) != 0 {
		t.Errorf("the two caches name different winners for %d position(s): %s", len(d), strings.Join(d, " | "))
	}

	// And after a rebuild of each from the spine alone, which is the other writer.
	for _, proc := range []string{"CALL rebuild_position_daily()", "CALL rebuild_position_current()"} {
		if _, err := f.pool.Exec(f.ctx, proc); err != nil {
			t.Fatalf("%s: %v", proc, err)
		}
	}
	if d := cacheDisagreement(t, f); len(d) != 0 {
		t.Errorf("the two caches disagree after a rebuild from the spine: %s", strings.Join(d, " | "))
	}
}

// dealTypeOf reads one cached day's deal_type, "NULL" when it is absent.
func (f *psFixture) dealTypeOf(t *testing.T, ik, date string) string {
	t.Helper()
	var dt *string
	if err := f.pool.QueryRow(f.ctx,
		`SELECT deal_type FROM position_daily WHERE instrument_key = $1 AND as_of_date = $2::date`, ik, date).Scan(&dt); err != nil {
		t.Fatalf("dealTypeOf(%s, %s): %v", ik, date, err)
	}
	if dt == nil {
		return "NULL"
	}
	return *dt
}

// dailyImagesFor is every stored row for one instrument with its physical identity: ctid and xmin
// move under an UPDATE, so comparing these catches a rewrite that leaves the row count alone.
func (f *psFixture) dailyImagesFor(t *testing.T, ik string) []string {
	t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		SELECT d.ctid::text || ' ' || d.xmin::text || ' ' || to_jsonb(d)::text
		  FROM position_daily_observation d WHERE d.instrument_key = $1 ORDER BY 1`, ik)
	if err != nil {
		t.Fatalf("dailyImagesFor(%s): %v", ik, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("dailyImagesFor(%s) iteration: %v", ik, err)
	}
	return out
}

// A correction at the SAME block_number carrying an earlier instant, crossing UTC midnight. Nothing
// refuses it: block_time_inverts_height compares a higher block against an earlier instant, and this
// pair shares a block. The correction lands on the EARLIER date, and position_daily_observation -- append-only, so
// nothing removes a row -- keeps the superseded observation standing on the later date. That later
// date is then its newest, so the two caches name different winners.
//
// Pinned rather than fixed: removing the stale day means a DELETE, which the append-only contract in
// the table's COMMENT (VEC-636) rules out. This test fails the day that changes, which is when the
// COMMENT and this comment must change too.
func TestPositionDailyKeepsADayACorrectionMovedAway(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()

	const ik = "moved-day"
	if n := f.mppN(t, "pv_moved", valuesOf(
		mppRow(ik, 100, 1000, 0, 0, "2026-06-02T00:00:05Z", "LOAN")), "the original, just after midnight"); n != 1 {
		t.Fatalf("seeding appended %d, want 1", n)
	}
	if n := f.mppN(t, "pv_moved", valuesOf(
		mppRow(ik, 555, 1000, 0, 1, "2026-06-01T23:59:58Z", "LOAN")), "the correction, just before it"); n != 1 {
		t.Fatalf("the correction appended %d, want 1: same block, so nothing refuses it", n)
	}
	// Nothing was withheld -- this is the sanctioned correction path, not a refusal case.
	var refused int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM position_projection_refusal WHERE detail LIKE 'ik=' || $1 || ' %'`, ik).Scan(&refused); err != nil {
		t.Fatal(err)
	}
	if refused != 0 {
		t.Fatalf("the correction was refused %d time(s); this case is about the path that is NOT refused", refused)
	}

	if got := cachedDays(t, f, ik); len(got) != 2 ||
		got[0] != "2026-06-01=555" || got[1] != "2026-06-02=100" {
		t.Errorf("position_daily_observation holds %v; want the correction on 2026-06-01 and the superseded row still on 2026-06-02", got)
	}
	// The known consequence: position_daily_observation's newest day is NOT the position's newest observation.
	if d := cacheDisagreement(t, f); len(d) != 1 {
		t.Errorf("the caches disagree on %d position(s), want exactly 1 -- if this is now 0 the gap is fixed "+
			"and the forward-only COMMENT plus this test must be updated; more than 1 means something else broke: %s",
			len(d), strings.Join(d, " | "))
	}
}
