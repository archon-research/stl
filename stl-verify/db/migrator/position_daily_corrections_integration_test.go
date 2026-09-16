//go:build integration

package migrator_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// callRetract runs the real writer and returns what it appended, or the error it raised.
func (f *positionDailyFixture) callRetract(id, date, ticket, reason string) (int64, error) {
	f.t.Helper()
	var appended int64
	err := f.pool.QueryRow(f.ctx,
		`CALL retract_position_daily(sha256($1::bytea), $2, $3, $4, NULL)`, id, date, ticket, reason).Scan(&appended)
	return appended, err
}

// anomalies returns the reasons position_daily_anomaly reports for one position, ordered, so a case
// asserts both what fired and what did not.
func (f *positionDailyFixture) anomalies(id string) []string {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		SELECT reason || ': ' || detail FROM position_daily_anomaly
		 WHERE position_id = sha256($1::bytea) ORDER BY reason, detail`, id)
	if err != nil {
		f.t.Fatalf("read position_daily_anomaly for %s: %v", id, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("position_daily_anomaly iteration: %v", err)
	}
	return out
}

func reasonsOf(rows []string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, strings.SplitN(r, ":", 2)[0])
	}
	return out
}

// The writer that appends a retraction. Before it, withdrawing a key meant hand-writing an INSERT
// and getting the coordinate, the sequence and the attribution right by eye.
func TestRetractPositionDaily(t *testing.T) {
	f := newPositionDailyFixture(t)
	const date = "2026-01-01"

	t.Run("withdraws the day and reports one row", func(t *testing.T) {
		const id = "corr-basic"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		n, err := f.callRetract(id, date, "VEC-636", "the key should never have existed")
		if err != nil {
			t.Fatalf("retract: %v", err)
		}
		if n != 1 {
			t.Errorf("the procedure reported %d appended, want 1", n)
		}
		if f.dayPresent(id, date) {
			t.Errorf("the day survived a retraction written by the procedure itself")
		}
	})

	t.Run("is idempotent on a re-run", func(t *testing.T) {
		const id = "corr-idem"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if _, err := f.callRetract(id, date, "VEC-636", "first"); err != nil {
			t.Fatalf("first retract: %v", err)
		}
		rows := f.dayRows(id, date)
		n, err := f.callRetract(id, date, "VEC-636", "a crashed run, redone")
		if err != nil {
			t.Fatalf("the re-run raised instead of doing nothing: %v", err)
		}
		if n != 0 {
			t.Errorf("the re-run reported %d appended, want 0", n)
		}
		if got := f.dayRows(id, date); got != rows {
			t.Errorf("the re-run added %d row(s); a retry must write nothing", got-rows)
		}
	})

	t.Run("refuses a day it does not hold", func(t *testing.T) {
		_, err := f.callRetract("corr-absent", date, "VEC-636", "nothing is there")
		if err == nil {
			t.Fatalf("retracting an absent day was accepted; a caller typo must not be a quiet zero")
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || !strings.Contains(pgErr.Message, "no rows for position") {
			t.Errorf("failed with %v, want the procedure's own 'no rows for position' exception", err)
		}
	})

	for _, tc := range []struct{ name, ticket, reason string }{
		{"a blank ticket", "  ", "why"},
		{"an empty ticket", "", "why"},
		{"a blank reason", "VEC-636", " "},
	} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			id := "corr-attr-" + strings.ReplaceAll(tc.name, " ", "-")
			f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
			f.crystallize()
			// P0001 is the procedure's own raise. Any other error would mean the table caught it
			// instead, which is a different guarantee.
			_, err := f.callRetract(id, date, tc.ticket, tc.reason)
			var pgErr *pgconn.PgError
			if err == nil {
				t.Errorf("accepted %s; an unattributed withdrawal is not auditable", tc.name)
			} else if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
				t.Errorf("%s failed with %v, want the procedure's own raise (P0001)", tc.name, err)
			}
			if !f.dayPresent(id, date) {
				t.Errorf("the day was withdrawn by a call that should have been refused")
			}
		})
	}

	// The attribution CHECK is on the table, so it holds against a hand-written INSERT too, in both
	// directions: a tombstone without a ticket, and a live row carrying one.
	t.Run("the table refuses unattributed or mis-attributed rows", func(t *testing.T) {
		const id = "corr-check"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		for _, tc := range []struct{ name, retracted, ticket string }{
			{"a tombstone with no ticket", "TRUE", "NULL"},
			{"a live row carrying a ticket", "NULL", "'VEC-636'"},
		} {
			_, err := f.pool.Exec(f.ctx, `
				INSERT INTO position_daily_observation
				    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
				     block_number, block_version, processing_version, block_timestamp, projection, build_id,
				     run_id, deal_type, is_retracted, correction_seq, retraction_ticket, retraction_reason)
				SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
				       d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
				       d.projection, d.build_id, d.run_id, d.deal_type, `+tc.retracted+`, d.correction_seq + 1,
				       `+tc.ticket+`, 'why'
				  FROM position_daily_observation d
				 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
				 ORDER BY d.correction_seq DESC LIMIT 1`, id, date)
			var pgErr *pgconn.PgError
			if err == nil {
				t.Errorf("%s was accepted", tc.name)
			} else if !errors.As(err, &pgErr) || pgErr.ConstraintName != "position_daily_observation_retraction_attribution_chk" {
				t.Errorf("%s failed with %v (constraint %q), want the attribution check", tc.name, err, pgErr.ConstraintName)
			}
		}
	})
}

// The view that makes the gaps queryable instead of leaving them as paragraphs.
func TestPositionDailyAnomaly(t *testing.T) {
	f := newPositionDailyFixture(t)
	const date = "2026-01-01"

	// A day the writer has already caught up on reports nothing: the steady state has to be empty,
	// or every real finding drowns.
	t.Run("a healthy day reports nothing", func(t *testing.T) {
		const id = "anom-clean"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if got := f.anomalies(id); len(got) != 0 {
			t.Errorf("a crystallized, uncorrected day reports %v, want nothing", got)
		}
	})

	// The writer behind the spine, and the same day after it catches up.
	t.Run("stale_day clears when the writer catches up", func(t *testing.T) {
		const id = "anom-stale"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		if got := reasonsOf(f.anomalies(id)); len(got) != 1 || got[0] != "stale_day" {
			t.Errorf("before the tick the view reports %v, want exactly [stale_day]", got)
		}
		f.crystallize()
		if got := f.anomalies(id); len(got) != 0 {
			t.Errorf("after the tick the view still reports %v; only a finding that SURVIVES a "+
				"crystallization is real", got)
		}
	})

	// The midnight case: same block, higher version, now dated a day earlier.
	t.Run("moved_day names the correction that crossed midnight", func(t *testing.T) {
		const id = "anom-moved"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-02T00:00:05Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 55, block: 100, pv: 1, ts: "2026-01-01T23:59:58Z", dealType: "LOAN"})
		f.crystallize()
		got := f.anomalies(id)
		var moved []string
		for _, a := range got {
			if strings.HasPrefix(a, "moved_day") {
				moved = append(moved, a)
			}
		}
		if len(moved) != 1 {
			t.Fatalf("the view reports %d moved_day row(s), want 1: %v", len(moved), got)
		}
		if !strings.Contains(moved[0], "2026-01-02") || !strings.Contains(moved[0], "2026-01-01") {
			t.Errorf("moved_day says %q; it must name the stranded date and the date the block moved to", moved[0])
		}
		// And the instrument closes it, which is the point of detecting it at all.
		if _, err := f.callRetract(id, "2026-01-02", "VEC-636", "the correction moved this block to 01-01"); err != nil {
			t.Fatalf("retract the stranded day: %v", err)
		}
		if got := f.anomalies(id); len(got) != 0 {
			t.Errorf("after retracting the stranded day the view still reports %v", got)
		}
	})

	// A date the spine no longer has any observation on. The crystallizer only appends, so no tick
	// can clear it; only a decision can. Reached here by deleting the spine row, which is what the
	// superuser recovery path in 20260818_130000 can do.
	t.Run("orphaned_day fires when the spine has nothing on that date", func(t *testing.T) {
		const id = "anom-orphan"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if got := f.anomalies(id); len(got) != 0 {
			t.Fatalf("the day reports %v before the spine row goes; this case starts from the wrong state", got)
		}
		if _, err := f.pool.Exec(f.ctx,
			`DELETE FROM position_state WHERE position_id = sha256($1::bytea)`, id); err != nil {
			t.Fatalf("remove the spine row: %v", err)
		}
		if got := reasonsOf(f.anomalies(id)); len(got) != 1 || got[0] != "orphaned_day" {
			t.Errorf("the view reports %v, want exactly [orphaned_day]", got)
		}
		// And the only instrument that clears it is a decision to withdraw the day.
		if _, err := f.callRetract(id, date, "VEC-636", "the spine no longer holds this date"); err != nil {
			t.Fatalf("retract the orphaned day: %v", err)
		}
		if got := f.anomalies(id); len(got) != 0 {
			t.Errorf("after withdrawing it the view still reports %v", got)
		}
	})

	// A key that came back after being withdrawn -- correct when the day gained a real observation,
	// and the signature of a mis-keyed projection that is still emitting.
	t.Run("resurrected fires when a live row outranks a tombstone", func(t *testing.T) {
		const id = "anom-resurrect"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if _, err := f.callRetract(id, date, "VEC-636", "withdrawn"); err != nil {
			t.Fatalf("retract: %v", err)
		}
		if got := f.anomalies(id); len(got) != 0 {
			t.Fatalf("a withdrawn key reports %v; a retraction on its own is not an anomaly", got)
		}
		f.observe(id, dailyObs{qty: 99, block: 300, ts: "2026-01-01T09:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if got := reasonsOf(f.anomalies(id)); len(got) != 1 || got[0] != "resurrected" {
			t.Errorf("after the day came back the view reports %v, want exactly [resurrected]", got)
		}
	})
}

// The cases the review found the suite could not see.
func TestPositionDailyCorrectionEdges(t *testing.T) {
	f := newPositionDailyFixture(t)
	const date = "2026-01-01"

	// A SQL NULL, not Go's empty string, so the procedure's IS NULL arms are actually executed.
	t.Run("a null ticket or reason is refused", func(t *testing.T) {
		const id = "edge-null"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		for _, q := range []string{
			`CALL retract_position_daily(sha256($1::bytea), $2, NULL, 'why', NULL)`,
			`CALL retract_position_daily(sha256($1::bytea), $2, 'VEC-636', NULL, NULL)`,
		} {
			// P0001 is the procedure's own RAISE. Without its IS NULL arms the call reaches the
			// INSERT and the CHECK refuses it as 23514, which is a different guarantee.
			_, err := f.pool.Exec(f.ctx, q, id, date)
			var pgErr *pgconn.PgError
			if err == nil {
				t.Errorf("%s was accepted", q)
			} else if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
				t.Errorf("%s failed with %v, want the procedure's own raise (P0001)", q, err)
			}
		}
		if !f.dayPresent(id, date) {
			t.Errorf("the day was withdrawn by a call that should have been refused")
		}
	})

	// Each blank leg on its own: covering them only together makes either one redundant.
	for _, tc := range []struct{ name, ticket, reason string }{
		{"a blank ticket", "'  '", "'why'"},
		{"a blank reason", "'VEC-636'", "'  '"},
	} {
		t.Run("the table refuses "+tc.name, func(t *testing.T) {
			id := "edge-" + strings.ReplaceAll(tc.name, " ", "-")
			f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
			f.crystallize()
			_, err := f.pool.Exec(f.ctx, `
				INSERT INTO position_daily_observation
				    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
				     block_number, block_version, processing_version, block_timestamp, projection, build_id,
				     run_id, deal_type, is_retracted, correction_seq, retraction_ticket, retraction_reason)
				SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
				       d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
				       d.projection, d.build_id, d.run_id, d.deal_type, TRUE, d.correction_seq + 1,
				       `+tc.ticket+`, `+tc.reason+`
				  FROM position_daily_observation d
				 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
				 ORDER BY d.correction_seq DESC LIMIT 1`, id, date)
			var pgErr *pgconn.PgError
			if err == nil {
				t.Errorf("a tombstone with %s was accepted", tc.name)
			} else if !errors.As(err, &pgErr) || pgErr.ConstraintName != "position_daily_observation_retraction_attribution_chk" {
				t.Errorf("failed with %v (constraint %q), want the attribution check", err, pgErr.ConstraintName)
			}
		})
	}

	t.Run("the table refuses blank attribution on both legs", func(t *testing.T) {
		const id = "edge-blank"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		_, err := f.pool.Exec(f.ctx, `
			INSERT INTO position_daily_observation
			    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id,
			     run_id, deal_type, is_retracted, correction_seq, retraction_ticket, retraction_reason)
			SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
			       d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
			       d.projection, d.build_id, d.run_id, d.deal_type, TRUE, d.correction_seq + 1, '  ', '  '
			  FROM position_daily_observation d
			 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
			 ORDER BY d.correction_seq DESC LIMIT 1`, id, date)
		var pgErr *pgconn.PgError
		if err == nil {
			t.Errorf("a tombstone with whitespace attribution was accepted")
		} else if !errors.As(err, &pgErr) || pgErr.ConstraintName != "position_daily_observation_retraction_attribution_chk" {
			t.Errorf("failed with %v (constraint %q), want the attribution check", err, pgErr.ConstraintName)
		}
	})

	// A tombstone aimed below the winner never withdrew anything, so the key was never resurrected.
	t.Run("an inert tombstone is not a resurrection", func(t *testing.T) {
		const id = "edge-inert"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retractLoser(id, date)
		if got := f.dayQty(id, date); got != 20 {
			t.Fatalf("the day reads %d; the tombstone was meant to be inert", got)
		}
		if got := reasonsOf(f.anomalies(id)); len(got) != 0 {
			t.Errorf("the view reports %v for a tombstone that never won the day, want nothing", got)
		}
	})

	// Two spine rows on one date differing only in block_timestamp: the reading is behind and the
	// view has to say so, which it cannot if the comparison stops at processing_version.
	t.Run("stale_day sees a block_timestamp-only move", func(t *testing.T) {
		const id = "edge-instant"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 100, ts: "2026-01-01T09:00:00Z", dealType: "LOAN"})
		if got := reasonsOf(f.anomalies(id)); len(got) != 1 || got[0] != "stale_day" {
			t.Errorf("the view reports %v, want exactly [stale_day]: the spine's winner differs only "+
				"in block_timestamp", got)
		}
	})
}

// The lock the procedure takes before it reads the day's winner. Without it, two callers can both
// read a live winner and each copy the other's tombstone.
func TestRetractPositionDailyTakesTheKeyLock(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, date = "lock-key", "2026-01-01"
	f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	f.crystallize()

	// A second connection holds the same lock the procedure will ask for.
	holder, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatalf("acquire a second connection: %v", err)
	}
	defer holder.Release()
	if _, err := holder.Exec(f.ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(f.ctx,
		`SELECT pg_advisory_xact_lock(hashtext('position_daily:' || encode(sha256($1::bytea), 'hex') || ':' || $2::date::text))`,
		id, date); err != nil {
		t.Fatalf("take the key lock: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		var n int64
		done <- f.pool.QueryRow(f.ctx,
			`CALL retract_position_daily(sha256($1::bytea), $2, 'VEC-636', 'blocked on the key lock', NULL)`,
			id, date).Scan(&n)
	}()

	// It must be waiting, not finished.
	select {
	case err := <-done:
		t.Fatalf("the retraction finished while another session held the key lock (err=%v); it does not take the lock", err)
	case <-time.After(2 * time.Second):
	}

	var waiting bool
	if err := f.pool.QueryRow(f.ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_stat_activity
		                WHERE query LIKE '%retract_position_daily%' AND wait_event_type = 'Lock')`).Scan(&waiting); err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Errorf("no session is waiting on a lock; the retraction is blocked on something else")
	}

	if _, err := holder.Exec(f.ctx, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("the retraction failed once the lock was free: %v", err)
	}
	if f.dayPresent(id, date) {
		t.Errorf("the day survived a retraction that reported success")
	}
}

// The cases the anomaly view emits that nothing else reaches.
func TestPositionDailyAnomalyBranches(t *testing.T) {
	f := newPositionDailyFixture(t)
	const date = "2026-01-01"

	// A spine row re-stamped in place. No writer here can repair it, so the view has to say so.
	t.Run("projection_drift when the spine row is re-stamped", func(t *testing.T) {
		const id = "br-drift"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if got := f.anomalies(id); len(got) != 0 {
			t.Fatalf("the day reports %v before the re-stamp", got)
		}
		if _, err := f.pool.Exec(f.ctx,
			`UPDATE position_state SET projection = 'public.proj-restamped' WHERE position_id = sha256($1::bytea)`,
			id); err != nil {
			t.Fatalf("re-stamp the spine row: %v", err)
		}
		if got := reasonsOf(f.anomalies(id)); len(got) != 1 || got[0] != "projection_drift" {
			t.Errorf("the view reports %v, want exactly [projection_drift]", got)
		}
	})

	// A correction on the same date is stale, not moved. moved_day must only fire when the block
	// actually changed date.
	t.Run("a same-date correction is stale, not moved", func(t *testing.T) {
		const id = "br-samedate"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 100, pv: 1, ts: "2026-01-01T02:00:00Z", dealType: "LOAN"})
		if got := reasonsOf(f.anomalies(id)); len(got) != 1 || got[0] != "stale_day" {
			t.Errorf("the view reports %v, want exactly [stale_day]", got)
		}
	})

	// A reorg is a different block_version, so moved_day must not claim it.
	t.Run("a reorg onto another date is not moved_day", func(t *testing.T) {
		const id = "br-reorg"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-02T00:00:05Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 100, bv: 1, ts: "2026-01-01T23:00:00Z", dealType: "LOAN"})
		f.crystallize()
		for _, a := range f.anomalies(id) {
			if strings.HasPrefix(a, "moved_day") {
				t.Errorf("the view reports %q; moved_day is for a processing_version correction at the same block_version", a)
			}
		}
	})

	// A tombstone written later but aimed lower never withdrew the day, so it is not a resurrection.
	t.Run("a later but lower tombstone is not a resurrection", func(t *testing.T) {
		const id = "br-lower"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retractLoser(id, date)
		if got := reasonsOf(f.anomalies(id)); len(got) != 0 {
			t.Errorf("the view reports %v for a tombstone aimed below the winner, want nothing", got)
		}
	})
}
