//go:build integration

package migrator_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// retractKeyed appends a retraction at an explicit coordinate rather than the day's winner, so a
// case can aim one below the winner, at an occupied coordinate, or at a date the row is not on.
// srcDate is the day the copied row is read from; stampDate is the day the tombstone claims. They
// are the same except in the case that checks a retraction cannot be parked on the wrong day.
func (f *positionDailyFixture) retractKeyed(id, srcDate, stampDate string, block, bv, pv int, ts string) error {
	f.t.Helper()
	_, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily_observation
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id,
		     run_id, deal_type, is_retracted)
		SELECT d.position_id, $3::date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
		       d.quantity, $4::bigint, $5::int, $6::int, $7::timestamptz,
		       d.projection, d.build_id, d.run_id, d.deal_type, TRUE
		  FROM position_daily_observation d
		 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
		 LIMIT 1`, id, srcDate, stampDate, block, bv, pv, ts)
	return err
}

// What breaks a retraction. Each case is a way the withdrawal fails to take, or takes too widely;
// the design's claim is only as good as these, because every one of them is an append that the
// append-only contract cannot undo once written.
func TestPositionDailyRetractionAdversarial(t *testing.T) {
	f := newPositionDailyFixture(t)
	const date = "2026-01-01"

	// A retraction is ranked like any other row, so one written below the day's winner is INERT --
	// it is in the table forever and changes nothing. The correction tool must allocate above the
	// current maximum; there is no constraint that can catch this for it.
	t.Run("a retraction below the day's winner is inert", func(t *testing.T) {
		const id = "adv-below"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if err := f.retractKeyed(id, date, date, 100, 0, 1, "2026-01-01T01:00:00Z"); err != nil {
			t.Fatalf("append a retraction at the losing block: %v", err)
		}
		if got := f.dayQty(id, date); got != 20 {
			t.Errorf("the day reads %d, want 20 -- a retraction below the winner must not withdraw it", got)
		}
		// One crystallized row -- the day's winner -- plus the inert tombstone, which is recorded
		// forever whether or not it takes.
		if n := f.dayRows(id, date); n != 2 {
			t.Errorf("the table holds %d rows, want 2 -- the inert retraction is still recorded", n)
		}
	})

	// The sharpest edge. A retraction withdraws a VERSION, not a key forever: a later spine
	// observation on that day crystallizes above it and the day comes back. Correct when the day
	// genuinely gained a new observation, and a live hazard when the key itself was wrong, because
	// the projection that produced it keeps producing it until the projection is fixed too.
	t.Run("a later spine observation resurrects a retracted day", func(t *testing.T) {
		const id = "adv-resurrect"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retract(id, date)
		if f.dayPresent(id, date) {
			t.Fatalf("the day survived its retraction; the rest of this case proves nothing")
		}
		f.observe(id, dailyObs{qty: 99, block: 300, ts: "2026-01-01T09:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if got := f.dayQty(id, date); got != 99 {
			t.Errorf("after a higher observation the day reads %d, want 99 -- a retraction withdraws "+
				"the version it outranks, not the key forever", got)
		}
	})

	// Writing the same retraction twice is refused by the PK rather than silently duplicated, so a
	// correction run that crashes and is re-run must offer it with ON CONFLICT DO NOTHING.
	t.Run("the same retraction twice is refused, not duplicated", func(t *testing.T) {
		const id = "adv-twice"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retract(id, date)
		before := f.dayRows(id, date)
		err := f.retractKeyed(id, date, date, 100, 0, 1, "2026-01-01T01:00:00Z")
		var pgErr *pgconn.PgError
		if err == nil {
			t.Errorf("a second retraction at the same coordinate was accepted; the table now holds "+
				"%d rows for the day", f.dayRows(id, date))
		} else if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Errorf("a repeat retraction failed with %v, want a unique_violation (23505)", err)
		}
		if n := f.dayRows(id, date); n != before {
			t.Errorf("the table gained %d row(s) from the refused repeat", n-before)
		}
	})

	// A tombstone that names a date its instant is not on is refused by the same CHECK that pins
	// every other row, so a retraction cannot be parked on a day it does not belong to.
	t.Run("a retraction cannot lie about its date", func(t *testing.T) {
		const id = "adv-date"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		err := f.retractKeyed(id, date, "2026-01-02", 100, 0, 1, "2026-01-01T01:00:00Z")
		var pgErr *pgconn.PgError
		if err == nil {
			t.Errorf("a retraction stamped 2026-01-02 for an instant on 2026-01-01 was accepted")
		} else if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Errorf("the mis-dated retraction failed with %v, want a check_violation (23514)", err)
		}
	})

	// FALSE is live, not merely NULL: a correction run that writes the column explicitly off must
	// not make the key disappear.
	t.Run("an explicit false is live", func(t *testing.T) {
		const id = "adv-false"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO position_daily_observation
			    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id,
			     run_id, deal_type, is_retracted)
			SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
			       55, d.block_number, d.block_version, d.processing_version + 1, d.block_timestamp,
			       d.projection, d.build_id, d.run_id, d.deal_type, FALSE
			  FROM position_daily_observation d
			 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2`, id, date); err != nil {
			t.Fatalf("append an explicitly live row: %v", err)
		}
		if got := f.dayQty(id, date); got != 55 {
			t.Errorf("the day reads %d, want 55 -- is_retracted = FALSE is live, the same as NULL", got)
		}
	})

	// Blast radius: a retraction withdraws exactly one (position, date) and touches no neighbour,
	// on either axis.
	t.Run("a retraction withdraws exactly one key", func(t *testing.T) {
		const id, other = "adv-blast", "adv-blast-other"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.observe(id, dailyObs{qty: 11, block: 110, ts: "2026-01-02T01:00:00Z", dealType: "LOAN"})
		f.observe(other, dailyObs{qty: 12, block: 120, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retract(id, date)
		if got := strings.Join(f.daily(id), ","); got != "2026-01-02=11" {
			t.Errorf("the retracted position reads %q, want only 2026-01-02=11 -- the next day must stand", got)
		}
		if got := strings.Join(f.daily(other), ","); got != "2026-01-01=12" {
			t.Errorf("a different position on the same date reads %q, want 2026-01-01=12", got)
		}
	})

	// Append-only, behaviourally: the row a retraction withdraws is byte-identical afterwards. The
	// ACL half (the owner holds no UPDATE or DELETE) is asserted by the grants case in
	// TestPositionDailyIsWrittenOnlyByItsOwnerUnderTheRealRole.
	t.Run("the retracted row itself is unchanged", func(t *testing.T) {
		const id = "adv-immutable"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		before := f.rowImage(id, date)
		f.retract(id, date)
		if after := f.rowImage(id, date); after != before {
			t.Errorf("the withdrawn row changed:\nbefore %s\nafter  %s", before, after)
		}
	})
}

// rowImage is the original (lowest-version) row of a day, whole, as text. It is read at the exact
// coordinate rather than "the day's winner", so a retraction appended above it does not change
// which row this returns.
func (f *positionDailyFixture) rowImage(id, date string) string {
	f.t.Helper()
	var img string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT (to_jsonb(d) - 'position_id')::text FROM position_daily_observation d
		 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
		 ORDER BY d.block_number, d.block_version, d.processing_version, d.block_timestamp
		 LIMIT 1`, id, date).Scan(&img); err != nil {
		f.t.Fatalf("rowImage(%s, %s): %v", id, date, err)
	}
	return img
}

// The case the retraction exists for, end to end from the projection through position_state: a
// correction at the same block carrying an earlier instant moves the reading across UTC midnight,
// so it lands on the earlier date and the superseded reading is left standing on the later one.
// No ordering rule can reach it -- the two rows are in different (position, date) groups, and
// block_timestamp is in position_state's own PK, so the spine keeps both too.
//
// TestPositionDailyKeepsADayACorrectionMovedAway pins that gap. This one closes it, and shows what
// closing it costs: position_daily stops being "the spine's winner on every date" by exactly the
// withdrawn key, which is the whole point of a retraction being a row and not a delete.
func TestPositionDailyRetractionClosesTheMovedDayGap(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()

	const ik = "retract-moved-day"
	if n := f.mppDaily(t, "pv_retract_moved", valuesOf(
		mppRow(ik, 100, 1000, 0, 0, "2026-06-02T00:00:05Z", "LOAN")), "the original, just after midnight"); n != 1 {
		t.Fatalf("seeding appended %d, want 1", n)
	}
	if n := f.mppDaily(t, "pv_retract_moved", valuesOf(
		mppRow(ik, 555, 1000, 0, 1, "2026-06-01T23:59:58Z", "LOAN")), "the correction, just before it"); n != 1 {
		t.Fatalf("the correction appended %d, want 1", n)
	}
	if got := cachedDays(t, f, ik); len(got) != 2 {
		t.Fatalf("position_daily holds %v before the retraction; want both days, or this case starts from the wrong state", got)
	}

	// The correction run withdraws the day it moved away from: the stale row's own coordinate at the
	// next processing_version, is_retracted TRUE, every other column copied.
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily_observation
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id,
		     run_id, deal_type, is_retracted)
		SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
		       d.quantity, d.block_number, d.block_version, d.processing_version + 1, d.block_timestamp,
		       d.projection, d.build_id, d.run_id, d.deal_type, TRUE
		  FROM position_daily_observation d
		 WHERE d.instrument_key = $1 AND d.as_of_date = '2026-06-02'`, ik); err != nil {
		t.Fatalf("retract the day the correction moved away from: %v", err)
	}

	if got := cachedDays(t, f, ik); len(got) != 1 || got[0] != "2026-06-01=555" {
		t.Errorf("position_daily holds %v; want only the corrected day 2026-06-01=555", got)
	}
	// The consequence the gap test pins -- the two caches naming different winners -- is gone.
	if d := cacheDisagreement(t, f); len(d) != 0 {
		t.Errorf("position_daily and position_current still disagree on %d position(s) after the "+
			"retraction: %s", len(d), strings.Join(d, " | "))
	}
	// And the spine is untouched: the retraction is a row in the daily table, not a repair upstream.
	var spineRows int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM position_state WHERE instrument_key = $1`, ik).Scan(&spineRows); err != nil {
		t.Fatal(err)
	}
	if spineRows != 2 {
		t.Errorf("position_state holds %d rows for %s, want 2 -- retracting must not touch the spine", spineRows, ik)
	}
	// The price, stated rather than hidden: position_daily is no longer the spine's winner on every
	// date. Exactly one date diverges, and it is the withdrawn one.
	div := cacheDivergence(t, f)
	var moved []string
	for _, d := range div {
		if strings.Contains(d, ik) {
			moved = append(moved, d)
		}
	}
	if len(moved) != 1 || !strings.Contains(moved[0], "2026-06-02") {
		t.Errorf("the retraction diverges from the spine on %d date(s), want exactly 2026-06-02: %s",
			len(moved), strings.Join(moved, " | "))
	}
}
