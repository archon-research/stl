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
//
// ORDER BY pins which row is copied once a day holds more than one, and a zero-row SELECT is
// reported rather than returning a silent nil: that is the vacuous-INSERT class this file has
// already been bitten by once.
func (f *positionDailyFixture) retractKeyed(id, srcDate, stampDate string, block, bv, pv, seq int, ts string) error {
	f.t.Helper()
	tag, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily_observation
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id,
		     run_id, deal_type, is_retracted, correction_seq, retraction_ticket, retraction_reason)
		SELECT d.position_id, $3::date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
		       d.quantity, $4::bigint, $5::int, $6::int, $7::timestamptz,
		       d.projection, d.build_id, d.run_id, d.deal_type, TRUE, $8::int,
		       'VEC-636', 'test: an explicitly aimed retraction'
		  FROM position_daily_observation d
		 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
		 ORDER BY d.block_number DESC, d.block_version DESC, d.processing_version DESC,
		          d.block_timestamp DESC, d.correction_seq DESC
		 LIMIT 1`, id, srcDate, stampDate, block, bv, pv, ts, seq)
	if err != nil {
		return err
	}
	if n := tag.RowsAffected(); n != 1 {
		f.t.Fatalf("retractKeyed(%s, %s) appended %d rows, want 1: it copied from a day that is not there",
			id, srcDate, n)
	}
	return nil
}

// What breaks a retraction. Each case is a way the withdrawal fails to take, or takes too widely;
// the design's claim is only as good as these, because every one of them is an append that the
// append-only contract cannot undo once written.
func retractionAdversarialCases(t *testing.T, f *positionDailyFixture) {
	const date = "2026-01-01"

	// A retraction is ranked like any other row, so one written below the day's winner is INERT --
	// it is in the table forever and changes nothing. The correction tool must allocate above the
	// current maximum; there is no constraint that can catch this for it.
	t.Run("a retraction below the day's winner is inert", func(t *testing.T) {
		const id = "adv-below"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if err := f.retractKeyed(id, date, date, 100, 0, 0, 1, "2026-01-01T01:00:00Z"); err != nil {
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
		err := f.retractKeyed(id, date, date, 100, 0, 0, 1, "2026-01-01T01:00:00Z")
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
		err := f.retractKeyed(id, date, "2026-01-02", 100, 0, 0, 1, "2026-01-01T01:00:00Z")
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
		tag, err := f.pool.Exec(f.ctx, `
			INSERT INTO position_daily_observation
			    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id,
			     run_id, deal_type, is_retracted, correction_seq)
			SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
			       55, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
			       d.projection, d.build_id, d.run_id, d.deal_type, FALSE, d.correction_seq + 1
			  FROM position_daily_observation d
			 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
			 ORDER BY d.block_number DESC, d.block_version DESC, d.processing_version DESC,
			          d.block_timestamp DESC, d.correction_seq DESC
			 LIMIT 1`, id, date)
		if err != nil {
			t.Fatalf("append an explicitly live row: %v", err)
		}
		if n := tag.RowsAffected(); n != 1 {
			t.Fatalf("the explicitly live row appended %d rows, want 1", n)
		}
		if got := f.dayQty(id, date); got != 55 {
			t.Errorf("the day reads %d, want 55 -- is_retracted = FALSE is live, the same as NULL", got)
		}
		if got := f.dayRow(id, date)["is_retracted"]; got != "false" {
			t.Errorf("the view reports is_retracted = %q, want false -- the column must be emitted as written", got)
		}
	})

	// The block_version leg. A reorg re-observes the same block at block_version 1; it outranks the
	// retraction on a leg no other case in this file exercises, so the day must come back. Drop
	// block_version from the read's ORDER BY and processing_version decides instead, leaving the day
	// dead.
	t.Run("a reorg outranks a retraction on block_version", func(t *testing.T) {
		const id = "adv-reorg"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retract(id, date)
		if f.dayPresent(id, date) {
			t.Fatalf("the day survived its retraction; the reorg below would prove nothing")
		}
		f.observe(id, dailyObs{qty: 44, block: 100, bv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if got := f.dayQty(id, date); got != 44 {
			t.Errorf("after the reorg the day reads %d, want 44 -- block_version must outrank the retraction", got)
		}
	})

	// The block_timestamp leg, the last one and the tie-break that makes the pick total. Two rows on
	// one day at the same block and version: retracting the EARLIER instant must not withdraw the day,
	// because the later instant wins it.
	t.Run("a retraction of the earlier instant leaves the day standing", func(t *testing.T) {
		const id = "adv-instant"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 100, ts: "2026-01-01T09:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if n := f.dayRows(id, date); n != 2 {
			t.Fatalf("the day holds %d rows, want 2 differing only in block_timestamp", n)
		}
		if err := f.retractKeyed(id, date, date, 100, 0, 0, 1, "2026-01-01T01:00:00Z"); err != nil {
			t.Fatalf("retract the earlier instant: %v", err)
		}
		if got := f.dayQty(id, date); got != 20 {
			t.Errorf("the day reads %d, want 20 -- the later instant still wins, so the earlier one's "+
				"retraction is inert", got)
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

	// Append-only, behaviourally: every row that existed before a retraction is untouched after it,
	// compared on ctid and xmin as well as content, so an in-place rewrite that preserved the values
	// would still fail. The ACL half (the owner holds no UPDATE or DELETE) is asserted by the grants
	// case in TestPositionDailyIsWrittenOnlyByItsOwnerUnderTheRealRole.
	t.Run("no existing row is touched by a retraction", func(t *testing.T) {
		const id = "adv-immutable"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		before := f.rowImages()
		if len(before) == 0 {
			t.Fatalf("no rows exist before the retraction; the comparison below would be vacuous")
		}
		f.retract(id, date)
		after := f.rowImages()
		for key, img := range before {
			switch got, ok := after[key]; {
			case !ok:
				t.Errorf("row %s disappeared across the retraction", key)
			case got != img:
				t.Errorf("row %s changed:\nbefore %s\nafter  %s", key, img, got)
			}
		}
		if len(after) != len(before)+1 {
			t.Errorf("the table went from %d rows to %d; a retraction appends exactly one", len(before), len(after))
		}
	})
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
	// The control the assertion below is worth nothing without: the caches DO disagree first.
	if d := cacheDisagreement(t, f); len(d) != 1 {
		t.Fatalf("the caches disagree on %d position(s) before the retraction, want exactly 1 -- if the "+
			"gap is already gone this case proves nothing: %s", len(d), strings.Join(d, " | "))
	}

	// The correction run withdraws the day it moved away from: the stale row's own coordinate at the
	// next processing_version, is_retracted TRUE, every other column copied.
	tag, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily_observation
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id,
		     run_id, deal_type, is_retracted, correction_seq, retraction_ticket, retraction_reason)
		SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
		       d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
		       d.projection, d.build_id, d.run_id, d.deal_type, TRUE, d.correction_seq + 1,
		       'VEC-636', 'test: the day the correction moved away from'
		  FROM position_daily_observation d
		 WHERE d.instrument_key = $1 AND d.as_of_date = '2026-06-02'
		 ORDER BY d.block_number DESC, d.block_version DESC, d.processing_version DESC,
		          d.block_timestamp DESC, d.correction_seq DESC
		 LIMIT 1`, ik)
	if err != nil {
		t.Fatalf("retract the day the correction moved away from: %v", err)
	}
	if n := tag.RowsAffected(); n != 1 {
		t.Fatalf("the retraction appended %d rows, want 1", n)
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
	if len(moved) != 1 || !strings.Contains(moved[0], "date=2026-06-02") {
		t.Errorf("the retraction diverges from the spine on %d date(s), want exactly 2026-06-02: %s",
			len(moved), strings.Join(moved, " | "))
	}
}

// The collision the tombstone's coordinate invites. A retraction written at the retracted row's
// coordinate with processing_version + 1 allocates a number in the SPINE's version namespace, which
// this table does not own. The spine's next correction of that same observation is allocated the
// same N by processing_version_log, crystallizes to the identical PK, and the writer's
// ON CONFLICT DO NOTHING drops it -- silently, reporting the zero it reports on a quiet run.
func retractionSquatCase(t *testing.T, f *positionDailyFixture) {
	const id, date = "adv-squat", "2026-01-01"

	f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	f.crystallize()
	f.retract(id, date)
	if f.dayPresent(id, date) {
		t.Fatalf("the day survived its retraction; this case starts from the wrong state")
	}

	// The spine's own correction of that observation, at the version a per-table allocator hands
	// out first: same block, same instant, processing_version 1.
	f.observe(id, dailyObs{qty: 33, block: 100, pv: 1, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
	f.crystallize()

	if !f.dayPresent(id, date) {
		t.Fatalf("the corrected reading never reached position_daily: the retraction is sitting on the "+
			"coordinate the spine correction crystallizes to, and ON CONFLICT DO NOTHING dropped it. "+
			"Rows for the day: %d", f.dayRows(id, date))
	}
	if got := f.dayQty(id, date); got != 33 {
		t.Errorf("the day reads %d, want 33 -- a genuine correction above the retraction must win", got)
	}
}

// Every position_daily object carries a COMMENT. A migration that DROPs and re-creates a view takes
// its COMMENT with it, and nothing else in this package notices -- which is exactly what happened
// when the retraction columns forced position_daily to be rebuilt.
func TestPositionDailyObjectsAreDocumented(t *testing.T) {
	f := newPositionDailyFixture(t)
	for _, o := range []struct{ kind, name string }{
		{"table", "position_daily_observation"},
		{"view", "position_daily"},
		{"view", "position_daily_anomaly"},
		{"function", "position_daily_as_of"},
		{"function", "position_daily_as_of_bound"},
		{"procedure", "crystallize_position_daily"},
		{"procedure", "retract_position_daily"},
	} {
		t.Run(o.name, func(t *testing.T) {
			var comment *string
			q := `SELECT obj_description(($1 || '')::regclass, 'pg_class')`
			if o.kind == "function" || o.kind == "procedure" {
				q = `SELECT obj_description(p.oid, 'pg_proc') FROM pg_proc p
				      JOIN pg_namespace n ON n.oid = p.pronamespace
				     WHERE n.nspname = 'public' AND p.proname = $1`
			}
			if err := f.pool.QueryRow(f.ctx, q, o.name).Scan(&comment); err != nil {
				t.Fatalf("read the comment on %s %s: %v", o.kind, o.name, err)
			}
			if comment == nil || *comment == "" {
				t.Errorf("%s %s carries no COMMENT; a DROP or a re-create dropped it", o.kind, o.name)
			}
		})
	}
}
