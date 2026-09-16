//go:build integration

package migrator_test

import (
	"testing"
	"time"
)

// retract appends the retraction row for one (position, date): the day's winning row copied whole,
// at its OWN spine coordinate -- processing_version included -- and correction_seq + 1, is_retracted
// TRUE. Taking processing_version + 1 instead would put the tombstone on the primary key the spine's
// next correction crystallizes to, and the writer's ON CONFLICT DO NOTHING would drop it.
func (f *positionDailyFixture) retract(id, date string) {
	f.t.Helper()
	f.retractAt(id, date, "DESC")
}

// retractLoser retracts the day's OLDEST row instead of its winner: the control for a retraction
// that must not change the day's answer.
func (f *positionDailyFixture) retractLoser(id, date string) {
	f.t.Helper()
	f.retractAt(id, date, "ASC")
}

func (f *positionDailyFixture) retractAt(id, date, dir string) {
	f.t.Helper()
	// dir is a literal from this file, never a parameter: ORDER BY direction cannot be bound.
	q := `
		INSERT INTO position_daily_observation
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id,
		     run_id, deal_type, is_retracted, correction_seq)
		SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
		       d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
		       d.projection, d.build_id, d.run_id, d.deal_type, TRUE, d.correction_seq + 1
		  FROM position_daily_observation d
		 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
		 ORDER BY d.block_number ` + dir + `, d.block_version ` + dir + `, d.processing_version ` + dir + `,
		          d.block_timestamp ` + dir + `, d.correction_seq ` + dir + `
		 LIMIT 1`
	tag, err := f.pool.Exec(f.ctx, q, id, date)
	if err != nil {
		f.t.Fatalf("retract %s on %s: %v", id, date, err)
	}
	if n := tag.RowsAffected(); n != 1 {
		f.t.Fatalf("retract %s on %s appended %d rows, want 1 -- the day it names was not there", id, date, n)
	}
}

// revive appends a live row above the retraction, on the same local axis: a later correction that
// says the key was real after all. is_retracted unset, so nothing special-cases a reversal.
func (f *positionDailyFixture) revive(id, date string, qty int) {
	f.t.Helper()
	tag, err := f.pool.Exec(f.ctx, `
		INSERT INTO position_daily_observation
		    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp, projection, build_id,
		     run_id, deal_type, correction_seq)
		SELECT d.position_id, d.as_of_date, d.chain_id, d.protocol_id, d.instrument_key, d.holder_id,
		       $3::numeric, d.block_number, d.block_version, d.processing_version, d.block_timestamp,
		       d.projection, d.build_id, d.run_id, d.deal_type, d.correction_seq + 1
		  FROM position_daily_observation d
		 WHERE d.position_id = sha256($1::bytea) AND d.as_of_date = $2
		 ORDER BY d.block_number DESC, d.block_version DESC, d.processing_version DESC,
		          d.block_timestamp DESC, d.correction_seq DESC
		 LIMIT 1`, id, date, qty)
	if err != nil {
		f.t.Fatalf("revive %s on %s: %v", id, date, err)
	}
	if n := tag.RowsAffected(); n != 1 {
		f.t.Fatalf("revive %s on %s appended %d rows, want 1", id, date, n)
	}
}

// dayQtyAsOf is the day's answer as it read at T, through the as-of function rather than the view.
// found is false when the key is absent at T, which is what a retraction makes it.
func (f *positionDailyFixture) dayQtyAsOf(id, date string, at time.Time) (int, bool) {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		SELECT quantity FROM position_daily_as_of($3)
		 WHERE position_id = sha256($1::bytea) AND as_of_date = $2`, id, date, at)
	if err != nil {
		f.t.Fatalf("dayQtyAsOf(%s, %s): %v", id, date, err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var q int
		if err := rows.Scan(&q); err != nil {
			f.t.Fatal(err)
		}
		got = append(got, q)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("dayQtyAsOf(%s, %s) iteration: %v", id, date, err)
	}
	if len(got) > 1 {
		f.t.Fatalf("dayQtyAsOf(%s, %s) returned %d rows, want at most 1: the as-of read is not one row per (position, date)", id, date, len(got))
	}
	if len(got) == 0 {
		return 0, false
	}
	return got[0], true
}

// dayPresent answers whether the view still carries the key at all.
func (f *positionDailyFixture) dayPresent(id, date string) bool {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM position_daily
		 WHERE position_id = sha256($1::bytea) AND as_of_date = $2`, id, date).Scan(&n); err != nil {
		f.t.Fatalf("dayPresent(%s, %s): %v", id, date, err)
	}
	if n > 1 {
		f.t.Fatalf("position_daily holds %d rows for (%s, %s), want at most 1", n, id, date)
	}
	return n == 1
}

// A retraction withdraws a (position, date) whose KEY is wrong -- the case no ordering rule can
// reach, because the corrected row hashes or dates to a different key and never collides with the
// bad one (ADR-0006 §3, ARCT-470). Everything here is an append: nothing is updated or deleted, so
// an as-of read taken before the retraction still reproduces the answer that was given.
func TestPositionDailyRetractionWithdrawsAKeyWithoutRewritingIt(t *testing.T) {
	f := newPositionDailyFixture(t)
	const date = "2026-01-01"

	t.Run("a retracted key is absent from position_daily", func(t *testing.T) {
		const id = "d-retract"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if !f.dayPresent(id, date) {
			t.Fatalf("the day is not there before the retraction; the rest of this case would pass vacuously")
		}
		f.retract(id, date)
		if f.dayPresent(id, date) {
			t.Errorf("position_daily still carries the day after its winning row was retracted")
		}
		// The withdrawal is an append, not a rewrite: both rows stand in the table underneath.
		if n := f.dayRows(id, date); n != 2 {
			t.Errorf("the table holds %d rows for the day, want 2 -- the retraction must ADD a row, never replace one", n)
		}
	})

	t.Run("a retracted key does not fall back to the row it retracts", func(t *testing.T) {
		const id = "d-retract-fallback"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		f.crystallize()
		if got := f.dayQty(id, date); got != 20 {
			t.Fatalf("the day reads %d before the retraction, want 20", got)
		}
		f.retract(id, date)
		if f.dayPresent(id, date) {
			t.Errorf("the day survived its retraction, reading %d -- a retracted key must be ABSENT, "+
				"not answered from the older row underneath it", f.dayQty(id, date))
		}
	})

	t.Run("an as-of read before the retraction still gives the old answer", func(t *testing.T) {
		const id = "d-retract-asof"
		f.observe(id, dailyObs{qty: 42, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		before := f.dbNow()
		f.retract(id, date)
		if got, ok := f.dayQtyAsOf(id, date, before); !ok || got != 42 {
			t.Errorf("as of before the retraction the day reads (%d, found=%v); want (42, true) -- "+
				"retracting must not change what an earlier read reproduces", got, ok)
		}
		if _, ok := f.dayQtyAsOf(id, date, f.dbNow()); ok {
			t.Errorf("as of now the day is still there; the retraction did not take effect at all")
		}
	})

	t.Run("crystallizing again does not resurrect a retracted key", func(t *testing.T) {
		const id = "d-retract-recrystallize"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retract(id, date)
		rowsBefore := f.dayRows(id, date)
		f.crystallize()
		if f.dayPresent(id, date) {
			t.Errorf("the next crystallization brought the retracted day back; the writer re-offers the "+
				"spine's winner and must lose to the retraction above it (day reads %d)", f.dayQty(id, date))
		}
		if n := f.dayRows(id, date); n != rowsBefore {
			t.Errorf("the re-run appended %d row(s); a retracted day is unchanged by re-crystallization", n-rowsBefore)
		}
	})

	t.Run("a later live version revives the key", func(t *testing.T) {
		const id = "d-retract-revive"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retract(id, date)
		if f.dayPresent(id, date) {
			t.Fatalf("the day survived its retraction; the revival below would prove nothing")
		}
		f.revive(id, date, 77)
		if got := f.dayQty(id, date); got != 77 {
			t.Errorf("after the revival the day reads %d, want 77 -- a later version with is_retracted "+
				"unset must bring the key back with no special case", got)
		}
	})

	t.Run("retracting a losing row leaves the day standing", func(t *testing.T) {
		const id = "d-retract-loser"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.observe(id, dailyObs{qty: 20, block: 200, ts: "2026-01-01T05:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retractLoser(id, date)
		if got := f.dayQty(id, date); got != 20 {
			t.Errorf("the day reads %d after a LOSING row was retracted, want 20 -- only a retraction "+
				"that wins the day may withdraw it", got)
		}
	})

	t.Run("the view exposes the column and never a retracted row", func(t *testing.T) {
		// Seeds its own retraction, so the case stands alone under -run rather than depending on
		// the subtests above having appended one.
		const id = "d-retract-view"
		f.observe(id, dailyObs{qty: 10, block: 100, ts: "2026-01-01T01:00:00Z", dealType: "LOAN"})
		f.crystallize()
		f.retract(id, date)
		var n int
		if err := f.pool.QueryRow(f.ctx,
			`SELECT count(*) FROM position_daily WHERE is_retracted IS TRUE`).Scan(&n); err != nil {
			t.Fatalf("read is_retracted through the view: %v", err)
		}
		if n != 0 {
			t.Errorf("position_daily returns %d retracted row(s); the filter is not on the view's path", n)
		}
		// The cases above appended retractions, so the table must hold some: without this the count
		// above is zero for the trivial reason and proves nothing.
		if err := f.pool.QueryRow(f.ctx,
			`SELECT count(*) FROM position_daily_observation WHERE is_retracted IS TRUE`).Scan(&n); err != nil {
			t.Fatalf("count retracted rows: %v", err)
		}
		if n == 0 {
			t.Errorf("no retracted rows exist at all, so the view check above was vacuous")
		}
	})
}
