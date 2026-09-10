//go:build integration

package migrator_test

import (
	"strconv"
	"strings"
	"testing"
)

// Closure's four clauses, asserted with the predecessor in STORED history rather than the batch,
// which nothing covered before. KNOWN LIMIT: a stored positive strictly BETWEEN two batch rows is
// masked by the batch predecessor, unreachable until VEC-566 emits subsets (see that ticket).
func TestPositionProjectionClosureAcrossStoredHistory(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()
	h := strings.Repeat("a", 40)
	row := func(ik string, bn, bv, pv int, ts, qty string) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + h + "'::text," + qty + "::numeric,'LOAN'::text," +
			strconv.Itoa(bn) + "::bigint," + strconv.Itoa(bv) + "::int," + strconv.Itoa(pv) + "::int,'" + ts + "'::timestamptz)"
	}
	view := func(rows ...string) string {
		return `SELECT * FROM (VALUES ` + strings.Join(rows, ",") + `) ` + mppCols
	}
	stored := func(t *testing.T, ik string) string {
		t.Helper()
		var s string
		if err := f.pool.QueryRow(f.ctx, `SELECT coalesce(string_agg(block_number||'/'||block_version||'='||quantity, ' ' ORDER BY block_number, block_version), '<none>')
		                                    FROM position_state WHERE instrument_key = $1`, ik).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	t.Run("a leading zero is dropped, with no history and after a stored zero", func(t *testing.T) {
		f.mppN(t, "pv_c2", view(row("c2", 100, 0, 0, "2026-05-01T00:00:00Z", "0")), "no history at all")
		if got := stored(t, "c2"); got != "<none>" {
			t.Errorf("stored %q; want nothing: a never-opened position has no observation", got)
		}
	})
	t.Run("a repeated zero is dropped when the preceding zero is STORED, not in the batch", func(t *testing.T) {
		f.mppN(t, "pv_c3", view(row("c3", 100, 0, 0, "2026-05-01T00:00:00Z", "50")), "open")
		f.mppN(t, "pv_c3", view(row("c3", 200, 0, 0, "2026-05-02T00:00:00Z", "0")), "the close, alone")
		f.mppN(t, "pv_c3", view(row("c3", 300, 0, 0, "2026-05-03T00:00:00Z", "0")), "a repeat, alone")
		if got := stored(t, "c3"); got != "100/0=50 200/0=0" {
			t.Errorf("stored %q; want the open and one close only", got)
		}
	})
	t.Run("the first zero after a positive is kept when both are in one batch", func(t *testing.T) {
		f.mppN(t, "pv_c4", view(
			row("c4", 100, 0, 0, "2026-05-01T00:00:00Z", "70"),
			row("c4", 200, 0, 0, "2026-05-02T00:00:00Z", "0"),
			row("c4", 300, 0, 0, "2026-05-03T00:00:00Z", "0")), "open, close, repeat")
		if got := stored(t, "c4"); got != "100/0=70 200/0=0" {
			t.Errorf("stored %q; want the open and one close, the repeat dropped", got)
		}
	})
	t.Run("a sibling version of a STORED close is an observation of that block", func(t *testing.T) {
		f.mppN(t, "pv_c5", view(row("c5", 100, 0, 0, "2026-05-01T00:00:00Z", "40")), "open")
		f.mppN(t, "pv_c5", view(row("c5", 200, 0, 0, "2026-05-02T00:00:00Z", "0")), "the close")
		f.mppN(t, "pv_c5", view(row("c5", 200, 1, 0, "2026-05-02T00:00:00Z", "0")), "its reorg sibling, alone")
		if got := stored(t, "c5"); got != "100/0=40 200/0=0 200/1=0" {
			t.Errorf("stored %q; want the sibling of the close kept", got)
		}
	})
	t.Run("a re-open after a stored close is kept, and closes again", func(t *testing.T) {
		f.mppN(t, "pv_c6", view(
			row("c6", 100, 0, 0, "2026-05-01T00:00:00Z", "10"),
			row("c6", 200, 0, 0, "2026-05-02T00:00:00Z", "0")), "open then close")
		f.mppN(t, "pv_c6", view(
			row("c6", 300, 0, 0, "2026-05-03T00:00:00Z", "25"),
			row("c6", 400, 0, 0, "2026-05-04T00:00:00Z", "0")), "re-open then close")
		if got := stored(t, "c6"); got != "100/0=10 200/0=0 300/0=25 400/0=0" {
			t.Errorf("stored %q; want both cycles intact", got)
		}
	})
	// The off-chain rule is a VIEW BUG check, so it must abort whatever the quantity: it used to run
	// after closure, where a zero-quantity row was dropped before the check could see it.
	t.Run("the off-chain block_number rule aborts on a zero quantity too", func(t *testing.T) {
		bad := func(qty string) string {
			return "(NULL::int,10::bigint,'c7'::text,'" + h + "'::text," + qty +
				"::numeric,'LOAN'::text,1::bigint,0::int,0::int,'2026-05-01T00:00:00Z'::timestamptz)"
		}
		f.mppErr(t, "pv_c7_zero", `SELECT * FROM (VALUES `+bad("0")+`) `+mppCols, "off-chain zero", "floor(epoch")
		f.mppErr(t, "pv_c7_pos", `SELECT * FROM (VALUES `+bad("5")+`) `+mppCols, "off-chain positive", "floor(epoch")
	})
}
