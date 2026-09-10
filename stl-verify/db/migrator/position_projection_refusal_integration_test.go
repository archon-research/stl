//go:build integration

package migrator_test

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// materialize_position_projection aborts on a view bug and continues on a data conflict. These pin the
// continue side: what is withheld, what is recorded, what the run record counts, and that closure reads
// stored history so a batch carrying only a position's close keeps it.
func TestPositionProjectionRefusal(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()
	h := strings.Repeat("a", 40)
	row := func(ik string, bn int, ts string, qty string, dt string) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + h + "'::text," + qty + "::numeric," + dt + "::text," +
			strconv.Itoa(bn) + "::bigint,0::int,0::int,'" + ts + "'::timestamptz)"
	}
	view := func(rows ...string) string {
		return `SELECT * FROM (VALUES ` + strings.Join(rows, ",") + `) ` + mppCols
	}
	count := func(t *testing.T, sql string, args ...any) int {
		t.Helper()
		var n int
		if err := f.pool.QueryRow(f.ctx, sql, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	refusals := func(t *testing.T, ik, reason string) int {
		return count(t, `SELECT count(*) FROM position_projection_refusal WHERE reason = $2 AND detail LIKE 'ik=' || $1 || ' %'`, ik, reason)
	}

	t.Run("a batch carrying only a position's close keeps it, judged against stored history", func(t *testing.T) {
		// Before this, closure saw the batch alone: a lone zero read as a leading zero and vanished, so a
		// projection that emits only new rows could never close a position and the caches stayed open.
		if n := f.mppN(t, "pv_close_hist", view(row("close-hist", 100, "2026-05-01T00:00:00Z", "500", "'LOAN'")), "open"); n != 1 {
			t.Fatalf("open inserted %d, want 1", n)
		}
		if n := f.mppN(t, "pv_close_hist", view(row("close-hist", 200, "2026-05-02T00:00:00Z", "0", "'LOAN'")), "lone close"); n != 1 {
			t.Errorf("a lone closing zero after a stored positive inserted %d, want 1", n)
		}
		if n := f.mppN(t, "pv_close_hist", view(row("close-hist", 300, "2026-05-03T00:00:00Z", "0", "'LOAN'")), "repeat zero"); n != 0 {
			t.Errorf("a repeated zero after a stored close inserted %d, want 0", n)
		}
		if n := f.mppN(t, "pv_close_never", view(row("close-never", 100, "2026-05-01T00:00:00Z", "0", "'LOAN'")), "leading zero"); n != 0 {
			t.Errorf("a leading zero on a never-opened position inserted %d, want 0", n)
		}
		// A reorg sibling of the stored close, arriving alone, is still an observation of that block.
		sibling := "(1::int,10::bigint,'close-hist'::text,'" + h + "'::text,0::numeric,'LOAN'::text,200::bigint,1::int,0::int,'2026-05-02T00:00:00Z'::timestamptz)"
		if n := f.mppN(t, "pv_close_hist", view(sibling), "reorg of the close"); n != 1 {
			t.Errorf("a same-block sibling of the stored close inserted %d, want 1", n)
		}
	})

	t.Run("closure across the batch boundary follows full key order, so same-block siblings count", func(t *testing.T) {
		sib := func(ik string, bn, bv int, qty string) string {
			return "(1::int,10::bigint,'" + ik + "'::text,'" + h + "'::text," + qty + "::numeric,'LOAN'::text," +
				strconv.Itoa(bn) + "::bigint," + strconv.Itoa(bv) + "::int,0::int,'2026-07-01T00:00:00Z'::timestamptz)"
		}
		// A reorg of the OPENING row into a zero, arriving alone: its predecessor is the stored sibling
		// at the same block, so it is that block's close, not a leading zero. A predecessor found by
		// block_number alone never sees a same-block sibling and drops this.
		if n := f.mppN(t, "pv_sib_open", view(sib("sib-open", 100, 0, "5")), "open"); n != 1 {
			t.Fatalf("open inserted %d, want 1", n)
		}
		if n := f.mppN(t, "pv_sib_open", view(sib("sib-open", 100, 1, "0")), "reorg of the open to zero, alone"); n != 1 {
			t.Errorf("a zero sibling of the stored opening row inserted %d, want 1", n)
		}
		// Two sibling zeros in one batch closing a position opened only in stored history: the second's
		// batch predecessor is a zero at the same block, so only the stored history can say it had opened.
		if n := f.mppN(t, "pv_sib_two", view(sib("sib-two", 100, 0, "5")), "open"); n != 1 {
			t.Fatalf("open inserted %d, want 1", n)
		}
		if n := f.mppN(t, "pv_sib_two", view(sib("sib-two", 200, 0, "0"), sib("sib-two", 200, 1, "0")), "two sibling closes"); n != 2 {
			t.Errorf("two sibling versions of the close inserted %d, want 2", n)
		}
		// A drifted re-emit of the stored OPENING row as zero, followed by its reorg sibling: the re-emit is
		// suppressed and recorded, and the sibling is the close. Only the STORED positive at that very key
		// can say the position had opened, so the history bound is "at or before", not "before".
		if n := f.mppN(t, "pv_sib_drift", view(sib("sib-drift", 100, 0, "5")), "open"); n != 1 {
			t.Fatalf("open inserted %d, want 1", n)
		}
		if n := f.mppN(t, "pv_sib_drift", view(sib("sib-drift", 100, 0, "0"), sib("sib-drift", 100, 1, "0")), "drifted re-emit then sibling close"); n != 1 {
			t.Errorf("the sibling close behind a drifted re-emit inserted %d, want 1", n)
		}
		if got := refusals(t, "sib-drift", "observation_drift"); got != 1 {
			t.Errorf("the drifted re-emit recorded %d observation_drift rows, want 1", got)
		}
	})

	t.Run("a full re-projection records only the NEW inverted observation, not the stored rows beside it", func(t *testing.T) {
		if n := f.mppN(t, "pv_full", view(row("full-inv", 200, "2026-08-02T00:00:00Z", "5", "'LOAN'")), "seed"); n != 1 {
			t.Fatalf("seed inserted %d, want 1", n)
		}
		// The view re-emits the stored row AND a lower block with a later instant, as every full
		// re-projection does. Only the new row is withheld and recorded; the stored one is not a refusal.
		body := view(row("full-inv", 200, "2026-08-02T00:00:00Z", "5", "'LOAN'"), row("full-inv", 100, "2026-08-03T00:00:00Z", "5", "'LOAN'"))
		if n := f.mppN(t, "pv_full", body, "re-projection with an inverted new row"); n != 0 {
			t.Errorf("inserted %d, want 0", n)
		}
		if got := refusals(t, "full-inv", "block_time_inverts_height"); got != 1 {
			t.Errorf("recorded %d refusals, want 1: the stored row re-emitted beside it is not a refusal", got)
		}
		if got := count(t, `SELECT count(*) FROM position_state WHERE instrument_key = 'full-inv'`); got != 1 {
			t.Errorf("stored %d rows, want the seed alone", got)
		}
	})

	t.Run("a changed quantity or instant on a stored key is kept-stored and recorded as observation_drift", func(t *testing.T) {
		if n := f.mppN(t, "pv_obs_drift", view(row("obs-drift", 100, "2026-05-01T00:00:00Z", "10", "'LOAN'")), "seed"); n != 1 {
			t.Fatalf("seed inserted %d, want 1", n)
		}
		for i := 0; i < 2; i++ {
			if n := f.mppN(t, "pv_obs_drift", view(row("obs-drift", 100, "2026-05-01T00:30:00Z", "999", "'LOAN'")), "drift"); n != 0 {
				t.Errorf("drift run inserted %d, want 0", n)
			}
		}
		var qty string
		if err := f.pool.QueryRow(f.ctx, `SELECT quantity::text FROM position_state WHERE instrument_key = 'obs-drift'`).Scan(&qty); err != nil {
			t.Fatal(err)
		}
		if qty != "10" {
			t.Errorf("stored quantity %s; want the original 10 kept", qty)
		}
		if got := refusals(t, "obs-drift", "observation_drift"); got != 1 {
			t.Errorf("%d observation_drift rows across two runs, want 1 (keyed on the observation)", got)
		}
		if got := refusals(t, "obs-drift", "deal_type_drift"); got != 0 {
			t.Errorf("%d deal_type_drift rows for a quantity-only drift, want 0", got)
		}
	})

	t.Run("the run record reconciles emitted, appended and refused", func(t *testing.T) {
		// Three emitted after closure: one position inverts against itself and is withheld, two land.
		body := view(
			row("rec-bad", 100, "2026-06-02T00:00:00Z", "5", "'LOAN'"),
			row("rec-bad", 200, "2026-06-01T00:00:00Z", "5", "'LOAN'"),
			row("rec-ok", 100, "2026-06-01T00:00:00Z", "7", "'LOAN'"),
			row("rec-ok2", 100, "2026-06-01T00:00:00Z", "8", "'LOAN'"))
		if n := f.mppN(t, "pv_rec", body, "mixed batch"); n != 2 {
			t.Fatalf("inserted %d, want 2", n)
		}
		var emitted, appended, refused int
		if err := f.pool.QueryRow(f.ctx, `SELECT rows_emitted, rows_appended, positions_refused FROM position_projection_run
			WHERE projection = 'public.pv_rec' ORDER BY created_at DESC LIMIT 1`).Scan(&emitted, &appended, &refused); err != nil {
			t.Fatal(err)
		}
		if emitted != 4 || appended != 2 || refused != 1 {
			t.Errorf("run record emitted=%d appended=%d refused=%d; want 4/2/1", emitted, appended, refused)
		}
	})

	t.Run("a view bug still aborts: a double-emitted key is not a data conflict", func(t *testing.T) {
		// Negative control for the whole design line: quarantine is for arrival-order conflicts only.
		f.mppErr(t, "pv_dup", view(
			row("dup", 100, "2026-06-01T00:00:00Z", "5", "'LOAN'"),
			row("dup", 100, "2026-06-01T00:00:00Z", "6", "'LOAN'")), "double emit", "double-emits")
		if got := count(t, `SELECT count(*) FROM position_projection_run WHERE projection = 'public.pv_dup'`); got != 0 {
			t.Errorf("an aborted run recorded %d run rows, want 0", got)
		}
	})

	t.Run("refusal rows are readable by the app roles and carry the actionable detail", func(t *testing.T) {
		var detail string
		if err := f.pool.QueryRow(f.ctx, `SELECT detail FROM position_projection_refusal
			WHERE reason = 'block_time_inverts_height' AND detail LIKE 'ik=rec-bad %' ORDER BY block_number LIMIT 1`).Scan(&detail); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"ik=rec-bad", "holder=" + h, "bn=", "@"} {
			if !strings.Contains(detail, want) {
				t.Errorf("detail %q lacks %q", detail, want)
			}
		}
		var canSelect bool
		if err := f.pool.QueryRow(f.ctx, `SELECT has_table_privilege('stl_readonly', 'position_projection_refusal', 'SELECT')`).Scan(&canSelect); err != nil {
			t.Fatal(err)
		}
		if !canSelect {
			t.Error("stl_readonly must be able to read refusals")
		}
	})

	t.Run("only the inverting observations are withheld; the position's monotone ones land", func(t *testing.T) {
		// Withholding the whole position left it frozen every run: the source is append-only, so the
		// next run re-derived the identical batch and refused it again, and nothing ever landed.
		v := view(
			row("inv-part", 100, "2026-07-01T10:00:00Z", "500", "'LOAN'"),
			row("inv-part", 200, "2026-07-01T09:00:00Z", "600", "'LOAN'"),
			row("inv-part", 300, "2026-07-01T11:00:00Z", "700", "'LOAN'"),
			row("inv-part", 400, "2026-07-01T12:00:00Z", "800", "'LOAN'"),
			row("inv-part", 500, "2026-07-01T13:00:00Z", "900", "'LOAN'"),
		)
		if n := f.mppN(t, "pv_inv_part", v, "inverted pair plus monotone rows"); n != 3 {
			t.Errorf("inserted %d, want the 3 monotone observations", n)
		}
		if got := count(t, `SELECT count(*) FROM position_state WHERE instrument_key = 'inv-part'
		                     AND block_number IN (100, 200)`); got != 0 {
			t.Errorf("%d of the inverting pair reached the spine; want none", got)
		}
		if got := count(t, `SELECT count(*) FROM position_state WHERE instrument_key = 'inv-part'`); got != 3 {
			t.Errorf("the position holds %d observations, want 3", got)
		}
		if got := refusals(t, "inv-part", "block_time_inverts_height"); got != 2 {
			t.Errorf("recorded %d refusals, want one per side of the pair", got)
		}
		// Re-running adds nothing: the pair is still ambiguous, the rest is already stored.
		if n := f.mppN(t, "pv_inv_part", v, "re-run"); n != 0 {
			t.Errorf("re-running inserted %d, want 0", n)
		}
	})
}

// Two projections minting one position_id must not both land. The ownership check is a snapshot
// read, so B has to be held off until A's row is visible; otherwise both append and every later run
// of both aborts, with recovery needing a superuser since UPDATE and DELETE are revoked.
func TestMaterializeSerialisesOnTheIdentityNotTheView(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()
	h := strings.Repeat("c", 40)
	body := func(bn int, ts string) string {
		return `SELECT * FROM (VALUES (1::int,10::bigint,'race-key'::text,'` + h +
			`'::text,500::numeric,'LOAN'::text,` + strconv.Itoa(bn) + `::bigint,0::int,0::int,'` + ts + `'::timestamptz)) ` + mppCols
	}
	for _, v := range []struct{ name, b string }{
		{"pv_race_a", body(100, "2026-08-01T00:00:00Z")},
		{"pv_race_b", body(200, "2026-08-02T00:00:00Z")},
	} {
		if _, err := f.pool.Exec(f.ctx, `CREATE OR REPLACE VIEW `+v.name+` AS `+v.b); err != nil {
			t.Fatalf("creating %s: %v", v.name, err)
		}
	}

	a, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	defer a.Release()
	txA, err := a.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin a: %v", err)
	}
	var appendedA int64
	if err := txA.QueryRow(f.ctx, `SELECT materialize_position_projection('pv_race_a'::regclass)`).Scan(&appendedA); err != nil {
		_ = txA.Rollback(f.ctx)
		t.Fatalf("projection a: %v", err)
	}

	// B runs while A is uncommitted. It must not decide ownership from a snapshot without A's row.
	type outcome struct {
		appended int64
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		b, err := f.pool.Acquire(f.ctx)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		defer b.Release()
		var n int64
		e := b.QueryRow(f.ctx, `SELECT materialize_position_projection('pv_race_b'::regclass)`).Scan(&n)
		done <- outcome{appended: n, err: e}
	}()

	time.Sleep(750 * time.Millisecond) // let B reach whatever it blocks on
	select {
	case got := <-done:
		t.Fatalf("projection b finished before a committed: appended=%d err=%v", got.appended, got.err)
	default:
	}
	if err := txA.Commit(f.ctx); err != nil {
		t.Fatalf("commit a: %v", err)
	}

	got := <-done
	if got.err == nil {
		t.Errorf("projection b appended %d rows for a position a owns; want the ownership abort", got.appended)
	} else if !strings.Contains(got.err.Error(), "owned by another projection") {
		t.Errorf("projection b failed with %v; want the cross-view ownership abort", got.err)
	}

	var owners int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(DISTINCT projection) FROM position_state WHERE instrument_key = 'race-key'`).Scan(&owners); err != nil {
		t.Fatal(err)
	}
	if owners != 1 {
		t.Errorf("%d projections own the position; want 1", owners)
	}
}
