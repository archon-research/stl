//go:build integration

package migrator_test

import (
	"strconv"
	"testing"
)

// psTestRunIDProvenance covers: run_id provenance (ADR-0006 §2). Every row the spine writes names the
// writer run that wrote it, wherever build_id already sits: the observations, the refusals and the run
// record. There is no FK, matching build_id, so these ids are plain bigints.
func psTestRunIDProvenance(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	const holder = "cafe0000000000000000000000000000000000ff"
	mppRun := func(t *testing.T, name, valuesBody string, runID any) int64 {
		t.Helper()
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+name+` AS `+valuesBody); err != nil {
			t.Fatalf("create view %s: %v", name, err)
		}
		var inserted int64
		if err := pool.QueryRow(ctx,
			`SELECT materialize_position_projection($1::regclass, 7, $2::bigint)`, name, runID).Scan(&inserted); err != nil {
			t.Fatalf("materialize %s: %v", name, err)
		}
		return inserted
	}
	obs := func(ik string, qty, bn int, ts string) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + holder + "'::text," + strconv.Itoa(qty) +
			"::numeric,'LOAN'::text," + strconv.Itoa(bn) + "::bigint,0::int,0::int,'" + ts + "'::timestamptz)"
	}
	scalar := func(t *testing.T, sql string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// The column has to exist on all three, be a nullable bigint, and carry no FK: a NOT NULL or a
	// default would rewrite history that predates tracking, and an FK probe per appended batch is the
	// cost build_id was deliberately spared.
	t.Run("position_state, its run record and its refusals all carry a nullable bigint run_id, no FK", func(t *testing.T) {
		for _, tbl := range []string{"position_state", "position_projection_run", "position_projection_refusal"} {
			var typ, nullable string
			var def *string
			err := pool.QueryRow(ctx, `
				SELECT data_type, is_nullable, column_default FROM information_schema.columns
				 WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'run_id'`, tbl).Scan(&typ, &nullable, &def)
			if err != nil {
				t.Fatalf("%s has no run_id column: %v", tbl, err)
			}
			if typ != "bigint" || nullable != "YES" || def != nil {
				t.Errorf("%s.run_id = %s nullable=%s default=%v, want bigint YES <nil>", tbl, typ, nullable, def)
			}
			if n := scalar(t, `
				SELECT count(*) FROM pg_constraint c
				 WHERE c.conrelid = $1::regclass AND c.contype = 'f'
				   AND 'run_id' = ANY (SELECT a.attname FROM pg_attribute a
				                        WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey))`, tbl); n != 0 {
				t.Errorf("%s.run_id carries %d foreign key(s), want none (build_id carries none either)", tbl, n)
			}
		}
	})

	t.Run("the appended observations and the run record name the run", func(t *testing.T) {
		if n := mppRun(t, "pv_run_ok", `SELECT * FROM (VALUES `+obs("run-ok", 500, 100, "2026-03-01T00:00:00Z")+`) `+mppCols, 4242); n != 1 {
			t.Fatalf("inserted %d, want 1", n)
		}
		if n := scalar(t, `SELECT count(*) FROM position_state WHERE instrument_key = 'run-ok' AND run_id = 4242 AND build_id = 7`); n != 1 {
			t.Errorf("appended rows carrying run_id 4242 = %d, want 1", n)
		}
		if n := scalar(t, `SELECT count(*) FROM position_projection_run WHERE projection = 'public.pv_run_ok' AND run_id = 4242`); n != 1 {
			t.Errorf("run records carrying run_id 4242 = %d, want 1", n)
		}
	})

	// A refusal is the row a reader consults when a projection and the spine disagree, so it has to
	// name the run that recorded it or the disagreement cannot be traced back to an artefact.
	t.Run("both refusal classes name the run that recorded them", func(t *testing.T) {
		body := `SELECT * FROM (VALUES ` + obs("run-drift", 500, 100, "2026-03-02T00:00:00Z") + `) ` + mppCols
		if n := mppRun(t, "pv_run_drift", body, 5150); n != 1 {
			t.Fatalf("open inserted %d, want 1", n)
		}
		// The same key re-emitted with a different quantity: the stored row wins and a drift is recorded.
		drifted := `SELECT * FROM (VALUES ` + obs("run-drift", 900, 100, "2026-03-02T00:00:00Z") + `) ` + mppCols
		if n := mppRun(t, "pv_run_drift", drifted, 5151); n != 0 {
			t.Fatalf("drift inserted %d, want 0", n)
		}
		if n := scalar(t, `
			SELECT count(*) FROM position_projection_refusal
			 WHERE reason = 'observation_drift' AND detail LIKE 'ik=run-drift %' AND run_id = 5151`); n != 1 {
			t.Errorf("drift refusals carrying run_id 5151 = %d, want 1", n)
		}
		// A higher block carrying an earlier instant: the position is withheld and an inversion recorded.
		inv := `SELECT * FROM (VALUES ` + obs("run-inv", 500, 100, "2026-03-05T00:00:00Z") + `) ` + mppCols
		if n := mppRun(t, "pv_run_inv", inv, 6160); n != 1 {
			t.Fatalf("open inserted %d, want 1", n)
		}
		back := `SELECT * FROM (VALUES ` + obs("run-inv", 700, 200, "2026-03-04T00:00:00Z") + `) ` + mppCols
		if n := mppRun(t, "pv_run_inv", back, 6161); n != 0 {
			t.Fatalf("inverted batch inserted %d, want 0", n)
		}
		if n := scalar(t, `
			SELECT count(*) FROM position_projection_refusal
			 WHERE reason = 'block_time_inverts_height' AND detail LIKE 'ik=run-inv %' AND run_id = 6161`); n != 1 {
			t.Errorf("inversion refusals carrying run_id 6161 = %d, want 1", n)
		}
	})

	// Pre-tracking rows are the reason the column is nullable, and the two-argument form is what every
	// wrapper that has not been threaded yet still calls: it must resolve, not raise "not unique".
	t.Run("an omitted run is NULL, not an error, and the two-argument call still resolves", func(t *testing.T) {
		name, body := "pv_run_absent", `SELECT * FROM (VALUES `+obs("run-absent", 500, 100, "2026-03-08T00:00:00Z")+`) `+mppCols
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+name+` AS `+body); err != nil {
			t.Fatal(err)
		}
		var inserted int64
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection($1::regclass, 7)`, name).Scan(&inserted); err != nil {
			t.Fatalf("two-argument call: %v", err)
		}
		if inserted != 1 {
			t.Fatalf("inserted %d, want 1", inserted)
		}
		if n := scalar(t, `SELECT count(*) FROM position_state WHERE instrument_key = 'run-absent' AND run_id IS NULL`); n != 1 {
			t.Errorf("rows appended by the two-argument call with a NULL run = %d, want 1", n)
		}
		if n := scalar(t, `SELECT count(*) FROM position_projection_run WHERE projection = 'public.' || $1 AND run_id IS NULL`, name); n != 1 {
			t.Errorf("run records with a NULL run = %d, want 1", n)
		}
		// One definition, so no call site can be ambiguous. A re-applied old file resurrects the
		// two-argument body; the re-apply group above covers that and puts it back.
		if n := scalar(t, `SELECT count(*) FROM pg_proc WHERE proname = 'materialize_position_projection'`); n != 1 {
			t.Errorf("materialize_position_projection has %d definitions, want 1", n)
		}
	})

	// run_id is metadata, so it must not enter identity: a re-run of an unchanged source under a new
	// run appends nothing. Were it in the suppression predicate, every run would duplicate history.
	t.Run("a new run over an unchanged source appends nothing", func(t *testing.T) {
		body := `SELECT * FROM (VALUES ` + obs("run-idem", 500, 100, "2026-03-09T00:00:00Z") + `) ` + mppCols
		if n := mppRun(t, "pv_run_idem", body, 7170); n != 1 {
			t.Fatalf("first run inserted %d, want 1", n)
		}
		if n := mppRun(t, "pv_run_idem", body, 7171); n != 0 {
			t.Errorf("second run under a different run_id inserted %d, want 0", n)
		}
		if n := scalar(t, `SELECT count(*) FROM position_state WHERE instrument_key = 'run-idem'`); n != 1 {
			t.Errorf("stored rows for run-idem = %d, want 1", n)
		}
		if n := scalar(t, `SELECT count(*) FROM position_state WHERE instrument_key = 'run-idem' AND run_id = 7170`); n != 1 {
			t.Errorf("the stored row's run_id changed; it must keep the run that wrote it")
		}
	})
}
