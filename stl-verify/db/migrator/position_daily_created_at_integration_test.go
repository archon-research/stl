//go:build integration

package migrator_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// createdAt is when one day's cached row was last written.
func (f *positionDailyFixture) createdAt(id, date string) time.Time {
	f.t.Helper()
	var at time.Time
	if err := f.pool.QueryRow(f.ctx,
		`SELECT created_at FROM position_daily
		  WHERE position_id = sha256($1::bytea) AND as_of_date = $2::date`, id, date).Scan(&at); err != nil {
		f.t.Fatalf("created_at(%s, %s): %v", id, date, err)
	}
	return at
}

// cacheLagsSpine is the staleness reading the created_at COMMENT names. An empty spine has nothing to
// lag, so it reads false.
func (f *positionDailyFixture) cacheLagsSpine() bool {
	f.t.Helper()
	var lag bool
	if err := f.pool.QueryRow(f.ctx, `
		SELECT COALESCE(COALESCE((SELECT max(created_at) FROM position_daily), '-infinity')
		                < (SELECT max(created_at) FROM position_state), false)`).Scan(&lag); err != nil {
		f.t.Fatalf("staleness reading: %v", err)
	}
	return lag
}

// created_at moves when a day's row content does, through either writer, and stays put for an
// observation for that same date which the newer-wins guard rejects. Consecutive statements run in
// separate transactions, so their now() differ.
func TestPositionDailyCreatedAtMarksTheLatestOverwrite(t *testing.T) {
	for _, writer := range []string{"trigger", "rebuild"} {
		t.Run(writer, func(t *testing.T) {
			f := newPositionDailyFixture(t)
			const id, day = "created-at", "2026-01-02"
			f.observe(id, dailyObs{qty: 11, block: 200, ts: "2026-01-02T10:00:00Z", dealType: "LOAN"})
			first := f.createdAt(id, day)

			// Same date, older coordinate: the trigger's guard rejects it, and the rebuild then sees a
			// winner equal to the cached row and must not touch it either.
			f.observe(id, dailyObs{qty: 5, block: 100, ts: "2026-01-02T01:00:00Z", dealType: "LOAN"})
			if writer == "rebuild" {
				f.rebuild()
			}
			if got := f.createdAt(id, day); !got.Equal(first) {
				t.Errorf("%s: created_at moved %s -> %s on an observation the guard rejects", writer, first, got)
			}

			switch writer {
			case "trigger":
				f.observe(id, dailyObs{qty: 22, block: 300, ts: "2026-01-02T20:00:00Z", dealType: "LOAN"})
			case "rebuild":
				// Stale the day's row by hand (owner role) so the rebuild is the writer that raises it.
				// block_timestamp stays inside the same date: the CHECK pins as_of_date to it.
				if _, err := f.pool.Exec(f.ctx, `
					UPDATE position_daily SET block_number = 100, block_timestamp = '2026-01-02T01:00:00Z',
					       quantity = 5, created_at = $3
					 WHERE position_id = sha256($1::bytea) AND as_of_date = $2::date`, id, day, first); err != nil {
					t.Fatalf("stale the day's row: %v", err)
				}
				f.rebuild()
			}
			if got := f.createdAt(id, day); !got.After(first) {
				t.Errorf("%s: created_at = %s after the row was raised past %s; want it to move with the content", writer, got, first)
			}
		})
	}
}

// The grain's own caveat, and the reason its COMMENT calls the reading weaker than position_current's:
// a late observation for a date with no row yet is an INSERT, so it advances max(created_at) instead of
// leaving it behind. Only a late observation losing to an existing row for the same date reads as lagging.
func TestPositionDailyCreatedAtReadingIsWeakerAtThisGrain(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id = "late"
	f.observe(id, dailyObs{qty: 11, block: 200, ts: "2026-01-02T10:00:00Z", dealType: "LOAN"})
	if f.cacheLagsSpine() {
		t.Fatal("the reading lags straight after an observation the trigger propagated")
	}

	// A date this position has no row for: an INSERT, so the reading stays level.
	f.observe(id, dailyObs{qty: 7, block: 150, ts: "2026-01-01T10:00:00Z", dealType: "LOAN"})
	if f.cacheLagsSpine() {
		t.Error("a late observation for an unseen date left the reading behind; it is an INSERT, so the COMMENT says it advances it")
	}
	if got := f.dayQty(id, "2026-01-01"); got != 7 {
		t.Errorf("the unseen date holds quantity %d, want 7 -- the late observation did not land at all", got)
	}

	// A date it does have a row for, losing to that row: rejected, so the reading falls behind.
	f.observe(id, dailyObs{qty: 3, block: 100, ts: "2026-01-02T01:00:00Z", dealType: "LOAN"})
	if !f.cacheLagsSpine() {
		t.Error("a late observation rejected for an existing date did not leave the reading behind")
	}
	if got := f.dayQty(id, "2026-01-02"); got != 11 {
		t.Errorf("the existing date holds quantity %d, want 11 -- the rejected observation overwrote the day", got)
	}
}

// A revised migration file re-applied over a table an earlier revision of it created. run_id is carried
// onto it by an idempotent ALTER, which is why the file has one. created_at cannot be: TimescaleDB rejects
// a DEFAULT now() column on a columnstore hypertable, so this pins the restriction the comment cites -- if
// a later TimescaleDB lifts it, this test fails and the file can carry the second ALTER after all.
func TestPositionDailyMigrationAddsRunIDButCannotAddCreatedAtInPlace(t *testing.T) {
	src := func(t *testing.T) string {
		t.Helper()
		// Read inline, as the other re-apply test in this file does: #644 carries a readMigration helper for
		// this package, and a second definition here would collide once both branches land.
		raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), positionDailyMigration))
		if err != nil {
			t.Fatalf("read the migration: %v", err)
		}
		return string(raw)
	}

	t.Run("run_id is added in place", func(t *testing.T) {
		f := newPositionDailyFixture(t)
		// A cached row before the drop, so the ALTER runs over a populated table.
		f.observe("earlier", dailyObs{qty: 1, block: 50, ts: "2026-01-01T10:00:00Z", dealType: "LOAN"})
		if _, err := f.pool.Exec(f.ctx, `ALTER TABLE position_daily DROP COLUMN run_id`); err != nil {
			t.Fatalf("shape the table as an earlier revision left it: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, src(t)); err != nil {
			t.Fatalf("re-applying the revised file over the earlier table: %v", err)
		}
		f.observe("revised", dailyObs{qty: 5, block: 100, ts: "2026-01-02T10:00:00Z", dealType: "LOAN"})
		if v, ok := f.dayRow("revised", "2026-01-02")["run_id"]; !ok || v == "NULL" {
			t.Errorf("a day observed after the re-apply carries run_id=%q (present: %v); want it written", v, ok)
		}
		// The row that predates the ALTER keeps NULL, and neither writer can fill it: the file's own
		// CALL rebuild_position_daily() is forward-only, and this row already sits at history's winning
		// coordinate, so the guard never fires. Recovering it needs the row deleted or the day re-observed.
		if v := f.dayRow("earlier", "2026-01-01")["run_id"]; v != "NULL" {
			t.Errorf("the pre-ALTER row carries run_id=%q; the forward-only rebuild cannot fill it, so NULL is the honest state", v)
		}
	})

	t.Run("created_at cannot be", func(t *testing.T) {
		f := newPositionDailyFixture(t)
		f.observe("earlier", dailyObs{qty: 1, block: 50, ts: "2026-01-01T10:00:00Z", dealType: "LOAN"})
		if _, err := f.pool.Exec(f.ctx, `ALTER TABLE position_daily DROP COLUMN created_at`); err != nil {
			t.Fatalf("drop created_at: %v", err)
		}
		_, err := f.pool.Exec(f.ctx, `ALTER TABLE position_daily ADD COLUMN created_at timestamptz NOT NULL DEFAULT now()`)
		if err == nil {
			t.Fatal("TimescaleDB accepted a DEFAULT now() column on a columnstore hypertable; the file can carry the ALTER after all")
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "0A000" {
			t.Fatalf("adding created_at failed with %v; want SQLSTATE 0A000 (feature_not_supported)", err)
		}
		// The documented consequence: the file cannot upgrade such a table, so it does not try.
		if strings.Contains(src(t), "ADD COLUMN IF NOT EXISTS created_at") {
			t.Error("the migration carries a created_at ALTER that cannot run on this table")
		}
	})
}

// The columnstore key is the stable identity a chunk is grouped and sorted by, so created_at -- which
// changes on every overwrite -- is deliberately not in it. Read from TimescaleDB's own settings, so a
// migration that adds it to either key is caught here rather than in a rewrite-heavy chunk.
func TestPositionDailyColumnstoreKeyExcludesCreatedAt(t *testing.T) {
	f := newPositionDailyFixture(t)
	var segmentby, orderby string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT COALESCE(segmentby, ''), COALESCE(orderby, '')
		FROM timescaledb_information.hypertable_compression_settings
		WHERE hypertable = 'position_daily'::regclass`).Scan(&segmentby, &orderby); err != nil {
		f.t.Fatalf("read the columnstore settings: %v", err)
	}
	if segmentby != "position_id" || orderby != "as_of_date DESC" {
		t.Errorf("columnstore keys are segmentby %q, orderby %q; want \"position_id\" and \"as_of_date DESC\"", segmentby, orderby)
	}
}

// A re-observation identical to what the day's row holds must not rewrite it: the guard is strict, so
// created_at marks an advance rather than beating on every re-emission, and a reprocess does not rewrite
// -- and on this table decompress -- every row it re-emits.
func TestPositionDailyTriggerDoesNotRewriteAnIdenticalRow(t *testing.T) {
	f := newPositionDailyFixture(t)
	const id, day = "noop", "2026-01-01"
	o := dailyObs{qty: 100, block: 100, ts: "2026-01-01T10:00:00Z", dealType: "LOAN"}
	f.observe(id, o)
	if _, err := f.pool.Exec(f.ctx,
		`DELETE FROM position_state WHERE position_id = sha256($1::bytea)`, id); err != nil {
		f.t.Fatalf("withdraw the spine row (owner role): %v", err)
	}
	ctid := func() string {
		f.t.Helper()
		var v string
		if err := f.pool.QueryRow(f.ctx,
			`SELECT ctid::text FROM position_daily
			  WHERE position_id = sha256($1::bytea) AND as_of_date = $2::date`, id, day).Scan(&v); err != nil {
			f.t.Fatalf("read ctid: %v", err)
		}
		return v
	}
	before, beforeAt := ctid(), f.createdAt(id, day)

	f.observe(id, o)
	if after := ctid(); after != before {
		t.Errorf("ctid moved %s -> %s: the trigger rewrote a row at identical coordinates, so every re-observation costs a heap write and WAL", before, after)
	}
	if at := f.createdAt(id, day); !at.Equal(beforeAt) {
		t.Errorf("created_at moved %s -> %s on an identical re-observation; it must mark an advance, not a re-emission", beforeAt, at)
	}
	// Negative control: a genuinely newer observation for the same date must still be written.
	f.observe(id, dailyObs{qty: 200, block: 200, ts: "2026-01-01T20:00:00Z", dealType: "LOAN"})
	if got := f.dayQty(id, day); got != 200 {
		t.Errorf("quantity = %d after a newer observation; want 200 -- the arm does not fire at all, so the no-op assertion above proves nothing", got)
	}
}
