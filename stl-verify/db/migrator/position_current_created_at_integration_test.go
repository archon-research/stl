//go:build integration

package migrator_test

import (
	"testing"
	"time"
)

// createdAt is when the cached row's content was last written.
func (f *positionCurrentFixture) createdAt(id string) time.Time {
	f.t.Helper()
	var at time.Time
	if err := f.pool.QueryRow(f.ctx,
		`SELECT created_at FROM position_current WHERE position_id = sha256($1::bytea)`, id).Scan(&at); err != nil {
		f.t.Fatalf("created_at(%s): %v", id, err)
	}
	return at
}

// cacheLagsSpine is the staleness reading the created_at COMMENT names: the cache's newest write is older
// than the spine's newest insert. An empty spine has nothing to lag, so it reads false.
func (f *positionCurrentFixture) cacheLagsSpine() bool {
	f.t.Helper()
	var lag bool
	if err := f.pool.QueryRow(f.ctx, `
		SELECT COALESCE(COALESCE((SELECT max(created_at) FROM position_current), '-infinity')
		                < (SELECT max(created_at) FROM position_state), false)`).Scan(&lag); err != nil {
		f.t.Fatalf("staleness reading: %v", err)
	}
	return lag
}

// created_at moves when the row's content does, through either writer, and stays put for an observation
// the newer-wins guard rejects -- the same shape as the sibling caches, so max(created_at) reads as the time
// of the cache's last advance. Consecutive statements run in separate transactions, so their now() differ.
func TestPositionCurrentCreatedAtMarksTheLatestOverwrite(t *testing.T) {
	for _, writer := range []string{"trigger", "rebuild"} {
		t.Run(writer, func(t *testing.T) {
			f := newPositionCurrentFixture(t)
			const id = "created-at"
			f.observe(id, obs{qty: 11, block: 200, ts: "2026-01-02T00:00:00Z", dealType: "LOAN"})
			first := f.createdAt(id)

			// The trigger's guard rejects an older observation; the rebuild then sees a winner equal to the
			// cache and, under the same strict guard, must not touch it either.
			f.observe(id, obs{qty: 5, block: 100, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
			if writer == "rebuild" {
				f.rebuild()
			}
			if got := f.createdAt(id); !got.Equal(first) {
				t.Errorf("%s: created_at moved %s -> %s on an observation the guard rejects", writer, first, got)
			}
			// The staleness reading cannot tell this from the replica-role gap: the spine's newest insert is
			// now ahead of the cache's newest write although the cache is right (the created_at COMMENT says so).
			if !f.cacheLagsSpine() {
				t.Errorf("%s: max(created_at) does not fall behind the spine after an older observation arrived late; the COMMENT's caveat no longer holds", writer)
			}

			switch writer {
			case "trigger":
				f.observe(id, obs{qty: 22, block: 300, ts: "2026-01-03T00:00:00Z", dealType: "LOAN"})
			case "rebuild":
				// Stale the cache by hand (owner role) so the rebuild, not the trigger, is the writer that raises it.
				if _, err := f.pool.Exec(f.ctx, `
					UPDATE position_current SET block_number = 100, block_timestamp = '2026-01-01T00:00:00Z', quantity = 5,
					       created_at = $2
					 WHERE position_id = sha256($1::bytea)`, id, first); err != nil {
					t.Fatalf("stale the cache: %v", err)
				}
				f.rebuild()
			}
			if got := f.createdAt(id); !got.After(first) {
				t.Errorf("%s: created_at = %s after the row was raised past %s; want it to move with the content", writer, got, first)
			}
			if f.cacheLagsSpine() {
				t.Errorf("%s: max(created_at) still falls behind the spine after the row was raised", writer)
			}
		})
	}
}

// A revised migration file re-applied over a table an earlier revision of it created: CREATE TABLE IF NOT
// EXISTS adds no column, so the ALTERs are what carry run_id and created_at onto it. Dropping either ALTER
// fails here at the first statement that names the column, its COMMENT ON COLUMN.
func TestPositionCurrentMigrationAddsTheColumnsAnEarlierRevisionLacked(t *testing.T) {
	f := newPositionCurrentFixture(t)
	// A cached row before the drop, so the NOT NULL DEFAULT now() ALTER runs over a populated table.
	f.observe("earlier", obs{qty: 1, block: 50, ts: "2026-01-01T00:00:00Z", dealType: "LOAN"})
	if _, err := f.pool.Exec(f.ctx, `ALTER TABLE position_current DROP COLUMN run_id, DROP COLUMN created_at`); err != nil {
		t.Fatalf("shape the table as an earlier revision left it: %v", err)
	}
	src, err := readMigration("20260819_150000_create_position_current.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, src); err != nil {
		t.Fatalf("re-applying the revised file over the earlier table: %v", err)
	}
	f.observe("revised", obs{qty: 5, block: 100, ts: "2026-01-02T00:00:00Z", dealType: "LOAN"})
	// The pre-existing row gets created_at from the ALTER's default; its run_id was dropped with the column.
	for _, tc := range []struct{ id, col string }{
		{"earlier", "created_at"}, {"revised", "created_at"}, {"revised", "run_id"},
	} {
		if v, ok := f.row(tc.id)[tc.col]; !ok || v == "NULL" {
			t.Errorf("after the re-apply the cache row %s carries %s=%q (present: %v); want it written", tc.id, tc.col, v, ok)
		}
	}
}
