//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestPositionDaily is the VEC-409 contract test. position_daily holds one row per (position, UTC
// date) -- the winning observation for that day, reorg and reprocess versions resolved, every
// historical date retained -- maintained by the AFTER INSERT trigger on position_state (#733's
// pattern with a per-date key). position_current is the latest day per position, a view over it.
//
// The tie case fails on the view this migration originally created: its ORDER BY omitted
// block_timestamp, so it returned the older row (11 where the newest observation was 22).
func TestPositionDaily(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	insert := func(t *testing.T, id, instrument string, qty, block, bv, pv int, ts string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO position_state
			    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id)
			VALUES (sha256($1::bytea), 1, 1, $2, 'aabb', $3, $4, $5, $6, $7, 'public.vtest', 7)`,
			[]byte(id), instrument, qty, block, bv, pv, ts); err != nil {
			t.Fatalf("insert %s block %d: %v", id, block, err)
		}
	}
	daily := func(t *testing.T, id string) []int {
		t.Helper()
		rows, err := pool.Query(ctx,
			`SELECT quantity FROM position_daily WHERE position_id = sha256($1::bytea) ORDER BY as_of_date`,
			[]byte(id))
		if err != nil {
			t.Fatalf("daily(%s): %v", id, err)
		}
		defer rows.Close()
		var out []int
		for rows.Next() {
			var q int
			if err := rows.Scan(&q); err != nil {
				t.Fatal(err)
			}
			out = append(out, q)
		}
		return out
	}
	current := func(t *testing.T, id string) (qty, block, bv int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT quantity, block_number, block_version FROM position_current
			  WHERE position_id = sha256($1::bytea)`, []byte(id)).Scan(&qty, &block, &bv); err != nil {
			t.Fatalf("current(%s): %v", id, err)
		}
		return
	}

	t.Run("the newest observation per position is current", func(t *testing.T) {
		insert(t, "a", "inst-a", 10, 100, 0, 0, "2026-01-01T00:00:00Z")
		insert(t, "a", "inst-a", 20, 200, 0, 0, "2026-01-02T00:00:00Z")
		if qty, block, _ := current(t, "a"); qty != 20 || block != 200 {
			t.Errorf("current = qty %d at block %d; want 20 at 200", qty, block)
		}
	})

	t.Run("a closing zero is current, not skipped", func(t *testing.T) {
		insert(t, "b", "inst-b", 5, 100, 0, 0, "2026-01-01T00:00:00Z")
		insert(t, "b", "inst-b", 0, 150, 0, 0, "2026-01-01T12:00:00Z")
		if qty, block, _ := current(t, "b"); qty != 0 || block != 150 {
			t.Errorf("current = qty %d at block %d; want the closing 0 at 150", qty, block)
		}
	})

	t.Run("an out-of-order backfill cannot regress the current row", func(t *testing.T) {
		insert(t, "c", "inst-c", 30, 600, 0, 0, "2026-03-01T00:00:00Z")
		insert(t, "c", "inst-c", 99, 400, 0, 0, "2026-02-01T00:00:00Z") // older, arrives later
		if qty, block, _ := current(t, "c"); qty != 30 || block != 600 {
			t.Errorf("current = qty %d at block %d; the older backfill regressed it", qty, block)
		}
	})

	t.Run("a reorg at the same block wins on block_version", func(t *testing.T) {
		insert(t, "d", "inst-d", 40, 700, 0, 0, "2026-04-01T00:00:00Z")
		insert(t, "d", "inst-d", 0, 700, 1, 0, "2026-04-01T00:00:00Z") // the zeroing reorg
		if qty, _, bv := current(t, "d"); qty != 0 || bv != 1 {
			t.Errorf("current = qty %d at bv %d; want the reorged 0 at bv 1", qty, bv)
		}
	})

	t.Run("two rows sharing the four-column key are broken by block_timestamp", func(t *testing.T) {
		// position_state's PK has five columns because block_timestamp is the partition column, so
		// this pair is legal. The original view ordered by the first four only and returned the OLDER
		// row (11); the comparison here includes block_timestamp, so the pick is total.
		insert(t, "e", "inst-e", 11, 500, 0, 0, "2026-05-01T00:00:00Z")
		insert(t, "e", "inst-e", 22, 500, 0, 0, "2026-05-02T00:00:00Z")
		if qty, _, _ := current(t, "e"); qty != 22 {
			t.Errorf("current = qty %d; want 22 (the later block_timestamp), not the older row", qty)
		}
	})

	t.Run("position_current is exactly one row per position", func(t *testing.T) {
		var rows, positions int
		if err := pool.QueryRow(ctx,
			`SELECT count(*), count(DISTINCT position_id) FROM position_current`).Scan(&rows, &positions); err != nil {
			t.Fatal(err)
		}
		// The invariant is one row per position, not a fixed count: subtests run in order and later
		// ones add positions, so a hardcoded number would only assert the ordering.
		if rows != positions {
			t.Errorf("position_current = %d rows over %d distinct positions; want one row each", rows, positions)
		}
		if rows == 0 {
			t.Error("position_current is empty; the view is not reading position_daily")
		}
	})

	t.Run("position_daily keeps every historical date, not just the latest", func(t *testing.T) {
		// Position A was observed on two different dates, so both must survive -- this is the whole
		// point of the per-date grain over a latest-only cache.
		if got := daily(t, "a"); len(got) != 2 || got[0] != 10 || got[1] != 20 {
			t.Errorf("daily(a) = %v; want [10 20], both dates retained", got)
		}
	})

	t.Run("a same-day reprocess replaces that day rather than adding one", func(t *testing.T) {
		insert(t, "f", "inst-f", 50, 800, 0, 0, "2026-06-10T00:00:00Z")
		insert(t, "f", "inst-f", 55, 800, 0, 1, "2026-06-10T06:00:00Z") // same date, higher pv
		if got := daily(t, "f"); len(got) != 1 || got[0] != 55 {
			t.Errorf("daily(f) = %v; want one row for the day at the winning pv, [55]", got)
		}
	})

	t.Run("rebuildable from history: TRUNCATE and re-run the backfill", func(t *testing.T) {
		before := map[string]int{}
		for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
			qty, _, _ := current(t, id)
			before[id] = qty
		}
		if _, err := pool.Exec(ctx, `TRUNCATE position_daily`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO position_daily
			    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
			     block_number, block_version, processing_version, block_timestamp, projection, build_id)
			SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
			       p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
			       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
			       p.processing_version, p.block_timestamp, p.projection, p.build_id
			FROM position_state p
			ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
			         p.block_number DESC, p.block_version DESC, p.processing_version DESC,
			         p.block_timestamp DESC
			ON CONFLICT (position_id, as_of_date) DO NOTHING`); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		for id, want := range before {
			if qty, _, _ := current(t, id); qty != want {
				t.Errorf("after rebuild, %s = qty %d; want %d (the rebuild must reproduce the cache)", id, qty, want)
			}
		}
	})
}
