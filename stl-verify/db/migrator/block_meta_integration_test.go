//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestBlockMeta drives the block_meta dimension (VEC-491) through one shared schema: the ADR-0002
// processing_version trigger semantics (retry vs reprocess), the reorg and per-chain axes, the
// corruption-guard CHECKs at the loader's chokepoint, the block_meta_current read surface, and
// concurrent same-key serialization via the trigger's advisory lock.
//
// Subtests own disjoint (chain_id, block_number) coordinates and are independently runnable under
// -run, EXCEPT the lifecycle subtest, which is a deliberate sequence and holds its steps internally
// rather than spreading them across sibling subtests that a filtered run would skip.
func TestBlockMeta(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	insert := func(t *testing.T, chain, bn, bv int, ts string, build int) error {
		t.Helper()
		_, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES ($1, $2, $3, $4::timestamptz, $5)`, chain, bn, bv, ts, build)
		return err
	}
	pvOf := func(t *testing.T, chain, bn, bv int) int {
		t.Helper()
		var pv int
		if err := pool.QueryRow(ctx,
			`SELECT processing_version FROM block_meta
			  WHERE chain_id = $1 AND block_number = $2 AND block_version = $3
			  ORDER BY processing_version DESC LIMIT 1`, chain, bn, bv).Scan(&pv); err != nil {
			t.Fatalf("pvOf(%d,%d,%d): %v", chain, bn, bv, err)
		}
		return pv
	}
	rowsAt := func(t *testing.T, chain, bn, bv int) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM block_meta
			  WHERE chain_id = $1 AND block_number = $2 AND block_version = $3`, chain, bn, bv).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// One sequence, not four subtests sharing a coordinate: retry needs the first write and reprocess
	// needs both, so as siblings they were order-dependent and `-run .../reprocess` alone failed.
	t.Run("processing_version lifecycle on one key", func(t *testing.T) {
		const ch, bn, bv = 1, 100, 0

		if err := insert(t, ch, bn, bv, "2026-01-01 00:00+00", 7); err != nil {
			t.Fatal(err)
		}
		if pv := pvOf(t, ch, bn, bv); pv != 0 {
			t.Errorf("first insert pv = %d; want 0", pv)
		}

		// Retry: same key AND same build. The trigger reuses the version so ON CONFLICT on the full PK
		// drops the duplicate rather than minting a phantom correction (ADR-0002 §3). Deliberately a
		// DIFFERENT timestamp: re-sending the identical value cannot distinguish "deduped" from
		// "correction silently discarded", which is the semantic that actually matters here.
		if _, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES ($1, $2, $3, '2026-01-01 00:00:04+00', 7)
			 ON CONFLICT (chain_id, block_number, block_version, processing_version) DO NOTHING`,
			ch, bn, bv); err != nil {
			t.Fatal(err)
		}
		if n := rowsAt(t, ch, bn, bv); n != 1 {
			t.Errorf("same-build retry produced %d rows; want 1 (deduped)", n)
		}
		var kept string
		if err := pool.QueryRow(ctx,
			`SELECT block_timestamp::text FROM block_meta
			  WHERE chain_id = $1 AND block_number = $2 AND block_version = $3`, ch, bn, bv).Scan(&kept); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(kept, "00:00:00") {
			t.Errorf("same-build retry kept %s; want the ORIGINAL 00:00:00 value", kept)
		}

		// Reprocess under a corrected build: coexists, and the highest pv carries the new value.
		if err := insert(t, ch, bn, bv, "2026-01-01 00:00:07+00", 8); err != nil {
			t.Fatal(err)
		}
		if n := rowsAt(t, ch, bn, bv); n != 2 {
			t.Fatalf("reprocess rows = %d; want 2 (original + correction coexist)", n)
		}
		if pv := pvOf(t, ch, bn, bv); pv != 1 {
			t.Errorf("reprocess pv = %d; want 1", pv)
		}

		// A-B-A: returning to build 7 must REUSE pv 0, not mint pv 2.
		if _, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES ($1, $2, $3, '2026-01-01 00:00:00+00', 7)
			 ON CONFLICT (chain_id, block_number, block_version, processing_version) DO NOTHING`,
			ch, bn, bv); err != nil {
			t.Fatal(err)
		}
		if n := rowsAt(t, ch, bn, bv); n != 2 {
			t.Errorf("A-B-A retry produced %d rows; want 2 (build 7 reuses pv 0)", n)
		}

		// The read surface. A raw three-column join returns both rows and would double any aggregate.
		var cur int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM block_meta_current
			  WHERE chain_id = $1 AND block_number = $2 AND block_version = $3`, ch, bn, bv).Scan(&cur); err != nil {
			t.Fatal(err)
		}
		if cur != 1 {
			t.Errorf("block_meta_current returned %d rows; want exactly 1 (that is its whole job)", cur)
		}
		var curTS string
		if err := pool.QueryRow(ctx,
			`SELECT block_timestamp::text FROM block_meta_current
			  WHERE chain_id = $1 AND block_number = $2 AND block_version = $3`, ch, bn, bv).Scan(&curTS); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(curTS, "00:00:07") {
			t.Errorf("block_meta_current resolved %s; want the highest-pv 00:00:07 value", curTS)
		}
	})

	t.Run("a reorg block_version starts its own processing_version at 0", func(t *testing.T) {
		if err := insert(t, 1, 300, 0, "2026-01-01 00:00+00", 8); err != nil {
			t.Fatal(err)
		}
		if err := insert(t, 1, 300, 1, "2026-01-01 00:00:13+00", 8); err != nil {
			t.Fatal(err)
		}
		if pv := pvOf(t, 1, 300, 1); pv != 0 {
			t.Errorf("reorg version pv = %d; want 0 (per-(chain,bn,bv) axis)", pv)
		}
	})

	// Pins chain_id in BOTH trigger lookups. Without it the suite passed while two chains at the same
	// height shared one version sequence — routine on real data (ETH and Base both reach 18,000,000) —
	// and the retry branch could hand back another chain's pv, a PK collision that the loader's
	// ON CONFLICT DO NOTHING swallows, dropping a whole chain's block.
	t.Run("two chains at the same height keep separate processing_version axes", func(t *testing.T) {
		if err := insert(t, 1, 400, 0, "2026-01-01 00:00+00", 21); err != nil {
			t.Fatal(err)
		}
		if err := insert(t, 137, 400, 0, "2026-01-01 00:00:05+00", 22); err != nil {
			t.Fatal(err)
		}
		if pv := pvOf(t, 1, 400, 0); pv != 0 {
			t.Errorf("chain 1 pv = %d; want 0", pv)
		}
		if pv := pvOf(t, 137, 400, 0); pv != 0 {
			t.Errorf("chain 137 pv = %d; want 0 (its own axis, not chain 1's next version)", pv)
		}
	})

	t.Run("corruption guards reject implausible timestamps and coordinates", func(t *testing.T) {
		// The loader parses hex header fields; a parse bug yields epoch-zero or a wild overshoot, a bad
		// S3 key yields nonsense coordinates. These must fail loudly at the chokepoint rather than be
		// served as event-time to every fill consumer.
		cases := []struct {
			name string
			err  error
			want string
		}{
			{"epoch-1970 timestamp", insert(t, 1, 200, 0, "1970-01-01 00:00+00", 0), "ts_sane"},
			{"pre-genesis timestamp", insert(t, 1, 201, 0, "2008-12-31 00:00+00", 0), "ts_sane"},
			{"base_fee parsed as timestamp (yr 2603)", insert(t, 1, 205, 0, "2603-10-11 00:00+00", 0), "ts_sane"},
			{"seconds scaled by 1000 (yr 55055)", insert(t, 1, 206, 0, "55055-04-13 00:00+00", 0), "ts_sane"},
			{"negative block_number", insert(t, 1, -5, 0, "2026-01-01 00:00+00", 0), "coord_nonneg"},
			{"negative block_version", insert(t, 1, 202, -1, "2026-01-01 00:00+00", 0), "coord_nonneg"},
			{"negative build_id", insert(t, 1, 207, 0, "2026-01-01 00:00+00", -5), "coord_nonneg"},
			{"chain_id zero", insert(t, 0, 203, 0, "2026-01-01 00:00+00", 0), "chain_pos"},
			{"chain_id negative", insert(t, -1, 208, 0, "2026-01-01 00:00+00", 0), "chain_pos"},
		}
		for _, c := range cases {
			if c.err == nil || !strings.Contains(c.err.Error(), c.want) {
				t.Errorf("%s: want %s violation, got %v", c.name, c.want, c.err)
			}
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES (1, 204, 0, NULL)`); err == nil || !strings.Contains(err.Error(), `column "block_timestamp"`) {
			t.Errorf("NULL block_timestamp: want not-null violation, got %v", err)
		}
	})

	// The guards must ADMIT the historical load, not merely reject nonsense. Without this the ts_sane
	// bound could be moved anywhere inside (2008-12-31, 2026-01-01] with the suite still green, while
	// rejecting 100% of the deep-tail backfill this table exists to hold.
	t.Run("guards admit legitimate historical blocks", func(t *testing.T) {
		accepted := []struct {
			name string
			err  error
		}{
			{"exact genesis boundary", insert(t, 1, 600, 0, "2009-01-03 00:00:00+00", 1)},
			{"Feb-2023 deep-tail block", insert(t, 1, 601, 0, "2023-02-01 12:00:00+00", 1)},
			{"a 2099 block, inside the ceiling", insert(t, 1, 602, 0, "2099-12-31 00:00:00+00", 1)},
		}
		for _, c := range accepted {
			if c.err != nil {
				t.Errorf("%s: must be accepted, got %v", c.name, c.err)
			}
		}
	})

	t.Run("block_time is dropped (superseded by block_meta)", func(t *testing.T) {
		var present bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('block_time') IS NOT NULL`).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if present {
			t.Error("block_time still exists; the migration should have dropped it")
		}
	})

	// Concurrency. The previous version of this subtest launched the second writer and committed txA
	// immediately, with no barrier: the goroutine had to open a fresh pool connection (TCP + startup)
	// and always lost that race to a single-round-trip COMMIT, so it ran sequentially and passed with
	// the advisory lock deleted outright. Two changes make it real — the second connection is acquired
	// BEFORE the goroutine starts, and txA does not commit until the second backend is observably
	// waiting on a lock. The negative control below proves the subtest can now fail.
	waitingOnLock := func(t *testing.T) bool {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_activity
				  WHERE datname = current_database() AND pid <> pg_backend_pid()
				    AND state = 'active' AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n > 0 {
				return true
			}
			time.Sleep(25 * time.Millisecond)
		}
		return false
	}

	race := func(t *testing.T, bn int) (secondErr error, blocked bool) {
		t.Helper()
		connA, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connA.Release()
		connB, err := pool.Acquire(ctx) // acquired up front: a cold connect would lose the race
		if err != nil {
			t.Fatal(err)
		}
		defer connB.Release()

		txA, err := connA.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txA.Rollback(ctx)
		if _, err := txA.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES (2, $1, 0, '2026-01-01 00:00+00', 11)`, bn); err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() {
			_, err := connB.Exec(ctx,
				`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
				 VALUES (2, $1, 0, '2026-01-01 00:00:03+00', 12)`, bn)
			done <- err
		}()

		blocked = waitingOnLock(t) // the barrier: only commit once B is provably stuck
		if err := txA.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return <-done, blocked
	}

	t.Run("concurrent same-key inserts serialize on the trigger's advisory lock", func(t *testing.T) {
		err, blocked := race(t, 500)
		if !blocked {
			t.Error("second writer never blocked; the subtest degenerated to a sequential run and proves nothing")
		}
		if err != nil {
			t.Fatalf("second concurrent insert failed: %v", err)
		}
		var pvs string
		if err := pool.QueryRow(ctx,
			`SELECT string_agg(processing_version::text, ',' ORDER BY processing_version)
			   FROM block_meta WHERE chain_id = 2 AND block_number = 500`).Scan(&pvs); err != nil {
			t.Fatal(err)
		}
		if pvs != "0,1" {
			t.Errorf("concurrent inserts produced pvs %s; want 0,1 (serialized, distinct)", pvs)
		}
	})

	// Negative control. Strip the advisory lock from the trigger and the same race must now break —
	// otherwise the subtest above is passing for reasons unrelated to the lock, which is exactly the
	// state it was in before. Restores the real function afterwards.
	t.Run("negative control: without the advisory lock the race is lost", func(t *testing.T) {
		var original string
		if err := pool.QueryRow(ctx,
			`SELECT pg_get_functiondef('assign_processing_version_block_meta'::regproc)`).Scan(&original); err != nil {
			t.Fatal(err)
		}
		// Excise the whole PERFORM statement, not a prefix of it: the call spans two lines, so a
		// line comment would orphan its continuation and fail on syntax rather than on the lock.
		start := strings.Index(original, "PERFORM pg_advisory_xact_lock")
		if start < 0 {
			t.Fatal("advisory lock call not found; the control would be vacuous")
		}
		term := strings.Index(original[start:], ";")
		if term < 0 {
			t.Fatal("advisory lock statement is unterminated; cannot excise it safely")
		}
		lockless := original[:start] + "PERFORM 1 /* advisory lock removed */" + original[start+term:]
		if _, err := pool.Exec(ctx, lockless); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := pool.Exec(ctx, original); err != nil {
				t.Fatalf("restoring the real trigger function: %v", err)
			}
		}()

		if err, _ := race(t, 501); err == nil {
			t.Error("lockless trigger completed cleanly; the concurrency subtest cannot detect a missing lock")
		}
	})
}
