//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestBlockMeta drives the block_meta dimension (VEC-491) through one shared schema: the ADR-0002
// processing_version trigger semantics (retry vs reprocess), the reorg axis, the corruption-guard
// CHECKs at the loader's chokepoint, and concurrent same-key serialization via the trigger's
// advisory lock. Distinct (chain_id, block_number) coordinates per subtest keep state isolated.
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

	t.Run("first insert gets processing_version 0", func(t *testing.T) {
		if err := insert(t, 1, 100, 0, "2026-01-01 00:00+00", 7); err != nil {
			t.Fatal(err)
		}
		if pv := pvOf(t, 1, 100, 0); pv != 0 {
			t.Errorf("first insert pv = %d; want 0", pv)
		}
	})

	t.Run("idempotent retry (same key, same build) dedupes via ON CONFLICT", func(t *testing.T) {
		// The trigger reuses the existing version for the same build, so the ON CONFLICT on the full
		// PK drops the duplicate instead of minting a phantom correction version (ADR-0002 §3).
		if _, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES (1, 100, 0, '2026-01-01 00:00+00', 7)
			 ON CONFLICT (chain_id, block_number, block_version, processing_version) DO NOTHING`); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM block_meta WHERE chain_id = 1 AND block_number = 100 AND block_version = 0`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("retry produced %d rows; want 1 (deduped)", n)
		}
	})

	t.Run("reprocess (different build) coexists and the latest wins", func(t *testing.T) {
		if err := insert(t, 1, 100, 0, "2026-01-01 00:00:07+00", 8); err != nil {
			t.Fatal(err)
		}
		var n int
		var ts string
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM block_meta WHERE chain_id = 1 AND block_number = 100 AND block_version = 0`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("reprocess rows = %d; want 2 (original + correction coexist)", n)
		}
		if pv := pvOf(t, 1, 100, 0); pv != 1 {
			t.Errorf("reprocess pv = %d; want 1", pv)
		}
		if err := pool.QueryRow(ctx,
			`SELECT block_timestamp::text FROM block_meta
			  WHERE chain_id = 1 AND block_number = 100 AND block_version = 0
			  ORDER BY processing_version DESC LIMIT 1`).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(ts, "00:00:07") {
			t.Errorf("latest-wins timestamp = %s; want the corrected 00:00:07 value", ts)
		}
	})

	t.Run("a reorg block_version starts its own processing_version at 0", func(t *testing.T) {
		if err := insert(t, 1, 100, 1, "2026-01-01 00:00:13+00", 8); err != nil {
			t.Fatal(err)
		}
		if pv := pvOf(t, 1, 100, 1); pv != 0 {
			t.Errorf("reorg version pv = %d; want 0 (per-(chain,bn,bv) axis)", pv)
		}
	})

	t.Run("corruption guards reject epoch timestamp, negative coordinates, chain 0, NULL timestamp", func(t *testing.T) {
		// The loader parses hex header fields; a parse bug yields epoch-zero, a bad S3 key yields
		// nonsense coordinates. These must fail loudly at the chokepoint, not be served as event-time.
		cases := []struct {
			name string
			err  error
			want string
		}{
			{"epoch-1970 timestamp", insert(t, 1, 200, 0, "1970-01-01 00:00+00", 0), "ts_sane"},
			{"pre-genesis timestamp", insert(t, 1, 201, 0, "2008-12-31 00:00+00", 0), "ts_sane"},
			{"negative block_number", insert(t, 1, -5, 0, "2026-01-01 00:00+00", 0), "coord_nonneg"},
			{"negative block_version", insert(t, 1, 202, -1, "2026-01-01 00:00+00", 0), "coord_nonneg"},
			{"chain_id zero", insert(t, 0, 203, 0, "2026-01-01 00:00+00", 0), "chain_pos"},
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

	t.Run("block_time is dropped (superseded by block_meta)", func(t *testing.T) {
		var present bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('block_time') IS NOT NULL`).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if present {
			t.Error("block_time still exists; the migration should have dropped it")
		}
	})

	t.Run("concurrent same-key inserts serialize on the trigger's advisory lock", func(t *testing.T) {
		// Two builds writing the same (chain, bn, bv) concurrently must serialize and get distinct
		// processing_versions — never a duplicate-PK error and never a shared version.
		connA, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connA.Release()
		txA, err := connA.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txA.Rollback(ctx)
		if _, err := txA.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES (2, 500, 0, '2026-01-01 00:00+00', 11)`); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			// blocks on the advisory lock until txA commits, then must take pv=1
			_, err := pool.Exec(ctx,
				`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
				 VALUES (2, 500, 0, '2026-01-01 00:00:03+00', 12)`)
			done <- err
		}()
		if err := txA.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
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
}
