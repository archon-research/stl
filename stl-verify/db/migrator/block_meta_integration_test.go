//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"
)

// VEC-491: block_meta is a plain dimension keyed on the natural key (chain_id, block_number,
// block_version, processing_version). UPDATE and DELETE are revoked, so a mis-parsed header time is
// corrected by appending the same block at a higher processing_version; readers take the highest.
func TestBlockMeta(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	insert := func(t *testing.T, chain, bn, bv int, ts string, build int) error {
		t.Helper()
		_, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES ($1, $2, $3, $4::timestamptz, $5)`, chain, bn, bv, ts, build)
		return err
	}
	timesAt := func(t *testing.T, chain, bn int) (rows int, distinctTimes int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT count(*), count(DISTINCT block_timestamp) FROM block_meta WHERE chain_id = $1 AND block_number = $2`,
			chain, bn).Scan(&rows, &distinctTimes); err != nil {
			t.Fatal(err)
		}
		return rows, distinctTimes
	}

	t.Run("the natural key is unique: a loader retry is a no-op and a second header time is refused", func(t *testing.T) {
		if err := insert(t, 1, 100, 0, "2026-01-01T00:00:00Z", 1); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp, build_id)
			 VALUES (1, 100, 0, '2026-01-01T00:00:05Z', 2) ON CONFLICT DO NOTHING`); err != nil {
			t.Fatalf("loader retry: %v", err)
		}
		var ts string
		if err := pool.QueryRow(ctx, `SELECT block_timestamp::text FROM block_meta WHERE chain_id = 1 AND block_number = 100 AND block_version = 0`).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		if rows, _ := timesAt(t, 1, 100); rows != 1 || !strings.HasPrefix(ts, "2026-01-01 00:00:00") {
			t.Errorf("after a retry: rows=%d ts=%s; want 1 row keeping the first header time", rows, ts)
		}
		err := insert(t, 1, 100, 0, "2026-01-01T00:00:05Z", 2)
		if err == nil || !strings.Contains(err.Error(), "block_meta_pkey") {
			t.Errorf("a plain insert of a second time for one block must hit block_meta_pkey, got %v", err)
		}
	})

	t.Run("a reorg block_version is a distinct block with its own header time", func(t *testing.T) {
		if err := insert(t, 1, 100, 1, "2026-01-01T00:00:12Z", 1); err != nil {
			t.Fatal(err)
		}
		if rows, times := timesAt(t, 1, 100); rows != 2 || times != 2 {
			t.Errorf("block 100: rows=%d distinct times=%d, want 2/2", rows, times)
		}
	})

	t.Run("two chains at the same height are distinct rows", func(t *testing.T) {
		if err := insert(t, 2, 100, 0, "2026-06-01T00:00:00Z", 1); err != nil {
			t.Fatal(err)
		}
		if rows, _ := timesAt(t, 2, 100); rows != 1 {
			t.Errorf("chain 2 block 100: rows=%d, want 1", rows)
		}
	})

	t.Run("corruption guards reject implausible timestamps and coordinates", func(t *testing.T) {
		for _, c := range []struct {
			name           string
			chain, bn, bv  int
			ts             string
			build          int
			wantConstraint string
		}{
			{"epoch-zero timestamp (unparsed field)", 1, 200, 0, "1970-01-01T00:00:00Z", 1, "block_meta_ts_sane_chk"},
			{"pre-genesis timestamp", 1, 201, 0, "2008-12-31T23:59:59Z", 1, "block_meta_ts_sane_chk"},
			{"base_fee parsed as the timestamp (year 2603)", 1, 202, 0, "2603-01-01T00:00:00Z", 1, "block_meta_ts_sane_chk"},
			{"negative block number (bad S3 key)", 1, -1, 0, "2026-01-01T00:00:00Z", 1, "block_meta_coord_nonneg_chk"},
			{"negative block version", 1, 203, -1, "2026-01-01T00:00:00Z", 1, "block_meta_coord_nonneg_chk"},
			{"negative build id", 1, 204, 0, "2026-01-01T00:00:00Z", -1, "block_meta_coord_nonneg_chk"},
			{"chain zero", 0, 205, 0, "2026-01-01T00:00:00Z", 1, "block_meta_chain_pos_chk"},
		} {
			err := insert(t, c.chain, c.bn, c.bv, c.ts, c.build)
			if err == nil || !strings.Contains(err.Error(), c.wantConstraint) {
				t.Errorf("%s: want %s violation, got %v", c.name, c.wantConstraint, err)
			}
		}
	})

	t.Run("guards admit legitimate historical blocks", func(t *testing.T) {
		for _, c := range []struct {
			name          string
			chain, bn, bv int
			ts            string
		}{
			{"genesis day floor", 1, 0, 0, "2009-01-03T00:00:00Z"},
			{"ethereum genesis", 1, 1, 0, "2015-07-30T15:26:28Z"},
			{"last admitted instant", 1, 2, 0, "2099-12-31T23:59:59Z"},
		} {
			if err := insert(t, c.chain, c.bn, c.bv, c.ts, 1); err != nil {
				t.Errorf("%s: %v", c.name, err)
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

	t.Run("a mis-parsed header is corrected by a higher processing_version, and the highest wins", func(t *testing.T) {
		// The record of a header time can be wrong even though the header itself cannot change, and
		// UPDATE and DELETE are revoked, so the correction axis is the only repair path there is.
		if err := insert(t, 1, 300, 0, "2026-01-01T00:00:00Z", 1); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO block_meta (chain_id, block_number, block_version, processing_version, block_timestamp, build_id)
			 VALUES (1, 300, 0, 1, '2026-01-01T00:00:07Z', 2)`); err != nil {
			t.Fatalf("appending the correction: %v", err)
		}
		var picked string
		if err := pool.QueryRow(ctx, `
			SELECT block_timestamp::text FROM block_meta
			 WHERE chain_id = 1 AND block_number = 300
			 ORDER BY block_version DESC, processing_version DESC LIMIT 1`).Scan(&picked); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(picked, "2026-01-01 00:00:07") {
			t.Errorf("the highest processing_version reads %s; want the corrected 00:00:07", picked)
		}
		if rows, times := timesAt(t, 1, 300); rows != 2 || times != 2 {
			t.Errorf("rows=%d distinct times=%d; want both rows kept, since nothing is rewritten", rows, times)
		}
		// Still no trigger and no current view: the pick is the reader's ORDER BY, not a maintained row.
		var triggers int
		var viewPresent bool
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'block_meta'::regclass AND NOT tgisinternal),
			       to_regclass('block_meta_current') IS NOT NULL`).Scan(&triggers, &viewPresent); err != nil {
			t.Fatal(err)
		}
		if triggers != 0 || viewPresent {
			t.Errorf("triggers=%d current view=%v; want neither", triggers, viewPresent)
		}
	})
}
