//go:build integration

package block_time_backfill

import (
	"context"
	"log/slog"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestRun_PopulatesFromBothSourcesIdempotently exercises the block_time backfill end
// to end: block_states is authoritative and applied first; onchain_token_price fills a
// deeper block it doesn't cover; an orphaned block_states row is skipped; and re-running
// inserts nothing (idempotent).
func TestRun_PopulatesFromBothSourcesIdempotently(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()
	ctx := context.Background()

	// Seed:
	//  - block_states 100 (canonical, ts 2026-06-01) -> block_time
	//  - block_states 101 (ORPHANED) -> must be skipped
	//  - onchain_token_price @100 (ts 2020, a wrong value) -> block_states must win on conflict
	//  - onchain_token_price @50  (ts 2026-05-01) -> a deeper block block_states lacks
	seed := `
DO $$
DECLARE tid bigint;
BEGIN
  INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING;
  INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, '\xaa', 'USDC', 6) RETURNING id INTO tid;
  INSERT INTO block_states (number, hash, parent_hash, received_at, is_orphaned, version, block_published, chain_id, created_at) VALUES
    (100, 'h100', 'p100', 1700000000, false, 0, true, 1, '2026-06-01T00:00:00Z'),
    (101, 'h101', 'p101', 1700000001, true,  0, true, 1, '2026-06-01T00:01:00Z');
  INSERT INTO onchain_token_price (token_id, oracle_id, block_number, block_version, "timestamp", price_usd, processing_version, build_id) VALUES
    (tid, 1, 100, 0, '2020-01-01T00:00:00Z', 1.0, 0, 0),
    (tid, 1, 50,  0, '2026-05-01T00:00:00Z', 1.0, 0, 0);
END $$;`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := Run(ctx, pool, slog.Default()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Exactly two rows: block 100 (from block_states) and block 50 (from onchain_token_price).
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_time`).Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != 2 {
		t.Fatalf("block_time rows = %d, want 2", total)
	}

	// block_states is authoritative: block 100 keeps the 2026 block_states timestamp,
	// NOT the wrong 2020 value from onchain_token_price (block_states ran first, later
	// sources DO NOTHING on conflict).
	var year100 int
	if err := pool.QueryRow(ctx,
		`SELECT extract(year FROM block_timestamp)::int FROM block_time WHERE chain_id=1 AND block_number=100`).Scan(&year100); err != nil {
		t.Fatalf("block 100 lookup: %v", err)
	}
	if year100 != 2026 {
		t.Errorf("block 100 timestamp year = %d, want 2026 (block_states must win over onchain_token_price)", year100)
	}

	// Deeper block 50 came from onchain_token_price (block_states had no such block).
	var has50 bool
	if err := pool.QueryRow(ctx, `SELECT exists(SELECT 1 FROM block_time WHERE chain_id=1 AND block_number=50)`).Scan(&has50); err != nil {
		t.Fatalf("block 50 lookup: %v", err)
	}
	if !has50 {
		t.Error("block 50 missing; onchain_token_price source did not contribute it")
	}

	// The orphaned block_states row is excluded.
	var has101 bool
	if err := pool.QueryRow(ctx, `SELECT exists(SELECT 1 FROM block_time WHERE chain_id=1 AND block_number=101)`).Scan(&has101); err != nil {
		t.Fatalf("block 101 lookup: %v", err)
	}
	if has101 {
		t.Error("orphaned block 101 was inserted; WHERE NOT is_orphaned did not exclude it")
	}

	// Idempotent: a second run inserts nothing.
	if err := Run(ctx, pool, slog.Default()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	var total2 int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_time`).Scan(&total2); err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if total2 != 2 {
		t.Errorf("after re-run: block_time rows = %d, want 2 (idempotent)", total2)
	}
}
