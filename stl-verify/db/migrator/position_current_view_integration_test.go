//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestPositionCurrentView is the VEC-409 contract test: after migrations,
// position_current returns exactly the latest observation per position_id from
// position_state (DISTINCT ON, newest block/version/processing_version), including a
// position whose latest observation is a closing quantity 0.
func TestPositionCurrentView(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Position A: two observations (block 100 qty 10, block 200 qty 20) -> current 20.
	// Position B: opened (block 100 qty 5) then closed (block 150 qty 0) -> current 0.
	if _, err := pool.Exec(ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp) VALUES
		    (sha256('a'::bytea), 1, 1, 'inst-a', 'holder-a', 10, 100, 0, 0, '2026-01-01T00:00:00Z'),
		    (sha256('a'::bytea), 1, 1, 'inst-a', 'holder-a', 20, 200, 0, 0, '2026-01-02T00:00:00Z'),
		    (sha256('b'::bytea), 1, 1, 'inst-b', 'holder-b',  5, 100, 0, 0, '2026-01-01T00:00:00Z'),
		    (sha256('b'::bytea), 1, 1, 'inst-b', 'holder-b',  0, 150, 0, 0, '2026-01-01T12:00:00Z')`); err != nil {
		t.Fatalf("seed position_state: %v", err)
	}

	// One row per position_id.
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_current`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 2 {
		t.Fatalf("position_current rows = %d, want 2 (one per position_id)", rows)
	}

	// A: latest observation is block 200, quantity 20.
	var qtyA, blockA int64
	if err := pool.QueryRow(ctx,
		`SELECT quantity, block_number FROM position_current WHERE position_id = sha256('a'::bytea)`).Scan(&qtyA, &blockA); err != nil {
		t.Fatalf("A lookup: %v", err)
	}
	if qtyA != 20 || blockA != 200 {
		t.Errorf("A current = (qty %d, block %d), want (20, 200)", qtyA, blockA)
	}

	// B: latest observation is the closing block 150, quantity 0.
	var qtyB, blockB int64
	if err := pool.QueryRow(ctx,
		`SELECT quantity, block_number FROM position_current WHERE position_id = sha256('b'::bytea)`).Scan(&qtyB, &blockB); err != nil {
		t.Fatalf("B lookup: %v", err)
	}
	if qtyB != 0 || blockB != 150 {
		t.Errorf("B current = (qty %d, block %d), want (0, 150) — the closing observation must win", qtyB, blockB)
	}
}
