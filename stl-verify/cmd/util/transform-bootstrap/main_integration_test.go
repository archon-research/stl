//go:build integration

package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestRunBootstrap_CopiesHistoryAndSeedsParity exercises the correctness-critical
// bootstrap binary end to end: it seeds a raw row, clears the change queue so only
// the bootstrap (not the worker) can materialise it, runs runBootstrap, and asserts
// the transformed table is populated and the parity ledger seeded (drift 0). This
// covers the binary's window loop, single-connection session setup, the
// _bootstrap_<t>() copy, and the _parity_verify_all ledger seed.
func TestRunBootstrap_CopiesHistoryAndSeedsParity(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()
	marketID := seedMorphoMarket(ctx, t, pool)
	seedMorphoMarketState(ctx, t, pool, marketID, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))

	// Simulate pre-trigger history: the insert enqueued the row; drop the queue
	// entry so only the bootstrap can materialise it.
	if _, err := pool.Exec(ctx, `DELETE FROM transformed."_pending_morpho_market_state"`); err != nil {
		t.Fatalf("clearing queue: %v", err)
	}
	if got := countTransformed(ctx, t, pool); got != 0 {
		t.Fatalf("before bootstrap: transformed count = %d, want 0", got)
	}

	// One window covering the seeded row, scoped to this one source.
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if err := runBootstrap(ctx, pool, from, 365*24*time.Hour, "morpho_market_state", slog.Default()); err != nil {
		t.Fatalf("runBootstrap: %v", err)
	}

	if got := countTransformed(ctx, t, pool); got != 1 {
		t.Fatalf("after bootstrap: transformed count = %d, want 1", got)
	}

	// _parity_verify_all seeded the ledger; drift must be 0 (raw == transformed, no pending).
	var drift int64
	if err := pool.QueryRow(ctx,
		`SELECT drift FROM transformed._parity_status WHERE source = 'morpho_market_state'`).Scan(&drift); err != nil {
		t.Fatalf("parity ledger not seeded by bootstrap: %v", err)
	}
	if drift != 0 {
		t.Fatalf("parity drift = %d, want 0 after bootstrap", drift)
	}
}

func countTransformed(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transformed."morpho_market_state"`).Scan(&n); err != nil {
		t.Fatalf("counting transformed.morpho_market_state: %v", err)
	}
	return n
}

// seedMorphoMarket inserts the FK parents (protocol + loan/collateral tokens) and
// one morpho_market row on the pre-seeded Ethereum chain (chain_id 1), returning
// the generated market id.
func seedMorphoMarket(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()

	var protocolID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		 VALUES (1, '\x00000000000000000000000000000000000000a1'::bytea, 'Morpho Blue', 'lending', 1)
		 RETURNING id`,
	).Scan(&protocolID); err != nil {
		t.Fatalf("seeding protocol: %v", err)
	}

	var loanTokenID, collateralTokenID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, '\x00000000000000000000000000000000000000b1'::bytea, 'LOAN', 18) RETURNING id`,
	).Scan(&loanTokenID); err != nil {
		t.Fatalf("seeding loan token: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, '\x00000000000000000000000000000000000000c1'::bytea, 'COLL', 18) RETURNING id`,
	).Scan(&collateralTokenID); err != nil {
		t.Fatalf("seeding collateral token: %v", err)
	}

	var marketID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO morpho_market
		     (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
		      oracle_address, irm_address, lltv, created_at_block)
		 VALUES (1, $1, '\x00000000000000000000000000000000000000000000000000000000000000d1'::bytea,
		         $2, $3,
		         '\x00000000000000000000000000000000000000e1'::bytea,
		         '\x00000000000000000000000000000000000000f1'::bytea,
		         860000000000000000, 1)
		 RETURNING id`,
		protocolID, loanTokenID, collateralTokenID,
	).Scan(&marketID); err != nil {
		t.Fatalf("seeding morpho_market: %v", err)
	}
	return marketID
}

// seedMorphoMarketState inserts one raw morpho_market_state row at ts (block 1000,
// build 40). processing_version is assigned by the table's BEFORE INSERT trigger.
func seedMorphoMarketState(ctx context.Context, t *testing.T, pool *pgxpool.Pool, marketID int64, ts time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO morpho_market_state
		     (morpho_market_id, block_number, block_version, timestamp,
		      total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
		      last_update, fee, build_id)
		 VALUES ($1, 1000, 0, $2, 0, 0, 0, 0, 0, 0, 40)`,
		marketID, ts,
	); err != nil {
		t.Fatalf("seeding morpho_market_state: %v", err)
	}
}
