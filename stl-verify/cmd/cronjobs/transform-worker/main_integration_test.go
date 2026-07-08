//go:build integration

package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestTransformWorker_RunOnce migrates a fresh DB (which creates the transformed
// schema, its _sources rows, the change queues, the enqueue triggers, the _run
// functions, and the grants), then wires the worker exactly as main() does via
// setupRunner and runs it end to end. It verifies the wiring: the worker lists
// sources from transformed._sources and invokes every _run_<t>() through the real
// adapter without error, and a second run is still clean. On freshly-migrated
// tables the raw sources are empty, so the runs upsert 0 rows; the queue-drain
// idempotency is proven at the SQL-function level (VEC-484) and the RunOnce
// control flow by service_test.go.
func TestTransformWorker_RunOnce(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("second run: %v", err)
	}

	// The migration seeds one _sources row per transformed table. Assert a lower
	// bound plus a known source rather than an exact count, so adding tables in a
	// later bucket does not break this test.
	var sources int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transformed._sources`).Scan(&sources); err != nil {
		t.Fatalf("counting _sources rows: %v", err)
	}
	if sources < 1 {
		t.Fatalf("transformed._sources rows = %d, want at least 1 (migration not applied?)", sources)
	}

	var present bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM transformed._sources WHERE source = 'morpho_market_state')`,
	).Scan(&present); err != nil {
		t.Fatalf("checking known source: %v", err)
	}
	if !present {
		t.Fatal("expected transformed._sources to contain source 'morpho_market_state'")
	}
}

// TestTransformWorker_QueueCapturesBackfill is the regression guard for the
// VEC-484 silent-data-hole. Refresh is queue-driven: an AFTER INSERT trigger on
// each raw table enqueues the new row's PK into transformed._pending_<t>, and the
// worker's _run_<t>() drains it. This is immune to the build_id-cursor bug (PR
// #545 review §1.1): a backfill or reorg row written with a LOWER block_number or
// an out-of-order build_id is enqueued like any other insert, so it cannot be
// skipped.
//
// The test seeds one live row (build 100, block 2000), runs the worker, then
// seeds a backfill row (build 101, block 1000 — lower block) and runs again. Both
// must land in transformed.morpho_market_state (count 2). A block_number or
// build_id ">=" watermark would have dropped the backfill row (count 1).
func TestTransformWorker_QueueCapturesBackfill(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	marketID := seedMorphoMarket(ctx, t, pool)

	// Live pipeline row: build 100 at block 2000.
	seedMorphoMarketState(ctx, t, pool, marketID, morphoStateRow{
		blockNumber: 2000,
		buildID:     100,
		timestamp:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if got := countTransformedMarketState(ctx, t, pool); got != 1 {
		t.Fatalf("after first run: transformed.morpho_market_state count = %d, want 1", got)
	}

	// Backfill row: LOWER block (1000), out-of-order build (101). The AFTER INSERT
	// trigger enqueues it like any other insert, so the queue drain must pick it up.
	seedMorphoMarketState(ctx, t, pool, marketID, morphoStateRow{
		blockNumber: 1000,
		buildID:     101,
		timestamp:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if got := countTransformedMarketState(ctx, t, pool); got != 2 {
		t.Fatalf("after backfill run: transformed.morpho_market_state count = %d, want 2 "+
			"(the backfilled lower-block row was dropped — a watermark cursor regressed the queue)", got)
	}

	// Parity backstop: with both rows drained, raw == transformed + pending, so
	// drift must be 0. A nonzero drift here would mean the view or the queue
	// invariant is broken.
	var drift int64
	if err := pool.QueryRow(ctx,
		`SELECT drift FROM transformed._parity_status WHERE source = 'morpho_market_state'`,
	).Scan(&drift); err != nil {
		t.Fatalf("reading parity status: %v", err)
	}
	if drift != 0 {
		t.Fatalf("transformed._parity_status drift = %d, want 0 (raw != transformed + pending)", drift)
	}
}

// morphoStateRow is the minimal set of raw fields the test varies per row; the
// remaining NOT NULL columns are filled with fixed valid values by the seeder.
type morphoStateRow struct {
	blockNumber int64
	buildID     int
	timestamp   time.Time
}

// seedMorphoMarket inserts the FK parents (protocol + loan/collateral tokens)
// and one morpho_market row on the pre-seeded Ethereum chain (chain_id 1), and
// returns the generated market id. Addresses are arbitrary but distinct.
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
		 VALUES (1, '\x00000000000000000000000000000000000000b1'::bytea, 'LOAN', 18)
		 RETURNING id`,
	).Scan(&loanTokenID); err != nil {
		t.Fatalf("seeding loan token: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, '\x00000000000000000000000000000000000000c1'::bytea, 'COLL', 18)
		 RETURNING id`,
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

// seedMorphoMarketState inserts one raw morpho_market_state row. processing_version
// is assigned by the table's BEFORE INSERT trigger, so it is not set here; every
// other NOT NULL column is given a fixed valid value.
func seedMorphoMarketState(ctx context.Context, t *testing.T, pool *pgxpool.Pool, marketID int64, row morphoStateRow) {
	t.Helper()

	if _, err := pool.Exec(ctx,
		`INSERT INTO morpho_market_state
		     (morpho_market_id, block_number, block_version, timestamp,
		      total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
		      last_update, fee, build_id)
		 VALUES ($1, $2, 0, $3, 0, 0, 0, 0, 0, 0, $4)`,
		marketID, row.blockNumber, row.timestamp, row.buildID,
	); err != nil {
		t.Fatalf("seeding morpho_market_state (block %d, build %d): %v", row.blockNumber, row.buildID, err)
	}
}

// countTransformedMarketState returns the number of rows in the transformed
// hypertable for the market state source.
func countTransformedMarketState(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transformed."morpho_market_state"`).Scan(&n); err != nil {
		t.Fatalf("counting transformed.morpho_market_state: %v", err)
	}
	return n
}
