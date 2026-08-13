//go:build integration

package main

import (
	"context"
	"log/slog"
	"slices"
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

	// The migration must register exactly the bucket-1 source set. This is the
	// concrete migration's own test, so a missing (or extra) registration is a real
	// regression that would leave a table unprocessed -- assert the exact set rather
	// than a lower bound. Update this list when a later bucket actually lands.
	want := []string{
		"fluid_vault_state",
		"maple_loan_collateral",
		"maple_loan_state",
		"maple_pool_state",
		"maple_sky_strategy_state",
		"maple_syrup_global_state",
		"morpho_market_position",
		"morpho_market_state",
		"morpho_vault_position",
		"morpho_vault_state",
		"offchain_token_price",
		"onchain_token_price",
		"token_total_supply",
	}
	rows, err := pool.Query(ctx, `SELECT source FROM transformed._sources ORDER BY source`)
	if err != nil {
		t.Fatalf("listing transformed._sources: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scanning transformed._sources: %v", err)
		}
		got = append(got, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating transformed._sources: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("transformed._sources = %v, want exactly %v", got, want)
	}
}

// TestTransformWorker_QueueCapturesBackfill is the regression guard for the
// VEC-484 silent-data-hole. Refresh is queue-driven: an AFTER INSERT trigger on
// each raw table enqueues the new row's PK into transformed._pending_<t>, and the
// worker's _run_<t>() drains it. Because enqueue is driven by the insert itself,
// a backfill or reorg row written with a LOWER block_number or an out-of-order
// build_id is enqueued like any other insert, so it cannot be skipped by an
// ordering cursor.
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

// TestTransformWorker_CorrectionReEnqueue verifies a reprocessing correction --
// the same natural key rewritten under a new build, so the processing_version
// trigger bumps the version -- lands as a distinct transformed row via the enqueue
// trigger (not overwriting the original).
func TestTransformWorker_CorrectionReEnqueue(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()
	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	marketID := seedMorphoMarket(ctx, t, pool)
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Original write (processing_version 0).
	seedMorphoMarketState(ctx, t, pool, marketID, morphoStateRow{blockNumber: 1000, buildID: 100, timestamp: ts})
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if got := countTransformedMarketState(ctx, t, pool); got != 1 {
		t.Fatalf("after original: count = %d, want 1", got)
	}

	// Correction: same (market, block, block_version, timestamp), new build, so the
	// processing_version trigger assigns version 1 -> a new PK -> re-enqueued.
	seedMorphoMarketState(ctx, t, pool, marketID, morphoStateRow{blockNumber: 1000, buildID: 101, timestamp: ts})
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("correction run: %v", err)
	}
	if got := countTransformedMarketState(ctx, t, pool); got != 2 {
		t.Fatalf("after correction: count = %d, want 2 (original + bumped processing_version)", got)
	}

	var maxPV int
	if err := pool.QueryRow(ctx,
		`SELECT max(processing_version) FROM transformed."morpho_market_state"`).Scan(&maxPV); err != nil {
		t.Fatalf("reading processing_version: %v", err)
	}
	if maxPV != 1 {
		t.Fatalf("max processing_version = %d, want 1 (the correction landed)", maxPV)
	}
}

// TestTransformWorker_BootstrapIdempotent verifies transformed._bootstrap_<t>()
// copies pre-existing history (written while the enqueue trigger was absent) and
// that re-running it is a guarded no-op.
func TestTransformWorker_BootstrapIdempotent(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()
	marketID := seedMorphoMarket(ctx, t, pool)

	// Simulate history the enqueue trigger never captured (pre-trigger rows): insert
	// the raw row (which enqueues it), then drop the queue entry so only bootstrap
	// can materialise it. DISABLE TRIGGER is unavailable on a compressed hypertable,
	// so clearing the queue is the portable way to reach the same state.
	seedMorphoMarketState(ctx, t, pool, marketID, morphoStateRow{
		blockNumber: 500, buildID: 40, timestamp: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	})
	if _, err := pool.Exec(ctx, `DELETE FROM transformed."_pending_morpho_market_state"`); err != nil {
		t.Fatalf("clearing queue to simulate pre-trigger history: %v", err)
	}

	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 0, 1)

	var inserted int64
	if err := pool.QueryRow(ctx,
		`SELECT transformed._bootstrap_morpho_market_state($1, $2)`, from, to).Scan(&inserted); err != nil {
		t.Fatalf("bootstrap run 1: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("bootstrap run 1 inserted = %d, want 1", inserted)
	}

	var rerun int64
	if err := pool.QueryRow(ctx,
		`SELECT transformed._bootstrap_morpho_market_state($1, $2)`, from, to).Scan(&rerun); err != nil {
		t.Fatalf("bootstrap run 2: %v", err)
	}
	if rerun != 0 {
		t.Fatalf("bootstrap run 2 (idempotent) affected = %d, want 0", rerun)
	}
	if got := countTransformedMarketState(ctx, t, pool); got != 1 {
		t.Fatalf("after bootstrap: count = %d, want 1", got)
	}
}

// TestTransformWorker_MultiIterationDrain seeds more than one drain batch (>10k
// rows), so RunTable must loop: the bounded _run consumes drainBatch rows, then
// the remainder, until the queue is empty and every row is materialized.
func TestTransformWorker_MultiIterationDrain(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()
	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	marketID := seedMorphoMarket(ctx, t, pool)
	const n = 10_001 // one past the 10k drain batch, so the drain needs two iterations
	if _, err := pool.Exec(ctx, `
		INSERT INTO public."morpho_market_state"
		    (morpho_market_id, block_number, block_version, timestamp,
		     total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
		     last_update, fee, build_id)
		SELECT $1, gs, 0, timestamptz '2026-01-01 00:00:00+00' + (gs * interval '1 second'),
		       0, 0, 0, 0, 0, 0, 100
		FROM generate_series(1, $2) gs`, marketID, n); err != nil {
		t.Fatalf("bulk seeding %d rows: %v", n, err)
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := countTransformedMarketState(ctx, t, pool); got != n {
		t.Fatalf("after multi-iteration drain: count = %d, want %d", got, n)
	}
}
