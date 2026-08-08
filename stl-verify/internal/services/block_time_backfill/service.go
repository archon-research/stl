// Package block_time_backfill populates the durable block_time dimension
// (chain_id, block_number -> on-chain block_timestamp, VEC-491) that the no-native-
// timestamp transform/position sources (borrower, borrower_collateral,
// allocation_position, protocol_event, sparklend_reserve_data) join to recover
// event-time. The migration (20260722_140000) creates the table empty; this service
// is the code form of docs/runbooks/block-time-backfill.md, run out of band (the
// migrator's single transaction can't hold a multi-million-row INSERT...SELECT).
//
// It is idempotent (every source is INSERT ... ON CONFLICT DO NOTHING), so running it
// once does the historical backfill and re-running (or scheduling it) tops up new
// blocks. It is Temporal-free (just a *pgxpool.Pool); cmd/backfillers/block-time-backfill
// wires it to DATABASE_URL.
//
// Coverage (verified against prod 2026-07-27): block_states + onchain_token_price
// together reach ETH block ~23.0M and cover Avalanche fully — ~100% of the row mass
// the consumers reference. The residual deep tail (below ~23.0M) needs a node/Alchemy
// fetch (external access) and is out of scope here; see the runbook Step 3.2.
package block_time_backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// source is one idempotent contributor to block_time.
type source struct {
	name string
	sql  string
}

// sources are applied in order. block_states is authoritative for on-chain time and
// runs FIRST, so the ON CONFLICT DO NOTHING on every later source means a deeper
// source never overrides a block_states timestamp — it only fills blocks below the
// block_states retention window.
func sources() []source {
	return []source{
		{
			// Step 1: block_states.created_at is the true on-chain block timestamp
			// (0s delta vs the natively-timestamped indexers; received_at is node
			// receipt time). Every canonical block in the retention window is present.
			name: "block_states",
			sql: `INSERT INTO block_time (chain_id, block_number, block_timestamp)
			      SELECT chain_id, number, created_at
			      FROM public.block_states
			      WHERE NOT is_orphaned
			      ON CONFLICT (chain_id, block_number) DO NOTHING`,
		},
		{
			// Step 3.1: the deepest high-row-mass native-timestamp source. Its raw
			// "timestamp" is the on-chain block time (the transform renames it to
			// block_timestamp); chain is attributed via token (the raw table carries no
			// chain_id). GROUP BY collapses the many-tokens-per-block rows to one block.
			name: "onchain_token_price",
			sql: `INSERT INTO block_time (chain_id, block_number, block_timestamp)
			      SELECT t.chain_id, otp.block_number, min(otp."timestamp")
			      FROM public.onchain_token_price otp
			      JOIN public.token t ON t.id = otp.token_id
			      WHERE otp."timestamp" IS NOT NULL
			      GROUP BY t.chain_id, otp.block_number
			      ON CONFLICT (chain_id, block_number) DO NOTHING`,
		},
	}
}

// Run applies every source on ONE acquired connection so the session GUCs
// (statement_timeout, tiered reads) hold for each large scan, then logs the total.
func Run(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	if err := prepareConn(ctx, conn, logger); err != nil {
		return err
	}

	for _, s := range sources() {
		tag, err := conn.Exec(ctx, s.sql)
		if err != nil {
			return fmt.Errorf("backfilling block_time from %q: %w", s.name, err)
		}
		logger.Info("block_time source applied", "source", s.name, "inserted", tag.RowsAffected())
	}

	var total int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM block_time").Scan(&total); err != nil {
		return fmt.Errorf("counting block_time: %w", err)
	}
	logger.Info("block_time backfill complete", "total_rows", total)
	return nil
}

// prepareConn lifts the statement timeout (full-history scans exceed any default) and
// includes S3-tiered history on the backfill's single connection. statement_timeout is
// fatal on failure. enable_tiered_reads is fatal too, except when the GUC is unknown
// (SQLSTATE 42704) — that environment has no tiering, so there is nothing to miss.
// Mirrors the transform-bootstrap service's connection setup and rationale.
func prepareConn(ctx context.Context, conn *pgxpool.Conn, logger *slog.Logger) error {
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		return fmt.Errorf("disabling statement timeout: %w", err)
	}
	if _, err := conn.Exec(ctx, "SET timescaledb.enable_tiered_reads = on"); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42704" { // undefined_object: GUC not present
			logger.Warn("tiered reads GUC unavailable; environment has no tiering, backfilling local data only", "error", err)
			return nil
		}
		return fmt.Errorf("enabling tiered reads (tiering is available but could not be enabled; would silently skip tiered history): %w", err)
	}
	return nil
}
