// Package main is a one-off job that backfills the transformation layer with
// pre-existing raw history. Steady-state refresh is queue-driven (an AFTER INSERT
// trigger on each raw table enqueues new rows, and the transform-worker drains
// the queues), but that only covers rows written after the trigger exists. This
// job copies everything older, by walking each source's observation-time column
// in windows and calling transformed._bootstrap_<source>(from, to), which upserts
// ON CONFLICT DO UPDATE guarded by IS DISTINCT FROM.
//
// Run it once after the migration is applied, out of band from the worker's
// 10-minute Temporal activity. It enables tiered reads and disables the statement
// timeout for its own session so S3-tiered history is included and large windows
// are not cut off. The enqueue triggers are already live, so any rows written
// while this runs are captured by the queue; the guarded upsert makes the overlap
// (and re-running the whole job) idempotent.
//
// Usage:
//
//	transform-bootstrap [-from 2025-01-01] [-step 720h] [-source morpho_market_state]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("transform-bootstrap failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	fromStr := flag.String("from", "2025-01-01", "start of the backfill window (RFC3339 or YYYY-MM-DD)")
	step := flag.Duration("step", 30*24*time.Hour, "window size per _bootstrap call")
	only := flag.String("source", "", "restrict to a single source (default: all)")
	flag.Parse()

	// A non-positive step never advances the per-window loop in bootstrapSource,
	// so guard it here rather than spin forever.
	if *step <= 0 {
		return fmt.Errorf("step must be positive, got %v", *step)
	}

	from, err := parseTime(*fromStr)
	if err != nil {
		return fmt.Errorf("parsing -from: %w", err)
	}

	logger := slog.Default()
	pool, err := openPool(ctx)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer pool.Close()

	return runBootstrap(ctx, pool, from, *step, *only, logger)
}

// runBootstrap copies each source's pre-existing history into the transformed
// layer over step-sized windows and seeds the parity ledger, all on ONE acquired
// connection so the session GUCs (statement_timeout, tiered reads) apply to every
// query. Setting them via pool.Exec would land each SET on an arbitrary pooled
// connection and leave the window queries on others with defaults -- and
// enable_tiered_reads defaults off, so those queries would silently skip S3-tiered
// history (parity cannot catch it: both sides undercount alike). Split out from
// run so tests can drive it with a pool and explicit params (no flag parsing).
func runBootstrap(ctx context.Context, pool *pgxpool.Pool, from time.Time, step time.Duration, only string, logger *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	if err := prepareConn(ctx, conn, logger); err != nil {
		return err
	}

	sources, err := selectSources(ctx, conn, only)
	if err != nil {
		return err
	}

	end := time.Now().UTC().Add(step) // one step past now so the live tail is included
	for _, source := range sources {
		total, err := bootstrapSource(ctx, conn, source, from, end, step, logger)
		if err != nil {
			return fmt.Errorf("bootstrapping %q: %w", source, err)
		}
		// Seed + freeze the parity ledger for this source on the same tiered-reads-on
		// connection, so historical (tiered) buckets are verified once here and the
		// per-tick worker refresh never has to re-read S3.
		fn := pgx.Identifier{"transformed", "_parity_verify_all"}.Sanitize()
		if _, err := conn.Exec(ctx, "SELECT "+fn+"($1)", source); err != nil {
			return fmt.Errorf("verifying parity for %q: %w", source, err)
		}
		logger.Info("bootstrap source complete", "source", source, "rows", total)
	}
	logger.Info("bootstrap complete", "sources", len(sources))
	return nil
}

// bootstrapSource walks [from, end) in step-sized windows, copying each window in
// its own autocommit statement so WAL and replication lag stay bounded on large
// tables. source comes from transformed._sources (our controlled table); it is
// quoted as an identifier regardless (same reasoning as the adapter's RunTable).
func bootstrapSource(ctx context.Context, conn *pgxpool.Conn, source string, from, end time.Time, step time.Duration, logger *slog.Logger) (int64, error) {
	fn := pgx.Identifier{"transformed", "_bootstrap_" + source}.Sanitize()
	var total int64
	for w := from; w.Before(end); w = w.Add(step) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		var rows int64
		if err := conn.QueryRow(ctx, "SELECT "+fn+"($1, $2)", w, w.Add(step)).Scan(&rows); err != nil {
			return total, fmt.Errorf("bootstrapping %q [%s, %s): %w", source, w, w.Add(step), err)
		}
		if rows > 0 {
			logger.Info("bootstrap window", "source", source, "from", w, "to", w.Add(step), "rows", rows)
		}
		total += rows
	}
	return total, nil
}

// prepareConn includes S3-tiered history and lifts the statement timeout on the
// bootstrap's single connection. statement_timeout must be lifted (full-history
// windows can exceed any default), so a failure is fatal.
//
// enable_tiered_reads is fatal too, with ONE exception: when the GUC is unrecognized
// (SQLSTATE 42704), this environment has no tiering (e.g. OSS TimescaleDB without
// OSM), so there is no tiered history to miss and we proceed on local data. Any
// other failure (a permission or configuration error where tiering DOES exist) is
// fatal: proceeding would silently bootstrap only local history, and parity cannot
// catch the gap because raw and transformed both undercount by the tiered rows.
func prepareConn(ctx context.Context, conn *pgxpool.Conn, logger *slog.Logger) error {
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		return fmt.Errorf("disabling statement timeout: %w", err)
	}
	if _, err := conn.Exec(ctx, "SET timescaledb.enable_tiered_reads = on"); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42704" { // undefined_object: GUC not present
			logger.Warn("tiered reads GUC unavailable; environment has no tiering, bootstrapping local data only", "error", err)
			return nil
		}
		return fmt.Errorf("enabling tiered reads (tiering is available but could not be enabled; would silently skip tiered history): %w", err)
	}
	return nil
}

func selectSources(ctx context.Context, conn *pgxpool.Conn, only string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT source FROM transformed._sources ORDER BY source`)
	if err != nil {
		return nil, fmt.Errorf("listing sources: %w", err)
	}
	defer rows.Close()
	var all []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("scanning source: %w", err)
		}
		all = append(all, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating sources: %w", err)
	}
	if only == "" {
		return all, nil
	}
	if slices.Contains(all, only) {
		return []string{only}, nil
	}
	return nil, fmt.Errorf("source %q not found in transformed._sources", only)
}

func openPool(ctx context.Context) (*pgxpool.Pool, error) {
	// Require DATABASE_URL: a one-off backfill that silently ran against a local
	// (empty) database would do nothing and report success.
	dsn, err := env.Require("DATABASE_URL")
	if err != nil {
		return nil, err
	}
	return postgres.PoolOpener(postgres.DefaultDBConfig(dsn))(ctx)
}

func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("want RFC3339 or YYYY-MM-DD, got %q", s)
	}
	return t.UTC(), nil
}
