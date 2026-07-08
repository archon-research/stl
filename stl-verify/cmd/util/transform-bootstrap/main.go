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
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

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

	if err := prepareSession(ctx, pool, logger); err != nil {
		return err
	}

	repo := postgres.NewTransformRunnerRepository(pool, logger)
	sources, err := selectSources(ctx, repo, *only)
	if err != nil {
		return err
	}

	end := time.Now().UTC().Add(*step) // one step past now so the live tail is included
	for _, source := range sources {
		total, err := bootstrapSource(ctx, repo, source, from, end, *step, logger)
		if err != nil {
			return fmt.Errorf("bootstrapping %q: %w", source, err)
		}
		logger.Info("bootstrap source complete", "source", source, "rows", total)
	}
	logger.Info("bootstrap complete", "sources", len(sources))
	return nil
}

// bootstrapSource walks [from, end) in step-sized windows, copying each window in
// its own transaction so WAL and replication lag stay bounded on large tables.
func bootstrapSource(ctx context.Context, repo *postgres.TransformRunnerRepository, source string, from, end time.Time, step time.Duration, logger *slog.Logger) (int64, error) {
	var total int64
	for w := from; w.Before(end); w = w.Add(step) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		rows, err := repo.BootstrapTable(ctx, source, w, w.Add(step))
		if err != nil {
			return total, err
		}
		if rows > 0 {
			logger.Info("bootstrap window", "source", source, "from", w, "to", w.Add(step), "rows", rows)
		}
		total += rows
	}
	return total, nil
}

// prepareSession includes S3-tiered history and lifts the statement timeout for
// this bootstrap connection. enable_tiered_reads is best-effort: it is absent on
// environments without tiering, which is not an error for the bootstrap.
func prepareSession(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	if _, err := pool.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		return fmt.Errorf("disabling statement timeout: %w", err)
	}
	if _, err := pool.Exec(ctx, "SET timescaledb.enable_tiered_reads = on"); err != nil {
		logger.Warn("could not enable tiered reads (tiering may be unavailable); bootstrapping untiered data only", "error", err)
	}
	return nil
}

func selectSources(ctx context.Context, repo *postgres.TransformRunnerRepository, only string) ([]string, error) {
	all, err := repo.ListSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing sources: %w", err)
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
	dsn := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
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
