// Package main populates the durable block_time dimension (VEC-491) by running the
// block_time_backfill service. It is the code form of docs/runbooks/block-time-backfill.md:
// run out of band with a WRITE role against staging/prod (not the read-only pooler),
// once for the historical backfill and re-runnable/schedulable as a top-up (idempotent).
//
// Usage:
//
//	DATABASE_URL=postgres://... block-time-backfill
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/block_time_backfill"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		slog.Error("block-time-backfill failed", "error", err)
		os.Exit(1)
	}
}

// run is the testable command boundary. The tool is env-driven (DATABASE_URL) and
// takes no positional arguments, so any are rejected rather than silently ignored.
func run(ctx context.Context, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("block-time-backfill takes no arguments; got %v", args)
	}
	logger := slog.Default()
	pool, err := openPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := block_time_backfill.Run(ctx, pool, logger); err != nil {
		return fmt.Errorf("running block_time backfill: %w", err)
	}
	return nil
}

func openPool(ctx context.Context) (*pgxpool.Pool, error) {
	// Require DATABASE_URL: a backfill that silently ran against a local (empty)
	// database would do nothing and report success.
	dsn, err := env.Require("DATABASE_URL")
	if err != nil {
		return nil, fmt.Errorf("requiring DATABASE_URL: %w", err)
	}
	pool, err := postgres.PoolOpener(postgres.DefaultDBConfig(dsn))(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening PostgreSQL pool: %w", err)
	}
	return pool, nil
}
