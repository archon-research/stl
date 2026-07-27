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
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/block_time_backfill"
)

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("block-time-backfill failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logger := slog.Default()
	pool, err := openPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	return block_time_backfill.Run(ctx, pool, logger)
}

func openPool(ctx context.Context) (*pgxpool.Pool, error) {
	// Require DATABASE_URL: a backfill that silently ran against a local (empty)
	// database would do nothing and report success.
	dsn, err := env.Require("DATABASE_URL")
	if err != nil {
		return nil, err
	}
	return postgres.PoolOpener(postgres.DefaultDBConfig(dsn))(ctx)
}
