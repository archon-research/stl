package main

import (
	"context"
	"fmt"
	"log"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}

	log.Println("✓ All migrations up to date")
}

func run() error {
	connStr, err := env.Require("DATABASE_URL")
	if err != nil {
		return err
	}
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("parsing DATABASE_URL: %w", err)
	}
	// pgx discards NOTICEs unless a handler is attached, and a migration that rewrites
	// tables for tens of minutes is otherwise silent until it commits or dies.
	cfg.ConnConfig.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		log.Printf("%s: %s", n.Severity, n.Message)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	m := migrator.New(pool, "./db/migrations")
	if err := m.ApplyAll(ctx); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	return nil
}
