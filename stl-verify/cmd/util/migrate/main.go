package main

import (
	"context"
	"fmt"
	"log"

	"github.com/archon-research/stl/stl-verify/db/migrator"
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

	pool, err := pgxpool.New(ctx, connStr)
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
