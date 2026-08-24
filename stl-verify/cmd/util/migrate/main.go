package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}

	log.Println("✓ All migrations up to date")
}

func run() error {
	connStr := requireEnv("DATABASE_URL")
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

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("required environment variable not set: %s", key)
	}
	return value
}
