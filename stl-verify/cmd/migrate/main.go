package main

import (
	"context"
	"log"
	"os"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	connStr := requireEnv("DATABASE_URL")
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	m := migrator.New(pool, "./db/migrations")
	if err := m.ApplyAll(ctx); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	log.Println("✓ All migrations up to date")
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("required environment variable not set: %s", key)
	}
	return value
}
