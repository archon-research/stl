package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	connStr := requireEnv("DATABASE_URL")

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	m := migrator.New(db, "./db/migrations")
	if err := m.ApplyAll(context.Background()); err != nil {
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
