package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	db, err := sql.Open("pgx",
		"postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
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
