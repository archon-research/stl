package testutil

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SkipWithoutTimescaleDB skips the test when TimescaleDB is not installed.
func SkipWithoutTimescaleDB(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var has bool
	if err := pool.QueryRow(context.Background(),
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')").Scan(&has); err != nil {
		t.Fatalf("check timescaledb: %v", err)
	}
	if !has {
		t.Skip("test requires TimescaleDB extension")
	}
}
