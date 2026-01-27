// Package postgres provides PostgreSQL adapters for the STL verification system.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DBConfig holds configuration for the PostgreSQL connection pool.
type DBConfig struct {
	// URL is the PostgreSQL connection string.
	// Example: "postgres://user:pass@localhost:5432/dbname?sslmode=disable"
	URL string

	// MaxConns is the maximum number of connections in the pool.
	// Default: 25
	MaxConns int32

	// MinConns is the minimum number of connections in the pool.
	// Default: 5
	MinConns int32

	// MaxConnLifetime is the maximum amount of time a connection may be reused.
	// Default: 5 minutes
	MaxConnLifetime time.Duration

	// MaxConnIdleTime is the maximum amount of time a connection may be idle.
	// Default: 1 minute
	MaxConnIdleTime time.Duration
}

// DefaultDBConfig returns a DBConfig with sensible defaults.
func DefaultDBConfig(url string) DBConfig {
	return DBConfig{
		URL:             url,
		MaxConns:        25,
		MinConns:        5,
		MaxConnLifetime: 5 * time.Minute,
		MaxConnIdleTime: 1 * time.Minute,
	}
}

// OpenPool creates and configures a PostgreSQL connection pool using pgxpool.
// It verifies connectivity by pinging the database before returning.
// The caller is responsible for closing the returned *pgxpool.Pool.
func OpenPool(ctx context.Context, cfg DBConfig) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	// Apply connection pool settings
	if cfg.MaxConns > 0 {
		poolConfig.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolConfig.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
