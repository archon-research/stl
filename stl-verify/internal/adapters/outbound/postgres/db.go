// Package postgres provides PostgreSQL adapters for the STL verification system.
package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
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

	// LockTimeout, if > 0, is applied as `SET lock_timeout` on every pooled
	// connection. It bounds how long a statement waits to ACQUIRE a lock before
	// failing, so a lock convoy (e.g. an idle-in-transaction holder blocking a
	// TimescaleDB compression policy) surfaces as a fast, retryable error
	// instead of an indefinite hang.
	//
	// Unset by DefaultDBConfig and set by WorkerDBConfig (10s): only the
	// latency-bounded SQS consumers opt in. Backfillers/validators/crons that
	// share DefaultDBConfig may legitimately wait on a lock, so they are left
	// uncapped.
	LockTimeout time.Duration

	// StatementTimeout, if > 0, is applied as `SET statement_timeout` on every
	// pooled connection. Unset by default: backfillers and validators sharing
	// this pool builder run legitimately long statements. Set it only on
	// latency-bounded services that should never run a long single statement.
	StatementTimeout time.Duration
}

// LogValue implements slog.LogValuer to redact the URL (which contains credentials).
func (c DBConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("url", "[REDACTED]"),
		slog.Int("max_conns", int(c.MaxConns)),
		slog.Int("min_conns", int(c.MinConns)),
		slog.Duration("max_conn_lifetime", c.MaxConnLifetime),
		slog.Duration("max_conn_idle_time", c.MaxConnIdleTime),
	)
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

// WorkerDBConfig is the pool config for latency-bounded SQS consumers (the block
// indexers). It is DefaultDBConfig plus a lock_timeout, so a lock convoy surfaces
// as a fast, retryable error instead of an indefinite hang. Backfillers,
// validators, and crons keep DefaultDBConfig (no lock_timeout) so their
// legitimately long lock waits are not aborted.
func WorkerDBConfig(url string) DBConfig {
	cfg := DefaultDBConfig(url)
	cfg.LockTimeout = 10 * time.Second
	return cfg
}

// connRuntimeParams returns the Postgres runtime parameters (GUCs) to set at
// connection startup, or nil when no per-connection timeouts are configured.
// Values are integer milliseconds. These ride the startup packet (no extra
// round-trip) rather than a SET query on every new connection.
func (c DBConfig) connRuntimeParams() map[string]string {
	params := map[string]string{}
	if c.LockTimeout > 0 {
		params["lock_timeout"] = strconv.FormatInt(c.LockTimeout.Milliseconds(), 10)
	}
	if c.StatementTimeout > 0 {
		params["statement_timeout"] = strconv.FormatInt(c.StatementTimeout.Milliseconds(), 10)
	}
	if len(params) == 0 {
		return nil
	}
	return params
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

	// Apply per-connection timeouts (lock_timeout/statement_timeout) as startup
	// runtime parameters so a stuck lock wait fails fast instead of hanging the
	// caller. Sent in the startup packet — no extra round-trip per connection.
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	for name, value := range cfg.connRuntimeParams() {
		poolConfig.ConnConfig.RuntimeParams[name] = value
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

// PoolOpener returns a function that opens a connection pool with the given config.
// This is useful for passing database initialization as a dependency without
// immediately opening the connection.
func PoolOpener(cfg DBConfig) func(ctx context.Context) (*pgxpool.Pool, error) {
	return func(ctx context.Context) (*pgxpool.Pool, error) {
		return OpenPool(ctx, cfg)
	}
}
