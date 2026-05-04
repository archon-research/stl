package testutil

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// StartTimescaleDB creates a TimescaleDB container and returns the DSN and a
// cleanup function. No pool connection or migrations are applied.
func StartTimescaleDB(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        ImageTimescaleDB,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("5432/tcp").
				WithStartupTimeout(60*time.Second),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		HandleContainerRuntimeError(t, err, "start container")
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	dsn = fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())
	cleanup = func() { _ = container.Terminate(context.Background()) }
	return dsn, cleanup
}

// ConnectPool creates a pgxpool.Pool for the given DSN with retry logic.
func ConnectPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	for range 30 {
		if pool.Ping(ctx) == nil {
			return pool
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("timed out waiting for database connection")
	return nil
}

// RunMigrations applies all SQL migrations from db/migrations/.
func RunMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}
}

// SetupTimescaleDB is the composed helper: container + pool + migrations.
// Most integration tests use this.
func SetupTimescaleDB(t *testing.T) (pool *pgxpool.Pool, dsn string, cleanup func()) {
	t.Helper()

	dsn, containerCleanup := StartTimescaleDB(t)
	pool = ConnectPool(t, dsn)
	RunMigrations(t, pool)

	cleanup = func() {
		pool.Close()
		containerCleanup()
	}
	return pool, dsn, cleanup
}

// StartTimescaleDBForMain starts a shared TimescaleDB container for use in
// TestMain (which receives *testing.M, not *testing.T). On error it calls
// log.Fatal instead of t.Fatalf.
//
// If the STL_TEST_DB_DSN environment variable is set, no container is started
// and the env-provided DSN is returned with a no-op cleanup. This lets CI (or
// local dev) share a single TimescaleDB instance across all integration test
// packages.
func StartTimescaleDBForMain() (dsn string, cleanup func()) {
	if envDSN := os.Getenv("STL_TEST_DB_DSN"); envDSN != "" {
		return envDSN, func() {}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        ImageTimescaleDB,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("5432/tcp").
				WithStartupTimeout(60*time.Second),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		if IsContainerRuntimeUnavailable(err) {
			log.Fatalf("container runtime unavailable (is Docker/Podman running?): %v", err)
		}
		log.Fatalf("start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		log.Fatalf("get port: %v", err)
	}

	dsn = fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())
	cleanup = func() { _ = container.Terminate(context.Background()) }
	return dsn, cleanup
}

// ConnectPoolForMain connects to the database for use in TestMain.
// On error it calls log.Fatal instead of t.Fatalf.
func ConnectPoolForMain(dsn string) *pgxpool.Pool {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	for range 30 {
		if pool.Ping(ctx) == nil {
			return pool
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Fatal("timed out waiting for database connection")
	return nil
}

// RunMigrationsForMain applies all SQL migrations for use in TestMain.
// On error it calls log.Fatal instead of t.Fatalf.
func RunMigrationsForMain(pool *pgxpool.Pool) {
	ctx := context.Background()

	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}
}

// --------------------------------------------------------------------------
// Schema-per-test-file helpers for test isolation
// --------------------------------------------------------------------------

// publicMigrationsRun tracks whether migrations have been run in public schema.
// This is used to ensure public schema has tables for migrations that reference
// public.* explicitly (like TimescaleDB integer_now functions).
var publicMigrationsRun bool
var publicMigrationsMu sync.Mutex

// EnsurePublicMigrations runs migrations in public schema if not already done.
// Some migrations reference public.* explicitly, so we need the tables to exist.
// On error it calls log.Fatal.
//
// Safe to call concurrently from multiple processes against the same database.
// A Postgres advisory lock serializes initial migration application, so when
// CI shares a single TimescaleDB container across all integration test
// packages there's no race on creating the public.migrations table.
func EnsurePublicMigrations(dsn string) {
	publicMigrationsMu.Lock()
	defer publicMigrationsMu.Unlock()

	if publicMigrationsRun {
		return
	}

	pool := ConnectPoolForMain(dsn)
	defer pool.Close()

	// Acquire a session-scoped advisory lock on a dedicated connection so
	// concurrent test processes serialize their first migration run. The
	// lock is released when the connection is returned to the pool (i.e.
	// when this function returns). Other processes block on
	// pg_advisory_lock until we release, then observe migrations as
	// already applied.
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire conn for migration lock: %v", err)
	}
	defer conn.Release()

	const migrationLockKey = int64(74249191) // arbitrary STL-specific key
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		log.Fatalf("acquire migration advisory lock: %v", err)
	}
	defer func() {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey); err != nil {
			log.Printf("warning: release migration advisory lock: %v", err)
		}
	}()

	RunMigrationsForMain(pool)
	publicMigrationsRun = true
}

// CreateSchemaForMain creates a new PostgreSQL schema for test isolation.
// The schema name should be unique per test file (e.g., "test_blockstate").
// On error it calls log.Fatal.
func CreateSchemaForMain(pool *pgxpool.Pool, schemaName string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	if err != nil {
		log.Fatalf("create schema %s: %v", schemaName, err)
	}
}

// DropSchemaForMain drops a PostgreSQL schema and all its objects.
// On error it calls log.Fatal.
func DropSchemaForMain(pool *pgxpool.Pool, schemaName string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
	if err != nil {
		log.Fatalf("drop schema %s: %v", schemaName, err)
	}
}

// ConnectPoolWithSchemaForMain creates a pgxpool.Pool with search_path set to the given schema.
// This ensures all queries in this pool use the specified schema by default.
// On error it calls log.Fatal.
func ConnectPoolWithSchemaForMain(dsn, schemaName string) *pgxpool.Pool {
	ctx := context.Background()

	// Append search_path to DSN
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	dsnWithSchema := fmt.Sprintf("%s%ssearch_path=%s,public", dsn, separator, schemaName)

	pool, err := pgxpool.New(ctx, dsnWithSchema)
	if err != nil {
		log.Fatalf("connect with schema %s: %v", schemaName, err)
	}

	for range 30 {
		if pool.Ping(ctx) == nil {
			return pool
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Fatalf("timed out waiting for database connection (schema: %s)", schemaName)
	return nil
}

// RunMigrationsInSchemaForMain applies all SQL migrations within a specific schema.
// It temporarily sets the search_path to run migrations in the target schema.
// On error it calls log.Fatal.
func RunMigrationsInSchemaForMain(pool *pgxpool.Pool, schemaName string) {
	ctx := context.Background()

	// Set search_path for this connection to the target schema
	_, err := pool.Exec(ctx, fmt.Sprintf("SET search_path TO %s, public", schemaName))
	if err != nil {
		log.Fatalf("set search_path for migrations: %v", err)
	}

	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		log.Fatalf("migrations in schema %s: %v", schemaName, err)
	}
}

// SetupSchemaForMain is a convenience function that creates a schema,
// connects with search_path set, and runs migrations.
// Returns the schema-specific pool. Caller should close the pool when done.
// On error it calls log.Fatal.
func SetupSchemaForMain(baseDSN, schemaName string) *pgxpool.Pool {
	// Ensure public schema has migrations run first - some migrations
	// reference public.* explicitly (e.g., TimescaleDB integer_now functions)
	EnsurePublicMigrations(baseDSN)

	// Create the test schema
	basePool := ConnectPoolForMain(baseDSN)
	CreateSchemaForMain(basePool, schemaName)
	basePool.Close()

	// Connect with schema in search_path
	schemaPool := ConnectPoolWithSchemaForMain(baseDSN, schemaName)

	// Run migrations in the schema
	RunMigrationsInSchemaForMain(schemaPool, schemaName)

	return schemaPool
}

// CleanupSchemaForMain drops the schema and closes the pool.
// On error it logs but does not fatal (cleanup should be best-effort).
func CleanupSchemaForMain(baseDSN string, schemaPool *pgxpool.Pool, schemaName string) {
	schemaPool.Close()

	// Connect to drop the schema
	ctx := context.Background()
	basePool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		log.Printf("warning: could not connect to drop schema %s: %v", schemaName, err)
		return
	}
	defer basePool.Close()

	_, err = basePool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
	if err != nil {
		log.Printf("warning: could not drop schema %s: %v", schemaName, err)
	}
}

// --------------------------------------------------------------------------
// Per-test schema isolation (for use with shared containers)
// --------------------------------------------------------------------------

// SetupTestSchema creates an isolated PostgreSQL schema backed by a shared
// container whose base DSN is passed in. Public-schema migrations are applied
// once; the test schema gets its own copy of all tables. This is a drop-in
// replacement for SetupTimescaleDB that reuses an existing container.
func SetupTestSchema(t *testing.T, baseDSN string) (pool *pgxpool.Pool, dsn string, cleanup func()) {
	t.Helper()

	EnsurePublicMigrations(baseDSN)

	schemaName := SanitizeTestName(t.Name())
	ctx := context.Background()

	// Create the schema using a short-lived base connection.
	basePool := ConnectPool(t, baseDSN)
	_, err := basePool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	if err != nil {
		t.Fatalf("create schema %s: %v", schemaName, err)
	}
	basePool.Close()

	// Build DSN with search_path scoped to this schema.
	separator := "?"
	if strings.Contains(baseDSN, "?") {
		separator = "&"
	}
	dsn = fmt.Sprintf("%s%ssearch_path=%s,public", baseDSN, separator, schemaName)

	pool = ConnectPool(t, dsn)

	// Pre-create an empty migrations table in the test schema. The migrator
	// checks public.migrations to decide IF a migrations table exists, but
	// then reads filenames via the unqualified "SELECT filename FROM migrations"
	// which resolves through search_path. By placing an empty migrations table
	// in the test schema (first in search_path), the migrator sees zero applied
	// migrations and runs them all, creating tables in the test schema.
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.migrations (
		id SERIAL PRIMARY KEY,
		filename TEXT NOT NULL UNIQUE,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		checksum TEXT
	)`, schemaName))
	if err != nil {
		t.Fatalf("create migrations table in %s: %v", schemaName, err)
	}

	RunMigrations(t, pool)

	cleanup = func() {
		pool.Close()
		dropCtx := context.Background()
		dropPool, dropErr := pgxpool.New(dropCtx, baseDSN)
		if dropErr != nil {
			return
		}
		defer dropPool.Close()
		_, _ = dropPool.Exec(dropCtx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
	}
	return pool, dsn, cleanup
}

// SanitizeTestName converts a test name to a string safe for use as a
// PostgreSQL identifier, Redis key prefix, or AWS resource name suffix.
// The result is lowercase alphanumeric + underscores, prefixed with "t_",
// and truncated to 63 characters (PostgreSQL identifier limit).
func SanitizeTestName(testName string) string {
	s := strings.ToLower(testName)
	var b strings.Builder
	b.WriteString("t_")
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	result := b.String()
	if len(result) > 63 {
		result = result[:63]
	}
	return result
}
