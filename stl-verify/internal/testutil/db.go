package testutil

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
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
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
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
		t.Fatalf("start container: %v", err)
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
	cleanup = func() { container.Terminate(ctx) }
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

	for i := 0; i < 30; i++ {
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
