//go:build integration

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// startStatsPostgres starts a Postgres with pg_stat_statements preloaded. The
// shared testutil container cannot be used: the extension is a shared library that
// has to be named in shared_preload_libraries at server start, and that container
// is reused across packages. Kept package-local rather than hoisted into testutil
// so the extra server flag cannot perturb any other package.
func startStatsPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testutil.ImageTimescaleDB,
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "test",
				"POSTGRES_PASSWORD": "test",
				"POSTGRES_DB":       "testdb",
			},
			Cmd: []string{
				"postgres",
				"-c", "shared_preload_libraries=timescaledb,pg_stat_statements",
			},
			WaitingFor: wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(90*time.Second),
				wait.ForListeningPort("5432/tcp").
					WithStartupTimeout(90*time.Second),
			),
		},
		Started: true,
	})
	if err != nil {
		testutil.HandleContainerRuntimeError(t, err, "start container")
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	pool := testutil.ConnectPool(t, fmt.Sprintf(
		"postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port()))
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		t.Fatalf("creating pg_stat_statements: %v", err)
	}
	return pool
}

// TestSetupRunner_WiresService proves the composition root: the runner main()
// builds reaches the real pg_stat_statements view through the real adapter and
// completes a tick. A miswired pool or reader would fail here.
func TestSetupRunner_WiresService(t *testing.T) {
	ctx := context.Background()
	pool := startStatsPostgres(t)

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	// Two ticks: the first baselines every fingerprint, the second exercises the
	// delta path against counters that really moved in between.
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("first runner.Run: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE wiring_probe (v int)`); err != nil {
		t.Fatalf("creating probe table: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO wiring_probe (v) VALUES ($1)`, 1); err != nil {
		t.Fatalf("seeding probe table: %v", err)
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("second runner.Run: %v", err)
	}
}

// TestRun_RequiresDatabaseURL pins the startup guard. Defaulting to localhost
// would let a deployed worker connect to an empty local database and report
// healthy while measuring a database nobody writes to.
func TestRun_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	err := run(context.Background())
	if err == nil {
		t.Fatal("run succeeded without DATABASE_URL; want an error")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Errorf("error %q should name the missing variable", err)
	}
}
