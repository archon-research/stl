package testutil

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ConnectPool creates a pgxpool.Pool for the given DSN with retry logic.
func ConnectPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()

	pool, err := connectPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	return pool
}

// StartTimescaleDBForMain gives this test binary a Postgres to work in, for use in
// TestMain (which receives *testing.M, not *testing.T). On error it calls log.Fatal
// instead of t.Fatalf.
//
// When STL_TEST_POSTGRES_DSN is set it carves a database for this process out of
// that server instead, so CI can own one TimescaleDB per shard rather than one
// per package.
func StartTimescaleDBForMain() (dsn string, cleanup func()) {
	if shared, ok := sharedService(EnvPostgresDSN); ok {
		return createProcessDatabase(shared)
	}

	dsn, cleanup, err := startTimescaleDBContainer()
	if err != nil {
		log.Fatalf("%v", err)
	}
	return dsn, cleanup
}

func startTimescaleDBContainer() (dsn string, cleanup func(), err error) {
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
			return "", nil, fmt.Errorf("container runtime unavailable (is Docker/Podman running?): %w", err)
		}
		return "", nil, fmt.Errorf("start container: %w", err)
	}

	// The container is running by now, so every later failure has to take it
	// down: the caller gets no cleanup callback to do it with.
	terminate := func() { _ = container.Terminate(context.Background()) }

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("get host: %w", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("get port: %w", err)
	}

	return fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port()),
		terminate, nil
}

// createProcessDatabase gives this test binary its own database on a server it
// shares with the other packages of the shard.
//
// A database, not a schema: packages assume the public schema is theirs alone —
// they TRUNCATE registry tables and assert on migration-seeded rows — so sharing
// one database between packages loses rows out from under them.
func createProcessDatabase(baseDSN string) (dsn string, cleanup func()) {
	ctx := context.Background()
	dbName := "stl_test_" + processTag()

	dsn, err := createDatabase(ctx, baseDSN, dbName)
	if err != nil {
		log.Fatalf("%v", err)
	}

	cleanup = func() {
		if err := dropDatabase(baseDSN, dbName); err != nil {
			log.Printf("warning: %v", err)
		}
	}
	return dsn, cleanup
}

// createDatabase drops any stale database of this name, recreates it, and
// returns the DSN pointing at it.
func createDatabase(ctx context.Context, baseDSN, dbName string) (string, error) {
	adminPool := ConnectPoolForMain(baseDSN)
	defer adminPool.Close()

	// A database left behind by a killed run would silently supply its rows here.
	if _, err := adminPool.Exec(ctx, dropDatabaseSQL(dbName)); err != nil {
		return "", fmt.Errorf("drop stale database %s: %w", dbName, err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		return "", fmt.Errorf("create database %s: %w", dbName, err)
	}

	dsn, err := replaceDatabase(baseDSN, dbName)
	if err != nil {
		return "", fmt.Errorf("build DSN for %s: %w", dbName, err)
	}
	return dsn, nil
}

// ConnectPoolForMain is ConnectPool for TestMain, where there is no *testing.T to
// fail. On error it calls log.Fatal.
func ConnectPoolForMain(dsn string) *pgxpool.Pool {
	pool, err := connectPool(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect pool: %v", err)
	}
	return pool
}

// dropDatabaseSQL drops dbName, terminating any backend still attached to it —
// a pool that outlived its test would otherwise block the drop outright.
func dropDatabaseSQL(dbName string) string {
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName)
}

// dropDatabase connects to baseDSN solely to remove dbName.
func dropDatabase(baseDSN, dbName string) error {
	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		return fmt.Errorf("connect to drop database %s: %w", dbName, err)
	}
	defer adminPool.Close()

	if _, err := adminPool.Exec(ctx, dropDatabaseSQL(dbName)); err != nil {
		return fmt.Errorf("drop database %s: %w", dbName, err)
	}
	return nil
}

// replaceDatabase points a DSN at another database on the same server.
func replaceDatabase(baseDSN, dbName string) (string, error) {
	u, err := url.Parse(baseDSN)
	if err != nil {
		return "", fmt.Errorf("parse base DSN: %w", err)
	}
	u.Path = "/" + dbName
	return u.String(), nil
}

// IsUniqueViolation reports whether err is a Postgres unique constraint
// violation. Matched through the SQLState interface rather than a pgconn type
// assertion, so a caller that wraps or substitutes the driver error still
// resolves, with the message as the last resort.
func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	type sqlStateProvider interface {
		SQLState() string
	}
	var p sqlStateProvider
	if errors.As(err, &p) {
		return p.SQLState() == uniqueViolationSQLState
	}
	msg := err.Error()
	return strings.Contains(msg, uniqueViolationSQLState) || strings.Contains(msg, "unique constraint")
}

// IsDeadlock reports whether err is a Postgres deadlock. Matched the same way as
// IsUniqueViolation above.
func IsDeadlock(err error) bool {
	if err == nil {
		return false
	}
	type sqlStateProvider interface {
		SQLState() string
	}
	var p sqlStateProvider
	if errors.As(err, &p) {
		return p.SQLState() == deadlockDetectedSQLState
	}
	msg := err.Error()
	return strings.Contains(msg, deadlockDetectedSQLState) || strings.Contains(msg, "deadlock detected")
}

const uniqueViolationSQLState = "23505"

const deadlockDetectedSQLState = "40P01"
