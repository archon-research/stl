package migrator_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func getMigrationsPath() string {
	_, filename, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(filename)
	return filepath.Join(testDir, "..", "migrations")
}

func setupPostgres(ctx context.Context, t *testing.T) (*sql.DB, func()) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:18-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "test_db",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	connStr := fmt.Sprintf("postgres://postgres:postgres@%s:%s/test_db?sslmode=disable", host, port.Port())

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close database: %v", err)
		}
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return db, cleanup
}

func TestMigrator_ApplyAll(t *testing.T) {
	ctx := context.Background()

	db, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(db, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'migrations'
		)`).Scan(&exists)
	if err != nil {
		t.Fatalf("failed to check migrations table: %v", err)
	}
	if !exists {
		t.Fatal("migrations table does not exist")
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM migrations").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count migrations: %v", err)
	}
	if count == 0 {
		t.Fatal("no migrations were applied")
	}
	t.Logf("✓ Applied %d migrations", count)

	migrations, err := m.ListApplied(ctx)
	if err != nil {
		t.Fatalf("failed to list migrations: %v", err)
	}
	t.Logf("✓ Migrations applied:")
	for _, migration := range migrations {
		t.Logf("  - %s", migration)
	}

	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("second ApplyAll failed: %v", err)
	}
	t.Logf("✓ Migrations are idempotent")

	var newCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM migrations").Scan(&newCount)
	if err != nil {
		t.Fatalf("failed to count migrations after second run: %v", err)
	}
	if newCount != count {
		t.Fatalf("migration count changed: expected %d, got %d", count, newCount)
	}
	t.Logf("✓ No duplicate migrations applied")
}

func TestMigrator_VerifySchema(t *testing.T) {
	ctx := context.Background()

	db, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(db, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	expectedTables := []string{
		"migrations",
		"block_states",
		"reorg_events",
		"backfill_watermark",
	}

	for _, tableName := range expectedTables {
		var exists bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = $1
			)`, tableName).Scan(&exists)
		if err != nil {
			t.Fatalf("failed to check table %s: %v", tableName, err)
		}
		if !exists {
			t.Errorf("expected table %s does not exist", tableName)
		} else {
			t.Logf("✓ Table %s exists", tableName)
		}
	}
}

func TestMigrator_ChecksumVerification(t *testing.T) {
	ctx := context.Background()

	db, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE migrations (
			id SERIAL PRIMARY KEY,
			filename TEXT NOT NULL UNIQUE,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			checksum TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create migrations table: %v", err)
	}

	tempDir := t.TempDir()
	testMigrationFile := filepath.Join(tempDir, "20260122_150000_test.sql")

	originalContent := `
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT
);

INSERT INTO migrations (filename) 
VALUES ('20260122_150000_test.sql')
ON CONFLICT (filename) DO NOTHING;
`

	if err := os.WriteFile(testMigrationFile, []byte(originalContent), 0644); err != nil {
		t.Fatalf("failed to write test migration: %v", err)
	}

	m := migrator.New(db, tempDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply initial migrations: %v", err)
	}
	t.Logf("✓ Initial migration applied")

	modifiedContent := `
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT
);

INSERT INTO migrations (filename) 
VALUES ('20260122_150000_test.sql')
ON CONFLICT (filename) DO NOTHING;
`

	if err := os.WriteFile(testMigrationFile, []byte(modifiedContent), 0644); err != nil {
		t.Fatalf("failed to modify test migration: %v", err)
	}
	t.Logf("✓ Migration file modified")

	err = m.ApplyAll(ctx)
	if err == nil {
		t.Fatal("expected error for modified migration, got nil")
	}

	if !strings.Contains(err.Error(), "checksum verification failed") &&
		!strings.Contains(err.Error(), "migration has been modified") {
		t.Fatalf("expected checksum error, got: %v", err)
	}

	t.Logf("✓ Checksum verification correctly detected modification: %v", err)
}
