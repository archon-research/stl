package migrator_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMigrator_ApplyAll(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("test_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2),
		),
	)
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	connStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close database: %v", err)
		}
	}()

	m := migrator.New(db, "../../db/migrations")
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	var exists bool
	err = db.QueryRowContext(ctx, `
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

	postgresContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("test_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2),
		),
	)
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	connStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close database: %v", err)
		}
	}()

	m := migrator.New(db, "../../db/migrations")
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
		err = db.QueryRowContext(ctx, `
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
