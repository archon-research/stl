//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTxManagerTest creates a PostgreSQL container and returns a TxManager.
func setupTxManagerTest(t *testing.T) (*TxManager, *pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
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

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := pool.Ping(ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Create a simple test table
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS tx_test (
			id SERIAL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("failed to create TxManager: %v", err)
	}

	cleanup := func() {
		pool.Close()
		container.Terminate(ctx)
	}

	return txm, pool, cleanup
}

func TestTxManager_WithTransaction_Commit(t *testing.T) {
	txm, pool, cleanup := setupTxManagerTest(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Execute a transaction that inserts data
	err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", "test_value")
		return err
	})
	if err != nil {
		t.Fatalf("WithTransaction failed: %v", err)
	}

	// Verify data was committed
	var value string
	err = pool.QueryRow(ctx, "SELECT value FROM tx_test WHERE value = $1", "test_value").Scan(&value)
	if err != nil {
		t.Fatalf("failed to query inserted data: %v", err)
	}
	if value != "test_value" {
		t.Errorf("expected 'test_value', got %q", value)
	}
}

func TestTxManager_WithTransaction_Rollback(t *testing.T) {
	txm, pool, cleanup := setupTxManagerTest(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Execute a transaction that fails
	testErr := errors.New("intentional failure")
	err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", "should_not_exist")
		if err != nil {
			return err
		}
		return testErr // Return error to trigger rollback
	})

	if !errors.Is(err, testErr) {
		t.Fatalf("expected testErr, got: %v", err)
	}

	// Verify data was NOT committed (rolled back)
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM tx_test WHERE value = $1", "should_not_exist").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows (rollback), got %d", count)
	}
}

func TestTxManager_WithTransaction_PanicRollback(t *testing.T) {
	txm, pool, cleanup := setupTxManagerTest(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Execute a transaction that panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic to be re-raised")
		}
	}()

	_ = txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", "panic_value")
		if err != nil {
			return err
		}
		panic("intentional panic")
	})

	// Verify data was NOT committed (rolled back due to panic)
	var count int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM tx_test WHERE value = $1", "panic_value").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows (rollback after panic), got %d", count)
	}
}

func TestTxManager_WithTransaction_MultipleOperations(t *testing.T) {
	txm, pool, cleanup := setupTxManagerTest(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Execute multiple operations in a single transaction
	err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		for i := 1; i <= 3; i++ {
			_, err := tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", fmt.Sprintf("multi_%d", i))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTransaction failed: %v", err)
	}

	// Verify all data was committed
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM tx_test WHERE value LIKE 'multi_%'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestTxManager_WithTransaction_PartialFailure(t *testing.T) {
	txm, pool, cleanup := setupTxManagerTest(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Execute operations where one fails midway
	err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", "partial_1")
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", "partial_2")
		if err != nil {
			return err
		}
		// Intentionally fail after two successful inserts
		return errors.New("partial failure")
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Verify NO data was committed (atomic rollback)
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM tx_test WHERE value LIKE 'partial_%'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows (all rolled back), got %d", count)
	}
}

func TestTxManager_WithTransactionOptions_ReadOnly(t *testing.T) {
	txm, pool, cleanup := setupTxManagerTest(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Insert test data first
	_, err := pool.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", "readonly_test")
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	// Read-only transaction should allow reads
	var value string
	err = txm.WithTransactionOptions(ctx, &TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT value FROM tx_test WHERE value = $1", "readonly_test").Scan(&value)
	})
	if err != nil {
		t.Fatalf("read-only transaction failed: %v", err)
	}
	if value != "readonly_test" {
		t.Errorf("expected 'readonly_test', got %q", value)
	}

	// Read-only transaction should reject writes
	err = txm.WithTransactionOptions(ctx, &TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", "should_fail")
		return err
	})
	if err == nil {
		t.Fatal("expected error for write in read-only transaction")
	}
}

func TestTxManager_ContextCancellation(t *testing.T) {
	txm, _, cleanup := setupTxManagerTest(t)
	t.Cleanup(cleanup)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Transaction should fail due to cancelled context
	err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestNewTxManager_NilDB(t *testing.T) {
	_, err := NewTxManager(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil database")
	}
}
