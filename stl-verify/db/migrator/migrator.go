package migrator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Migrator struct {
	pool          *pgxpool.Pool
	migrationsDir string
}

func New(pool *pgxpool.Pool, migrationsDir string) *Migrator {
	return &Migrator{
		pool:          pool,
		migrationsDir: migrationsDir,
	}
}

func (m *Migrator) ApplyAll(ctx context.Context) error {
	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	files, err := m.getMigrationFiles()
	if err != nil {
		return fmt.Errorf("failed to get migration files: %w", err)
	}

	for _, filename := range files {
		if applied[filename] {
			if err := m.verifyChecksum(ctx, filename); err != nil {
				return fmt.Errorf("checksum verification failed for %s: %w", filename, err)
			}
			continue
		}

		if err := m.applyMigration(ctx, filename); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", filename, err)
		}
	}

	return nil
}

func (m *Migrator) getAppliedMigrations(ctx context.Context) (map[string]bool, error) {
	var exists bool
	err := m.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'migrations'
		)`).Scan(&exists)
	if err != nil {
		return nil, err
	}

	if !exists {
		return make(map[string]bool), nil
	}

	rows, err := m.pool.Query(ctx, "SELECT filename FROM migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			return nil, err
		}
		applied[filename] = true
	}

	return applied, rows.Err()
}

func (m *Migrator) getMigrationFiles() ([]string, error) {
	entries, err := os.ReadDir(m.migrationsDir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if strings.HasPrefix(entry.Name(), "README") {
			continue
		}
		files = append(files, entry.Name())
	}

	sort.Strings(files)

	return files, nil
}

func (m *Migrator) verifyChecksum(ctx context.Context, filename string) error {
	path := filepath.Join(m.migrationsDir, filename)

	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	currentChecksum := fmt.Sprintf("%x", sha256.Sum256(content))

	var storedChecksum *string
	err = m.pool.QueryRow(ctx,
		"SELECT checksum FROM migrations WHERE filename = $1",
		filename).Scan(&storedChecksum)
	if err != nil {
		return err
	}

	if storedChecksum != nil && *storedChecksum != currentChecksum {
		return fmt.Errorf("migration has been modified (expected checksum %s, got %s)",
			*storedChecksum, currentChecksum)
	}

	return nil
}

// noTransactionDirective is a comment that can be placed at the top of a migration
// to indicate it should run outside a transaction. This is required for operations
// like CREATE INDEX CONCURRENTLY which cannot run inside a transaction block.
const noTransactionDirective = "-- migrate: no-transaction"

// requiresNoTransaction checks if a migration file contains the no-transaction directive.
func requiresNoTransaction(content []byte) bool {
	// Check if any of the first few lines contain the directive
	lines := strings.SplitN(string(content), "\n", 10)
	for _, line := range lines {
		if strings.TrimSpace(line) == noTransactionDirective {
			return true
		}
	}
	return false
}

func (m *Migrator) applyMigration(ctx context.Context, filename string) error {
	path := filepath.Join(m.migrationsDir, filename)

	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	checksum := fmt.Sprintf("%x", sha256.Sum256(content))

	// Check if this migration needs to run outside a transaction
	if requiresNoTransaction(content) {
		return m.applyMigrationNoTx(ctx, filename, content, checksum)
	}

	return m.applyMigrationWithTx(ctx, filename, content, checksum)
}

// applyMigrationWithTx applies a migration within a transaction (default behavior).
func (m *Migrator) applyMigrationWithTx(ctx context.Context, filename string, content []byte, checksum string) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			fmt.Printf("warning: failed to rollback transaction: %v\n", err)
		}
	}()

	if _, err := tx.Exec(ctx, string(content)); err != nil {
		return fmt.Errorf("failed to execute migration SQL: %w", err)
	}

	_, err = tx.Exec(ctx,
		"UPDATE migrations SET checksum = $1 WHERE filename = $2",
		checksum, filename)
	if err != nil {
		return fmt.Errorf("failed to update checksum: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	fmt.Printf("✓ Applied migration: %s (checksum: %s)\n", filename, checksum[:8])
	return nil
}

// applyMigrationNoTx applies a migration without a transaction wrapper.
// This is required for operations like CREATE INDEX CONCURRENTLY.
// Note: If this migration fails partway through, manual cleanup may be required.
func (m *Migrator) applyMigrationNoTx(ctx context.Context, filename string, content []byte, checksum string) error {
	// Split the content into individual statements and execute each one separately.
	// This ensures each statement runs in its own implicit transaction (or no transaction
	// for statements like CREATE INDEX CONCURRENTLY that cannot run in a transaction).
	statements := splitStatements(string(content))

	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := m.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute statement %d: %w", i+1, err)
		}
	}

	// Update the checksum (this runs in its own implicit transaction)
	_, err := m.pool.Exec(ctx,
		"UPDATE migrations SET checksum = $1 WHERE filename = $2",
		checksum, filename)
	if err != nil {
		return fmt.Errorf("failed to update checksum: %w", err)
	}

	fmt.Printf("✓ Applied migration (no-tx): %s (checksum: %s)\n", filename, checksum[:8])
	return nil
}

// splitStatements splits SQL content into individual statements by semicolons.
// It handles comments and preserves statement integrity.
func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder
	inLineComment := false

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip pure comment lines
		if strings.HasPrefix(trimmed, "--") {
			inLineComment = true
			continue
		}
		inLineComment = false

		// Add line to current statement
		if current.Len() > 0 {
			current.WriteString("\n")
		}
		current.WriteString(line)

		// Check if line ends with semicolon (end of statement)
		if strings.HasSuffix(trimmed, ";") && !inLineComment {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	// Add any remaining content as the last statement
	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}

func (m *Migrator) ListApplied(ctx context.Context) ([]string, error) {
	rows, err := m.pool.Query(ctx,
		"SELECT filename FROM migrations ORDER BY applied_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []string
	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			return nil, err
		}
		migrations = append(migrations, filename)
	}

	return migrations, rows.Err()
}
