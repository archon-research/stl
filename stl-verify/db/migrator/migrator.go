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

func (m *Migrator) applyMigration(ctx context.Context, filename string) error {
	path := filepath.Join(m.migrationsDir, filename)

	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	checksum := fmt.Sprintf("%x", sha256.Sum256(content))

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
