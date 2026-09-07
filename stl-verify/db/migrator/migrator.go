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
// Semicolons inside dollar-quoted string literals (PostgreSQL `$$ ... $$` or
// `$tag$ ... $tag$`), single-quoted string literals, `--` line comments, and
// `/* ... */` block comments are ignored, so DO blocks, function bodies, and
// literals carrying embedded `;` or `$$` stay intact. This is the shared splitter
// for every no-transaction migration, so it must not mis-parse a future migration
// that puts a `$$` or `;` inside a string or a comment.
//
// It is line-oriented: a statement is recognised when a line's non-comment code
// ends in `;`. That means every statement must live on its own line — do NOT put
// two statements on a single line in a no-transaction migration, or they will be
// executed together (which breaks statements that require standalone execution,
// e.g. CREATE INDEX CONCURRENTLY or a DO block with COMMIT). Every migration in
// this repo already follows the one-statement-per-line convention.
//
// Two SQL constructs are outside the supported grammar (no migration uses either;
// add handling if one ever needs to): PostgreSQL E-string backslash escapes (a
// backslash-escaped quote inside an E-string is not recognised, so the literal is
// mis-read as unterminated) and nested block comments (`/* a /* b */ still */`
// closes at the first `*/`). Keep both out of no-transaction migrations.
func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder

	inDollarQuote := false
	inSingleQuote := false
	inBlockComment := false
	dollarTag := ""

	for line := range strings.SplitSeq(content, "\n") {
		trimmed := strings.TrimSpace(line)

		// Pure comment lines are dropped only when we are between statements.
		// Inside a dollar-quoted or single-quoted body, a leading `--` is
		// legitimate content (a comment inside a PL/pgSQL body, or a literal
		// that begins with `--`) and must be kept.
		if !inDollarQuote && !inSingleQuote && !inBlockComment && strings.HasPrefix(trimmed, "--") {
			continue
		}

		if current.Len() > 0 {
			current.WriteString("\n")
		}
		current.WriteString(line)

		// Advance the quote/comment state across this line and recover the
		// non-comment code so the terminating semicolon is detected on real SQL,
		// not on a `;` inside a comment or string.
		code := scanLine(line, &inDollarQuote, &dollarTag, &inSingleQuote, &inBlockComment)

		if !inDollarQuote && !inSingleQuote && !inBlockComment && strings.HasSuffix(strings.TrimSpace(code), ";") {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}

// scanLine walks `line` once, updating the dollar-quote, single-quote, and
// block-comment state, and returns the line's non-comment code (both `--` line
// comments and `/* ... */` block comments removed). PostgreSQL rules honoured: a
// doubled single quote is an escaped quote and stays inside the literal; a `$`,
// `;`, or comment marker inside a single-quoted literal is content, not a
// delimiter; and a dollar tag only closes on its exact match. Block-comment state
// is threaded through the pointer because a `/* ... */` may span lines.
func scanLine(line string, inDollarQuote *bool, dollarTag *string, inSingleQuote, inBlockComment *bool) string {
	var code strings.Builder
	for i := 0; i < len(line); {
		switch {
		case *inBlockComment:
			if i+1 < len(line) && line[i] == '*' && line[i+1] == '/' {
				*inBlockComment = false
				i += 2
				continue
			}
			i++
		case *inSingleQuote:
			code.WriteByte(line[i])
			if line[i] == '\'' {
				if i+1 < len(line) && line[i+1] == '\'' {
					code.WriteByte(line[i+1])
					i += 2 // escaped quote: remain inside the literal
					continue
				}
				*inSingleQuote = false
			}
			i++
		case *inDollarQuote:
			if tag, ok := matchDollarTag(line, i); ok {
				if tag == *dollarTag {
					*inDollarQuote = false
					*dollarTag = ""
				}
				code.WriteString(tag)
				i += len(tag)
				continue
			}
			code.WriteByte(line[i])
			i++
		case line[i] == '\'':
			*inSingleQuote = true
			code.WriteByte(line[i])
			i++
		case line[i] == '-' && i+1 < len(line) && line[i+1] == '-':
			return code.String() // rest of the line is a line comment
		case line[i] == '/' && i+1 < len(line) && line[i+1] == '*':
			*inBlockComment = true
			i += 2
		default:
			if tag, ok := matchDollarTag(line, i); ok {
				*inDollarQuote = true
				*dollarTag = tag
				code.WriteString(tag)
				i += len(tag)
				continue
			}
			code.WriteByte(line[i])
			i++
		}
	}
	return code.String()
}

// matchDollarTag reports whether a PostgreSQL dollar-quote tag begins at
// `line[i]`, returning the full tag including both `$` delimiters. A tag is `$$`
// or `$identifier$` where the identifier is letters, digits, or underscores and
// does not start with a digit. A lone `$` (e.g. a `$1` parameter, or a `$` with
// no closing `$` on the line) is not a tag.
func matchDollarTag(line string, i int) (string, bool) {
	if line[i] != '$' {
		return "", false
	}
	j := i + 1
	for j < len(line) && line[j] != '$' {
		c := line[j]
		if !(c == '_' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9')) {
			return "", false
		}
		// Identifier cannot start with a digit.
		if j == i+1 && (c >= '0' && c <= '9') {
			return "", false
		}
		j++
	}
	if j >= len(line) {
		return "", false
	}
	return line[i : j+1], true
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
