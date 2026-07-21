//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestEntityRefCodes is the VEC-414 contract test: after migrations, entity_ref_codes accepts a
// valid code mapping, the code_type CHECK and PK guard bad input, and entity_ref_codes_current
// resolves each code to its latest re-point. This is the resolver the holder step (VEC-417) joins.
func TestEntityRefCodes(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// A valid mapping inserts and reads back.
	if _, err := pool.Exec(ctx,
		`INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason)
		 VALUES ('BLOCKCHAIN_ADDRESS', 'de0b295669a9fd93d5f28d9ec85e40f4cb697bae', 'em-000042', 'seed')`); err != nil {
		t.Fatalf("valid insert: %v", err)
	}
	var entityID string
	if err := pool.QueryRow(ctx,
		`SELECT entity_id FROM entity_ref_codes_current
		 WHERE code_type = 'BLOCKCHAIN_ADDRESS' AND code_value = 'de0b295669a9fd93d5f28d9ec85e40f4cb697bae'`).
		Scan(&entityID); err != nil {
		t.Fatalf("read current: %v", err)
	}
	if entityID != "em-000042" {
		t.Errorf("entity_id = %q, want em-000042", entityID)
	}

	// A re-point (higher processing_version) is a new row; current resolves to the latest.
	if _, err := pool.Exec(ctx,
		`INSERT INTO entity_ref_codes (code_type, code_value, entity_id, processing_version, change_reason)
		 VALUES ('BLOCKCHAIN_ADDRESS', 'de0b295669a9fd93d5f28d9ec85e40f4cb697bae', 'em-000099', 2, 're-point')`); err != nil {
		t.Fatalf("re-point insert: %v", err)
	}
	var current string
	var rowCount int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT entity_id FROM entity_ref_codes_current
		     WHERE code_type = 'BLOCKCHAIN_ADDRESS' AND code_value = 'de0b295669a9fd93d5f28d9ec85e40f4cb697bae'),
		  (SELECT count(*)::int FROM entity_ref_codes
		     WHERE code_type = 'BLOCKCHAIN_ADDRESS' AND code_value = 'de0b295669a9fd93d5f28d9ec85e40f4cb697bae')`).
		Scan(&current, &rowCount); err != nil {
		t.Fatalf("read after re-point: %v", err)
	}
	if current != "em-000099" {
		t.Errorf("current after re-point = %q, want em-000099", current)
	}
	if rowCount != 2 {
		t.Errorf("row count = %d, want 2 (both versions retained)", rowCount)
	}

	// Guards must fail hard rather than accept a silently-wrong mapping. Assert the specific SQLSTATE
	// (23514 check_violation / 23505 unique_violation) so a typo in the guard SQL can't make the test
	// pass for the wrong reason.
	for _, g := range []struct{ name, sql, code string }{
		{"code_type CHECK", `INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason)
			VALUES ('WALLET', 'abc', 'em-1', 'bad type')`, "23514"},
		{"processing_version CHECK", `INSERT INTO entity_ref_codes (code_type, code_value, entity_id, processing_version, change_reason)
			VALUES ('LEI', '5493001KJTIIGC8Y1R12', 'em-1', 0, 'bad version')`, "23514"},
		{"address format CHECK", `INSERT INTO entity_ref_codes (code_type, code_value, entity_id, change_reason)
			VALUES ('BLOCKCHAIN_ADDRESS', '0xDE0B295669a9FD93d5F28D9Ec85E40f4cb697BAe', 'em-1', 'not lowercase hex')`, "23514"},
		{"PK duplicate", `INSERT INTO entity_ref_codes (code_type, code_value, entity_id, processing_version, change_reason)
			VALUES ('BLOCKCHAIN_ADDRESS', 'de0b295669a9fd93d5f28d9ec85e40f4cb697bae', 'em-x', 1, 'dup pv')`, "23505"},
	} {
		t.Run(g.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, g.sql)
			if err == nil {
				t.Fatalf("expected the insert to be rejected, got none")
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("expected a *pgconn.PgError, got %T: %v", err, err)
			}
			if pgErr.Code != g.code {
				t.Errorf("SQLSTATE = %s, want %s", pgErr.Code, g.code)
			}
		})
	}
}
