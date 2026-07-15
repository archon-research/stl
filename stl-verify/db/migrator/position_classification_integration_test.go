//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestPositionClassification is the VEC-401 contract test: after migrations,
// position_classification exists, accepts a seeded deal_type_ref code, and rejects an unknown
// deal_type_code (FK) and a non-LONG/SHORT direction (CHECK). deal_type_ref is seeded by the
// reference-tables migration earlier in the chain.
func TestPositionClassification(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Valid: a seeded deal_type_ref code (LOAN) with a valid direction inserts.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code, direction)
		 VALUES (sha256('valid'::bytea), 'LOAN', 'LONG')`); err != nil {
		t.Fatalf("valid classification insert: %v", err)
	}

	// FK: an unknown deal_type_code is rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code)
		 VALUES (sha256('fk'::bytea), 'NOT_A_DEAL_TYPE')`); err == nil {
		t.Error("expected a foreign-key violation for an unknown deal_type_code")
	}

	// CHECK: a direction that is not LONG/SHORT is rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code, direction)
		 VALUES (sha256('chk'::bytea), 'LOAN', 'SIDEWAYS')`); err == nil {
		t.Error("expected a check violation for an invalid direction")
	}

	// PK: a duplicate position_id is rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code)
		 VALUES (sha256('valid'::bytea), 'BORROW')`); err == nil {
		t.Error("expected a primary-key violation for a duplicate position_id")
	}

	// CHECK: position_id must be exactly 32 bytes (sha256 width); a mis-sized id is rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code)
		 VALUES ('\x00'::bytea, 'LOAN')`); err == nil {
		t.Error("expected a check violation for a position_id that is not 32 bytes")
	}
}
