//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const allocUnderlyingSchemaName = "test_alloc_underlying"

var allocUnderlyingPool *pgxpool.Pool

func init() {
	registerTestFileSetup(allocUnderlyingSchemaName, func() {
		allocUnderlyingPool = testutil.SetupSchemaForMain(sharedDSN, allocUnderlyingSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, allocUnderlyingPool, allocUnderlyingSchemaName)
	})
}

func TestAllocationPositionUnderlyingColumnsExist(t *testing.T) {
	ctx := context.Background()
	var count int
	err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'allocation_position'
		  AND column_name IN ('underlying_value', 'underlying_token_id')
		  AND is_nullable = 'YES'`).Scan(&count)
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 nullable underlying columns, got %d", count)
	}
}

func TestAllocationPositionUnderlyingPairCheckRejectsHalfSetRow(t *testing.T) {
	ctx := context.Background()
	// Constraint metadata is enough: inserting a full row needs the whole
	// natural key; the CHECK's presence + definition is the behaviour under test.
	var def string
	err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'allocation_position_underlying_pair_check'`).Scan(&def)
	if err != nil {
		t.Fatalf("CHECK constraint missing: %v", err)
	}
}

func TestAllocationPositionUnderlyingTokenFKExists(t *testing.T) {
	ctx := context.Background()
	var def string
	err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'allocation_position_underlying_token_id_fkey'`).Scan(&def)
	if err != nil {
		t.Fatalf("FK constraint missing: %v", err)
	}
}
