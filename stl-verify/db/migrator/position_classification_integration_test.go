//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"
)

// TestPositionClassification is the VEC-401 contract test: after migrations,
// position_classification exists, accepts a seeded ref_deal_type code, and rejects an unknown
// deal_type_code (FK) and a non-LONG/SHORT direction (CHECK). ref_deal_type is seeded by the
// reference-tables migration earlier in the chain.
//
// Since VEC-402 (20260818_130000) the table also enforces classification provenance by trigger:
// every write must present as_of_(block, block_version, processing_version) coordinates that ARE the
// canonical latest non-zero observation for that position in position_state. The fixtures below
// therefore seed a matching observation first and pass provenance; the FK/CHECK/PK assertions name
// the error they expect, so a provenance rejection can no longer masquerade as the constraint under
// test (the trigger is BEFORE INSERT, so it fires ahead of both).
func TestPositionClassification(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	prov := ``
	provVals := ``

	// Valid: a seeded ref_deal_type code (LOAN) with a valid direction and a real basis inserts.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code, direction`+prov+`)
		 VALUES (sha256('valid'::bytea), 'LOAN', 'LONG'`+provVals+`)`); err != nil {
		t.Fatalf("valid classification insert: %v", err)
	}

	// FK: an unknown deal_type_code is rejected by the foreign key.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code`+prov+`)
		 VALUES (sha256('fk'::bytea), 'NOT_A_DEAL_TYPE'`+provVals+`)`); err == nil ||
		!strings.Contains(err.Error(), "deal_type") {
		t.Errorf("unknown deal_type_code: got %v; want a foreign-key violation naming deal_type", err)
	}

	// CHECK: a direction that is not LONG/SHORT is rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code, direction`+prov+`)
		 VALUES (sha256('chk'::bytea), 'LOAN', 'SIDEWAYS'`+provVals+`)`); err == nil ||
		!strings.Contains(err.Error(), "direction") {
		t.Errorf("invalid direction: got %v; want a check violation naming direction", err)
	}

	// PK: a duplicate position_id is rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code`+prov+`)
		 VALUES (sha256('valid'::bytea), 'LOAN'`+provVals+`)`); err == nil ||
		!strings.Contains(err.Error(), "position_classification_pkey") {
		t.Errorf("duplicate position_id: got %v; want a primary-key violation", err)
	}

}
