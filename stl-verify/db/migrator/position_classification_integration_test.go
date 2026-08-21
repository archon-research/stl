//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
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
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// One observation per fixture position, so each classification below has a valid basis.
	seed := func(t *testing.T, tag string) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`INSERT INTO position_state (position_id, chain_id, protocol_id, instrument_key, holder_id,
				quantity, block_number, block_version, processing_version, block_timestamp, projection)
			 VALUES (sha256($1::bytea), 1, 10, $2, 'aa', 5, 100, 0, 0, '2026-01-01+00', 'vec401-test')`,
			tag, tag); err != nil {
			t.Fatalf("seed observation for %s: %v", tag, err)
		}
	}
	// The table is the append-only decision history (VEC-402), so every insert carries the decision's
	// version and reason alongside the provenance of the observation it classified.
	prov := `, classification_version, change_reason, as_of_block, as_of_block_version, as_of_processing_version`
	provVals := `, 1, 'vec401-test', 100, 0, 0`

	// Valid: a seeded ref_deal_type code (LOAN) with a valid direction and a real basis inserts.
	seed(t, "valid")
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code, direction`+prov+`)
		 VALUES (sha256('valid'::bytea), 'LOAN', 'LONG'`+provVals+`)`); err != nil {
		t.Fatalf("valid classification insert: %v", err)
	}

	// FK: an unknown deal_type_code is rejected by the foreign key.
	seed(t, "fk")
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code`+prov+`)
		 VALUES (sha256('fk'::bytea), 'NOT_A_DEAL_TYPE'`+provVals+`)`); err == nil ||
		!strings.Contains(err.Error(), "deal_type") {
		t.Errorf("unknown deal_type_code: got %v; want a foreign-key violation naming deal_type", err)
	}

	// CHECK: a direction that is not LONG/SHORT is rejected.
	seed(t, "chk")
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code, direction`+prov+`)
		 VALUES (sha256('chk'::bytea), 'LOAN', 'SIDEWAYS'`+provVals+`)`); err == nil ||
		!strings.Contains(err.Error(), "direction") {
		t.Errorf("invalid direction: got %v; want a check violation naming direction", err)
	}

	// PK: the same (position_id, classification_version) twice is rejected; a NEW version is not.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code`+prov+`)
		 VALUES (sha256('valid'::bytea), 'LOAN'`+provVals+`)`); err == nil ||
		!strings.Contains(err.Error(), "position_classification_pkey") {
		t.Errorf("duplicate (position_id, classification_version): got %v; want a primary-key violation", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_classification (position_id, deal_type_code, classification_version,
			change_reason, as_of_block, as_of_block_version, as_of_processing_version)
		 VALUES (sha256('valid'::bytea), 'BORROW', 2, 'vec401-test', 100, 0, 0)`); err != nil {
		t.Errorf("a second decision for the same position must append: %v", err)
	}

	// Provenance is structural now, not trigger-enforced: the columns are NOT NULL, so a write with no
	// basis cannot be stored at all.
	for _, c := range []struct{ name, stmt, want string }{
		{"no provenance", `INSERT INTO position_classification (position_id, deal_type_code,
			classification_version, change_reason) VALUES (sha256('noprov'::bytea), 'LOAN', 1, 'r')`, "as_of_block"},
		{"no version", `INSERT INTO position_classification (position_id, deal_type_code, change_reason,
			as_of_block, as_of_block_version, as_of_processing_version)
			VALUES (sha256('nover'::bytea), 'LOAN', 'r', 100, 0, 0)`, "classification_version"},
		{"zero version", `INSERT INTO position_classification (position_id, deal_type_code,
			classification_version, change_reason, as_of_block, as_of_block_version, as_of_processing_version)
			VALUES (sha256('zerover'::bytea), 'LOAN', 0, 'r', 100, 0, 0)`, "version_pos_chk"},
	} {
		if _, err := pool.Exec(ctx, c.stmt); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v; want an error naming %q", c.name, err, c.want)
		}
	}
}
