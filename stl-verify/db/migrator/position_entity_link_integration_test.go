//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestPositionEntityLink is the VEC-415 contract test: after migrations, position_entity_link
// accepts several roles per position (seeded counterparty_role_ref codes), and rejects an unknown
// role (FK) and a duplicate (position_id, entity_role) (PK). counterparty_role_ref is seeded by the
// reference-tables migration earlier in the chain.
func TestPositionEntityLink(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Valid: a seeded role links an entity (by natural key) to a position.
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_entity_link (position_id, entity_role, entity_id)
		 VALUES (sha256('pos1'::bytea), 'CUSTODIAN', 'em-000042')`); err != nil {
		t.Fatalf("valid link insert: %v", err)
	}

	// A second role on the SAME position is a distinct row (PK is position_id + entity_role).
	if _, err := pool.Exec(ctx,
		`INSERT INTO position_entity_link (position_id, entity_role, entity_id)
		 VALUES (sha256('pos1'::bytea), 'PROTOCOL_OPERATOR', 'em-000099')`); err != nil {
		t.Fatalf("second role insert: %v", err)
	}
	var roles int
	if err := pool.QueryRow(ctx,
		`SELECT count(*)::int FROM position_entity_link WHERE position_id = sha256('pos1'::bytea)`).
		Scan(&roles); err != nil {
		t.Fatalf("count roles: %v", err)
	}
	if roles != 2 {
		t.Errorf("roles for position = %d, want 2", roles)
	}

	// Guards must fail hard rather than accept a silently-wrong link.
	for _, g := range []struct{ name, sql string }{
		{"entity_role FK", `INSERT INTO position_entity_link (position_id, entity_role, entity_id)
			VALUES (sha256('pos2'::bytea), 'NOT_A_ROLE', 'em-1')`},
		{"PK duplicate", `INSERT INTO position_entity_link (position_id, entity_role, entity_id)
			VALUES (sha256('pos1'::bytea), 'CUSTODIAN', 'em-x')`},
		// ISSUER is a seeded role (passes the FK) but the non-issuer CHECK must reject it: the
		// issuer is resolved on the position itself, not linked here.
		{"ISSUER excluded", `INSERT INTO position_entity_link (position_id, entity_role, entity_id)
			VALUES (sha256('pos3'::bytea), 'ISSUER', 'em-issuer')`},
	} {
		if _, err := pool.Exec(ctx, g.sql); err == nil {
			t.Errorf("%s: expected the insert to be rejected, got none", g.name)
		}
	}
}
