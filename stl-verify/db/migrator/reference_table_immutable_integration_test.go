//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// referenceTables is the full controlled-vocabulary set that 20260714_160000 makes append-only.
// It must stay in lockstep with that migration's table array: the loop in TestReferenceTableImmutable
// asserts every one still carries its immutability trigger, so a typo or omission in the migration
// (e.g. dropping currency_ref) leaves that table mutable and fails here rather than shipping silently.
var referenceTables = []string{
	"asset_class_ref", "security_type_ref", "security_subtype_ref", "credit_rating_ref",
	"sector_ref", "industry_group_ref", "currency_ref", "country_ref", "deal_type_ref",
	"entity_type_ref", "counterparty_role_ref", "origination_type_ref", "corporate_action_type_ref",
}

// TestReferenceTableImmutable pins the fix for the FK-insert blocker (Simon review on #574): after
// migrations, the reference tables still reject UPDATE/DELETE (now via a BEFORE UPDATE OR DELETE
// trigger rather than an owner ACL revoke, so the FK row-lock probe keeps working), while INSERT of
// a new row is still allowed. The ACL restoration that unblocks the FK probe under the non-superuser
// prod roles is verified manually against real roles on pg17/pg18; the superuser test harness cannot
// exercise the ACL path, but the trigger it pairs with is role-independent and is asserted here.
func TestReferenceTableImmutable(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Pin the full set the migration protects: every reference table must carry its immutability
	// trigger. This is what makes the migration's 13-table array load-bearing in the test, not just
	// the single deal_type_ref case below.
	for _, tbl := range referenceTables {
		var exists bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.triggers
				WHERE event_object_table = $1
				AND trigger_name = $2
			)`, tbl, tbl+"_immutable").Scan(&exists); err != nil {
			t.Fatalf("trigger check for %s: %v", tbl, err)
		}
		if !exists {
			t.Errorf("%s is missing its %s_immutable trigger (dropped from the migration?)", tbl, tbl)
		}
	}

	// deal_type_ref proves the trigger's runtime behavior end to end (the loop above only pins that
	// the triggers exist). INSERT of a new code is still allowed (append-only permits deliberate
	// additions).
	if _, err := pool.Exec(ctx,
		`INSERT INTO deal_type_ref (deal_type, direction, description)
		 VALUES ('TEST_IMMUTABLE', 'LONG', 'immutability test row')`); err != nil {
		t.Fatalf("insert into reference table should be allowed: %v", err)
	}

	// UPDATE raises via the immutability trigger (fires for any role, including superuser).
	_, err := pool.Exec(ctx,
		`UPDATE deal_type_ref SET description = 'mutated' WHERE deal_type = 'TEST_IMMUTABLE'`)
	if err == nil {
		t.Error("UPDATE on a reference table should raise via the immutability trigger")
	} else if !strings.Contains(err.Error(), "append-only") {
		t.Errorf("UPDATE error = %v, want the append-only immutability message", err)
	}

	// DELETE also raises.
	_, err = pool.Exec(ctx,
		`DELETE FROM deal_type_ref WHERE deal_type = 'TEST_IMMUTABLE'`)
	if err == nil {
		t.Error("DELETE on a reference table should raise via the immutability trigger")
	} else if !strings.Contains(err.Error(), "append-only") {
		t.Errorf("DELETE error = %v, want the append-only immutability message", err)
	}
}
