//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

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

	// INSERT of a new code is still allowed (append-only permits deliberate additions).
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
	if _, err := pool.Exec(ctx,
		`DELETE FROM deal_type_ref WHERE deal_type = 'TEST_IMMUTABLE'`); err == nil {
		t.Error("DELETE on a reference table should raise via the immutability trigger")
	}
}
