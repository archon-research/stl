//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"
)

// VEC-810 contract: the advisory lock key that serialises runs of one position projection has ONE
// definition, position_projection_lock_key(regclass). The spine calls it, and every wrapper calls it
// before its own pre-checks, so the pre-check and the append stay one unit.
//
// Note on what is NOT testable from a wrapper: the spine takes this lock itself, so a wrapper whose
// key drifted would STILL block -- inside the spine, after its pre-checks had already run. A test
// that merely observes "something blocked" cannot tell the two apart. What discriminates is moving
// the one definition and requiring the caller to follow it.

const lockKeyProjection = "public.position_anchorage_custody"

// Moving the single definition must move the spine's lock. Against a spine that builds the key
// inline, the redefinition is ignored, the spine takes its own key, and the run proceeds -- which is
// the drift this ticket exists to make impossible.
func TestProjectionLockKeyDefinitionDrivesTheSpine(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	const sentinel = 4242424242

	// Negative control first: unheld, the run completes. So the block below is the lock's doing and
	// not the projection failing for some unrelated reason.
	if _, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass)`, lockKeyProjection); err != nil {
		t.Fatalf("the projection must run cleanly when nothing holds its lock: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION position_projection_lock_key(p_view regclass)
		    RETURNS bigint LANGUAGE sql STABLE PARALLEL SAFE AS
		$fn$ SELECT 4242424242::bigint $fn$`); err != nil {
		t.Fatalf("redefining the key: %v", err)
	}

	holder, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	held, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback(ctx)
	if _, err := held.Exec(ctx, `SELECT pg_advisory_xact_lock($1::bigint)`, sentinel); err != nil {
		t.Fatalf("holding the redefined key: %v", err)
	}

	runner, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Release()
	run, err := runner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Rollback(ctx)
	if _, err := run.Exec(ctx, `SET LOCAL lock_timeout = '3s'`); err != nil {
		t.Fatal(err)
	}

	_, err = run.Exec(ctx, `SELECT materialize_position_projection($1::regclass)`, lockKeyProjection)
	if err == nil {
		t.Fatal("the spine ran while the redefined key was held: it is not taking its lock from position_projection_lock_key(), so a wrapper that drifts from it would go unnoticed")
	}
	if !strings.Contains(err.Error(), "lock timeout") && !strings.Contains(err.Error(), "55P03") {
		t.Errorf("the run failed, but not by blocking on the redefined key: %v", err)
	}
}

// The structural half. A spine that kept its own copy alongside a call to the helper would pass the
// behavioural test above while still carrying the duplication this ticket removes.
func TestProjectionLockKeyIsNotSpelledInTheSpine(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	var spine, helper string
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_functiondef('materialize_position_projection(regclass,integer,bigint,interval)'::regprocedure),
		       pg_get_functiondef('position_projection_lock_key(regclass)'::regprocedure)`).
		Scan(&spine, &helper); err != nil {
		t.Fatalf("reading the catalogue definitions: %v", err)
	}

	if !strings.Contains(helper, "materialize_position_projection.") {
		t.Error("position_projection_lock_key does not build the key, so it is not the definition")
	}
	if strings.Contains(spine, "hashtextextended('materialize_position_projection.") {
		t.Error("the spine still builds the lock key inline; it must call position_projection_lock_key()")
	}
	if !strings.Contains(spine, "position_projection_lock_key") {
		t.Error("the spine does not call position_projection_lock_key()")
	}
}

// An oid naming no relation must not silently lock nothing: the helper returns NULL and
// pg_advisory_xact_lock rejects it, so the run cannot proceed unserialised.
func TestProjectionLockKeyIsNullForAnUnknownRelation(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	var key *int64
	if err := pool.QueryRow(ctx, `SELECT position_projection_lock_key(0::regclass)`).Scan(&key); err != nil {
		t.Fatalf("calling the helper with an unknown oid: %v", err)
	}
	if key != nil {
		t.Errorf("an unknown relation returned key %d; want NULL, which pg_advisory_xact_lock refuses", *key)
	}
}

// The key must separate projections: two views sharing one key would serialise runs that have no
// reason to exclude each other, and would mask a genuine collision.
func TestProjectionLockKeyDiffersPerProjection(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	var same bool
	if err := pool.QueryRow(ctx, `
		SELECT position_projection_lock_key('public.position_anchorage_custody'::regclass)
		     = position_projection_lock_key('public.position_current'::regclass)`).Scan(&same); err != nil {
		t.Fatalf("comparing two relations' keys: %v", err)
	}
	if same {
		t.Error("two different relations render one lock key")
	}
}
