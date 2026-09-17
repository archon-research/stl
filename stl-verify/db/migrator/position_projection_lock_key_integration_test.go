//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// VEC-810 contract: the advisory lock key that serialises runs of one position projection has ONE
// definition, position_projection_lock_key(regclass). The spine calls it, and every wrapper calls it
// before its own pre-checks, so the pre-check and the append stay one unit.

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

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION position_projection_lock_key(p_view regclass)
		    RETURNS bigint LANGUAGE sql STABLE PARALLEL SAFE AS
		$fn$ SELECT %d::bigint $fn$`, sentinel)); err != nil {
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
	if strings.Contains(spine, "hashtextextended") {
		t.Error("the spine still builds the lock key inline; it must call position_projection_lock_key()")
	}
	if !strings.Contains(spine, "position_projection_lock_key") {
		t.Error("the spine does not call position_projection_lock_key()")
	}
}

// An oid naming no relation raises, so a caller cannot lock nothing: pg_advisory_xact_lock is strict
// and takes no lock on a NULL key.
func TestProjectionLockKeyRaisesForAnUnknownRelation(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	var key *int64
	err := pool.QueryRow(ctx, `SELECT position_projection_lock_key(0::regclass)`).Scan(&key)
	if err == nil {
		t.Fatalf("an unknown relation returned key %v instead of raising, so a caller would take no lock", key)
	}
	if !strings.Contains(err.Error(), "does not name an existing relation") {
		t.Errorf("the helper failed, but not by rejecting the unknown relation: %v", err)
	}
}

// Wrappers still spell the key by hand until each adopts the helper, and nothing else compares the
// two. Delete once no wrapper carries a literal.
func TestProjectionLockKeyMatchesTheWrapperLiteral(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	var same bool
	if err := pool.QueryRow(ctx, `
		SELECT hashtextextended('materialize_position_projection.public.position_anchorage_custody', 0)
		     = position_projection_lock_key($1::regclass)`, lockKeyProjection).Scan(&same); err != nil {
		t.Fatalf("comparing the helper with the hand-written key: %v", err)
	}
	if !same {
		t.Errorf("position_projection_lock_key(%s) no longer equals the key its wrapper spells by hand", lockKeyProjection)
	}
}

// The key must separate projections: two views sharing one key would serialise runs that have no
// reason to exclude each other.
func TestProjectionLockKeyDiffersPerProjection(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	var same bool
	if err := pool.QueryRow(ctx, `
		SELECT position_projection_lock_key($1::regclass)
		     = position_projection_lock_key('public.position_prime_allocation'::regclass)`, lockKeyProjection).Scan(&same); err != nil {
		t.Fatalf("comparing two relations' keys: %v", err)
	}
	if same {
		t.Error("two different projections render one lock key")
	}
}

// The key must not depend on the caller's search_path: a text || text operator shadowing pg_catalog
// would move the helper's key while the spine and the wrappers' literals kept theirs.
func TestProjectionLockKeyIgnoresTheCallersSearchPath(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, stmt := range []string{
		`CREATE SCHEMA shadow`,
		`CREATE FUNCTION shadow.cat(text, text) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT $2 || $1 $$`,
		`CREATE OPERATOR shadow.|| (LEFTARG = text, RIGHTARG = text, FUNCTION = shadow.cat)`,
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	var clean int64
	if err := tx.QueryRow(ctx, `SELECT position_projection_lock_key($1::regclass)`, lockKeyProjection).Scan(&clean); err != nil {
		t.Fatalf("key under the default path: %v", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path = shadow, pg_catalog, public`); err != nil {
		t.Fatal(err)
	}
	var shadowed int64
	if err := tx.QueryRow(ctx, `SELECT public.position_projection_lock_key($1::regclass)`, lockKeyProjection).Scan(&shadowed); err != nil {
		t.Fatalf("key under the shadowing path: %v", err)
	}
	if shadowed != clean {
		t.Errorf("a shadowing || on the caller's path moved the key from %d to %d", clean, shadowed)
	}
}
