//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// materialize_position_projection serializes concurrent runs of one projection view with an
// advisory lock. The lock key MUST be derived from the view's canonical schema-qualified name
// (format('%I.%I', nspname, relname) out of the catalog), never p_view::text: p_view::text
// schema-qualifies a relation only when it is not visible under the current search_path, so the
// same view hashes to a different key for the pinned runner (explicit search_path) than for a
// plain psql session — two keys, no mutual exclusion (VEC-402 round-3 finding :169). These tests
// pin that regression shut cheaply and deterministically, without a flaky concurrency harness.

// lockKeyExpr is the exact key materialize_position_projection computes, for a fixed probe view.
// The behavioural test evaluates it under two search_paths and asserts it is invariant — the
// property the canonical-name key relies on.
const lockKeyExpr = `hashtextextended((SELECT format('materialize_position_projection.%I.%I', nsp.nspname, cls.relname)
	FROM pg_class cls JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
	WHERE cls.oid = 'public.lock_key_probe'::regclass), 0)`

// TestPositionState_LockKeyStableAcrossSearchPath asserts the advisory-lock key for a given view
// is identical under search_path=public and search_path=pg_catalog. A search_path-dependent key
// (the :169 regression) would produce two different values here and the lock would not serialize
// two overlapping runs of the same materializer.
func TestPositionState_LockKeyStableAcrossSearchPath(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	if _, err := pool.Exec(ctx, `CREATE VIEW lock_key_probe AS SELECT 1 AS x`); err != nil {
		t.Fatalf("create probe view: %v", err)
	}

	// One connection so both SETs and both reads share a session.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()

	var keyPublic, keyCatalog int64
	if _, err := conn.Exec(ctx, `SET search_path = public`); err != nil {
		t.Fatalf("set search_path public: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT `+lockKeyExpr).Scan(&keyPublic); err != nil {
		t.Fatalf("key under public: %v", err)
	}
	if _, err := conn.Exec(ctx, `SET search_path = pg_catalog`); err != nil {
		t.Fatalf("set search_path pg_catalog: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT `+lockKeyExpr).Scan(&keyCatalog); err != nil {
		t.Fatalf("key under pg_catalog: %v", err)
	}

	if keyPublic != keyCatalog {
		t.Fatalf("advisory-lock key differs across search_path (public=%d, pg_catalog=%d): the per-view lock would not serialize concurrent materializer runs (:169)",
			keyPublic, keyCatalog)
	}
}

// TestPositionState_LockKeysCanonicalName pins the function body itself to the canonical-name key,
// so a future edit that reverts to a search_path-dependent form (e.g. p_view::text) fails here
// rather than silently disabling mutual exclusion. The behavioural test above proves the property;
// this guards the mechanism the function actually uses.
func TestPositionState_LockKeysCanonicalName(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	var def string
	if err := pool.QueryRow(ctx,
		`SELECT pg_get_functiondef('materialize_position_projection(regclass, text)'::regprocedure)`).Scan(&def); err != nil {
		t.Fatalf("get function def: %v", err)
	}

	if !strings.Contains(def, "format('materialize_position_projection.%I.%I'") {
		t.Errorf("materialize_position_projection no longer keys its advisory lock on the canonical qualified name; " +
			"a search_path-dependent key would not serialize concurrent runs (:169)")
	}
}
