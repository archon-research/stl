//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// convertedAppendOnlyTables are the tables whose creating migration revokes UPDATE and
// DELETE from the application role. Keep in sync with the strict append-only rule in
// db/migrations/AGENTS.md; a table listed there without a REVOKE fails here.
//
// An explicit list rather than an enumeration: the converted set is deliberately small
// and named, the rest of the schema is still being converted table by table, and an
// enumeration would either pass vacuously or fail on every legacy table.
var convertedAppendOnlyTables = []string{
	"morpho_adapter",
	"morpho_adapter_membership",
	"morpho_adapter_state",
	"morpho_vault_cap",
	"morpho_vault_fee",
	"position_state",
	// Its sanctioned UPDATE channel is column-scoped, and has_table_privilege reports table-level
	// privilege only, so the narrow GRANT UPDATE (deal_type_code, …) does not flip the assertion
	// below — the table-wide UPDATE/DELETE it used to hold is what must stay revoked.
	"position_classification",
}

// TestConvertedTablesAreAppendOnly asserts the DB-level half of the append-only rule:
// the application role keeps SELECT and INSERT on every converted table and holds
// neither UPDATE nor DELETE, so a `DO UPDATE`, an `UPDATE` or a `DELETE` reintroduced by
// a future change fails at runtime instead of silently corrupting history.
//
// It asserts the CATALOGUE rather than a denied statement, because the harness connects
// as the container's bootstrap superuser (testutil.StartTimescaleDBForMain sets
// POSTGRES_USER=test) and a superuser bypasses ACLs entirely — the trap
// 20260714_130000 and 20260714_160000 both recorded in writing. has_table_privilege on
// the NOLOGIN group role needs no SET ROLE and reports exactly what production will do.
// The end-to-end half is TestConvertedTablesRejectUpdateAsTheLoginRole below.
//
// This lives in db/migrator, not in a package that uses testutil.SetupTestSchema: a
// per-test SCHEMA is outside `ALTER DEFAULT PRIVILEGES IN SCHEMA public`, so
// stl_readwrite would hold no grants there at all and every assertion would pass
// vacuously. db/migrator gives each test its own DATABASE and re-runs
// 20260122_140100 in it before the morpho migrations.
func TestConvertedTablesAreAppendOnly(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	for _, table := range convertedAppendOnlyTables {
		t.Run(table, func(t *testing.T) {
			var canSelect, canInsert, canUpdate, canDelete bool
			if err := pool.QueryRow(ctx, `
				SELECT has_table_privilege('stl_readwrite', $1, 'SELECT'),
				       has_table_privilege('stl_readwrite', $1, 'INSERT'),
				       has_table_privilege('stl_readwrite', $1, 'UPDATE'),
				       has_table_privilege('stl_readwrite', $1, 'DELETE')`, table,
			).Scan(&canSelect, &canInsert, &canUpdate, &canDelete); err != nil {
				t.Fatalf("read grants for %s: %v", table, err)
			}
			if !canSelect || !canInsert {
				t.Errorf("%s: stl_readwrite must keep SELECT+INSERT, got select=%v insert=%v", table, canSelect, canInsert)
			}
			if canUpdate || canDelete {
				t.Errorf("%s: stl_readwrite must not hold UPDATE/DELETE, got update=%v delete=%v — is the REVOKE missing from the creating migration?", table, canUpdate, canDelete)
			}
		})
	}
}

// TestConvertedTablesRejectUpdateAsTheLoginRole proves end-to-end what the catalogue
// assertion above proves by inspection: connecting as the login user the workers really
// use (stl_read_write, a member of the stl_readwrite group — see
// k8s/base/morpho-indexer + 20260122_140100), an UPDATE on a converted table is refused
// with SQLSTATE 42501 before it can match a single row.
//
// One table is enough: the privilege semantics are identical across the five, and the
// catalogue test is what enumerates them. What this adds is the proof that the group
// membership actually carries the revoke through to the role that connects.
func TestConvertedTablesRejectUpdateAsTheLoginRole(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	appPool, err := pgxpool.New(ctx, loginRoleDSN(t, pool))
	if err != nil {
		t.Fatalf("connect as stl_read_write: %v", err)
	}
	defer appPool.Close()

	// A WHERE that matches nothing: privileges are checked at executor start, so the
	// refusal cannot be confused with a row-level effect.
	_, err = appPool.Exec(ctx, `UPDATE morpho_adapter SET asset_token_id = asset_token_id WHERE id = -1`)
	if err == nil {
		t.Fatal("UPDATE on morpho_adapter succeeded as stl_read_write; the REVOKE is not reaching the login role")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("UPDATE failed with %v, want SQLSTATE 42501 (insufficient_privilege)", err)
	}

	// The same role must still be able to read and append.
	var n int
	if err := appPool.QueryRow(ctx, `SELECT count(*) FROM morpho_adapter`).Scan(&n); err != nil {
		t.Errorf("stl_read_write must keep SELECT on a converted table: %v", err)
	}
}

// loginRoleDSN rewrites the admin DSN to connect as the stl_read_write login user, whose
// password 20260122_140100 creates as a literal placeholder (Terraform sets the real one
// in the deployed environments).
func loginRoleDSN(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	cfg := pool.Config().ConnConfig
	return fmt.Sprintf("postgres://stl_read_write:%s@%s:%d/%s?sslmode=disable",
		url.QueryEscape("PLACEHOLDER_SET_VIA_TERRAFORM"), cfg.Host, cfg.Port, cfg.Database)
}
