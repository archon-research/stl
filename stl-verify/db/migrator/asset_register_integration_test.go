//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The asset register (20260916_120000, VEC-812) takes the reference-table form: the owner
// keeps UPDATE because child tables will FK it and the FK integrity probe runs as the
// parent's owner with FOR KEY SHARE, so append-only rests on reference_table_immutable()
// rather than on the owner's ACL. These tests pin both halves and the two-column contract.

func TestAssetRegisterGrantsTheAppRoleSelectAndInsertOnly(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	got := appRolePrivileges(ctx, t, pool, "asset")
	want := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": false, "DELETE": false, "TRUNCATE": false}
	for priv, wantHeld := range want {
		if got[priv] != wantHeld {
			t.Errorf("stl_readwrite %s on asset = %v, want %v", priv, got[priv], wantHeld)
		}
	}
	var readonlyCanSelect bool
	if err := pool.QueryRow(ctx, `SELECT has_table_privilege('stl_readonly', 'asset', 'SELECT')`).Scan(&readonlyCanSelect); err != nil {
		t.Fatalf("read stl_readonly grant: %v", err)
	}
	if !readonlyCanSelect {
		t.Error("stl_readonly must be able to SELECT from asset")
	}
}

func TestAssetRegisterOwnerKeepsUpdateForTheFKProbe(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	if !ownerACLHolds(ctx, t, pool, "asset", "UPDATE") {
		t.Error("the owner lost UPDATE on asset: every INSERT into a child that FKs it would fail the RI probe under the prod roles (20260714_160000)")
	}
	for _, priv := range []string{"DELETE", "TRUNCATE"} {
		if ownerACLHolds(ctx, t, pool, "asset", priv) {
			t.Errorf("the owner still holds %s on asset in the ACL", priv)
		}
	}
}

func TestAssetRegisterRejectsMutationViaTheImmutabilityTrigger(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	insertAsset(ctx, t, pool, "sec-t-immutable")

	cases := map[string]string{
		"update": `UPDATE asset SET source_system = 'mutated' WHERE security_id = 'sec-t-immutable'`,
		"delete": `DELETE FROM asset WHERE security_id = 'sec-t-immutable'`,
	}
	for name, stmt := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(ctx, stmt)
			assertSQLState(t, err, "P0001")
		})
	}
}

func TestAssetRegisterRefusesRowsOutsideItsContract(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	insertAsset(ctx, t, pool, "sec-t-taken")

	cases := []struct {
		name, securityID, sourceSystem, wantState string
	}{
		{"entity_node_id", "em-t-not-a-security", "test", "23514"},
		{"empty_source_system", "sec-t-blank", "", "23514"},
		{"second_row_for_one_security", "sec-t-taken", "test", "23505"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, `INSERT INTO asset (security_id, source_system) VALUES ($1, $2)`, tc.securityID, tc.sourceSystem)
			assertSQLState(t, err, tc.wantState)
		})
	}
}

func TestAssetRegisterLoginRoleCanAppendButNotUpdate(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	appPool, err := pgxpool.New(ctx, loginRoleDSN(t, pool))
	if err != nil {
		t.Fatalf("connect as stl_read_write: %v", err)
	}
	defer appPool.Close()

	if _, err := appPool.Exec(ctx, `INSERT INTO asset (security_id, source_system) VALUES ('sec-t-login', 'test')`); err != nil {
		t.Fatalf("the login role must be able to append a register row: %v", err)
	}
	// A WHERE that matches nothing: the refusal is the ACL check at executor start, not a
	// row-level effect and not the trigger.
	_, err = appPool.Exec(ctx, `UPDATE asset SET source_system = source_system WHERE security_id = 'nothing-matches'`)
	assertSQLState(t, err, "42501")
}

// TestAssetRegisterHoldsNothingButTheTwoKeys pins rule 2 of the design: the register is a
// key pair plus audit columns. An attribute of the security (symbol, name, class) belongs on
// the sec_node and its edges, and adding one here fails this test on purpose.
func TestAssetRegisterHoldsNothingButTheTwoKeys(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	rows, err := pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'asset' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read asset columns: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate asset columns: %v", err)
	}
	want := []string{"id", "security_id", "source_system", "run_id", "created_at"}
	if len(got) != len(want) {
		t.Fatalf("asset columns = %v, want exactly %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("asset columns = %v, want exactly %v", got, want)
		}
	}
}

func insertAsset(ctx context.Context, t *testing.T, pool *pgxpool.Pool, securityID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO asset (security_id, source_system) VALUES ($1, 'test')`, securityID); err != nil {
		t.Fatalf("seed asset %s: %v", securityID, err)
	}
}

func assertSQLState(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("got %v, want SQLSTATE %s", err, want)
	}
}

func appRolePrivileges(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) map[string]bool {
	t.Helper()
	held := map[string]bool{}
	for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT has_table_privilege('stl_readwrite', $1, $2)`, table, priv).Scan(&ok); err != nil {
			t.Fatalf("read stl_readwrite %s on %s: %v", priv, table, err)
		}
		held[priv] = ok
	}
	return held
}

// ownerACLHolds reads the ACL rather than has_table_privilege(): the owner in the harness is
// the bootstrap superuser, for which every privilege check reports true.
func ownerACLHolds(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table, priv string) bool {
	t.Helper()
	var held bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_class c,
			     aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
			WHERE c.oid = $1::regclass
			  AND a.grantee = c.relowner
			  AND a.privilege_type = $2
		)`, table, priv).Scan(&held); err != nil {
		t.Fatalf("read %s ACL for %s: %v", table, priv, err)
	}
	return held
}
