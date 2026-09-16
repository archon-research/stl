//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The asset register (20260916_120000, VEC-812) takes the reference-table form: the owner keeps
// UPDATE for the FK integrity probe, so append-only rests on reference_table_immutable().

func TestAssetRegisterGrantsMatchTheReferenceTableForm(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("app_role_holds_select_and_insert_only", func(t *testing.T) {
		got := appRolePrivileges(ctx, t, pool, "asset")
		want := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": false, "DELETE": false, "TRUNCATE": false}
		for priv, wantHeld := range want {
			if got[priv] != wantHeld {
				t.Errorf("stl_readwrite %s on asset = %v, want %v", priv, got[priv], wantHeld)
			}
		}
	})
	t.Run("readonly_role_holds_select", func(t *testing.T) {
		var canSelect bool
		if err := pool.QueryRow(ctx, `SELECT has_table_privilege('stl_readonly', 'asset', 'SELECT')`).Scan(&canSelect); err != nil {
			t.Fatalf("read stl_readonly grant: %v", err)
		}
		if !canSelect {
			t.Error("stl_readonly must be able to SELECT from asset")
		}
	})
	t.Run("owner_keeps_update_and_loses_delete_and_truncate", func(t *testing.T) {
		if !ownerACLHolds(ctx, t, pool, "asset", "UPDATE") {
			t.Error("the owner lost UPDATE on asset: every INSERT into a child that FKs it would fail the RI probe under the prod roles (20260714_160000)")
		}
		for _, priv := range []string{"DELETE", "TRUNCATE"} {
			if ownerACLHolds(ctx, t, pool, "asset", priv) {
				t.Errorf("the owner still holds %s on asset in the ACL", priv)
			}
		}
	})
}

func TestAssetRegisterRejectsMutationViaTheImmutabilityTrigger(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	insertAsset(ctx, t, pool, "sec-t-immutable")

	cases := []struct{ name, stmt string }{
		{"update", `UPDATE asset SET source_system = 'mutated' WHERE security_id = 'sec-t-immutable'`},
		{"delete", `DELETE FROM asset WHERE security_id = 'sec-t-immutable'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, tc.stmt)
			assertSQLState(t, err, "P0001", tc.name+" of a register row")
		})
	}
}

func TestAssetRegisterRefusesRowsOutsideItsContract(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	insertAsset(ctx, t, pool, "sec-t-taken")

	cases := []struct {
		name, securityID, sourceSystem string
		runID                          any
		wantState                      string
	}{
		{"entity_node_id", "em-t-not-a-security", "test", nil, "23514"},
		{"empty_source_system", "sec-t-blank", "", nil, "23514"},
		{"second_row_for_one_security", "sec-t-taken", "test", nil, "23505"},
		{"unknown_writer_run", "sec-t-orphan-run", "test", int64(999999), "23503"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, `INSERT INTO asset (security_id, source_system, run_id) VALUES ($1, $2, $3)`,
				tc.securityID, tc.sourceSystem, tc.runID)
			assertSQLState(t, err, tc.wantState, "insert of a "+tc.name+" row")
		})
	}
}

func TestAssetRegisterLoginRoleCanAppendButNotUpdate(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	runID := openWriterRun(ctx, t, pool)

	appPool, err := pgxpool.New(ctx, loginRoleDSN(t, pool))
	if err != nil {
		t.Fatalf("connect as stl_read_write: %v", err)
	}
	defer appPool.Close()

	// A real run_id makes the INSERT probe writer_run's owner ACL, the path a tracked writer takes.
	if _, err := appPool.Exec(ctx, `INSERT INTO asset (security_id, source_system, run_id) VALUES ('sec-t-login', 'test', $1)`, runID); err != nil {
		t.Fatalf("the login role must be able to append a register row naming its writer run: %v", err)
	}
	// A WHERE that matches nothing: the refusal is the ACL check at executor start, not a
	// row-level effect and not the trigger.
	_, err = appPool.Exec(ctx, `UPDATE asset SET source_system = source_system WHERE security_id = 'nothing-matches'`)
	assertSQLState(t, err, "42501", "UPDATE as the login role")
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

func openWriterRun(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO writer_run (build_id, reference_snapshot, reference_effective_at)
		VALUES (0, pg_current_snapshot()::text, now()) RETURNING id`).Scan(&runID); err != nil {
		t.Fatalf("open a writer run: %v", err)
	}
	return runID
}

func assertSQLState(t *testing.T, err error, want, what string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("%s: got %v, want SQLSTATE %s", what, err, want)
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
// the bootstrap superuser, for which every privilege check reports true. acldefault() stands in
// for a NULL relacl, so a table that never had a REVOKE reads as "still held", not "no ACL".
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
