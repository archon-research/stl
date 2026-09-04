//go:build integration

package migrator_test

import (
	"bytes"
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
	// VEC-652: append-only from birth, REVOKE in the creating migration.
	"offchain_asset_price",
	"psm3_alm_shares",
	// VEC-402 (#625): SELECT+INSERT only, with the owner-side REVOKE too. position_classification
	// is NOT here — #625 no longer touches it, and its own migration still grants full DML.
	"position_state",
	"oracle_asset",
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
// This lives in db/migrator, not in a package that clones the migrated template: the
// clone arrives fully migrated, and this test needs to control migration order —
// db/migrator gives each test its own database and re-runs 20260122_140100 in it
// before the morpho migrations.
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
// One table is enough: the privilege semantics are identical across the converted set, and the
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

// triggerOnlyCacheTables are the derived `*_current` caches whose write path is closed
// structurally: the only two writers are the cache's own SECURITY DEFINER trigger and the
// migrator's backfill, and no login role holds a write grant on them (VEC-660).
//
// A separate list from convertedAppendOnlyTables rather than an addition to it, because the
// assertion differs at INSERT: a converted history table KEEPS INSERT — ingest appends to it —
// while a cache holds none at all, since stating the current row is the trigger's job and not a
// caller's.
//
// One entry for now. The four VEC-577 caches (borrower_current, borrower_collateral_current,
// sparklend_reserve_data_current, token_price_current) still carry the older
// `GRANT INSERT, UPDATE` form; aligning them is a follow-up.
var triggerOnlyCacheTables = []string{
	"allocation_position_current",
}

// TestTriggerOnlyCachesGrantTheAppRoleNoWrite asserts that the application role keeps SELECT and
// holds no INSERT, UPDATE or DELETE on a trigger-only cache, so any write reintroduced by a
// future change fails at runtime instead of silently forking the cache from history.
//
// The assertion is the CATALOGUE, for the reason TestConvertedTablesAreAppendOnly records: the
// harness migrates as the container's bootstrap superuser, which bypasses ACLs entirely.
// has_table_privilege on the NOLOGIN group role reports exactly what production will do. The
// end-to-end half is TestAllocationPositionCurrentIsWrittenOnlyByItsTrigger below.
//
// The REVOKE these grants rest on is load-bearing, not decorative: 20260122_140100 sets
// `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO
// stl_readwrite`, so a new migrator-owned table arrives with full DML whether or not its
// migration grants any.
func TestTriggerOnlyCachesGrantTheAppRoleNoWrite(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	for _, table := range triggerOnlyCacheTables {
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
			if !canSelect {
				t.Errorf("%s: stl_readwrite must keep SELECT — the reads select from the cache", table)
			}
			if canInsert || canUpdate || canDelete {
				t.Errorf("%s: stl_readwrite must hold no write grant, got insert=%v update=%v delete=%v — is the "+
					"REVOKE missing from the creating migration? ALTER DEFAULT PRIVILEGES hands it full DML "+
					"on every migrator-owned table, so only an explicit REVOKE removes them",
					table, canInsert, canUpdate, canDelete)
			}
		})
	}
}

// TestAllocationPositionCurrentIsWrittenOnlyByItsTrigger proves end-to-end what the catalogue
// assertion above proves by inspection: connecting as the login user the workers really use
// (stl_read_write, a member of the stl_readwrite group), a direct INSERT and a direct UPDATE on
// the cache are both refused with SQLSTATE 42501, while an INSERT into the allocation_position
// HISTORY still lands a cache row — the SECURITY DEFINER trigger writing it under the table
// owner's privileges.
//
// The third case is what makes the first two safe to ship: a REVOKE that also broke the trigger
// would be indistinguishable from a REVOKE that worked if only the refusals were asserted.
func TestAllocationPositionCurrentIsWrittenOnlyByItsTrigger(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	// The FK rows the history row needs, seeded as the owner: this test is about the cache's
	// grants, not the reference tables'.
	var primeID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM prime WHERE name = 'spark'`).Scan(&primeID); err != nil {
		t.Fatalf("read the seeded prime: %v", err)
	}
	var tokenID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, $1, 'APCGRANT', 18) RETURNING id`, bytes.Repeat([]byte{0xb1}, 20),
	).Scan(&tokenID); err != nil {
		t.Fatalf("seed the token: %v", err)
	}

	appPool, err := pgxpool.New(ctx, loginRoleDSN(t, pool))
	if err != nil {
		t.Fatalf("connect as stl_read_write: %v", err)
	}
	defer appPool.Close()

	proxy := bytes.Repeat([]byte{0xb2}, 20)
	txHash := bytes.Repeat([]byte{0xb3}, 32)

	t.Run("direct INSERT is refused", func(t *testing.T) {
		_, err := appPool.Exec(ctx, `
			INSERT INTO allocation_position_current
				(proxy_address, chain_id, token_id, balance, tx_amount, direction, tx_hash,
				 block_timestamp, block_number, block_version, log_index, processing_version)
			VALUES ($1, 1, $2, 1, 1, 'in', $3, now(), 1, 0, 0, 0)`, proxy, tokenID, txHash)
		requireInsufficientPrivilege(t, err, "INSERT INTO allocation_position_current")
	})

	// A WHERE that matches nothing: privileges are checked at executor start, so the refusal
	// cannot be confused with a row-level effect.
	t.Run("direct UPDATE is refused", func(t *testing.T) {
		_, err := appPool.Exec(ctx,
			`UPDATE allocation_position_current SET balance = balance WHERE chain_id = -1`)
		requireInsufficientPrivilege(t, err, "UPDATE allocation_position_current")
	})

	// The sanctioned path: an append to the history, through the real BEFORE trigger (which
	// assigns processing_version) and AFTER trigger (which writes the cache).
	t.Run("an append to the history still fills the cache", func(t *testing.T) {
		if _, err := appPool.Exec(ctx, `
			INSERT INTO allocation_position
				(chain_id, token_id, prime_id, proxy_address, balance, block_number, block_version,
				 tx_hash, log_index, tx_amount, direction, build_id)
			VALUES (1, $1, $2, $3, 4200, 21000000, 0, $4, 0, 4200, 'in', 0)`,
			tokenID, primeID, proxy, txHash); err != nil {
			t.Fatalf("append to allocation_position as stl_read_write: %v", err)
		}

		var balance, blockNumber int64
		if err := appPool.QueryRow(ctx, `
			SELECT balance::bigint, block_number FROM allocation_position_current
			WHERE proxy_address = $1 AND chain_id = 1 AND token_id = $2`, proxy, tokenID,
		).Scan(&balance, &blockNumber); err != nil {
			t.Fatalf("no cache row after the append — the SECURITY DEFINER trigger did not write it, or "+
				"stl_read_write lost SELECT: %v", err)
		}
		if balance != 4200 || blockNumber != 21000000 {
			t.Errorf("cache row = (balance %d, block %d), want (4200, 21000000)", balance, blockNumber)
		}
	})
}

// requireInsufficientPrivilege fails unless err is PostgreSQL's permission refusal.
func requireInsufficientPrivilege(t *testing.T, err error, statement string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded as stl_read_write; nothing but the trigger and the backfill may write the cache", statement)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("%s failed with %v, want SQLSTATE 42501 (insufficient_privilege)", statement, err)
	}
}
