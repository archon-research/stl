//go:build integration

package migrator_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

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
	"asset_price",
	"psm3_alm_shares",
	// VEC-402: SELECT+INSERT only, with the owner-side REVOKE too.
	"position_state",
	"oracle_asset",
	// VEC-617 (#875): the combined master's two stores. Append-only from birth, with the owner's
	// mutation privileges revoked as well — the position_state pattern, which does the same
	// (20260818_130000), and which is safe on all three because nothing FKs them, so the
	// owner-side revoke cannot break an RI probe. What differs is how the owner is found: these
	// two derive it from pg_class.relowner instead of naming stl_migrator, so the revoke also
	// fires in CI, where that role does not exist and position_state's owner-side revoke
	// silently no-ops.
	//
	// This list is what answers "which tables are append-only" for the ACL-enforced set;
	// TestSecStoreWave1IsAppendOnlyUnderTheRealRoles covers the vocabularies, which are FK
	// parents and enforce append-only through reference_table_immutable() (20260714_160000, #574).
	"sec_node",
	"sec_edge",
	// VEC-475 (#711): append-only from birth; the creating migration REVOKEs all seven.
	"uniswap_v4_pool_manager",
	"uniswap_v4_pool",
	"uniswap_v4_pool_state",
	"uniswap_v4_swap",
	"uniswap_v4_liquidity_event",
	"uniswap_v4_tick",
	"uniswap_v4_pool_event",
	// VEC-572 (#736): append-only from birth, REVOKE in the creating migration.
	"uniswap_v4_position",
	// VEC-401: run records are append-only; SELECT+INSERT only for the app role.
	"position_projection_run",
	"position_projection_refusal",
	// VEC-598: provenance tables. The owner keeps UPDATE for the FK integrity probe
	// (20260714_160000); a statement-level trigger raises on any real mutation.
	"build_registry",
	"writer_run",
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
// The four VEC-577 caches (borrower_current, borrower_collateral_current,
// sparklend_reserve_data_current, token_price_current) still carry the older
// `GRANT INSERT, UPDATE` form; aligning them is a follow-up.
var triggerOnlyCacheTables = []string{
	"allocation_position_current",
	"morpho_market_position_current",
	// VEC-659: the two Morpho state caches the backed-breakdown read joins beside it.
	"morpho_vault_state_current",
	"morpho_market_state_current",
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

// seedMorphoMarket inserts, as the owner, the protocol, loan and collateral tokens and Blue
// market a Morpho history row references, with addresses derived from seed so two callers in
// one database never collide. Returns (protocolID, loanTokenID, collateralTokenID, marketID).
func seedMorphoMarket(ctx context.Context, t *testing.T, pool *pgxpool.Pool, seed byte, tag string) (int64, int64, int64, int64) {
	t.Helper()
	var protocolID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
		VALUES (1, $1, $2, 'morpho_blue', 1, now()) RETURNING id`,
		bytes.Repeat([]byte{seed}, 20), "Morpho Blue Grant Test "+tag,
	).Scan(&protocolID); err != nil {
		t.Fatalf("seed the protocol: %v", err)
	}
	var loanTokenID, collateralTokenID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, $1, $2, 6) RETURNING id`, bytes.Repeat([]byte{seed + 1}, 20), tag+"LOAN",
	).Scan(&loanTokenID); err != nil {
		t.Fatalf("seed the loan token: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, $1, $2, 8) RETURNING id`, bytes.Repeat([]byte{seed + 2}, 20), tag+"COLL",
	).Scan(&collateralTokenID); err != nil {
		t.Fatalf("seed the collateral token: %v", err)
	}
	var marketID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO morpho_market
			(chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
			 oracle_address, irm_address, lltv, created_at_block)
		VALUES (1, $1, $2, $3, $4, $5, $5, 860000000000000000, 1) RETURNING id`,
		protocolID, bytes.Repeat([]byte{seed + 3}, 32), loanTokenID, collateralTokenID,
		bytes.Repeat([]byte{seed + 4}, 20),
	).Scan(&marketID); err != nil {
		t.Fatalf("seed the morpho market: %v", err)
	}
	return protocolID, loanTokenID, collateralTokenID, marketID
}

// TestMorphoMarketPositionCurrentIsWrittenOnlyByItsTrigger mirrors the allocation test above for
// the morpho cache (VEC-753): direct writes as the login role are refused, while an append to the
// morpho_market_position HISTORY still lands a cache row through the SECURITY DEFINER trigger.
func TestMorphoMarketPositionCurrentIsWrittenOnlyByItsTrigger(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	// The FK rows the history row needs, seeded as the owner: this test is about the cache's
	// grants, not the reference tables'.
	_, _, _, marketID := seedMorphoMarket(ctx, t, pool, 0xc1, "MMPC")
	var userID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO "user" (chain_id, address) VALUES (1, $1) RETURNING id`,
		bytes.Repeat([]byte{0xc6}, 20),
	).Scan(&userID); err != nil {
		t.Fatalf("seed the user: %v", err)
	}

	appPool, err := pgxpool.New(ctx, loginRoleDSN(t, pool))
	if err != nil {
		t.Fatalf("connect as stl_read_write: %v", err)
	}
	defer appPool.Close()

	t.Run("direct INSERT is refused", func(t *testing.T) {
		_, err := appPool.Exec(ctx, `
			INSERT INTO morpho_market_position_current
				(user_id, morpho_market_id, supply_shares, borrow_shares, collateral,
				 supply_assets, borrow_assets, block_timestamp, block_number, block_version,
				 processing_version)
			VALUES ($1, $2, 0, 0, 0, 0, 0, now(), 1, 0, 0)`, userID, marketID)
		requireInsufficientPrivilege(t, err, "INSERT INTO morpho_market_position_current")
	})

	// A WHERE that matches nothing: privileges are checked at executor start, so the refusal
	// cannot be confused with a row-level effect.
	t.Run("direct UPDATE is refused", func(t *testing.T) {
		_, err := appPool.Exec(ctx,
			`UPDATE morpho_market_position_current SET collateral = collateral WHERE user_id = -1`)
		requireInsufficientPrivilege(t, err, "UPDATE morpho_market_position_current")
	})

	// The sanctioned path: an append to the history, through the real BEFORE trigger (which
	// assigns processing_version) and AFTER trigger (which writes the cache).
	t.Run("an append to the history still fills the cache", func(t *testing.T) {
		if _, err := appPool.Exec(ctx, `
			INSERT INTO morpho_market_position
				(user_id, morpho_market_id, block_number, block_version, "timestamp",
				 supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, build_id)
			VALUES ($1, $2, 21000000, 0, now(), 0, 77, 4200, 0, 88, 0)`,
			userID, marketID); err != nil {
			t.Fatalf("append to morpho_market_position as stl_read_write: %v", err)
		}

		var collateral, borrowAssets, blockNumber int64
		if err := appPool.QueryRow(ctx, `
			SELECT collateral::bigint, borrow_assets::bigint, block_number
			FROM morpho_market_position_current
			WHERE user_id = $1 AND morpho_market_id = $2`, userID, marketID,
		).Scan(&collateral, &borrowAssets, &blockNumber); err != nil {
			t.Fatalf("no cache row after the append — the SECURITY DEFINER trigger did not write it, or "+
				"stl_read_write lost SELECT: %v", err)
		}
		if collateral != 4200 || borrowAssets != 88 || blockNumber != 21000000 {
			t.Errorf("cache row = (collateral %d, borrow_assets %d, block %d), want (4200, 88, 21000000)",
				collateral, borrowAssets, blockNumber)
		}
	})

	// An older row must not regress the cache: the newer-wins guard is what makes the cache a
	// function of history rather than of arrival order.
	t.Run("an older history row does not regress the cache", func(t *testing.T) {
		if _, err := appPool.Exec(ctx, `
			INSERT INTO morpho_market_position
				(user_id, morpho_market_id, block_number, block_version, "timestamp",
				 supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, build_id)
			VALUES ($1, $2, 20999999, 0, now() - interval '1 hour', 0, 1, 1, 0, 1, 0)`,
			userID, marketID); err != nil {
			t.Fatalf("append the older row: %v", err)
		}
		var blockNumber int64
		if err := appPool.QueryRow(ctx, `
			SELECT block_number FROM morpho_market_position_current
			WHERE user_id = $1 AND morpho_market_id = $2`, userID, marketID,
		).Scan(&blockNumber); err != nil {
			t.Fatalf("read the cache row back: %v", err)
		}
		if blockNumber != 21000000 {
			t.Errorf("cache regressed to block %d after an older append, want 21000000", blockNumber)
		}
	})
}

// TestMorphoStateCurrentCachesAreWrittenOnlyByTheirTriggers mirrors the position test above for
// the two Morpho state caches (VEC-659): direct writes as the login role are refused, while an
// append to the morpho_vault_state / morpho_market_state HISTORY still lands a cache row through
// the SECURITY DEFINER trigger, and an older row arriving late does not regress it.
func TestMorphoStateCurrentCachesAreWrittenOnlyByTheirTriggers(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	// The FK rows the history rows need, seeded as the owner: this test is about the caches'
	// grants, not the reference tables'.
	protocolID, loanTokenID, _, marketID := seedMorphoMarket(ctx, t, pool, 0xd1, "MSC")
	var vaultID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO morpho_vault
			(chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
		VALUES (1, $1, $2, 'Morpho State Grant Vault', 'msgv', $3, 1, 1) RETURNING id`,
		protocolID, bytes.Repeat([]byte{0xd6}, 20), loanTokenID,
	).Scan(&vaultID); err != nil {
		t.Fatalf("seed the morpho vault: %v", err)
	}

	appPool, err := pgxpool.New(ctx, loginRoleDSN(t, pool))
	if err != nil {
		t.Fatalf("connect as stl_read_write: %v", err)
	}
	defer appPool.Close()

	const newerBlock, olderBlock = int64(21000000), int64(20999999)
	newerAt := time.Now().UTC().Truncate(time.Second)
	olderAt := newerAt.Add(-time.Hour)

	caches := []struct {
		cache       string
		keyID       int64
		insertCache string // a direct INSERT of the key into the cache; must be refused
		updateCache string // a direct UPDATE matching nothing; must be refused at executor start
		appendHist  string // ($1 key, $2 block, $3 timestamp, $4 value): an append to the history;
		//                    the market row carries an in-bounds last_update epoch so the
		//                    trigger's canonical cast runs rather than its NULL arm
		readCache string // ($1 key): the value the append carried, and its block
	}{
		{
			cache: "morpho_vault_state_current",
			keyID: vaultID,
			insertCache: `
				INSERT INTO morpho_vault_state_current
					(morpho_vault_id, total_assets, total_shares, block_timestamp,
					 block_number, block_version, processing_version)
				VALUES ($1, 0, 0, now(), 1, 0, 0)`,
			updateCache: `UPDATE morpho_vault_state_current SET total_assets = total_assets WHERE morpho_vault_id = -1`,
			appendHist: `
				INSERT INTO morpho_vault_state
					(morpho_vault_id, block_number, block_version, "timestamp", total_assets, total_shares, build_id)
				VALUES ($1, $2, 0, $3, $4, $4, 0)`,
			readCache: `
				SELECT total_assets::bigint, block_number FROM morpho_vault_state_current
				WHERE morpho_vault_id = $1`,
		},
		{
			cache: "morpho_market_state_current",
			keyID: marketID,
			insertCache: `
				INSERT INTO morpho_market_state_current
					(morpho_market_id, total_supply_assets, total_supply_shares, total_borrow_assets,
					 total_borrow_shares, last_update_at, fee, block_timestamp,
					 block_number, block_version, processing_version)
				VALUES ($1, 0, 0, 0, 0, now(), 0, now(), 1, 0, 0)`,
			updateCache: `UPDATE morpho_market_state_current SET fee = fee WHERE morpho_market_id = -1`,
			appendHist: `
				INSERT INTO morpho_market_state
					(morpho_market_id, block_number, block_version, "timestamp",
					 total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
					 last_update, fee, build_id)
				VALUES ($1, $2, 0, $3, $4, $4, 0, 0, 1800000000, 0, 0)`,
			readCache: `
				SELECT total_supply_assets::bigint, block_number FROM morpho_market_state_current
				WHERE morpho_market_id = $1`,
		},
	}

	for _, tc := range caches {
		t.Run(tc.cache, func(t *testing.T) {
			t.Run("direct INSERT is refused", func(t *testing.T) {
				_, err := appPool.Exec(ctx, tc.insertCache, tc.keyID)
				requireInsufficientPrivilege(t, err, "INSERT INTO "+tc.cache)
			})

			t.Run("direct UPDATE is refused", func(t *testing.T) {
				_, err := appPool.Exec(ctx, tc.updateCache)
				requireInsufficientPrivilege(t, err, "UPDATE "+tc.cache)
			})

			// The sanctioned path: an append to the history, through the real BEFORE trigger
			// (which assigns processing_version) and AFTER trigger (which writes the cache).
			t.Run("an append to the history still fills the cache", func(t *testing.T) {
				if _, err := appPool.Exec(ctx, tc.appendHist, tc.keyID, newerBlock, newerAt, int64(4200)); err != nil {
					t.Fatalf("append to the history as stl_read_write: %v", err)
				}
				var value, blockNumber int64
				if err := appPool.QueryRow(ctx, tc.readCache, tc.keyID).Scan(&value, &blockNumber); err != nil {
					t.Fatalf("no cache row after the append — the SECURITY DEFINER trigger did not write it, or "+
						"stl_read_write lost SELECT: %v", err)
				}
				if value != 4200 || blockNumber != newerBlock {
					t.Errorf("cache row = (value %d, block %d), want (4200, %d)", value, blockNumber, newerBlock)
				}
			})

			// An older row must not regress the cache: the newer-wins guard is what makes the cache
			// a function of history rather than of arrival order.
			t.Run("an older history row does not regress the cache", func(t *testing.T) {
				if _, err := appPool.Exec(ctx, tc.appendHist, tc.keyID, olderBlock, olderAt, int64(1)); err != nil {
					t.Fatalf("append the older row: %v", err)
				}
				var value, blockNumber int64
				if err := appPool.QueryRow(ctx, tc.readCache, tc.keyID).Scan(&value, &blockNumber); err != nil {
					t.Fatalf("read the cache row back: %v", err)
				}
				if value != 4200 || blockNumber != newerBlock {
					t.Errorf("cache regressed to (value %d, block %d) after an older append, want (4200, %d)",
						value, blockNumber, newerBlock)
				}
			})
		})
	}
}
