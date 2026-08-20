//go:build integration

package postgres

import (
	"bytes"
	"context"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	uniswapV4SchemaName = "test_uniswap_v4_migration"

	uniswapV4PoolManagerHex = "\\x000000000004444c5dc75cB358380D2e3dE08A90"
	uniswapV4StateViewHex   = "\\x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"
	uniswapV4DeployBlock    = 21688329

	uniswapV4NativeCurrencyHex = "\\x0000000000000000000000000000000000000000"
	uniswapV4EthPlaceholderHex = "\\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
	uniswapV4NoHooksHex        = "\\x0000000000000000000000000000000000000000"

	uniswapV4ReadWriteRole = "stl_readwrite"
)

var uniswapV4TestPool *pgxpool.Pool

func init() {
	registerTestFileSetup(uniswapV4SchemaName, func() {
		uniswapV4TestPool = testutil.SetupSchemaForMain(sharedDSN, uniswapV4SchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, uniswapV4TestPool, uniswapV4SchemaName)
	})
}

// uniswapV4Tables are the 7 tables created by
// 20260819_120000_create_uniswap_v4_tables.sql.
var uniswapV4Tables = []string{
	"uniswap_v4_pool_manager",
	"uniswap_v4_pool",
	"uniswap_v4_pool_state",
	"uniswap_v4_swap",
	"uniswap_v4_liquidity_event",
	"uniswap_v4_tick",
	"uniswap_v4_pool_event",
}

// uniswapV4VersionedTables is every table in the migration: registry rows are
// versioned and append-only too, so all 7 carry a processing_version trigger.
var uniswapV4VersionedTables = uniswapV4Tables

// uniswapV4Hypertables excludes uniswap_v4_tick, which is append-on-change and
// deliberately a plain table.
var uniswapV4Hypertables = []string{
	"uniswap_v4_pool_state",
	"uniswap_v4_swap",
	"uniswap_v4_liquidity_event",
	"uniswap_v4_pool_event",
}

func TestUniswapV4MigrationCreatesTables(t *testing.T) {
	ctx := context.Background()

	for _, table := range uniswapV4Tables {
		var exists bool
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_name = $1
			)`, table).Scan(&exists); err != nil {
			t.Fatalf("checking table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s does not exist", table)
		}
	}
}

func TestUniswapV4MigrationRegistersHypertables(t *testing.T) {
	ctx := context.Background()

	for _, table := range uniswapV4Hypertables {
		var exists bool
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM _timescaledb_catalog.hypertable h
				JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id
				WHERE h.table_name = $1
				  AND d.column_name = 'block_timestamp'
			)`, table).Scan(&exists); err != nil {
			t.Fatalf("checking hypertable %s: %v", table, err)
		}
		if !exists {
			t.Errorf("hypertable %s is not registered on block_timestamp", table)
		}
	}
}

func TestUniswapV4TickIsNotAHypertable(t *testing.T) {
	ctx := context.Background()

	var exists bool
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM _timescaledb_catalog.hypertable
			WHERE table_name = 'uniswap_v4_tick'
		)`).Scan(&exists); err != nil {
		t.Fatalf("checking uniswap_v4_tick hypertable registration: %v", err)
	}
	if exists {
		t.Error("uniswap_v4_tick should be a regular append-on-change table, not a hypertable")
	}
}

func TestUniswapV4ProcessingVersionTriggersExist(t *testing.T) {
	ctx := context.Background()

	for _, table := range uniswapV4VersionedTables {
		var exists bool
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_trigger tg
				JOIN pg_class c ON c.oid = tg.tgrelid
				JOIN pg_proc p ON p.oid = tg.tgfoid
				WHERE NOT tg.tgisinternal
				  AND c.relname = $1
				  AND p.proname LIKE 'assign_processing_version_uniswap_v4_%'
			)`, table).Scan(&exists); err != nil {
			t.Fatalf("checking processing_version trigger on %s: %v", table, err)
		}
		if !exists {
			t.Errorf("assign_processing_version_uniswap_v4_* trigger missing on %s", table)
		}
	}
}

func TestUniswapV4ProcessingVersionTriggersForceCustomPlan(t *testing.T) {
	ctx := context.Background()

	for _, table := range uniswapV4VersionedTables {
		var proconfig []string
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT COALESCE(p.proconfig, '{}')
			FROM pg_trigger tg
			JOIN pg_class c ON c.oid = tg.tgrelid
			JOIN pg_proc p ON p.oid = tg.tgfoid
			WHERE NOT tg.tgisinternal
			  AND c.relname = $1
			  AND p.proname LIKE 'assign_processing_version_uniswap_v4_%'`,
			table).Scan(&proconfig); err != nil {
			t.Fatalf("reading proconfig for %s trigger function: %v", table, err)
		}
		if !slices.Contains(proconfig, "plan_cache_mode=force_custom_plan") {
			t.Errorf("%s trigger function proconfig = %v, want plan_cache_mode=force_custom_plan", table, proconfig)
		}
	}
}

func TestUniswapV4RegistryTablesAreUniquePerVersion(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		table   string
		columns []string
	}{
		{"uniswap_v4_pool_manager", []string{"chain_id", "processing_version"}},
		{"uniswap_v4_pool", []string{"chain_id", "pool_id", "processing_version"}},
	}

	for _, tc := range cases {
		var exists bool
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_constraint con
				JOIN pg_class c ON c.oid = con.conrelid
				WHERE c.relname = $1
				  AND con.contype = 'u'
				  AND con.conkey = (
				      SELECT array_agg(a.attnum ORDER BY a.attnum)
				      FROM pg_attribute a
				      WHERE a.attrelid = c.oid
				        AND a.attname = ANY($2::text[])
				  )
			)`, tc.table, tc.columns).Scan(&exists); err != nil {
			t.Fatalf("checking %s UNIQUE%v: %v", tc.table, tc.columns, err)
		}
		if !exists {
			t.Errorf("UNIQUE%v constraint missing on %s", tc.columns, tc.table)
		}
	}
}

func TestUniswapV4ForeignKeys(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		table    string
		column   string
		refTable string
	}{
		{"uniswap_v4_pool_manager", "chain_id", "chain"},
		{"uniswap_v4_pool_manager", "protocol_id", "protocol"},
		{"uniswap_v4_pool", "chain_id", "chain"},
		{"uniswap_v4_pool", "currency0_token_id", "token"},
		{"uniswap_v4_pool", "currency1_token_id", "token"},
		{"uniswap_v4_pool_state", "pool_id", "uniswap_v4_pool"},
		{"uniswap_v4_swap", "pool_id", "uniswap_v4_pool"},
		{"uniswap_v4_liquidity_event", "pool_id", "uniswap_v4_pool"},
		{"uniswap_v4_tick", "pool_id", "uniswap_v4_pool"},
		{"uniswap_v4_pool_event", "pool_id", "uniswap_v4_pool"},
	}

	for _, tc := range cases {
		var exists bool
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_constraint con
				JOIN pg_class c ON c.oid = con.conrelid
				JOIN pg_class rc ON rc.oid = con.confrelid
				JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = con.conkey[1]
				WHERE c.relname = $1
				  AND con.contype = 'f'
				  AND array_length(con.conkey, 1) = 1
				  AND a.attname = $2
				  AND rc.relname = $3
			)`, tc.table, tc.column, tc.refTable).Scan(&exists); err != nil {
			t.Fatalf("checking FK %s.%s -> %s: %v", tc.table, tc.column, tc.refTable, err)
		}
		if !exists {
			t.Errorf("FK %s.%s -> %s missing", tc.table, tc.column, tc.refTable)
		}
	}
}

func TestUniswapV4PoolRejectsShortPoolID(t *testing.T) {
	ctx := context.Background()
	wstETH := seedUniswapV4Token(t, ctx, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18)
	usdc := seedUniswapV4Token(t, ctx, "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6)

	_, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO uniswap_v4_pool
		    (chain_id, pool_id, currency0, currency1,
		     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block)
		VALUES (1, '\x00112233445566778899aabbccddeeff00112233445566778899aabbccddee'::bytea,
		        '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,
		        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
		        $1, $2, 3000, 60, '\x0000000000000000000000000000000000000000'::bytea, 22000000)`,
		wstETH, usdc)
	if err == nil {
		t.Fatal("31-byte pool_id was accepted, want a CHECK violation")
	}
}

func TestUniswapV4PoolRejectsUnorderedCurrencies(t *testing.T) {
	ctx := context.Background()
	wstETH := seedUniswapV4Token(t, ctx, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18)
	usdc := seedUniswapV4Token(t, ctx, "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6)

	_, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO uniswap_v4_pool
		    (chain_id, pool_id, currency0, currency1,
		     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block)
		VALUES (1, '\xdeadbeef00000000000000000000000000000000000000000000000000000001'::bytea,
		        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
		        '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,
		        $1, $2, 3000, 60, '\x0000000000000000000000000000000000000000'::bytea, 22000000)`,
		usdc, wstETH)
	if err == nil {
		t.Fatal("currency0 > currency1 was accepted, want a CHECK violation")
	}
}

func TestUniswapV4TablesAreAppendOnlyForReadWriteRole(t *testing.T) {
	ctx := context.Background()

	var roleExists bool
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, uniswapV4ReadWriteRole).Scan(&roleExists); err != nil {
		t.Fatalf("checking for role %s: %v", uniswapV4ReadWriteRole, err)
	}
	if !roleExists {
		t.Skipf("role %s does not exist in this database", uniswapV4ReadWriteRole)
	}

	// INSERT/SELECT must stay granted: they prove the default-privileges grant
	// reached these tables, so a false UPDATE/DELETE is the REVOKE, not a gap.
	for _, table := range uniswapV4Tables {
		for privilege, want := range map[string]bool{
			"SELECT": true, "INSERT": true, "UPDATE": false, "DELETE": false, "TRUNCATE": false,
		} {
			var granted bool
			if err := uniswapV4TestPool.QueryRow(ctx,
				`SELECT has_table_privilege($1, $2, $3)`,
				uniswapV4ReadWriteRole, table, privilege).Scan(&granted); err != nil {
				t.Fatalf("checking %s privilege on %s: %v", privilege, table, err)
			}
			if granted != want {
				t.Errorf("has_table_privilege(%s, %s, %s) = %v, want %v",
					uniswapV4ReadWriteRole, table, privilege, granted, want)
			}
		}
	}
}

func TestUniswapV4ProcessingVersionTriggerAppendsPoolManagerCorrection(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4PoolManager(t, ctx)

	var protocolID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT protocol_id FROM uniswap_v4_pool_manager WHERE chain_id = 1 ORDER BY processing_version DESC LIMIT 1`,
	).Scan(&protocolID); err != nil {
		t.Fatalf("reading seeded protocol_id: %v", err)
	}

	const correctionBuildID = 1
	insert := `
		INSERT INTO uniswap_v4_pool_manager
		    (chain_id, protocol_id, state_view_address, deploy_block, build_id)
		VALUES (1, $1, $2::bytea, $3, $4)
		ON CONFLICT (chain_id, processing_version) DO NOTHING
		RETURNING processing_version`

	var pv int
	if err := uniswapV4TestPool.QueryRow(ctx, insert,
		protocolID, uniswapV4StateViewHex,
		uniswapV4DeployBlock, correctionBuildID).Scan(&pv); err != nil {
		t.Fatalf("appending a corrected pool manager version: %v", err)
	}
	if pv != 1 {
		t.Errorf("processing_version = %d for the first write under build %d, want 1", pv, correctionBuildID)
	}

	tag, err := uniswapV4TestPool.Exec(ctx, insert,
		protocolID, uniswapV4StateViewHex,
		uniswapV4DeployBlock, correctionBuildID)
	if err != nil {
		t.Fatalf("re-inserting under the same build: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Errorf("same-build re-insert wrote %d rows, want 0 (it must be an idempotent no-op, not a correction)", tag.RowsAffected())
	}
}

func TestUniswapV4ProcessingVersionTriggerAppendsPoolCorrection(t *testing.T) {
	ctx := context.Background()
	const poolIDHex = "\\x22000000000000000000000000000000000000000000000000000000000000ff"
	insertTestUniswapV4Pool(t, ctx, poolIDHex)

	wstETH := seedUniswapV4Token(t, ctx, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18)
	usdc := seedUniswapV4Token(t, ctx, "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6)

	const correctionBuildID = 1
	insert := `
		INSERT INTO uniswap_v4_pool
		    (chain_id, pool_id, currency0, currency1,
		     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block, build_id)
		VALUES (1, $1::bytea,
		        '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,
		        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
		        $2, $3, 3000, 60, '\x0000000000000000000000000000000000000000'::bytea, 21743145, $4)
		ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING
		RETURNING processing_version`

	var pv int
	if err := uniswapV4TestPool.QueryRow(ctx, insert,
		poolIDHex, wstETH, usdc, correctionBuildID).Scan(&pv); err != nil {
		t.Fatalf("appending a corrected pool version: %v", err)
	}
	if pv != 1 {
		t.Errorf("processing_version = %d for the first write under build %d, want 1", pv, correctionBuildID)
	}

	tag, err := uniswapV4TestPool.Exec(ctx, insert, poolIDHex, wstETH, usdc, correctionBuildID)
	if err != nil {
		t.Fatalf("re-inserting under the same build: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Errorf("same-build re-insert wrote %d rows, want 0 (it must be an idempotent no-op, not a correction)", tag.RowsAffected())
	}
}

// uniswapV4FactInsert is one versioned fact table's INSERT, parameterised on the
// values its CHECK constraints govern. $1 is always the pool surrogate id and
// the last placeholder is always build_id.
const (
	uniswapV4PoolStateInsertSQL = `
		INSERT INTO uniswap_v4_pool_state
		    (pool_id, block_number, block_version, block_timestamp,
		     sqrt_price_x96, tick, protocol_fee, lp_fee, liquidity,
		     fee_growth_global0_x128, fee_growth_global1_x128, build_id)
		VALUES ($1, 22000000, 0, '2025-02-01T00:00:00Z'::timestamptz,
		        $2::numeric, $3, $4, $5, 1000000000000000000, 0, 0, $6)`

	uniswapV4SwapInsertSQL = `
		INSERT INTO uniswap_v4_swap
		    (pool_id, block_number, block_version, block_timestamp,
		     tx_hash, log_index, sender, amount0, amount1,
		     sqrt_price_x96, liquidity, tick, fee, build_id)
		VALUES ($1, 22000001, 0, '2025-02-01T00:01:00Z'::timestamptz,
		        '\xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd'::bytea,
		        0, '\x3333333333333333333333333333333333333333'::bytea,
		        -1000000000000000000, 990000000000000000,
		        $2::numeric, 1000000000000000000, $3, $4, $5)`

	uniswapV4LiquidityEventInsertSQL = `
		INSERT INTO uniswap_v4_liquidity_event
		    (pool_id, block_number, block_version, block_timestamp,
		     tx_hash, log_index, sender, tick_lower, tick_upper, liquidity_delta, salt, build_id)
		VALUES ($1, 22000002, 0, '2025-02-01T00:02:00Z'::timestamptz,
		        '\xbbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddee'::bytea,
		        0, '\x6666666666666666666666666666666666666666'::bytea,
		        $2, $3, 1000000000000000000,
		        '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea, $4)`

	uniswapV4TickInsertSQL = `
		INSERT INTO uniswap_v4_tick
		    (pool_id, tick, block_number, block_version, block_timestamp,
		     liquidity_gross, liquidity_net, fee_growth_outside0_x128,
		     fee_growth_outside1_x128, build_id)
		VALUES ($1, $2, 22000003, 0, '2025-02-01T00:03:00Z'::timestamptz,
		        1000000000000000000, 1000000000000000000, 0, 0, $3)`

	uniswapV4PoolEventInsertSQL = `
		INSERT INTO uniswap_v4_pool_event
		    (pool_id, block_number, block_version, block_timestamp,
		     tx_hash, log_index, event_name, params, build_id)
		VALUES ($1, 22000004, 0, '2025-02-01T00:04:00Z'::timestamptz,
		        '\xccddeeffccddeeffccddeeffccddeeffccddeeffccddeeffccddeeffccddeeff'::bytea,
		        0, 'initialize',
		        '{"sqrtPriceX96": "79228162514264337593543950336", "tick": 0}'::jsonb, $2)`
)

// uniswapV4ValidFactRow is one fact table plus in-range values for every column
// its CHECK constraints govern, ordered as the INSERT expects them between
// pool_id and build_id.
type uniswapV4ValidFactRow struct {
	table     string
	insert    string
	poolIDHex string
	args      []any
}

// uniswapV4ValidFactRows covers all five versioned fact tables. Each gets its own
// registry pool so the shared block/log_index values cannot collide.
var uniswapV4ValidFactRows = []uniswapV4ValidFactRow{
	{
		table:     "uniswap_v4_pool_state",
		insert:    uniswapV4PoolStateInsertSQL,
		poolIDHex: "\\x1100000000000000000000000000000000000000000000000000000000000001",
		args:      []any{"79228162514264337593543950336", 0, 0, 3000},
	},
	{
		table:     "uniswap_v4_swap",
		insert:    uniswapV4SwapInsertSQL,
		poolIDHex: "\\x1100000000000000000000000000000000000000000000000000000000000002",
		args:      []any{"79228162514264337593543950336", 0, 3000},
	},
	{
		table:     "uniswap_v4_liquidity_event",
		insert:    uniswapV4LiquidityEventInsertSQL,
		poolIDHex: "\\x1100000000000000000000000000000000000000000000000000000000000003",
		args:      []any{-120, 120},
	},
	{
		table:     "uniswap_v4_tick",
		insert:    uniswapV4TickInsertSQL,
		poolIDHex: "\\x1100000000000000000000000000000000000000000000000000000000000004",
		args:      []any{-120},
	},
	{
		table:     "uniswap_v4_pool_event",
		insert:    uniswapV4PoolEventInsertSQL,
		poolIDHex: "\\x1100000000000000000000000000000000000000000000000000000000000005",
		args:      nil,
	},
}

// uniswapV4FactInsertArgs assembles the placeholder list: pool surrogate id,
// then the constrained values, then build_id.
func uniswapV4FactInsertArgs(poolID int64, values []any, buildID int) []any {
	args := make([]any, 0, len(values)+2)
	args = append(args, poolID)
	args = append(args, values...)
	return append(args, buildID)
}

// A correction is a re-insert under a new build_id, which the trigger turns into
// an appended version; a re-insert under the same build_id must write nothing.
func TestUniswapV4ProcessingVersionTriggerAppendsFactCorrection(t *testing.T) {
	ctx := context.Background()

	for _, tc := range uniswapV4ValidFactRows {
		t.Run(tc.table, func(t *testing.T) {
			poolID := insertTestUniswapV4Pool(t, ctx, tc.poolIDHex)
			insert := tc.insert + `
		ON CONFLICT DO NOTHING
		RETURNING processing_version`

			var pv int
			if err := uniswapV4TestPool.QueryRow(ctx, insert,
				uniswapV4FactInsertArgs(poolID, tc.args, 0)...).Scan(&pv); err != nil {
				t.Fatalf("inserting under build 0: %v", err)
			}
			if pv != 0 {
				t.Errorf("processing_version = %d for the first write under build 0, want 0", pv)
			}

			if err := uniswapV4TestPool.QueryRow(ctx, insert,
				uniswapV4FactInsertArgs(poolID, tc.args, 1)...).Scan(&pv); err != nil {
				t.Fatalf("appending a correction under build 1: %v", err)
			}
			if pv != 1 {
				t.Errorf("processing_version = %d for the first write under build 1, want 1", pv)
			}

			tag, err := uniswapV4TestPool.Exec(ctx, tc.insert+" ON CONFLICT DO NOTHING",
				uniswapV4FactInsertArgs(poolID, tc.args, 1)...)
			if err != nil {
				t.Fatalf("re-inserting under build 1: %v", err)
			}
			if tag.RowsAffected() != 0 {
				t.Errorf("same-build re-insert wrote %d rows, want 0 (it must be an idempotent no-op, not a correction)", tag.RowsAffected())
			}

			var versions int
			if err := uniswapV4TestPool.QueryRow(ctx,
				"SELECT count(*) FROM "+tc.table+" WHERE pool_id = $1", poolID).Scan(&versions); err != nil {
				t.Fatalf("counting %s versions: %v", tc.table, err)
			}
			if versions != 2 {
				t.Errorf("%s rows for the corrected key = %d, want 2 (original + correction)", tc.table, versions)
			}
		})
	}
}

// The CHECKs are the last line of defence for values the decoder derives from
// int24/uint24 event fields, so each bound gets an explicit rejection case.
func TestUniswapV4FactTableChecksRejectOutOfRangeValues(t *testing.T) {
	ctx := context.Background()
	poolID := insertTestUniswapV4Pool(t, ctx, "\\x1200000000000000000000000000000000000000000000000000000000000001")

	cases := []struct {
		name   string
		insert string
		args   []any
	}{
		{"pool_state_zero_sqrt_price", uniswapV4PoolStateInsertSQL, []any{"0", 0, 0, 3000}},
		{"pool_state_tick_above_max", uniswapV4PoolStateInsertSQL, []any{"79228162514264337593543950336", 887273, 0, 3000}},
		{"pool_state_tick_below_min", uniswapV4PoolStateInsertSQL, []any{"79228162514264337593543950336", -887273, 0, 3000}},
		{"pool_state_protocol_fee_above_uint24", uniswapV4PoolStateInsertSQL, []any{"79228162514264337593543950336", 0, 16777216, 3000}},
		{"pool_state_protocol_fee_zero_for_one_above_1000", uniswapV4PoolStateInsertSQL, []any{"79228162514264337593543950336", 0, 1001, 3000}},
		{"pool_state_protocol_fee_one_for_zero_above_1000", uniswapV4PoolStateInsertSQL, []any{"79228162514264337593543950336", 0, 1001 << 12, 3000}},
		{"pool_state_lp_fee_above_100_percent", uniswapV4PoolStateInsertSQL, []any{"79228162514264337593543950336", 0, 0, 1000001}},
		{"swap_zero_sqrt_price", uniswapV4SwapInsertSQL, []any{"0", 0, 3000}},
		{"swap_tick_above_max", uniswapV4SwapInsertSQL, []any{"79228162514264337593543950336", 887273, 3000}},
		{"swap_fee_above_100_percent", uniswapV4SwapInsertSQL, []any{"79228162514264337593543950336", 0, 1000001}},
		{"liquidity_event_tick_lower_below_min", uniswapV4LiquidityEventInsertSQL, []any{-887273, 120}},
		{"liquidity_event_tick_upper_above_max", uniswapV4LiquidityEventInsertSQL, []any{-120, 887273}},
		{"liquidity_event_tick_lower_equals_upper", uniswapV4LiquidityEventInsertSQL, []any{120, 120}},
		{"liquidity_event_tick_lower_above_upper", uniswapV4LiquidityEventInsertSQL, []any{240, 120}},
		{"tick_above_max", uniswapV4TickInsertSQL, []any{887273}},
		{"tick_below_min", uniswapV4TickInsertSQL, []any{-887273}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := uniswapV4TestPool.Exec(ctx, tc.insert,
				uniswapV4FactInsertArgs(poolID, tc.args, 0)...); err == nil {
				t.Fatal("out-of-range row was accepted, want a CHECK violation")
			}
		})
	}
}

// The bounds are inclusive on both ends, so a legal extreme must still insert.
func TestUniswapV4FactTableChecksAcceptBoundaryValues(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name      string
		insert    string
		poolIDHex string
		args      []any
	}{
		{
			"pool_state_min_tick_max_packed_protocol_fee",
			uniswapV4PoolStateInsertSQL,
			"\\x1300000000000000000000000000000000000000000000000000000000000001",
			[]any{"1", -887272, (1000 << 12) | 1000, 1000000},
		},
		{
			"swap_max_tick_max_fee",
			uniswapV4SwapInsertSQL,
			"\\x1300000000000000000000000000000000000000000000000000000000000002",
			[]any{"1", 887272, 1000000},
		},
		{
			"liquidity_event_full_tick_range",
			uniswapV4LiquidityEventInsertSQL,
			"\\x1300000000000000000000000000000000000000000000000000000000000003",
			[]any{-887272, 887272},
		},
		{
			"tick_max",
			uniswapV4TickInsertSQL,
			"\\x1300000000000000000000000000000000000000000000000000000000000004",
			[]any{887272},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poolID := insertTestUniswapV4Pool(t, ctx, tc.poolIDHex)
			if _, err := uniswapV4TestPool.Exec(ctx, tc.insert,
				uniswapV4FactInsertArgs(poolID, tc.args, 0)...); err != nil {
				t.Fatalf("boundary row was rejected: %v", err)
			}
		})
	}
}

// A reorg that orphans a pool's Initialize makes StateView answer all zeros on
// the re-read; that tombstone has to persist to supersede the orphaned fork's
// snapshot, while a zero at block_version 0 stays a registry bug.
func TestUniswapV4PoolStateZeroSqrtPriceOnlyAtAReorgVersion(t *testing.T) {
	ctx := context.Background()

	insert := `
		INSERT INTO uniswap_v4_pool_state
		    (pool_id, block_number, block_version, block_timestamp,
		     sqrt_price_x96, tick, protocol_fee, lp_fee, liquidity,
		     fee_growth_global0_x128, fee_growth_global1_x128, build_id)
		VALUES ($1, 22000010, $2, '2025-02-01T00:00:00Z'::timestamptz,
		        0, 0, 0, 0, 0, 0, 0, 0)`

	cases := []struct {
		name         string
		poolIDHex    string
		blockVersion int
		wantAccept   bool
	}{
		{"first observation", "\\x1800000000000000000000000000000000000000000000000000000000000001", 0, false},
		{"reorg re-read", "\\x1800000000000000000000000000000000000000000000000000000000000002", 1, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poolID := insertTestUniswapV4Pool(t, ctx, tc.poolIDHex)
			_, err := uniswapV4TestPool.Exec(ctx, insert, poolID, tc.blockVersion)
			if tc.wantAccept && err != nil {
				t.Fatalf("all-zero state at block_version %d was rejected: %v", tc.blockVersion, err)
			}
			if !tc.wantAccept && err == nil {
				t.Fatalf("all-zero state at block_version %d was accepted, want a CHECK violation", tc.blockVersion)
			}
		})
	}
}

// 8388608 (0x800000) is the dynamic-LP-fee sentinel, the one value above the
// 100% cap a PoolKey may legally carry.
func TestUniswapV4PoolFeeCheckAllowsOnlyRatesAndTheDynamicSentinel(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4PoolManager(t, ctx)
	wstETH := seedUniswapV4Token(t, ctx, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18)
	usdc := seedUniswapV4Token(t, ctx, "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6)

	cases := []struct {
		name       string
		poolIDHex  string
		fee        int
		wantAccept bool
	}{
		{"static_max_fee", "\\x1400000000000000000000000000000000000000000000000000000000000001", 1000000, true},
		{"dynamic_fee_sentinel", "\\x1400000000000000000000000000000000000000000000000000000000000002", 8388608, true},
		{"above_100_percent", "\\x1400000000000000000000000000000000000000000000000000000000000003", 1000001, false},
		{"max_uint24", "\\x1400000000000000000000000000000000000000000000000000000000000004", 16777215, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := uniswapV4TestPool.Exec(ctx, `
				INSERT INTO uniswap_v4_pool
				    (chain_id, pool_id, currency0, currency1,
				     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block)
				VALUES (1, $1::bytea,
				        '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,
				        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
				        $2, $3, $4, 60, '\x0000000000000000000000000000000000000000'::bytea, 22000000)`,
				tc.poolIDHex, wstETH, usdc, tc.fee)
			if tc.wantAccept && err != nil {
				t.Fatalf("fee %d was rejected: %v", tc.fee, err)
			}
			if !tc.wantAccept && err == nil {
				t.Fatalf("fee %d was accepted, want a CHECK violation", tc.fee)
			}
		})
	}
}

func TestUniswapV4ColumnComments(t *testing.T) {
	ctx := context.Background()

	type uncommentedCol struct {
		table  string
		column string
	}
	var missing []uncommentedCol

	for _, table := range uniswapV4Tables {
		// The per-schema migration run is a no-op once public.migrations is
		// populated, so these tables live in public, not this file's schema.
		rows, err := uniswapV4TestPool.Query(ctx, `
			SELECT a.attname
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			WHERE c.relname = $1
			  AND pg_table_is_visible(c.oid)
			  AND a.attnum > 0
			  AND NOT a.attisdropped
			  AND col_description(a.attrelid, a.attnum) IS NULL
			ORDER BY a.attnum`, table)
		if err != nil {
			t.Fatalf("querying uncommented columns for %s: %v", table, err)
		}
		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err != nil {
				rows.Close()
				t.Fatalf("scanning row for %s: %v", table, err)
			}
			missing = append(missing, uncommentedCol{table: table, column: col})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("iterating rows for %s: %v", table, err)
		}
	}

	for _, m := range missing {
		t.Errorf("uniswap_v4 column missing COMMENT: %s.%s", m.table, m.column)
	}

	for _, table := range uniswapV4Tables {
		var comment *string
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT obj_description(c.oid)
			FROM pg_class c
			WHERE c.relname = $1 AND pg_table_is_visible(c.oid)`, table).Scan(&comment); err != nil {
			t.Fatalf("querying table comment for %s: %v", table, err)
		}
		if comment == nil || *comment == "" {
			t.Errorf("uniswap_v4 table missing COMMENT: %s", table)
		}
	}
}

// uniswapV4ColumnComment returns one column's COMMENT, failing the test when
// the column or its comment is absent.
func uniswapV4ColumnComment(t *testing.T, ctx context.Context, table, column string) string {
	t.Helper()

	var comment *string
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT col_description(a.attrelid, a.attnum)
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname = $1
		  AND pg_table_is_visible(c.oid)
		  AND a.attname = $2`, table, column).Scan(&comment); err != nil {
		t.Fatalf("reading %s.%s comment: %v", table, column, err)
	}
	if comment == nil {
		t.Fatalf("%s.%s has no COMMENT", table, column)
	}
	return *comment
}

// v4-core applies the swap BalanceDelta to msg.sender, so the sign rule is the
// swapper's; a comment opening with the pool's side inverts it for every reader.
func TestUniswapV4SwapAmountCommentsLeadWithTheSwapperPerspective(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		column string
		keep   []string
	}{
		{"amount0", []string{"afterSwap", "_RETURNS_DELTA", "uniswap_v3_swap.amount0"}},
		{"amount1", []string{"pre-hook-delta", "amount0"}},
	}

	for _, tc := range cases {
		t.Run(tc.column, func(t *testing.T) {
			comment := uniswapV4ColumnComment(t, ctx, "uniswap_v4_swap", tc.column)
			if !strings.Contains(strings.ToLower(comment), "swapper's perspective") {
				t.Errorf("comment %q does not lead with the swapper's perspective", comment)
			}
			if strings.Contains(comment, "pool's swap BalanceDelta") {
				t.Errorf("comment %q still frames the delta from the pool's side", comment)
			}
			for _, want := range tc.keep {
				if !strings.Contains(comment, want) {
					t.Errorf("comment %q dropped %q", comment, want)
				}
			}
		})
	}
}

// uniswapV4SeedToken is a token row the 21 seeded pools reference.
type uniswapV4SeedToken struct {
	addrHex  string
	symbol   string
	decimals int
}

// uniswapV4SeedTokens are the counterparty tokens the seeded pools resolve
// currency*_token_id against, plus the 0xEeee… native-ETH placeholder that
// address(0) maps to. Symbols and decimals are cast-verified against mainnet.
var uniswapV4SeedTokens = []uniswapV4SeedToken{
	{uniswapV4EthPlaceholderHex, "ETH", 18},
	{"\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18},
	{"\\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", "stETH", 18},
	{"\\xBe9895146f7AF43049ca1c1AE358B0541Ea49704", "cbETH", 18},
	{"\\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "WBTC", 8},
	{"\\x111111111117dC0aa78b770fA6A738034120C302", "1INCH", 18},
	{"\\xf951E335afb289353dc249e82926178EaC7DEd78", "swETH", 18},
	{"\\x93ED3FBe21207Ec2E8f2d3c3de6e058Cb73Bc04d", "PNK", 18},
	{"\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6},
	{"\\xae78736Cd615f374D3085123A210448E74Fc6393", "rETH", 18},
	{"\\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD", "sUSDS", 18},
	{"\\x6c3ea9036406852006290770BEdFcAbA0e23A0e8", "PYUSD", 6},
	{"\\xdC035D45d973E3EC169d2276DDab16f1e407384F", "USDS", 18},
	{"\\xdAC17F958D2ee523a2206206994597C13D831ec7", "USDT", 6},
	{"\\x56072C95FAA701256059aa122697B133aDEd9279", "SKY", 18},
	{"\\x68749665FF8D2d112Fa859AA293F07A622782F38", "XAUt", 6},
}

// uniswapV4ExpectedPool is one seeded pool's registry key, transcribed from the
// verified Initialize-log scan that 20260819_120000_create_uniswap_v4_tables.sql
// seeds from.
type uniswapV4ExpectedPool struct {
	name         string
	poolIDHex    string
	currency0Hex string
	currency1Hex string
	fee          int64
	tickSpacing  int64
	hooksHex     string
	deployBlock  int64
}

var uniswapV4ExpectedPools = []uniswapV4ExpectedPool{
	{"eth_wsteth_100", "\\x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76", uniswapV4NativeCurrencyHex, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 100, 1, uniswapV4NoHooksHex, 21743144},
	{"eth_steth_800", "\\xbc21dd4a44766fadfd6447f4b222a6185dcc2e6a3b15eb79e0cc637e30e7e97f", uniswapV4NativeCurrencyHex, "\\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", 800, 16, uniswapV4NoHooksHex, 25199556},
	{"eth_steth_1000", "\\x056c3c5d8aceeb400b674c27db54e4a90d2f468d786582571ee9394b4c5e3a11", uniswapV4NativeCurrencyHex, "\\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", 1000, 20, uniswapV4NoHooksHex, 25199299},
	{"eth_steth_2500", "\\x9e0032112d580d8f45a0e356c48148846a3306a991da398dde4f92071e853d09", uniswapV4NativeCurrencyHex, "\\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", 2500, 50, uniswapV4NoHooksHex, 24857024},
	{"wsteth_cbeth_500", "\\xaea49399167b73015a01e9ca9754c2b438e8aaf42d911468443540eea235735e", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xBe9895146f7AF43049ca1c1AE358B0541Ea49704", 500, 10, uniswapV4NoHooksHex, 25494004},
	{"wbtc_wsteth_2500", "\\x58299b9ad89104f189f5efcdf4910615cb9e3296afb0c5a1d1d3befdd1bf7ae4", "\\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 2500, 50, uniswapV4NoHooksHex, 23188451},
	{"oneinch_wsteth_10000", "\\x1d6ebf506eacf0e98a8c4566687380ddf1601192acd9bce29feeaf0c0245ea6f", "\\x111111111117dC0aa78b770fA6A738034120C302", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 10000, 200, uniswapV4NoHooksHex, 24363425},
	{"wsteth_sweth_3000", "\\xe7c7bbac1cb017812f5129246ba1ace4aeaadb96fed67cc43d94ac2c6c5048d8", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xf951E335afb289353dc249e82926178EaC7DEd78", 3000, 60, uniswapV4NoHooksHex, 23796248},
	{"wsteth_pnk_3000", "\\xbb78d828ded564d7dfcf041eb1316200e4ec5380dc601c7b4872c0a2727a580e", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\x93ED3FBe21207Ec2E8f2d3c3de6e058Cb73Bc04d", 3000, 60, uniswapV4NoHooksHex, 24284325},
	{"wsteth_usdc_3000", "\\x84a2753546221b6aedf1b96098235f8eb5494b1ddd7d57583d99b2d174cd2103", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 3000, 60, uniswapV4NoHooksHex, 22962297},
	{"wbtc_wsteth_3000", "\\xef3a1d51982c20ee2f125e6d6d1f9d3a10c1e94391b828040943005a1ea27e14", "\\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 3000, 60, uniswapV4NoHooksHex, 22552041},
	{"eth_wsteth_50_hooked", "\\x904e8ad11c6f8abb44ea77c507355900e7f9d2907ab0a29353cb1ef0f06b0852", uniswapV4NativeCurrencyHex, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 50, 1, "\\x4440854B2d02C57A0Dc5c58b7A884562D875c0c4", 23326185},
	{"wsteth_reth_500", "\\xa068c5ab2de0c5fed15f8c187d911915437ed55e6a47d2e42710f9174e6db9a2", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xae78736Cd615f374D3085123A210448E74Fc6393", 500, 10, uniswapV4NoHooksHex, 22240740},
	{"wsteth_susds_10000", "\\x4d9cc597ec7d8848af463fca5f4c750279f0d02d2745844c1e9f52a7930cc4d7", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD", 10000, 200, uniswapV4NoHooksHex, 25492487},
	{"pyusd_usds_5", "\\xe63e32b2ae40601662f760d6bf5d771057324fbd97784fe1d3717069f7b75d45", "\\x6c3ea9036406852006290770BEdFcAbA0e23A0e8", "\\xdC035D45d973E3EC169d2276DDab16f1e407384F", 5, 1, uniswapV4NoHooksHex, 24229945},
	{"usdt_usds_5", "\\x3b1b1f2e775a6db1664f8e7d59ad568605ea2406312c11aef03146c0cf89d5b9", "\\xdAC17F958D2ee523a2206206994597C13D831ec7", "\\xdC035D45d973E3EC169d2276DDab16f1e407384F", 5, 1, uniswapV4NoHooksHex, 24230047},
	{"usdt_usds_100", "\\xb54ece65cc2ddd3eaec0ad18657470fb043097220273d87368a062c7d4e59180", "\\xdAC17F958D2ee523a2206206994597C13D831ec7", "\\xdC035D45d973E3EC169d2276DDab16f1e407384F", 100, 1, uniswapV4NoHooksHex, 23153381},
	{"pyusd_usdc_100", "\\xa2a5a544a8cbd9c24557b8393fd909360779cf0f48a0b88895a7d9d83ce9d437", "\\x6c3ea9036406852006290770BEdFcAbA0e23A0e8", "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 100, 1, uniswapV4NoHooksHex, 22268982},
	{"sky_usds_500", "\\x2d04d518afae8b57a702a6f679edf49f39593d818f9342cc57b457ea738a7460", "\\x56072C95FAA701256059aa122697B133aDEd9279", "\\xdC035D45d973E3EC169d2276DDab16f1e407384F", 500, 10, uniswapV4NoHooksHex, 25036987},
	{"eth_susds_3000", "\\x51ccd46db78d6988ab156c9b0d023e14b2e848240bc719718e63c4cc5c258bcf", uniswapV4NativeCurrencyHex, "\\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD", 3000, 60, uniswapV4NoHooksHex, 22989795},
	{"xaut_susds_10000", "\\x2f5dff74b96e2df0fa8a5695318d59839c3ce5d058b19024fbfe276100b676ff", "\\x68749665FF8D2d112Fa859AA293F07A622782F38", "\\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD", 10000, 200, uniswapV4NoHooksHex, 24363921},
}

// seedUniswapV4Registry mirrors (rather than relies on) the migration's seed:
// sibling test files TRUNCATE protocol/token CASCADE, which cascades away the
// pool manager and every pool row before this file runs.
func seedUniswapV4Registry(t *testing.T, ctx context.Context) int64 {
	t.Helper()

	poolManagerID := seedUniswapV4PoolManager(t, ctx)
	tokenIDs := make(map[string]int64, len(uniswapV4SeedTokens))
	for _, tok := range uniswapV4SeedTokens {
		tokenIDs[tok.addrHex] = seedUniswapV4Token(t, ctx, tok.addrHex, tok.symbol, tok.decimals)
	}

	for _, pool := range uniswapV4ExpectedPools {
		currency0TokenID := tokenIDs[uniswapV4TokenAddrFor(pool.currency0Hex)]
		currency1TokenID := tokenIDs[uniswapV4TokenAddrFor(pool.currency1Hex)]
		if _, err := uniswapV4TestPool.Exec(ctx, `
			INSERT INTO uniswap_v4_pool
			    (chain_id, pool_id, currency0, currency1,
			     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block, build_id)
			VALUES (1, $1::bytea, $2::bytea, $3::bytea, $4, $5, $6, $7, $8::bytea, $9, 0)
			ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING`,
			pool.poolIDHex, pool.currency0Hex, pool.currency1Hex,
			currency0TokenID, currency1TokenID, pool.fee, pool.tickSpacing,
			pool.hooksHex, pool.deployBlock,
		); err != nil {
			t.Fatalf("seeding pool %s: %v", pool.name, err)
		}
	}
	return poolManagerID
}

// uniswapV4TokenAddrFor maps a Currency value to the token row that carries its
// symbol/decimals: address(0) is native ETH, which has no ERC-20 contract and
// resolves to the 0xEeee… placeholder (same convention as curve_pool_coin).
func uniswapV4TokenAddrFor(currencyHex string) string {
	if currencyHex == uniswapV4NativeCurrencyHex {
		return uniswapV4EthPlaceholderHex
	}
	return currencyHex
}

func TestUniswapV4PoolSeedCount(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4Registry(t, ctx)

	var count int
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT count(*) FROM uniswap_v4_pool
		WHERE chain_id = 1 AND pool_id = ANY($1::bytea[])`,
		uniswapV4SeededPoolIDs()).Scan(&count); err != nil {
		t.Fatalf("counting seeded pools: %v", err)
	}
	if count != len(uniswapV4ExpectedPools) {
		t.Errorf("seeded uniswap_v4_pool count = %d, want %d", count, len(uniswapV4ExpectedPools))
	}
}

func TestUniswapV4PoolManagerHasOneIdentityPerChain(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4Registry(t, ctx)

	var addresses int
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT count(DISTINCT (pr.address, m.state_view_address))
		FROM uniswap_v4_pool_manager m
		JOIN protocol pr ON pr.id = m.protocol_id
		WHERE m.chain_id = 1`).Scan(&addresses); err != nil {
		t.Fatalf("counting distinct pool manager identities: %v", err)
	}
	if addresses != 1 {
		t.Errorf("distinct (protocol.address, state_view_address) pairs on chain 1 = %d, want 1; versions of one manager may accumulate, two concurrent managers may not", addresses)
	}
}

// The PoolManager address is the FK'd protocol row's address; a second copy on
// uniswap_v4_pool_manager is a registry duplicate that can disagree with it.
func TestUniswapV4PoolManagerHasNoDuplicateAddressColumn(t *testing.T) {
	ctx := context.Background()

	var exists bool
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'uniswap_v4_pool_manager'
			  AND column_name = 'pool_manager_address'
		)`).Scan(&exists); err != nil {
		t.Fatalf("checking for uniswap_v4_pool_manager.pool_manager_address: %v", err)
	}
	if exists {
		t.Error("uniswap_v4_pool_manager.pool_manager_address duplicates protocol.address; resolve it through the protocol FK instead")
	}
}

// A pool is snapshotted unless a maintainer deliberately excludes it, so the
// curated gate defaults to TRUE: forgetting the column must not silently stop
// state and tick rows for a newly registered pool.
func TestUniswapV4PoolSnapshotSupportedDefaultsToTrue(t *testing.T) {
	ctx := context.Background()
	poolID := insertTestUniswapV4Pool(t, ctx, "\\x1700000000000000000000000000000000000000000000000000000000000001")

	var supported bool
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT snapshot_supported FROM uniswap_v4_pool WHERE id = $1`, poolID).Scan(&supported); err != nil {
		t.Fatalf("reading snapshot_supported for pool %d: %v", poolID, err)
	}
	if !supported {
		t.Error("snapshot_supported defaulted to false; a pool must be snapshotted unless deliberately excluded")
	}
}

// deploy_block gates every snapshot read, so the schema makes a NULL
// unrepresentable rather than leaving it to the seed or a runtime check.
func TestUniswapV4DeployBlockIsNotNullable(t *testing.T) {
	ctx := context.Background()

	for _, table := range []string{"uniswap_v4_pool", "uniswap_v4_pool_manager"} {
		var isNullable string
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT is_nullable
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'deploy_block'`,
			table).Scan(&isNullable); err != nil {
			t.Fatalf("reading %s.deploy_block nullability: %v", table, err)
		}
		if isNullable != "NO" {
			t.Errorf("%s.deploy_block is_nullable = %q, want %q", table, isNullable, "NO")
		}
	}
}

func TestUniswapV4PoolSeedKeyPerPool(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4Registry(t, ctx)

	for _, want := range uniswapV4ExpectedPools {
		t.Run(want.name, func(t *testing.T) {
			var currency0, currency1, hooks []byte
			var fee, tickSpacing int64
			var deployBlock int64
			err := uniswapV4TestPool.QueryRow(ctx, `
				SELECT currency0, currency1, fee, tick_spacing, hooks, deploy_block
				FROM uniswap_v4_pool
				WHERE chain_id = 1 AND pool_id = $1::bytea`,
				want.poolIDHex,
			).Scan(&currency0, &currency1, &fee, &tickSpacing, &hooks, &deployBlock)
			if err != nil {
				t.Fatalf("reading pool %s: %v", want.name, err)
			}

			if got, wantBytes := currency0, decodeBytea(t, want.currency0Hex); !bytes.Equal(got, wantBytes) {
				t.Errorf("currency0 = %x, want %x", got, wantBytes)
			}
			if got, wantBytes := currency1, decodeBytea(t, want.currency1Hex); !bytes.Equal(got, wantBytes) {
				t.Errorf("currency1 = %x, want %x", got, wantBytes)
			}
			if got, wantBytes := hooks, decodeBytea(t, want.hooksHex); !bytes.Equal(got, wantBytes) {
				t.Errorf("hooks = %x, want %x", got, wantBytes)
			}
			if fee != want.fee {
				t.Errorf("fee = %d, want %d", fee, want.fee)
			}
			if tickSpacing != want.tickSpacing {
				t.Errorf("tick_spacing = %d, want %d", tickSpacing, want.tickSpacing)
			}
			if deployBlock != want.deployBlock {
				t.Errorf("deploy_block = %d, want %d", deployBlock, want.deployBlock)
			}
		})
	}
}

func TestUniswapV4PoolSeedMapsNativeEthToPlaceholderToken(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4Registry(t, ctx)

	for _, want := range uniswapV4ExpectedPools {
		if want.currency0Hex != uniswapV4NativeCurrencyHex {
			continue
		}
		t.Run(want.name, func(t *testing.T) {
			var tokenAddr []byte
			var decimals *int
			err := uniswapV4TestPool.QueryRow(ctx, `
				SELECT t.address, t.decimals
				FROM uniswap_v4_pool p
				JOIN token t ON t.id = p.currency0_token_id
				WHERE p.chain_id = 1 AND p.pool_id = $1::bytea`,
				want.poolIDHex,
			).Scan(&tokenAddr, &decimals)
			if err != nil {
				t.Fatalf("reading currency0 token: %v", err)
			}

			if wantAddr := decodeBytea(t, uniswapV4EthPlaceholderHex); !bytes.Equal(tokenAddr, wantAddr) {
				t.Errorf("currency0_token_id resolves to %x, want the native-ETH placeholder %x", tokenAddr, wantAddr)
			}
			if decimals == nil || *decimals != 18 {
				t.Errorf("native-ETH placeholder decimals = %v, want 18", decimals)
			}
		})
	}
}

func TestUniswapV4PoolSeedTokensResolveToTheirCurrency(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4Registry(t, ctx)

	rows, err := uniswapV4TestPool.Query(ctx, `
		SELECT p.pool_id, p.currency0, t0.address, t0.decimals, p.currency1, t1.address, t1.decimals
		FROM uniswap_v4_pool p
		JOIN token t0 ON t0.id = p.currency0_token_id
		JOIN token t1 ON t1.id = p.currency1_token_id
		WHERE p.chain_id = 1 AND p.pool_id = ANY($1::bytea[])`,
		uniswapV4SeededPoolIDs())
	if err != nil {
		t.Fatalf("reading pool currency mappings: %v", err)
	}
	defer rows.Close()

	placeholder := decodeBytea(t, uniswapV4EthPlaceholderHex)
	native := decodeBytea(t, uniswapV4NativeCurrencyHex)

	var seen int
	for rows.Next() {
		seen++
		var poolID, currency0, token0Addr, currency1, token1Addr []byte
		var decimals0, decimals1 *int
		if err := rows.Scan(&poolID, &currency0, &token0Addr, &decimals0, &currency1, &token1Addr, &decimals1); err != nil {
			t.Fatalf("scanning pool currency mapping: %v", err)
		}
		assertUniswapV4CurrencyMapping(t, poolID, currency0, token0Addr, decimals0, native, placeholder)
		assertUniswapV4CurrencyMapping(t, poolID, currency1, token1Addr, decimals1, native, placeholder)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating pool currency mappings: %v", err)
	}
	if seen != len(uniswapV4ExpectedPools) {
		t.Errorf("checked %d pool currency mappings, want %d", seen, len(uniswapV4ExpectedPools))
	}
}

func assertUniswapV4CurrencyMapping(t *testing.T, poolID, currency, tokenAddr []byte, decimals *int, native, placeholder []byte) {
	t.Helper()

	wantAddr := currency
	if bytes.Equal(currency, native) {
		wantAddr = placeholder
	}
	if !bytes.Equal(tokenAddr, wantAddr) {
		t.Errorf("pool %x: currency %x resolves to token %x, want %x", poolID, currency, tokenAddr, wantAddr)
	}
	if decimals == nil {
		t.Errorf("pool %x: token %x has NULL decimals", poolID, tokenAddr)
	}
}

func TestUniswapV4PoolSeedPoolIDMatchesKeccakOfPoolKey(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4Registry(t, ctx)

	rows, err := uniswapV4TestPool.Query(ctx, `
		SELECT pool_id, currency0, currency1, fee, tick_spacing, hooks
		FROM uniswap_v4_pool
		WHERE chain_id = 1 AND pool_id = ANY($1::bytea[])
		ORDER BY pool_id`, uniswapV4SeededPoolIDs())
	if err != nil {
		t.Fatalf("reading seeded pool keys: %v", err)
	}
	defer rows.Close()

	type seededKey struct {
		poolID      []byte
		currency0   []byte
		currency1   []byte
		fee         int64
		tickSpacing int64
		hooks       []byte
	}
	var keys []seededKey
	for rows.Next() {
		var k seededKey
		if err := rows.Scan(&k.poolID, &k.currency0, &k.currency1, &k.fee, &k.tickSpacing, &k.hooks); err != nil {
			t.Fatalf("scanning seeded pool key: %v", err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating seeded pool keys: %v", err)
	}
	if len(keys) != len(uniswapV4ExpectedPools) {
		t.Fatalf("read %d seeded pools, want %d", len(keys), len(uniswapV4ExpectedPools))
	}

	for _, k := range keys {
		got := uniswapV4PoolID(t, k.currency0, k.currency1, k.fee, k.tickSpacing, k.hooks)
		if !bytes.Equal(got, k.poolID) {
			t.Errorf("pool %x: keccak256(abi.encode(PoolKey)) = %x", k.poolID, got)
		}
	}
}

// uniswapV4PoolID recomputes PoolId =
// keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks)), the
// identity v4-core derives a pool from, so a transcription error in the seed
// cannot pass as a valid registry row.
func uniswapV4PoolID(t *testing.T, currency0, currency1 []byte, fee, tickSpacing int64, hooks []byte) []byte {
	t.Helper()

	addressT, err := abi.NewType("address", "", nil)
	if err != nil {
		t.Fatalf("abi.NewType address: %v", err)
	}
	uint24T, err := abi.NewType("uint24", "", nil)
	if err != nil {
		t.Fatalf("abi.NewType uint24: %v", err)
	}
	int24T, err := abi.NewType("int24", "", nil)
	if err != nil {
		t.Fatalf("abi.NewType int24: %v", err)
	}

	args := abi.Arguments{
		{Type: addressT}, {Type: addressT}, {Type: uint24T}, {Type: int24T}, {Type: addressT},
	}
	packed, err := args.Pack(
		common.BytesToAddress(currency0),
		common.BytesToAddress(currency1),
		big.NewInt(fee),
		big.NewInt(tickSpacing),
		common.BytesToAddress(hooks),
	)
	if err != nil {
		t.Fatalf("packing PoolKey: %v", err)
	}
	return crypto.Keccak256(packed)
}

func uniswapV4SeededPoolIDs() []string {
	ids := make([]string, len(uniswapV4ExpectedPools))
	for i, p := range uniswapV4ExpectedPools {
		ids[i] = p.poolIDHex
	}
	return ids
}

// seedUniswapV4PoolManager appends the mainnet PoolManager row if this build
// has not written it yet and returns the id of the current (highest-version)
// row. Never an upsert: the table is append-only and versioned.
func seedUniswapV4PoolManager(t *testing.T, ctx context.Context) int64 {
	t.Helper()

	if _, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, metadata)
		VALUES (1, $1::bytea, 'UniswapV4', 'dex', $2, '{"role":"pool_manager"}'::jsonb)
		ON CONFLICT (chain_id, address) DO NOTHING`,
		uniswapV4PoolManagerHex, uniswapV4DeployBlock); err != nil {
		t.Fatalf("seeding UniswapV4 protocol row: %v", err)
	}

	var protocolID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE chain_id = 1 AND address = $1::bytea`,
		uniswapV4PoolManagerHex).Scan(&protocolID); err != nil {
		t.Fatalf("reading UniswapV4 protocol id: %v", err)
	}

	if _, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO uniswap_v4_pool_manager
		    (chain_id, protocol_id, state_view_address, deploy_block, build_id)
		VALUES (1, $1, $2::bytea, $3, 0)
		ON CONFLICT (chain_id, processing_version) DO NOTHING`,
		protocolID, uniswapV4StateViewHex, uniswapV4DeployBlock,
	); err != nil {
		t.Fatalf("seeding uniswap_v4_pool_manager row: %v", err)
	}

	return currentUniswapV4PoolManagerID(t, ctx)
}

func currentUniswapV4PoolManagerID(t *testing.T, ctx context.Context) int64 {
	t.Helper()

	var poolManagerID int64
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT id FROM uniswap_v4_pool_manager
		WHERE chain_id = 1
		ORDER BY processing_version DESC
		LIMIT 1`).Scan(&poolManagerID); err != nil {
		t.Fatalf("reading current uniswap_v4_pool_manager row: %v", err)
	}
	return poolManagerID
}

func seedUniswapV4Token(t *testing.T, ctx context.Context, addrHex, symbol string, decimals int) int64 {
	t.Helper()

	if _, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, $1::bytea, $2, $3)
		ON CONFLICT (chain_id, address) DO NOTHING`, addrHex, symbol, decimals); err != nil {
		t.Fatalf("seeding token %s: %v", symbol, err)
	}

	var tokenID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM token WHERE chain_id = 1 AND address = $1::bytea`, addrHex).Scan(&tokenID); err != nil {
		t.Fatalf("reading token %s id: %v", symbol, err)
	}
	return tokenID
}

// insertTestUniswapV4Pool appends a throwaway pool keyed by poolIDHex so the
// fact-table tests, which all write block/log_index 0, cannot collide under the
// natural-key advisory lock. Returns the current version's surrogate id.
func insertTestUniswapV4Pool(t *testing.T, ctx context.Context, poolIDHex string) int64 {
	t.Helper()

	seedUniswapV4PoolManager(t, ctx)
	wstETH := seedUniswapV4Token(t, ctx, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18)
	usdc := seedUniswapV4Token(t, ctx, "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6)

	if _, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO uniswap_v4_pool
		    (chain_id, pool_id, currency0, currency1,
		     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block, build_id)
		VALUES (1, $1::bytea,
		        '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,
		        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
		        $2, $3, 3000, 60, '\x0000000000000000000000000000000000000000'::bytea, 21743144, 0)
		ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING`,
		poolIDHex, wstETH, usdc); err != nil {
		t.Fatalf("inserting test uniswap_v4_pool: %v", err)
	}

	var poolID int64
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT id FROM uniswap_v4_pool
		WHERE chain_id = 1 AND pool_id = $1::bytea
		ORDER BY processing_version DESC
		LIMIT 1`, poolIDHex).Scan(&poolID); err != nil {
		t.Fatalf("reading test uniswap_v4_pool id: %v", err)
	}
	return poolID
}

// uniswapV4ViewChainID keeps the pool-manager view test off chain 1, whose
// single manager identity other tests in this package assert on.
const uniswapV4ViewChainID = 475001

func TestUniswapV4PoolManagerCurrentViewReturnsOnlyTheLatestVersion(t *testing.T) {
	ctx := context.Background()

	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES ($1, 'uniswap_v4_view_test') ON CONFLICT (chain_id) DO NOTHING`,
		uniswapV4ViewChainID); err != nil {
		t.Fatalf("seeding view-test chain: %v", err)
	}
	if _, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		VALUES ($1, $2::bytea, 'UniswapV4', 'dex', $3)
		ON CONFLICT (chain_id, address) DO NOTHING`,
		uniswapV4ViewChainID, uniswapV4PoolManagerHex, uniswapV4DeployBlock); err != nil {
		t.Fatalf("seeding view-test protocol: %v", err)
	}

	var protocolID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE chain_id = $1 AND address = $2::bytea`,
		uniswapV4ViewChainID, uniswapV4PoolManagerHex).Scan(&protocolID); err != nil {
		t.Fatalf("reading view-test protocol id: %v", err)
	}

	const correctedStateViewHex = "\\x1111111111111111111111111111111111111111"
	insert := `
		INSERT INTO uniswap_v4_pool_manager
		    (chain_id, protocol_id, state_view_address, deploy_block, build_id)
		VALUES ($1, $2, $3::bytea, $4, $5)
		ON CONFLICT (chain_id, processing_version) DO NOTHING`
	for _, seed := range []struct {
		stateViewHex string
		buildID      int
	}{
		{uniswapV4StateViewHex, 0},
		{correctedStateViewHex, 1},
	} {
		if _, err := uniswapV4TestPool.Exec(ctx, insert,
			uniswapV4ViewChainID, protocolID,
			seed.stateViewHex, uniswapV4DeployBlock, seed.buildID); err != nil {
			t.Fatalf("seeding pool manager version under build %d: %v", seed.buildID, err)
		}
	}

	rows, err := uniswapV4TestPool.Query(ctx, `
		SELECT processing_version, state_view_address
		FROM uniswap_v4_pool_manager_current
		WHERE chain_id = $1`, uniswapV4ViewChainID)
	if err != nil {
		t.Fatalf("reading uniswap_v4_pool_manager_current: %v", err)
	}
	defer rows.Close()

	var seen int
	for rows.Next() {
		seen++
		var pv int
		var stateView []byte
		if err := rows.Scan(&pv, &stateView); err != nil {
			t.Fatalf("scanning uniswap_v4_pool_manager_current row: %v", err)
		}
		if pv != 1 {
			t.Errorf("processing_version = %d, want 1 (the correction)", pv)
		}
		if want := decodeBytea(t, correctedStateViewHex); !bytes.Equal(stateView, want) {
			t.Errorf("state_view_address = %x, want the corrected %x", stateView, want)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating uniswap_v4_pool_manager_current: %v", err)
	}
	if seen != 1 {
		t.Errorf("uniswap_v4_pool_manager_current rows for chain %d = %d, want 1", uniswapV4ViewChainID, seen)
	}
}

func TestUniswapV4PoolCurrentViewReturnsOnlyTheLatestVersion(t *testing.T) {
	ctx := context.Background()

	const poolIDHex = "\\x1500000000000000000000000000000000000000000000000000000000000001"
	insertTestUniswapV4Pool(t, ctx, poolIDHex)

	wstETH := seedUniswapV4Token(t, ctx, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18)
	usdc := seedUniswapV4Token(t, ctx, "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6)

	const correctedFee = 500
	if _, err := uniswapV4TestPool.Exec(ctx, `
		INSERT INTO uniswap_v4_pool
		    (chain_id, pool_id, currency0, currency1,
		     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block, build_id)
		VALUES (1, $1::bytea,
		        '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,
		        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
		        $2, $3, $4, 60, '\x0000000000000000000000000000000000000000'::bytea, 21743144, 1)
		ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING`,
		poolIDHex, wstETH, usdc, correctedFee); err != nil {
		t.Fatalf("appending a corrected pool version: %v", err)
	}

	rows, err := uniswapV4TestPool.Query(ctx, `
		SELECT processing_version, fee
		FROM uniswap_v4_pool_current
		WHERE chain_id = 1 AND pool_id = $1::bytea`, poolIDHex)
	if err != nil {
		t.Fatalf("reading uniswap_v4_pool_current: %v", err)
	}
	defer rows.Close()

	var seen int
	for rows.Next() {
		seen++
		var pv, fee int
		if err := rows.Scan(&pv, &fee); err != nil {
			t.Fatalf("scanning uniswap_v4_pool_current row: %v", err)
		}
		if pv != 1 {
			t.Errorf("processing_version = %d, want 1 (the correction)", pv)
		}
		if fee != correctedFee {
			t.Errorf("fee = %d, want the corrected %d", fee, correctedFee)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating uniswap_v4_pool_current: %v", err)
	}
	if seen != 1 {
		t.Errorf("uniswap_v4_pool_current rows for the corrected pool = %d, want 1", seen)
	}
}

// A WITH (tsdb.hypertable, ...) declaration creates its own 1-day compression
// policy, and add_compression_policy then returns -1 instead of widening it, so
// the declared 2 days has to be asserted rather than assumed.
func TestUniswapV4HypertablesCompressAfterTwoDays(t *testing.T) {
	ctx := context.Background()

	for _, table := range uniswapV4Hypertables {
		var policies int
		var compressAfter string
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT count(*), COALESCE(min(config->>'compress_after'), '')
			FROM timescaledb_information.jobs
			WHERE proc_name = 'policy_compression'
			  AND hypertable_schema = 'public'
			  AND hypertable_name = $1`, table).Scan(&policies, &compressAfter); err != nil {
			t.Fatalf("reading compression policy for %s: %v", table, err)
		}
		if policies != 1 {
			t.Errorf("%s has %d compression policies, want exactly 1", table, policies)
			continue
		}

		var isTwoDays bool
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT $1::interval = INTERVAL '2 days'`, compressAfter).Scan(&isTwoDays); err != nil {
			t.Fatalf("comparing compress_after for %s: %v", table, err)
		}
		if !isTwoDays {
			t.Errorf("%s compress_after = %q, want 2 days", table, compressAfter)
		}
	}
}

func TestUniswapV4CreatedAtIsTimestamptz(t *testing.T) {
	ctx := context.Background()

	for _, table := range []string{"uniswap_v4_pool_manager", "uniswap_v4_pool"} {
		var dataType, columnDefault string
		if err := uniswapV4TestPool.QueryRow(ctx, `
			SELECT data_type, column_default
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'created_at'`,
			table).Scan(&dataType, &columnDefault); err != nil {
			t.Fatalf("reading %s.created_at definition: %v", table, err)
		}
		if want := "timestamp with time zone"; dataType != want {
			t.Errorf("%s.created_at data_type = %q, want %q", table, dataType, want)
		}
		if want := "now()"; columnDefault != want {
			t.Errorf("%s.created_at column_default = %q, want %q", table, columnDefault, want)
		}
	}
}

// created_at holds an instant, so a row written under a non-UTC session still
// compares equal to now(); a naive UTC wall-clock column would be read back as
// the session's local time and drift by that session's UTC offset.
func TestUniswapV4CreatedAtIgnoresSessionTimeZone(t *testing.T) {
	ctx := context.Background()

	const poolIDHex = "\\x1600000000000000000000000000000000000000000000000000000000000001"
	seedUniswapV4PoolManager(t, ctx)
	wstETH := seedUniswapV4Token(t, ctx, "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18)
	usdc := seedUniswapV4Token(t, ctx, "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6)

	conn, err := pgx.Connect(ctx, sharedDSN)
	if err != nil {
		t.Fatalf("opening a dedicated connection: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `SET TIME ZONE 'America/New_York'`); err != nil {
		t.Fatalf("setting the session TimeZone: %v", err)
	}

	var createdAt time.Time
	var driftSeconds float64
	if err := conn.QueryRow(ctx, `
		INSERT INTO uniswap_v4_pool
		    (chain_id, pool_id, currency0, currency1,
		     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block)
		VALUES (1, $1::bytea,
		        '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,
		        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
		        $2, $3, 3000, 60, '\x0000000000000000000000000000000000000000'::bytea, 21743144)
		RETURNING created_at, EXTRACT(EPOCH FROM (now() - created_at))::double precision`,
		poolIDHex, wstETH, usdc).Scan(&createdAt, &driftSeconds); err != nil {
		t.Fatalf("inserting under a non-UTC session TimeZone: %v", err)
	}

	if driftSeconds < -10 || driftSeconds > 10 {
		t.Errorf("created_at = %s, %.0fs away from now() under a non-UTC session; it is not stored as an instant",
			createdAt, driftSeconds)
	}
}
