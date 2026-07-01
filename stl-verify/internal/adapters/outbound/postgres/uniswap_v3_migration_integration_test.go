//go:build integration

package postgres

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const uniswapV3SchemaName = "test_uniswap_v3_migration"

var uniswapV3TestPool *pgxpool.Pool

func init() {
	registerTestFileSetup(uniswapV3SchemaName, func() {
		uniswapV3TestPool = testutil.SetupSchemaForMain(sharedDSN, uniswapV3SchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, uniswapV3TestPool, uniswapV3SchemaName)
	})
}

// TestUniswapV3Migration verifies that the
// 20260701_100000_create_uniswap_v3_tables.sql migration applies cleanly and
// produces the expected tables, hypertables, triggers, and FKs.
func TestUniswapV3Migration(t *testing.T) {
	ctx := context.Background()

	t.Run("tables_exist", func(t *testing.T) {
		tables := []string{
			"uniswap_v3_pool",
			"uniswap_v3_pool_state",
			"uniswap_v3_swap",
			"uniswap_v3_liquidity_event",
			"uniswap_v3_tick",
			"uniswap_v3_pool_event",
		}
		for _, table := range tables {
			var exists bool
			if err := uniswapV3TestPool.QueryRow(ctx, `
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
	})

	t.Run("hypertables_registered", func(t *testing.T) {
		hypertables := []string{
			"uniswap_v3_pool_state",
			"uniswap_v3_swap",
			"uniswap_v3_liquidity_event",
			"uniswap_v3_pool_event",
		}
		for _, table := range hypertables {
			var exists bool
			if err := uniswapV3TestPool.QueryRow(ctx, `
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
	})

	t.Run("uniswap_v3_tick_is_not_a_hypertable", func(t *testing.T) {
		var exists bool
		if err := uniswapV3TestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM _timescaledb_catalog.hypertable
				WHERE table_name = 'uniswap_v3_tick'
			)`).Scan(&exists); err != nil {
			t.Fatalf("checking uniswap_v3_tick hypertable registration: %v", err)
		}
		if exists {
			t.Error("uniswap_v3_tick should be a regular append-on-change table, not a hypertable")
		}
	})

	t.Run("processing_version_triggers_exist", func(t *testing.T) {
		tables := []string{
			"uniswap_v3_pool_state",
			"uniswap_v3_swap",
			"uniswap_v3_liquidity_event",
			"uniswap_v3_tick",
			"uniswap_v3_pool_event",
		}
		for _, table := range tables {
			var exists bool
			if err := uniswapV3TestPool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM pg_trigger tg
					JOIN pg_class c ON c.oid = tg.tgrelid
					JOIN pg_proc p ON p.oid = tg.tgfoid
					WHERE NOT tg.tgisinternal
					  AND c.relname = $1
					  AND p.proname LIKE 'assign_processing_version_uniswap_v3_%'
				)`, table).Scan(&exists); err != nil {
				t.Fatalf("checking processing_version trigger on %s: %v", table, err)
			}
			if !exists {
				t.Errorf("assign_processing_version_uniswap_v3_* trigger missing on %s", table)
			}
		}
	})

	t.Run("uniswap_v3_pool_unique_chain_and_address", func(t *testing.T) {
		var exists bool
		if err := uniswapV3TestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_constraint con
				JOIN pg_class c ON c.oid = con.conrelid
				WHERE c.relname = 'uniswap_v3_pool'
				  AND con.contype = 'u'
				  AND con.conkey = (
				      SELECT array_agg(a.attnum ORDER BY a.attnum)
				      FROM pg_attribute a
				      WHERE a.attrelid = c.oid
				        AND a.attname IN ('chain_id', 'pool_address')
				  )
			)`).Scan(&exists); err != nil {
			t.Fatalf("checking uniswap_v3_pool UNIQUE(chain_id, pool_address): %v", err)
		}
		if !exists {
			t.Error("UNIQUE(chain_id, pool_address) constraint missing on uniswap_v3_pool")
		}
	})

	t.Run("uniswap_v3_pool_foreign_keys", func(t *testing.T) {
		fks := map[string]string{
			"chain_id":    "chain",
			"protocol_id": "protocol",
			"token0_id":   "token",
			"token1_id":   "token",
		}
		for col, refTable := range fks {
			var exists bool
			if err := uniswapV3TestPool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM pg_constraint con
					JOIN pg_class c ON c.oid = con.conrelid
					JOIN pg_class rc ON rc.oid = con.confrelid
					JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = con.conkey[1]
					WHERE c.relname = 'uniswap_v3_pool'
					  AND con.contype = 'f'
					  AND array_length(con.conkey, 1) = 1
					  AND a.attname = $1
					  AND rc.relname = $2
				)`, col, refTable).Scan(&exists); err != nil {
				t.Fatalf("checking FK uniswap_v3_pool.%s -> %s: %v", col, refTable, err)
			}
			if !exists {
				t.Errorf("FK uniswap_v3_pool.%s -> %s missing", col, refTable)
			}
		}
	})

	t.Run("processing_version_trigger_fires_on_uniswap_v3_pool_state", func(t *testing.T) {
		poolID := insertTestUniswapV3Pool(t, ctx, "\\x1111111111111111111111111111111111111111")

		var pv int
		err := uniswapV3TestPool.QueryRow(ctx, `
			INSERT INTO uniswap_v3_pool_state
			    (pool_id, block_number, block_version, block_timestamp,
			     sqrt_price_x96, tick, observation_index, observation_cardinality,
			     observation_cardinality_next, fee_protocol, unlocked, liquidity,
			     fee_growth_global0_x128, fee_growth_global1_x128,
			     protocol_fees_token0, protocol_fees_token1, balance0, balance1, build_id)
			VALUES ($1, 18000000, 0, '2023-06-01T00:00:00Z'::timestamptz,
			        79228162514264337593543950336, 0, 0, 1, 1, 0, true, 1000000000000000000,
			        0, 0, 0, 0, 1000000000000000000, 1000000000000000000, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test pool_state: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("processing_version_trigger_fires_on_uniswap_v3_swap", func(t *testing.T) {
		poolID := insertTestUniswapV3Pool(t, ctx, "\\x2222222222222222222222222222222222222222")

		var pv int
		err := uniswapV3TestPool.QueryRow(ctx, `
			INSERT INTO uniswap_v3_swap
			    (pool_id, block_number, block_version, block_timestamp,
			     tx_hash, log_index, sender, recipient, amount0, amount1,
			     sqrt_price_x96, liquidity, tick, build_id)
			VALUES ($1, 18000001, 0, '2023-06-01T00:01:00Z'::timestamptz,
			        '\xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd'::bytea,
			        0,
			        '\x3333333333333333333333333333333333333333'::bytea,
			        '\x4444444444444444444444444444444444444444'::bytea,
			        1000000000000000000, -990000000000000000,
			        79228162514264337593543950336, 1000000000000000000, 0, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test swap: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("processing_version_trigger_fires_on_uniswap_v3_liquidity_event", func(t *testing.T) {
		poolID := insertTestUniswapV3Pool(t, ctx, "\\x5555555555555555555555555555555555555555")

		var pv int
		err := uniswapV3TestPool.QueryRow(ctx, `
			INSERT INTO uniswap_v3_liquidity_event
			    (pool_id, block_number, block_version, block_timestamp,
			     tx_hash, log_index, event_name, owner, tick_lower, tick_upper,
			     amount, amount0, amount1, build_id)
			VALUES ($1, 18000002, 0, '2023-06-01T00:02:00Z'::timestamptz,
			        '\xbbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddee'::bytea,
			        0, 'mint',
			        '\x6666666666666666666666666666666666666666'::bytea,
			        -100, 100,
			        1000000000000000000, 1000000000000000000, 990000000000000000, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test liquidity event: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("processing_version_trigger_fires_on_uniswap_v3_tick", func(t *testing.T) {
		poolID := insertTestUniswapV3Pool(t, ctx, "\\x7777777777777777777777777777777777777777")

		var pv int
		err := uniswapV3TestPool.QueryRow(ctx, `
			INSERT INTO uniswap_v3_tick
			    (pool_id, tick, block_number, block_version, block_timestamp,
			     liquidity_gross, liquidity_net, fee_growth_outside0_x128,
			     fee_growth_outside1_x128, initialized, build_id)
			VALUES ($1, -100, 18000003, 0, '2023-06-01T00:03:00Z'::timestamptz,
			        1000000000000000000, 1000000000000000000, 0, 0, true, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test tick: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("processing_version_trigger_fires_on_uniswap_v3_pool_event", func(t *testing.T) {
		poolID := insertTestUniswapV3Pool(t, ctx, "\\x8888888888888888888888888888888888888888")

		var pv int
		err := uniswapV3TestPool.QueryRow(ctx, `
			INSERT INTO uniswap_v3_pool_event
			    (pool_id, block_number, block_version, block_timestamp,
			     tx_hash, log_index, event_name, params, build_id)
			VALUES ($1, 18000004, 0, '2023-06-01T00:04:00Z'::timestamptz,
			        '\xccddeeffccddeeffccddeeffccddeeffccddeeffccddeeffccddeeffccddeeff'::bytea,
			        0, 'initialize', '{"sqrtPriceX96": "79228162514264337593543950336", "tick": 0}'::jsonb, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test pool event: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})
}

// TestUniswapV3ColumnComments asserts that every column on every
// uniswap_v3_* table has a COMMENT set. Failures indicate that
// 20260701_100100_uniswap_v3_comments.sql is missing entries for newly added
// columns or tables.
func TestUniswapV3ColumnComments(t *testing.T) {
	ctx := context.Background()

	uniswapV3Tables := []string{
		"uniswap_v3_pool",
		"uniswap_v3_pool_state",
		"uniswap_v3_swap",
		"uniswap_v3_liquidity_event",
		"uniswap_v3_tick",
		"uniswap_v3_pool_event",
	}

	type uncommentedCol struct {
		table  string
		column string
	}
	var missing []uncommentedCol

	for _, table := range uniswapV3Tables {
		// pg_table_is_visible resolves through this connection's search_path,
		// the same way an unqualified table name in application SQL would.
		// This harness's per-schema migration run is a no-op once
		// public.migrations is populated (Migrator.getAppliedMigrations
		// hardcodes table_schema='public'), so uniswap_v3_* tables live in
		// public regardless of this test file's own schema; filtering on
		// n.nspname = current_schema() would silently match zero rows.
		rows, err := uniswapV3TestPool.Query(ctx, `
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

	if len(missing) != 0 {
		for _, m := range missing {
			t.Errorf("uniswap_v3 column missing COMMENT: %s.%s", m.table, m.column)
		}
	}
}

// uniswapV3SeedTokens are the counterparty tokens referenced by the 18 seeded
// pools that are NOT already guaranteed present by earlier migrations
// (WETH/USDC/USDT/wstETH/weETH/rsETH/ezETH via 20260204_110000_seed_sparklend_
// tokens.sql, stETH via 20260521_105000_seed_steth_token.sql). Upserted here
// (idempotent ON CONFLICT DO NOTHING) so this test is self-sufficient even
// though 20260701_100200_seed_uniswap_v3_pools.sql itself is what seeds them
// on a real DB: sibling integration test files in this package TRUNCATE
// shared tables between runs, so this test must not depend on migration-seeded
// rows surviving to when it runs (see insertTestUniswapV3Pool below).
var uniswapV3SeedTokens = []struct {
	addrHex  string
	symbol   string
	decimals int
}{
	{"\\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH", 18},
	{"\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC", 6},
	{"\\xdAC17F958D2ee523a2206206994597C13D831ec7", "USDT", 6},
	{"\\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", "stETH", 18},
	{"\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18},
	{"\\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee", "weETH", 18},
	{"\\xA35b1B31Ce002FBF2058D22F30f95D405200A15b", "ETHx", 18},
	{"\\xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7", "rsETH", 18},
	{"\\xbf5495Efe5DB9ce00f80364C8B423567e58d2110", "ezETH", 18},
	{"\\x8c9532a60E0e7C6Bbd2b2c1303F63aCE1c3E9811", "pzETH", 18},
	{"\\xf073bAC22DAb7FaF4a3Dd6c6189a70D54110525C", "inETH", 18},
	{"\\x49446A0874197839D15395B908328a74ccc96Bc0", "mstETH", 18},
	{"\\xe2237a34246eeDcEB43283073Ba9adEF0450351E", "fstETH", 18},
	{"\\x4f3cc6359364004b245ad5be36e6ad4e805dc961", "urLRT", 18},
	{"\\x7690202e2C2297bcD03664e31116D1dFFE7e3B73", "boxETH", 18},
}

// uniswapV3MaxLiquidityPerTick mirrors the tickSpacingToMaxLiquidityPerTick()
// constants used in 20260701_100200_seed_uniswap_v3_pools.sql, keyed by
// tick_spacing. Same four values for every pool sharing that spacing.
var uniswapV3MaxLiquidityPerTick = map[int]string{
	1:   "191757530477355301479181766273477",
	10:  "1917569901783203986719870431555990",
	60:  "11505743598341114571880798222544994",
	200: "38350317471085141830651933667504588",
}

// uniswapV3ExpectedPool describes one of the 18 seeded wstETH/stETH pools for
// TestUniswapV3PoolSeed assertions.
type uniswapV3ExpectedPool struct {
	addrHex     string
	token0Hex   string
	token1Hex   string
	fee         int
	tickSpacing int
	deployBlock int64
}

var uniswapV3ExpectedPools = []uniswapV3ExpectedPool{
	{"\\x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 100, 1, 15384250},
	{"\\xD340B57AAcDD10F96FC1CF10e15921936F41E29c", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 500, 10, 12376093},
	{"\\xC12aF0C4AA39D3061c56cD3CB19f5e62dEeaeBdE", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 3000, 60, 14943576},
	{"\\x4622Df6fB2d9Bee0DCDaCF545aCDB6a2b2f4f863", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 500, 10, 16065412},
	{"\\x173821f6aD4c5324cd35753A9FD12D92f2eaAB29", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 3000, 60, 14420259},
	{"\\xeC5055067d60292Ab2c514A1090Bc8E014e4aBAA", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xdAC17F958D2ee523a2206206994597C13D831ec7", 500, 10, 18461625},
	{"\\x63818BbDd21E69bE108A23aC1E84cBf66399Bd7D", "\\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", "\\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 10000, 200, 14937573},
	{"\\xf47F04a8605bE181E525D6391233cbA1f7474182", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee", 500, 10, 19629516},
	{"\\xC9eD5de354D4BE9fd576D3108C7637a71C01faA1", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xA35b1B31Ce002FBF2058D22F30f95D405200A15b", 100, 1, 20525114},
	{"\\x7a27c7b7E2536e452c57d3E8b909d9ecba2e2eee", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7", 500, 10, 19970144},
	{"\\x1b9d58bEa5eD5d935cc2E818Dde1D796abFf0bc0", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xbf5495Efe5DB9ce00f80364C8B423567e58d2110", 500, 10, 20011672},
	{"\\xfc354f5cf57125a7d85E1165f4FCDfd3006db61a", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\x8c9532a60E0e7C6Bbd2b2c1303F63aCE1c3E9811", 100, 1, 20192842},
	{"\\x3c0a1a9e0E22b9Acc9248D9f358286e9e9205b0a", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xf073bAC22DAb7FaF4a3Dd6c6189a70D54110525C", 500, 10, 19232685},
	{"\\x7f13847459450236d2D233f1c08D742Ed69D2997", "\\x49446A0874197839D15395B908328a74ccc96Bc0", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 500, 10, 20011699},
	{"\\x526389df2DCc8c5F7af69E93aD9E0d8FC21799f6", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xe2237a34246eeDcEB43283073Ba9adEF0450351E", 10000, 200, 16478531},
	{"\\x39683566C148851464781f0673112eF0746B9578", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "\\xe2237a34246eeDcEB43283073Ba9adEF0450351E", 3000, 60, 16486285},
	{"\\x104b3e3aCd2396a7292223b5778EA1CACdb68eC9", "\\x4f3cc6359364004b245ad5be36e6ad4e805dc961", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 100, 1, 20832240},
	{"\\x703A177Fcb4dEf281d180d3619a5edbAE67Ec7b5", "\\x7690202e2C2297bcD03664e31116D1dFFE7e3B73", "\\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", 10000, 200, 23506288},
}

// TestUniswapV3PoolSeed verifies that
// 20260701_100200_seed_uniswap_v3_pools.sql inserts exactly the 18 real
// wstETH/stETH pools from the VEC-261 design spec (§4), with correct
// fee/tick_spacing pairing, non-null deploy_block, and byte-for-byte
// token0/token1 addresses -- including the 3 pools where wstETH is token1
// (mstETH, urLRT, boxETH are token0), which must NOT be "normalized".
func TestUniswapV3PoolSeed(t *testing.T) {
	ctx := context.Background()

	// Self-seed the UniswapV3 protocol row and every counterparty token:
	// sibling integration test files TRUNCATE protocol/token between runs,
	// so this test cannot rely on the dex_prereqs/sparklend/steth migration
	// seeds surviving to when it executes.
	if _, err := uniswapV3TestPool.Exec(ctx, `
		INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		VALUES (1, '\x1F98431c8aD98523631AE4a59f267346ea31F984'::bytea, 'UniswapV3', 'dex', 12369621)
		ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name`,
	); err != nil {
		t.Fatalf("seeding UniswapV3 protocol row: %v", err)
	}

	for _, tok := range uniswapV3SeedTokens {
		if _, err := uniswapV3TestPool.Exec(ctx, `
			INSERT INTO token (chain_id, address, symbol, decimals)
			VALUES (1, $1::bytea, $2, $3)
			ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals`,
			tok.addrHex, tok.symbol, tok.decimals,
		); err != nil {
			t.Fatalf("seeding token %s: %v", tok.symbol, err)
		}
	}

	// Self-seed the 18 pool rows too (mirroring, not re-running,
	// 20260701_100200_seed_uniswap_v3_pools.sql): protocol_repository_
	// integration_test.go and token_repository_integration_test.go TRUNCATE
	// protocol/token CASCADE, which wipes uniswap_v3_pool's FK rows, so a
	// migration-only seed does not survive to whichever test runs last in
	// this package. ON CONFLICT DO UPDATE keeps this idempotent no matter how
	// many times TestUniswapV3PoolSeed itself re-runs.
	for _, pool := range uniswapV3ExpectedPools {
		if _, err := uniswapV3TestPool.Exec(ctx, `
			INSERT INTO uniswap_v3_pool
			    (chain_id, protocol_id, pool_address, token0_id, token1_id,
			     fee, tick_spacing, max_liquidity_per_tick, deploy_block)
			SELECT 1, pr.id, $1::bytea, t0.id, t1.id, $4, $5, $6, $7
			FROM protocol pr, token t0, token t1
			WHERE pr.chain_id = 1 AND pr.name = 'UniswapV3'
			  AND t0.chain_id = 1 AND t0.address = $2::bytea
			  AND t1.chain_id = 1 AND t1.address = $3::bytea
			ON CONFLICT (chain_id, pool_address) DO UPDATE SET
			    token0_id = EXCLUDED.token0_id,
			    token1_id = EXCLUDED.token1_id,
			    fee = EXCLUDED.fee,
			    tick_spacing = EXCLUDED.tick_spacing,
			    max_liquidity_per_tick = EXCLUDED.max_liquidity_per_tick,
			    deploy_block = EXCLUDED.deploy_block`,
			pool.addrHex, pool.token0Hex, pool.token1Hex,
			pool.fee, pool.tickSpacing, uniswapV3MaxLiquidityPerTick[pool.tickSpacing], pool.deployBlock,
		); err != nil {
			t.Fatalf("seeding pool %s: %v", pool.addrHex, err)
		}
	}

	// Scoped to the addresses this seed migration owns, not "every pool under
	// the UniswapV3 protocol": TestUniswapV3Migration (same file, same shared
	// schema) inserts its own throwaway fixture pools via
	// insertTestUniswapV3Pool under the same protocol row, which would
	// inflate an unscoped count.
	seededAddrs := make([]string, len(uniswapV3ExpectedPools))
	for i, p := range uniswapV3ExpectedPools {
		seededAddrs[i] = p.addrHex
	}

	t.Run("exactly_18_pools_for_uniswap_v3", func(t *testing.T) {
		var count int
		if err := uniswapV3TestPool.QueryRow(ctx, `
			SELECT count(*)
			FROM uniswap_v3_pool p
			JOIN protocol pr ON pr.id = p.protocol_id
			WHERE pr.name = 'UniswapV3' AND pr.chain_id = 1
			  AND p.pool_address = ANY($1::bytea[])`,
			seededAddrs,
		).Scan(&count); err != nil {
			t.Fatalf("counting uniswap_v3_pool rows: %v", err)
		}
		if count != 18 {
			t.Errorf("uniswap_v3_pool count for the seeded pool addresses = %d, want 18", count)
		}
	})

	t.Run("no_null_deploy_block", func(t *testing.T) {
		var nullCount int
		if err := uniswapV3TestPool.QueryRow(ctx, `
			SELECT count(*)
			FROM uniswap_v3_pool p
			JOIN protocol pr ON pr.id = p.protocol_id
			WHERE pr.name = 'UniswapV3' AND pr.chain_id = 1 AND p.deploy_block IS NULL
			  AND p.pool_address = ANY($1::bytea[])`,
			seededAddrs,
		).Scan(&nullCount); err != nil {
			t.Fatalf("counting NULL deploy_block rows: %v", err)
		}
		if nullCount != 0 {
			t.Errorf("uniswap_v3_pool rows with NULL deploy_block = %d, want 0", nullCount)
		}
	})

	t.Run("fee_tick_spacing_and_deploy_block_per_pool", func(t *testing.T) {
		for _, want := range uniswapV3ExpectedPools {
			var fee, tickSpacing int
			var deployBlock int64
			err := uniswapV3TestPool.QueryRow(ctx, `
				SELECT fee, tick_spacing, deploy_block
				FROM uniswap_v3_pool
				WHERE chain_id = 1 AND pool_address = $1::bytea`,
				want.addrHex,
			).Scan(&fee, &tickSpacing, &deployBlock)
			if err != nil {
				t.Fatalf("pool %s: %v", want.addrHex, err)
			}
			if fee != want.fee {
				t.Errorf("pool %s: fee = %d, want %d", want.addrHex, fee, want.fee)
			}
			if tickSpacing != want.tickSpacing {
				t.Errorf("pool %s: tick_spacing = %d, want %d", want.addrHex, tickSpacing, want.tickSpacing)
			}
			if deployBlock != want.deployBlock {
				t.Errorf("pool %s: deploy_block = %d, want %d", want.addrHex, deployBlock, want.deployBlock)
			}
		}
	})

	t.Run("token0_token1_address_equality", func(t *testing.T) {
		// Representative subset per the task spec: pool #1 (wstETH/WETH,
		// 0.01% fee), pool #7 (stETH/WETH, 1% fee), and pool #14
		// (mstETH/wstETH) where wstETH is token1, not token0 -- the
		// orientation is authoritative from a live cast scan and must not be
		// "normalized" by this seed.
		representative := []struct {
			name string
			pool uniswapV3ExpectedPool
		}{
			{"pool1_wstETH_WETH_001pct", uniswapV3ExpectedPools[0]},
			{"pool7_stETH_WETH_1pct", uniswapV3ExpectedPools[6]},
			{"pool14_mstETH_wstETH_wstETH_is_token1", uniswapV3ExpectedPools[13]},
		}

		for _, tc := range representative {
			t.Run(tc.name, func(t *testing.T) {
				var token0Addr, token1Addr []byte
				err := uniswapV3TestPool.QueryRow(ctx, `
					SELECT t0.address, t1.address
					FROM uniswap_v3_pool p
					JOIN token t0 ON t0.id = p.token0_id
					JOIN token t1 ON t1.id = p.token1_id
					WHERE p.chain_id = 1 AND p.pool_address = $1::bytea`,
					tc.pool.addrHex,
				).Scan(&token0Addr, &token1Addr)
				if err != nil {
					t.Fatalf("pool %s: %v", tc.pool.addrHex, err)
				}

				wantToken0 := decodeBytea(t, tc.pool.token0Hex)
				wantToken1 := decodeBytea(t, tc.pool.token1Hex)

				if !bytes.Equal(token0Addr, wantToken0) {
					t.Errorf("pool %s: token0 address = %x, want %x", tc.pool.addrHex, token0Addr, wantToken0)
				}
				if !bytes.Equal(token1Addr, wantToken1) {
					t.Errorf("pool %s: token1 address = %x, want %x", tc.pool.addrHex, token1Addr, wantToken1)
				}
			})
		}
	})
}

// decodeBytea converts a "\xHEXADDR" literal (as used throughout this file
// and the migrations) into raw bytes for comparison against a scanned bytea
// column.
func decodeBytea(t *testing.T, hexLiteral string) []byte {
	t.Helper()
	hexPart := strings.TrimPrefix(hexLiteral, "\\x")
	b, err := hex.DecodeString(hexPart)
	if err != nil {
		t.Fatalf("decoding hex literal %s: %v", hexLiteral, err)
	}
	return b
}

// insertTestUniswapV3Pool inserts a minimal uniswap_v3_pool row (with its
// own protocol row plus token0/token1 rows) using addrHex as the pool
// address, and returns the new pool's id. Each test that needs a pool calls
// this with a distinct address so that fact-table tests inserting rows for
// "block 0, log_index 0" don't collide with each other under the
// natural-key advisory lock.
//
// The protocol row is upserted here rather than assumed from the
// 20260521_100000_create_dex_prereqs.sql seed: sibling integration tests in
// this package TRUNCATE protocol between runs, so this test must not depend
// on migration-seeded rows surviving to when it runs.
func insertTestUniswapV3Pool(t *testing.T, ctx context.Context, addrHex string) int64 {
	t.Helper()

	var protocolID int64
	if err := uniswapV3TestPool.QueryRow(ctx, `
		INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		VALUES (1, '\x1F98431c8aD98523631AE4a59f267346ea31F984'::bytea, 'UniswapV3', 'dex', 12369621)
		ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		RETURNING id`,
	).Scan(&protocolID); err != nil {
		t.Fatalf("seeding UniswapV3 protocol row: %v", err)
	}

	var token0ID, token1ID int64
	if err := uniswapV3TestPool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, 'USDC', 6)
		ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
		RETURNING id`,
	).Scan(&token0ID); err != nil {
		t.Fatalf("seeding token0: %v", err)
	}
	if err := uniswapV3TestPool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea, 'WETH', 18)
		ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
		RETURNING id`,
	).Scan(&token1ID); err != nil {
		t.Fatalf("seeding token1: %v", err)
	}

	var poolID int64
	if err := uniswapV3TestPool.QueryRow(ctx, `
		INSERT INTO uniswap_v3_pool
		    (chain_id, protocol_id, pool_address, token0_id, token1_id,
		     fee, tick_spacing, max_liquidity_per_tick, deploy_block)
		VALUES (1, $1, $2::bytea, $3, $4, 3000, 60, 11505743598341114571880798222544994, 12369739)
		ON CONFLICT (chain_id, pool_address) DO UPDATE SET pool_address = EXCLUDED.pool_address
		RETURNING id`,
		protocolID, addrHex, token0ID, token1ID,
	).Scan(&poolID); err != nil {
		t.Fatalf("inserting test uniswap_v3_pool: %v", err)
	}
	return poolID
}
