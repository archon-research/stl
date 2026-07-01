//go:build integration

package postgres

import (
	"context"
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
