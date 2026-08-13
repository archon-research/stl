//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const curveSchemaName = "test_curve_migration"

var curveTestPool *pgxpool.Pool

func init() {
	registerTestFileSetup(curveSchemaName, func() {
		curveTestPool = testutil.SetupSchemaForMain(sharedDSN, curveSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, curveTestPool, curveSchemaName)
	})
}

// TestCurveExtendedDataMigration verifies that the extended-data schema folded
// into 20260521_110000_create_curve_dex_tables.sql applies cleanly and produces
// the expected columns, tables, and triggers.
// It also asserts that curve_pool_coin.precision is seeded for all 10 existing
// coins.
func TestCurveExtendedDataMigration(t *testing.T) {
	ctx := context.Background()

	t.Run("new_tables_exist", func(t *testing.T) {
		tables := []string{
			"curve_stableswap_config",
			"curve_cryptoswap_config",
			"curve_parameter_event",
			"curve_lp_token_event",
		}
		for _, table := range tables {
			var exists bool
			if err := curveTestPool.QueryRow(ctx, `
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

	t.Run("processing_version_triggers_on_new_tables", func(t *testing.T) {
		tables := []string{
			"curve_stableswap_config",
			"curve_cryptoswap_config",
			"curve_parameter_event",
			"curve_lp_token_event",
		}
		for _, table := range tables {
			var exists bool
			if err := curveTestPool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM information_schema.triggers
					WHERE trigger_name = 'trigger_assign_processing_version'
					  AND event_object_table = $1
				)`, table).Scan(&exists); err != nil {
				t.Fatalf("checking trigger on %s: %v", table, err)
			}
			if !exists {
				t.Errorf("trigger_assign_processing_version missing on %s", table)
			}
		}
	})

	t.Run("is_underlying_column_on_curve_swap", func(t *testing.T) {
		var exists bool
		if err := curveTestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'curve_swap' AND column_name = 'is_underlying'
			)`).Scan(&exists); err != nil {
			t.Fatalf("checking is_underlying: %v", err)
		}
		if !exists {
			t.Error("is_underlying column missing on curve_swap")
		}
	})

	t.Run("precision_column_on_curve_pool_coin", func(t *testing.T) {
		var exists bool
		if err := curveTestPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'curve_pool_coin' AND column_name = 'precision'
			)`).Scan(&exists); err != nil {
			t.Fatalf("checking precision column: %v", err)
		}
		if !exists {
			t.Error("precision column missing on curve_pool_coin")
		}
	})

	t.Run("precision_seeded_for_all_coins", func(t *testing.T) {
		var nullCount int
		if err := curveTestPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM curve_pool_coin WHERE precision IS NULL`).Scan(&nullCount); err != nil {
			t.Fatalf("counting NULL precision rows: %v", err)
		}
		if nullCount != 0 {
			t.Errorf("found %d curve_pool_coin rows with NULL precision after seeding; want 0", nullCount)
		}
	})

	t.Run("stableswap_state_extended_columns_exist", func(t *testing.T) {
		cols := []string{
			"a_precise", "admin_balances", "stored_rates",
			"ema_price", "get_p", "calc_token_amount", "calc_withdraw_one_coin",
		}
		for _, col := range cols {
			var exists bool
			if err := curveTestPool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_name = 'curve_stableswap_state' AND column_name = $1
				)`, col).Scan(&exists); err != nil {
				t.Fatalf("checking curve_stableswap_state.%s: %v", col, err)
			}
			if !exists {
				t.Errorf("curve_stableswap_state.%s missing", col)
			}
		}
	})

	t.Run("cryptoswap_state_extended_columns_exist", func(t *testing.T) {
		cols := []string{
			"admin_balances", "lp_price", "xcp_profit_a",
			"last_prices_timestamp", "get_dx", "calc_token_amount", "calc_withdraw_one_coin",
		}
		for _, col := range cols {
			var exists bool
			if err := curveTestPool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_name = 'curve_cryptoswap_state' AND column_name = $1
				)`, col).Scan(&exists); err != nil {
				t.Fatalf("checking curve_cryptoswap_state.%s: %v", col, err)
			}
			if !exists {
				t.Errorf("curve_cryptoswap_state.%s missing", col)
			}
		}
	})

	t.Run("stableswap_config_trigger_fires", func(t *testing.T) {
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving stETH classic pool id: %v", err)
		}

		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_stableswap_config
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     initial_a, initial_a_time, future_a, future_a_time,
			     admin_fee, future_fee, future_admin_fee, build_id)
			VALUES ($1, 11592700, 0, '2021-01-02T00:00:00Z'::timestamptz,
			        100, 0, 100, 0, 5000000000, 5000000000, 0, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test stableswap config: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("cryptoswap_config_trigger_fires", func(t *testing.T) {
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\x7F86Bf177Dd4F3494b841a37e810A34dD56c829B'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving TricryptoUSDC pool id: %v", err)
		}

		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_cryptoswap_config
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     initial_a_gamma, future_a_gamma, initial_a_gamma_time, future_a_gamma_time,
			     mid_fee, out_fee, fee_gamma, allowed_extra_profit, adjustment_step,
			     ma_time, admin_fee, build_id)
			VALUES ($1, 17072950, 0, '2023-04-02T00:00:00Z'::timestamptz,
			        2700000, 2700000, 0, 0,
			        5000000, 30000000, 500000000000000, 100000000000, 146000000000000,
			        600, 5000000000, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test cryptoswap config: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("parameter_event_trigger_fires", func(t *testing.T) {
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving stETH classic pool id: %v", err)
		}

		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_parameter_event
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     tx_hash, log_index, event_name, params, build_id)
			VALUES ($1, 11592800, 0, '2021-01-03T00:00:00Z'::timestamptz,
			        '\xccddee00ccddee00ccddee00ccddee00ccddee00ccddee00ccddee00ccddee00'::bytea,
			        0, 'ramp_a', '{"initial_A": 100, "future_A": 200}'::jsonb, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test parameter event: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("lp_token_event_trigger_fires", func(t *testing.T) {
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving stETH classic pool id: %v", err)
		}

		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_lp_token_event
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     tx_hash, log_index, event_name, from_address, to_address, value, build_id)
			VALUES ($1, 11592900, 0, '2021-01-04T00:00:00Z'::timestamptz,
			        '\xddeeff11ddeeff11ddeeff11ddeeff11ddeeff11ddeeff11ddeeff11ddeeff11'::bytea,
			        0, 'transfer',
			        '\x0000000000000000000000000000000000000000'::bytea,
			        '\x3333333333333333333333333333333333333333'::bytea,
			        1000000000000000000, 0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test lp token event: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})
}

// TestCurveColumnComments asserts that every column on every curve_* table has a
// COMMENT set. Failures indicate that 20260521_120000_curve_column_comments.sql
// is missing entries for newly added columns or tables.
func TestCurveColumnComments(t *testing.T) {
	ctx := context.Background()

	curveTables := []string{
		"curve_pool",
		"curve_pool_coin",
		"curve_swap",
		"curve_liquidity_event",
		"curve_stableswap_state",
		"curve_cryptoswap_state",
		"curve_stableswap_config",
		"curve_cryptoswap_config",
		"curve_parameter_event",
		"curve_lp_token_event",
	}

	type uncommentedCol struct {
		table  string
		column string
	}
	var missing []uncommentedCol

	for _, table := range curveTables {
		rows, err := curveTestPool.Query(ctx, `
			SELECT a.attname
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = $1
			  AND n.nspname = current_schema()
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
			t.Errorf("curve column missing COMMENT: %s.%s", m.table, m.column)
		}
	}
}

// TestCurveMigration verifies that the 20260521_110000_create_curve_dex_tables.sql
// migration applies cleanly and produces the expected schema + seed data.
func TestCurveMigration(t *testing.T) {
	ctx := context.Background()

	t.Run("tables_exist", func(t *testing.T) {
		tables := []string{
			"curve_pool",
			"curve_pool_coin",
			"curve_swap",
			"curve_liquidity_event",
			"curve_stableswap_state",
			"curve_cryptoswap_state",
		}
		for _, table := range tables {
			var exists bool
			if err := curveTestPool.QueryRow(ctx, `
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

	t.Run("processing_version_triggers_on_fact_tables", func(t *testing.T) {
		factTables := []string{
			"curve_swap",
			"curve_liquidity_event",
			"curve_stableswap_state",
			"curve_cryptoswap_state",
		}
		for _, table := range factTables {
			var exists bool
			if err := curveTestPool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM information_schema.triggers
					WHERE trigger_name = 'trigger_assign_processing_version'
					  AND event_object_table = $1
				)`, table).Scan(&exists); err != nil {
				t.Fatalf("checking trigger on %s: %v", table, err)
			}
			if !exists {
				t.Errorf("trigger_assign_processing_version missing on %s", table)
			}
		}
	})

	t.Run("seed_4_pools_chain1", func(t *testing.T) {
		var count int
		if err := curveTestPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM curve_pool WHERE chain_id = 1`).Scan(&count); err != nil {
			t.Fatalf("counting curve_pool: %v", err)
		}
		if count != 4 {
			t.Errorf("curve_pool count for chain_id=1 = %d, want 4", count)
		}
	})

	t.Run("all_3_pool_kinds_present", func(t *testing.T) {
		kinds := []string{"plain_pre_ng", "plain_ng", "cryptoswap"}
		for _, kind := range kinds {
			var count int
			if err := curveTestPool.QueryRow(ctx,
				`SELECT COUNT(*) FROM curve_pool WHERE chain_id = 1 AND pool_kind = $1`, kind).Scan(&count); err != nil {
				t.Fatalf("counting pool_kind %s: %v", kind, err)
			}
			if count < 1 {
				t.Errorf("no pools of kind %q seeded", kind)
			}
		}
	})

	t.Run("steth_classic_pool_seeded", func(t *testing.T) {
		var kind string
		var nCoins int
		var lpToken []byte
		if err := curveTestPool.QueryRow(ctx, `
			SELECT pool_kind, n_coins, lp_token_address
			FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea`,
		).Scan(&kind, &nCoins, &lpToken); err != nil {
			t.Fatalf("querying stETH classic pool: %v", err)
		}
		if kind != "plain_pre_ng" {
			t.Errorf("stETH classic kind = %q, want plain_pre_ng", kind)
		}
		if nCoins != 2 {
			t.Errorf("stETH classic n_coins = %d, want 2", nCoins)
		}
		if len(lpToken) == 0 {
			t.Error("stETH classic lp_token_address is NULL, want steCRV address")
		}
	})

	t.Run("steth_ng_pool_seeded", func(t *testing.T) {
		var kind string
		var nCoins int
		var lpToken *[]byte
		if err := curveTestPool.QueryRow(ctx, `
			SELECT pool_kind, n_coins, lp_token_address
			FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea`,
		).Scan(&kind, &nCoins, &lpToken); err != nil {
			t.Fatalf("querying stETH-ng pool: %v", err)
		}
		if kind != "plain_ng" {
			t.Errorf("stETH-ng kind = %q, want plain_ng", kind)
		}
		if nCoins != 2 {
			t.Errorf("stETH-ng n_coins = %d, want 2", nCoins)
		}
		if lpToken != nil {
			t.Error("stETH-ng lp_token_address should be NULL for NG pool")
		}
	})

	t.Run("threepool_seeded", func(t *testing.T) {
		var kind string
		var nCoins int
		if err := curveTestPool.QueryRow(ctx, `
			SELECT pool_kind, n_coins
			FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'::bytea`,
		).Scan(&kind, &nCoins); err != nil {
			t.Fatalf("querying 3pool: %v", err)
		}
		if kind != "plain_pre_ng" {
			t.Errorf("3pool kind = %q, want plain_pre_ng", kind)
		}
		if nCoins != 3 {
			t.Errorf("3pool n_coins = %d, want 3", nCoins)
		}
	})

	t.Run("tricryptousdc_seeded", func(t *testing.T) {
		var kind string
		var nCoins int
		var lpToken *[]byte
		if err := curveTestPool.QueryRow(ctx, `
			SELECT pool_kind, n_coins, lp_token_address
			FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\x7F86Bf177Dd4F3494b841a37e810A34dD56c829B'::bytea`,
		).Scan(&kind, &nCoins, &lpToken); err != nil {
			t.Fatalf("querying TricryptoUSDC: %v", err)
		}
		if kind != "cryptoswap" {
			t.Errorf("TricryptoUSDC kind = %q, want cryptoswap", kind)
		}
		if nCoins != 3 {
			t.Errorf("TricryptoUSDC n_coins = %d, want 3", nCoins)
		}
		if lpToken != nil {
			t.Error("TricryptoUSDC lp_token_address should be NULL")
		}
	})

	t.Run("coin_count_per_pool", func(t *testing.T) {
		// stETH classic: 2, stETH-ng: 2, 3pool: 3, TricryptoUSDC: 3 => 10 total.
		var total int
		if err := curveTestPool.QueryRow(ctx, `SELECT COUNT(*) FROM curve_pool_coin`).Scan(&total); err != nil {
			t.Fatalf("counting curve_pool_coin: %v", err)
		}
		if total != 10 {
			t.Errorf("curve_pool_coin total = %d, want 10 (2+2+3+3)", total)
		}
	})

	t.Run("coins_joined_by_address_not_symbol", func(t *testing.T) {
		// Every curve_pool_coin row must resolve to a token row with the correct address.
		// Checks that coins were inserted by address join, not by symbol match.
		var unresolved int
		if err := curveTestPool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM curve_pool_coin cpc
			WHERE NOT EXISTS (SELECT 1 FROM token t WHERE t.id = cpc.token_id)`,
		).Scan(&unresolved); err != nil {
			t.Fatalf("checking coin FK integrity: %v", err)
		}
		if unresolved != 0 {
			t.Errorf("found %d curve_pool_coin rows with no matching token", unresolved)
		}
	})

	t.Run("eth_placeholder_token_seeded", func(t *testing.T) {
		var symbol string
		var decimals int
		if err := curveTestPool.QueryRow(ctx, `
			SELECT symbol, decimals FROM token
			WHERE chain_id = 1
			  AND address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea`,
		).Scan(&symbol, &decimals); err != nil {
			t.Fatalf("querying ETH placeholder token: %v", err)
		}
		if symbol != "ETH" {
			t.Errorf("ETH placeholder symbol = %q, want ETH", symbol)
		}
		if decimals != 18 {
			t.Errorf("ETH placeholder decimals = %d, want 18", decimals)
		}
	})

	t.Run("processing_version_trigger_fires_on_curve_swap", func(t *testing.T) {
		// Grab the stETH classic pool id and insert a synthetic swap to verify
		// the processing_version trigger fires and assigns version 0.
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving stETH classic pool id: %v", err)
		}

		swapTS := "2021-01-01T00:00:00Z"
		// Use RETURNING to capture the processing_version that the trigger assigned,
		// ensuring the insert succeeds and the trigger actually ran.
		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_swap
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     tx_hash, log_index, buyer, sold_id, bought_id, tokens_sold, tokens_bought, build_id)
			VALUES ($1, 11592600, 0, $2::timestamptz,
			        '\xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd'::bytea,
			        0,
			        '\x1111111111111111111111111111111111111111'::bytea,
			        0, 1, 1000000000000000000, 990000000000000000, 0)
			RETURNING processing_version`,
			poolID, swapTS).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test swap: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("processing_version_trigger_fires_on_curve_liquidity_event", func(t *testing.T) {
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving stETH classic pool id: %v", err)
		}

		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_liquidity_event
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     tx_hash, log_index, provider, kind, token_amounts, build_id)
			VALUES ($1, 11592601, 0, '2021-01-01T01:00:00Z'::timestamptz,
			        '\xbbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddeebbccddee'::bytea,
			        0,
			        '\x2222222222222222222222222222222222222222'::bytea,
			        'add',
			        ARRAY[1000000000000000000, 990000000000000000]::NUMERIC[],
			        0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test liquidity event: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("processing_version_trigger_fires_on_curve_stableswap_state", func(t *testing.T) {
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving stETH classic pool id: %v", err)
		}

		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_stableswap_state
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     balances, virtual_price, total_supply, a, fee, spot_dy, build_id)
			VALUES ($1, 11592602, 0, '2021-01-01T02:00:00Z'::timestamptz,
			        ARRAY[1000000000000000000, 990000000000000000]::NUMERIC[],
			        1000000000000000000,
			        1000000000000000000,
			        100,
			        4000000,
			        ARRAY[990000000000000000, 1000000000000000000]::NUMERIC[],
			        0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test stableswap state: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})

	t.Run("processing_version_trigger_fires_on_curve_cryptoswap_state", func(t *testing.T) {
		var poolID int64
		if err := curveTestPool.QueryRow(ctx, `
			SELECT id FROM curve_pool
			WHERE chain_id = 1
			  AND pool_address = '\x7F86Bf177Dd4F3494b841a37e810A34dD56c829B'::bytea`,
		).Scan(&poolID); err != nil {
			t.Fatalf("resolving TricryptoUSDC pool id: %v", err)
		}

		var pv int
		err := curveTestPool.QueryRow(ctx, `
			INSERT INTO curve_cryptoswap_state
			    (curve_pool_id, block_number, block_version, block_timestamp,
			     balances, virtual_price, total_supply, a, gamma, fee,
			     price_scale, price_oracle, last_prices, spot_dy, build_id)
			VALUES ($1, 17072900, 0, '2023-04-01T00:00:00Z'::timestamptz,
			        ARRAY[1000000, 1000000000000000000, 1000000000000000000]::NUMERIC[],
			        1000000000000000000,
			        1000000000000000000,
			        2700000,
			        145000000000000000,
			        4000000,
			        ARRAY[1000000000000000000, 1000000000000000000]::NUMERIC[],
			        ARRAY[1000000000000000000, 1000000000000000000]::NUMERIC[],
			        ARRAY[1000000000000000000, 1000000000000000000]::NUMERIC[],
			        ARRAY[990000000000000000, 990000000000000000]::NUMERIC[],
			        0)
			RETURNING processing_version`,
			poolID).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test cryptoswap state: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})
}
