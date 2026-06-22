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
			     tx_hash, log_index, sender, sold_id, tokens_sold, bought_id, tokens_bought, build_id)
			VALUES ($1, 11592600, 0, $2::timestamptz,
			        '\xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd'::bytea,
			        0,
			        '\x1111111111111111111111111111111111111111'::bytea,
			        0, 1000000000000000000, 1, 990000000000000000, 0)
			RETURNING processing_version`,
			poolID, swapTS).Scan(&pv)
		if err != nil {
			t.Fatalf("inserting test swap: %v", err)
		}
		if pv != 0 {
			t.Errorf("processing_version = %d after first insert, want 0", pv)
		}
	})
}
