//go:build integration

package oracle_price_worker

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// VEC-210: Per-protocol smoke tests that the new aave_v3_lido and aave_v3_rwa
// oracles seeded in 20260505_135100_add_aave_lido_horizon_oracles.sql are picked
// up by the price worker and emit ≥1 price for the protocol's most common
// underlyings. Aave V2 has its own ticket — its mainnet oracle is
// ETH-denominated and isn't covered by the existing aave_oracle pipeline.

func TestIntegration_AaveV3LidoOracle_EmitsPricesForCommonUnderlyings(t *testing.T) {
	// Lido reserves intersected with the issue's "USDT/USDC/DAI/sUSDe" set:
	// USDC and sUSDe (Lido has no USDT or DAI listing — USDS replaces DAI).
	assertOracleEmitsPricesForSymbols(t,
		"aave_v3_lido",
		[]string{"USDC", "sUSDe"},
	)
}

func TestIntegration_AaveV3RWAOracle_EmitsPricesForCommonUnderlyings(t *testing.T) {
	// Aave V3 RWA (Horizon) only carries USDC of the issue's common stable
	// set; the other reserves are RWA tokens not yet seeded.
	assertOracleEmitsPricesForSymbols(t,
		"aave_v3_rwa",
		[]string{"USDC"},
	)
}

// VEC-215: lock in the exact canonical reserve set bound to each new oracle.
// The original VEC-210 migration JOINed by symbol; this caused staging to
// pick up stale duplicate-symbol token rows and revert on every block. The
// fix-forward migration (20260507_120000) re-seeds by address. This test
// fails loudly if the bound set drifts from the address-book canonical list.
func TestIntegration_AaveV3LidoOracle_AssetBindingsAreCanonical(t *testing.T) {
	assertOracleAssetsExactlyMatchAddresses(t, "aave_v3_lido", []string{
		"0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", // wstETH
		"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH
		"0xdC035D45d973E3EC169d2276DDab16f1e407384F", // USDS
		"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
		"0xbf5495Efe5DB9ce00f80364C8B423567e58d2110", // ezETH
		"0x9D39A5DE30e57443BfF2A8307A4256c8797A3497", // sUSDe
		"0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f", // GHO
		"0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7", // rsETH
	})
}

func TestIntegration_AaveV3RWAOracle_AssetBindingsAreCanonical(t *testing.T) {
	assertOracleAssetsExactlyMatchAddresses(t, "aave_v3_rwa", []string{
		"0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f", // GHO
		"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
		"0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD", // RLUSD (added by 20260709_120000_add_er_missing_price_feeds.sql)
	})
}

// assertOracleEmitsPricesForSymbols runs the price worker against a real schema
// (so the migration-seeded oracle is exercised end-to-end), pushes a single block
// event with stubbed multicall responses, and asserts that an onchain_token_price
// row was written for every requested symbol on chain_id=1.
func assertOracleEmitsPricesForSymbols(t *testing.T, oracleName string, symbols []string) {
	t.Helper()

	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	// Isolate the oracle under test: every other migration-seeded oracle is
	// disabled so the worker only loads this one (the multicall mock is
	// parameterised for a single oracle's asset list).
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false WHERE name <> $1`, oracleName); err != nil {
		t.Fatalf("disable other oracles: %v", err)
	}

	var oracleID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM oracle WHERE name = $1`, oracleName).Scan(&oracleID); err != nil {
		t.Fatalf("oracle %q not seeded by migrations: %v", oracleName, err)
	}

	var numAssets int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM oracle_asset
		WHERE oracle_id = $1 AND enabled = true AND feed_address IS NULL
	`, oracleID).Scan(&numAssets); err != nil {
		t.Fatalf("count enabled assets: %v", err)
	}
	if numAssets == 0 {
		t.Fatalf("oracle %q has no enabled aave-style assets", oracleName)
	}

	for _, symbol := range symbols {
		var bound bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM oracle_asset oa
				JOIN token t ON t.id = oa.token_id
				WHERE oa.oracle_id = $1 AND oa.enabled = true AND oa.feed_address IS NULL
				  AND t.symbol = $2 AND t.chain_id = 1
			)
		`, oracleID, symbol).Scan(&bound); err != nil {
			t.Fatalf("check %s binding: %v", symbol, err)
		}
		if !bound {
			t.Fatalf("oracle %q is missing required underlying %q in oracle_asset", oracleName, symbol)
		}
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 0, 100)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}

	mc := integrationMulticallerBlockDependent(t, numAssets)

	const blockNumber = int64(20_500_000)
	blockTimestamp := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).Unix()
	consumer := consumerWithMessages([]outbound.SQSMessage{
		blockEventMessage(blockNumber, 1, blockTimestamp, "receipt-"+oracleName),
	})

	cfg := shared.SQSConsumerConfig{
		PollInterval: 1 * time.Millisecond,
		Logger:       logger,
		ChainID:      1,
	}

	svc, err := NewService(cfg, consumer, defaultBlockCacheReader(), repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		var count int
		pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM onchain_token_price
			WHERE oracle_id = $1 AND block_number = $2
		`, oracleID, blockNumber).Scan(&count)
		return count >= numAssets
	}, "prices to be stored for "+oracleName)

	for _, symbol := range symbols {
		var priced bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM onchain_token_price p
				JOIN token t ON t.id = p.token_id
				WHERE p.oracle_id = $1 AND p.block_number = $2
				  AND t.symbol = $3 AND t.chain_id = 1
			)
		`, oracleID, blockNumber, symbol).Scan(&priced)
		if err != nil {
			t.Fatalf("query stored price for %s: %v", symbol, err)
		}
		if !priced {
			t.Errorf("oracle %q: expected at least one onchain_token_price row for %q at block %d", oracleName, symbol, blockNumber)
		}
	}
}

// assertOracleAssetsExactlyMatchAddresses asserts that the post-migration
// aave-style oracle_asset bindings for the given oracle exactly match the
// canonical address set (case-insensitive, order-insensitive). Picks up both
// missing canonical reserves and bogus extras swept in by a symbol JOIN.
func assertOracleAssetsExactlyMatchAddresses(t *testing.T, oracleName string, expectedAddrs []string) {
	t.Helper()

	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()

	rows, err := pool.Query(ctx, `
		SELECT t.address
		FROM oracle_asset oa
		JOIN oracle o ON o.id = oa.oracle_id
		JOIN token  t ON t.id = oa.token_id
		WHERE o.name = $1 AND oa.enabled = true AND oa.feed_address IS NULL
		ORDER BY t.address
	`, oracleName)
	if err != nil {
		t.Fatalf("query oracle_asset: %v", err)
	}
	defer rows.Close()

	got := make(map[string]struct{})
	for rows.Next() {
		var addr []byte
		if err := rows.Scan(&addr); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[strings.ToLower("0x"+hex.EncodeToString(addr))] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := make(map[string]struct{}, len(expectedAddrs))
	for _, a := range expectedAddrs {
		want[strings.ToLower(a)] = struct{}{}
	}

	for a := range got {
		if _, ok := want[a]; !ok {
			t.Errorf("oracle %q: unexpected oracle_asset address %s (not in canonical set)", oracleName, a)
		}
	}
	for a := range want {
		if _, ok := got[a]; !ok {
			t.Errorf("oracle %q: missing canonical oracle_asset address %s", oracleName, a)
		}
	}
}
