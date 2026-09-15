//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const primeDexSeedDBName = "test_prime_dex_pool_seed"

var primeDexSeedPool *pgxpool.Pool

func init() {
	useFileDatabase(primeDexSeedDBName, &primeDexSeedPool)
}

// primeDexCurvePool is one of the five Curve stableswap-NG pools seeded by
// 20260831_120000_seed_prime_dex_pools.sql (ARCT-384). Every field is the
// on-chain value measured for that pool, so a drifted seed fails here.
type primeDexCurvePool struct {
	name        string
	addrHex     string
	deployBlock int64
}

var primeDexCurvePools = []primeDexCurvePool{
	{"sUSDS_USDT", "\\x00836fe54625be242bcfa286207795405ca4fd10", 22219093},
	{"PYUSD_USDS", "\\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f", 23301123},
	{"USDC_AUSD", "\\xe79c1c7e24755574438a26d5e062ad2626c04662", 20457618},
	{"USDC_USDT", "\\x4f493b7de8aac7d55f71853688b1f7c8f0243c85", 21702976},
	{"WETH_weETH", "\\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5", 19714579},
}

// primeDexCurveCoin pins one coins(i) slot to the token it must resolve to and
// the 10^(18-decimals) normalizer that token implies. coin_index is the live
// coins(i) ordering; a swapped pair fails.
type primeDexCurveCoin struct {
	name          string
	poolAddrHex   string
	coinIndex     int
	tokenAddrHex  string
	wantPrecision string
}

var primeDexCurveCoins = []primeDexCurveCoin{
	{"sUSDS_USDT_0_sUSDS", "\\x00836fe54625be242bcfa286207795405ca4fd10", 0, "\\xa3931d71877c0e7a3148cb7eb4463524fec27fbd", "1"},
	{"sUSDS_USDT_1_USDT", "\\x00836fe54625be242bcfa286207795405ca4fd10", 1, "\\xdac17f958d2ee523a2206206994597c13d831ec7", "1000000000000"},
	{"PYUSD_USDS_0_PYUSD", "\\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f", 0, "\\x6c3ea9036406852006290770bedfcaba0e23a0e8", "1000000000000"},
	{"PYUSD_USDS_1_USDS", "\\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f", 1, "\\xdc035d45d973e3ec169d2276ddab16f1e407384f", "1"},
	{"USDC_AUSD_0_USDC", "\\xe79c1c7e24755574438a26d5e062ad2626c04662", 0, "\\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "1000000000000"},
	{"USDC_AUSD_1_AUSD", "\\xe79c1c7e24755574438a26d5e062ad2626c04662", 1, "\\x00000000efe302beaa2b3e6e1b18d08d69a9012a", "1000000000000"},
	{"USDC_USDT_0_USDC", "\\x4f493b7de8aac7d55f71853688b1f7c8f0243c85", 0, "\\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "1000000000000"},
	{"USDC_USDT_1_USDT", "\\x4f493b7de8aac7d55f71853688b1f7c8f0243c85", 1, "\\xdac17f958d2ee523a2206206994597c13d831ec7", "1000000000000"},
	{"WETH_weETH_0_WETH", "\\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5", 0, "\\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "1"},
	{"WETH_weETH_1_weETH", "\\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5", 1, "\\xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee", "1"},
}

// TestPrimeDexSeedUniswapV3Pool asserts the AUSD/USDC 0.01% pool row that
// 20260831_120000_seed_prime_dex_pools.sql adds, field by field against the
// values measured on mainnet (token0()/token1()/fee()/tickSpacing()/
// maxLiquidityPerTick(), and the factory PoolCreated log for deploy_block).
func TestPrimeDexSeedUniswapV3Pool(t *testing.T) {
	ctx := context.Background()

	var (
		protocolName         string
		token0Hex, token1Hex string
		fee, tickSpacing     int
		maxLiquidityPerTick  string
		deployBlock          int64
	)
	err := primeDexSeedPool.QueryRow(ctx, `
		SELECT pr.name,
		       '\x' || encode(t0.address, 'hex'),
		       '\x' || encode(t1.address, 'hex'),
		       p.fee, p.tick_spacing, p.max_liquidity_per_tick::text, p.deploy_block
		FROM uniswap_v3_pool p
		JOIN protocol pr ON pr.id = p.protocol_id
		JOIN token t0 ON t0.id = p.token0_id
		JOIN token t1 ON t1.id = p.token1_id
		WHERE p.chain_id = 1
		  AND p.pool_address = '\xbAFeAd7c60Ea473758ED6c6021505E8BBd7e8E5d'::bytea`,
	).Scan(&protocolName, &token0Hex, &token1Hex, &fee, &tickSpacing, &maxLiquidityPerTick, &deployBlock)
	if err != nil {
		t.Fatalf("querying the seeded AUSD/USDC UniswapV3 pool: %v", err)
	}

	if protocolName != "UniswapV3" {
		t.Errorf("protocol = %q, want UniswapV3", protocolName)
	}
	if got, want := token0Hex, "\\x00000000efe302beaa2b3e6e1b18d08d69a9012a"; got != want {
		t.Errorf("token0 = %s, want %s (AUSD)", got, want)
	}
	if got, want := token1Hex, "\\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"; got != want {
		t.Errorf("token1 = %s, want %s (USDC)", got, want)
	}
	if fee != 100 {
		t.Errorf("fee = %d, want 100", fee)
	}
	if tickSpacing != 1 {
		t.Errorf("tick_spacing = %d, want 1", tickSpacing)
	}
	if want := "191757530477355301479181766273477"; maxLiquidityPerTick != want {
		t.Errorf("max_liquidity_per_tick = %s, want %s", maxLiquidityPerTick, want)
	}
	if deployBlock != 20508739 {
		t.Errorf("deploy_block = %d, want 20508739", deployBlock)
	}
}

// TestPrimeDexSeedCurvePools asserts each of the five seeded Curve pools is a
// plain_ng 2-coin pool of the stableswap-NG factory, is its own LP token
// (lp_token_address NULL), exposes A_precise(), and carries its exact deploy
// block.
func TestPrimeDexSeedCurvePools(t *testing.T) {
	ctx := context.Background()

	for _, want := range primeDexCurvePools {
		t.Run(want.name, func(t *testing.T) {
			var (
				protocolHex string
				poolKind    string
				nCoins      int
				lpToken     *[]byte
				hasAPrecise bool
				deployBlock int64
			)
			err := primeDexSeedPool.QueryRow(ctx, `
				SELECT '\x' || encode(pr.address, 'hex'),
				       p.pool_kind, p.n_coins, p.lp_token_address, p.has_a_precise, p.deploy_block
				FROM curve_pool p
				JOIN protocol pr ON pr.id = p.protocol_id
				WHERE p.chain_id = 1 AND p.pool_address = $1::bytea`,
				want.addrHex,
			).Scan(&protocolHex, &poolKind, &nCoins, &lpToken, &hasAPrecise, &deployBlock)
			if err != nil {
				t.Fatalf("querying curve pool %s: %v", want.addrHex, err)
			}

			if got, wantAddr := protocolHex, "\\x6a8cbed756804b16e05e741edabd5cb544ae21bf"; got != wantAddr {
				t.Errorf("protocol address = %s, want %s (stableswap-NG factory)", got, wantAddr)
			}
			if poolKind != "plain_ng" {
				t.Errorf("pool_kind = %q, want plain_ng", poolKind)
			}
			if nCoins != 2 {
				t.Errorf("n_coins = %d, want 2", nCoins)
			}
			if lpToken != nil {
				t.Errorf("lp_token_address = %x, want NULL (an NG pool is its own LP token)", *lpToken)
			}
			if !hasAPrecise {
				t.Error("has_a_precise = false, want true (A_precise() answers on-chain)")
			}
			if deployBlock != want.deployBlock {
				t.Errorf("deploy_block = %d, want %d", deployBlock, want.deployBlock)
			}
		})
	}
}

// TestPrimeDexSeedCurvePoolCoins asserts every seeded coin slot resolves to the
// token at that live coins(i) index, with the precision its decimals imply.
func TestPrimeDexSeedCurvePoolCoins(t *testing.T) {
	ctx := context.Background()

	for _, want := range primeDexCurveCoins {
		t.Run(want.name, func(t *testing.T) {
			var tokenHex, precision string
			err := primeDexSeedPool.QueryRow(ctx, `
				SELECT '\x' || encode(tk.address, 'hex'), cpc.precision::text
				FROM curve_pool_coin cpc
				JOIN curve_pool p ON p.id = cpc.curve_pool_id
				JOIN token tk ON tk.id = cpc.token_id
				WHERE p.chain_id = 1 AND p.pool_address = $1::bytea AND cpc.coin_index = $2`,
				want.poolAddrHex, want.coinIndex,
			).Scan(&tokenHex, &precision)
			if err != nil {
				t.Fatalf("querying coin %d of pool %s: %v", want.coinIndex, want.poolAddrHex, err)
			}

			if got, wantAddr := tokenHex, want.tokenAddrHex; got != wantAddr {
				t.Errorf("token = %s, want %s", got, wantAddr)
			}
			if precision != want.wantPrecision {
				t.Errorf("precision = %s, want %s", precision, want.wantPrecision)
			}
		})
	}
}
