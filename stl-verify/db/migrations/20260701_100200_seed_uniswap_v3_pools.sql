-- Seed the 18 real, cast-verified, active wstETH/stETH Uniswap V3 pools
-- (VEC-261 design spec §4) into uniswap_v3_pool, plus any counterparty token
-- rows not already present.
--
-- Prerequisites (already applied):
--   20260701_100000_create_uniswap_v3_tables.sql  -- uniswap_v3_pool + fact tables
--   20260521_100000_create_dex_prereqs.sql        -- UniswapV3 protocol row
--   20260521_105000_seed_steth_token.sql          -- stETH token row
--   20260204_110000_seed_sparklend_tokens.sql     -- WETH/USDC/USDT/wstETH/weETH/rsETH/ezETH
--
-- max_liquidity_per_tick is NOT NULL and derived purely from tick_spacing via
-- Uniswap V3's tickSpacingToMaxLiquidityPerTick(): floor((2^128-1) / numTicks),
-- where numTicks is computed from MIN_TICK/MAX_TICK (887272) truncated to each
-- tick spacing. Same four fee-tier constants for every pool sharing that
-- tick_spacing:
--   tick_spacing=1   -> 191757530477355301479181766273477
--   tick_spacing=10  -> 1917569901783203986719870431555990
--   tick_spacing=60  -> 11505743598341114571880798222544994
--   tick_spacing=200 -> 38350317471085141830651933667504588
--
-- token0/token1 orientation below is exactly as returned by each pool's
-- live token0()/token1() (cast-verified) -- it is NOT alphabetized or
-- normalized to always put wstETH/stETH first. Pools #14, #17, #18 have
-- wstETH as token1 (mstETH/urLRT/boxETH are token0 respectively).

-- ----------------------------------------------------------------------------
-- 1. Counterparty tokens not already seeded by an earlier migration.
-- WETH/USDC/USDT/wstETH/weETH/rsETH/ezETH (20260204_110000) and stETH
-- (20260521_105000) already exist; listed here only for readability, not
-- re-inserted.
-- ----------------------------------------------------------------------------

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\xA35b1B31Ce002FBF2058D22F30f95D405200A15b'::bytea, 'ETHx', 18),
    (1, '\x8c9532a60E0e7C6Bbd2b2c1303F63aCE1c3E9811'::bytea, 'pzETH', 18),
    (1, '\xf073bAC22DAb7FaF4a3Dd6c6189a70D54110525C'::bytea, 'inETH', 18),
    (1, '\x49446A0874197839D15395B908328a74ccc96Bc0'::bytea, 'mstETH', 18),
    (1, '\xe2237a34246eeDcEB43283073Ba9adEF0450351E'::bytea, 'fstETH', 18),
    (1, '\x4f3cc6359364004b245ad5be36e6ad4e805dc961'::bytea, 'urLRT', 18),
    (1, '\x7690202e2C2297bcD03664e31116D1dFFE7e3B73'::bytea, 'boxETH', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 2. Seed the 18 pools. token0_id/token1_id/protocol_id resolved by natural
-- key (address / name), never by symbol.
-- ----------------------------------------------------------------------------

-- 1. wstETH/WETH 0.01%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 15384250
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 2. wstETH/WETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xD340B57AAcDD10F96FC1CF10e15921936F41E29c'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 12376093
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 3. wstETH/WETH 0.30%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xC12aF0C4AA39D3061c56cD3CB19f5e62dEeaeBdE'::bytea, t0.id, t1.id, 3000, 60, 11505743598341114571880798222544994, 14943576
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 4. wstETH/USDC 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x4622Df6fB2d9Bee0DCDaCF545aCDB6a2b2f4f863'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 16065412
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 5. wstETH/USDC 0.30%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x173821f6aD4c5324cd35753A9FD12D92f2eaAB29'::bytea, t0.id, t1.id, 3000, 60, 11505743598341114571880798222544994, 14420259
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 6. wstETH/USDT 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea  -- USDT
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xeC5055067d60292Ab2c514A1090Bc8E014e4aBAA'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 18461625
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 7. stETH/WETH 1.00%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea  -- stETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x63818BbDd21E69bE108A23aC1E84cBf66399Bd7D'::bytea, t0.id, t1.id, 10000, 200, 38350317471085141830651933667504588, 14937573
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 8. wstETH/weETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee'::bytea  -- weETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xf47F04a8605bE181E525D6391233cbA1f7474182'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 19629516
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 9. wstETH/ETHx 0.01%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA35b1B31Ce002FBF2058D22F30f95D405200A15b'::bytea  -- ETHx
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xC9eD5de354D4BE9fd576D3108C7637a71C01faA1'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 20525114
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 10. wstETH/rsETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7'::bytea  -- rsETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x7a27c7b7E2536e452c57d3E8b909d9ecba2e2eee'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 19970144
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 11. wstETH/ezETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xbf5495Efe5DB9ce00f80364C8B423567e58d2110'::bytea  -- ezETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x1b9d58bEa5eD5d935cc2E818Dde1D796abFf0bc0'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 20011672
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 12. wstETH/pzETH 0.01%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x8c9532a60E0e7C6Bbd2b2c1303F63aCE1c3E9811'::bytea  -- pzETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xfc354f5cf57125a7d85E1165f4FCDfd3006db61a'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 20192842
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 13. wstETH/inETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xf073bAC22DAb7FaF4a3Dd6c6189a70D54110525C'::bytea  -- inETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x3c0a1a9e0E22b9Acc9248D9f358286e9e9205b0a'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 19232685
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 14. mstETH/wstETH 0.05% -- wstETH is token1, NOT token0 here.
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x49446A0874197839D15395B908328a74ccc96Bc0'::bytea  -- mstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x7f13847459450236d2D233f1c08D742Ed69D2997'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 20011699
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 15. wstETH/fstETH 1.00%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xe2237a34246eeDcEB43283073Ba9adEF0450351E'::bytea  -- fstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x526389df2DCc8c5F7af69E93aD9E0d8FC21799f6'::bytea, t0.id, t1.id, 10000, 200, 38350317471085141830651933667504588, 16478531
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 16. wstETH/fstETH 0.30%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xe2237a34246eeDcEB43283073Ba9adEF0450351E'::bytea  -- fstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x39683566C148851464781f0673112eF0746B9578'::bytea, t0.id, t1.id, 3000, 60, 11505743598341114571880798222544994, 16486285
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 17. urLRT/wstETH 0.01% -- wstETH is token1, NOT token0 here.
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x4f3cc6359364004b245ad5be36e6ad4e805dc961'::bytea  -- urLRT
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x104b3e3aCd2396a7292223b5778EA1CACdb68eC9'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 20832240
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 18. boxETH/wstETH 1.00% -- wstETH is token1, NOT token0 here.
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7690202e2C2297bcD03664e31116D1dFFE7e3B73'::bytea  -- boxETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x703A177Fcb4dEf281d180d3619a5edbAE67Ec7b5'::bytea, t0.id, t1.id, 10000, 200, 38350317471085141830651933667504588, 23506288
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 3. Post-seed assertions.
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    pool_count       INT;
    null_deploy_count INT;
BEGIN
    SELECT count(*) INTO pool_count
    FROM uniswap_v3_pool p
    JOIN protocol pr ON pr.id = p.protocol_id
    WHERE pr.name = 'UniswapV3' AND pr.chain_id = 1;
    IF pool_count <> 18 THEN
        RAISE EXCEPTION 'expected exactly 18 UniswapV3 pools, got %', pool_count;
    END IF;

    SELECT count(*) INTO null_deploy_count
    FROM uniswap_v3_pool p
    JOIN protocol pr ON pr.id = p.protocol_id
    WHERE pr.name = 'UniswapV3' AND pr.chain_id = 1 AND p.deploy_block IS NULL;
    IF null_deploy_count <> 0 THEN
        RAISE EXCEPTION 'expected 0 UniswapV3 pools with NULL deploy_block, got %', null_deploy_count;
    END IF;
END $$;

-- Address-equality spot checks: pool #1 (wstETH/WETH 0.01%), pool #7
-- (stETH/WETH 1%), and pool #14 (mstETH/wstETH, wstETH is token1) --
-- confirms the orientation from the live cast scan was not "normalized".
DO $$
DECLARE
    got_token0 BYTEA;
    got_token1 BYTEA;
BEGIN
    SELECT t0.address, t1.address INTO got_token0, got_token1
    FROM uniswap_v3_pool p
    JOIN token t0 ON t0.id = p.token0_id
    JOIN token t1 ON t1.id = p.token1_id
    WHERE p.chain_id = 1 AND p.pool_address = '\x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa'::bytea;
    IF got_token0 <> '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea
        OR got_token1 <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea THEN
        RAISE EXCEPTION 'pool #1 wstETH/WETH token0/token1 mismatch: token0=%, token1=%', got_token0, got_token1;
    END IF;

    SELECT t0.address, t1.address INTO got_token0, got_token1
    FROM uniswap_v3_pool p
    JOIN token t0 ON t0.id = p.token0_id
    JOIN token t1 ON t1.id = p.token1_id
    WHERE p.chain_id = 1 AND p.pool_address = '\x63818BbDd21E69bE108A23aC1E84cBf66399Bd7D'::bytea;
    IF got_token0 <> '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea
        OR got_token1 <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea THEN
        RAISE EXCEPTION 'pool #7 stETH/WETH token0/token1 mismatch: token0=%, token1=%', got_token0, got_token1;
    END IF;

    SELECT t0.address, t1.address INTO got_token0, got_token1
    FROM uniswap_v3_pool p
    JOIN token t0 ON t0.id = p.token0_id
    JOIN token t1 ON t1.id = p.token1_id
    WHERE p.chain_id = 1 AND p.pool_address = '\x7f13847459450236d2D233f1c08D742Ed69D2997'::bytea;
    IF got_token0 <> '\x49446A0874197839D15395B908328a74ccc96Bc0'::bytea
        OR got_token1 <> '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea THEN
        RAISE EXCEPTION 'pool #14 mstETH/wstETH token0/token1 mismatch (wstETH must be token1): token0=%, token1=%', got_token0, got_token1;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260701_100200_seed_uniswap_v3_pools.sql')
ON CONFLICT (filename) DO NOTHING;
