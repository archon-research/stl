-- Seed the prime-held Curve and Uniswap V3 pools (ARCT-384).
--
-- Additive registry seed only: 1 uniswap_v3_pool row, 5 curve_pool rows and
-- their 10 curve_pool_coin rows. No schema change, no existing row touched.
--
-- Every value below was measured on Ethereum mainnet on 2026-08-31, not
-- inferred. How each field was fixed:
--   V3 pool     token0()/token1()/fee()/tickSpacing()/maxLiquidityPerTick(), and
--               the factory PoolCreated log in block 20508739 (exactly one, its
--               pool param this address).
--   Curve pools N_COINS(), coins(0)/coins(1), A_precise() returns (hence
--               has_a_precise), totalSupply() answers on the pool itself (hence
--               the NG pool is its own LP token, lp_token_address NULL).
--   plain_ng    the stableswap-NG factory 0x6A8cbed7...21bf reports each pool
--               with is_meta=false and get_n_coins=2. A non-zero get_n_coins is
--               proof of registration in that factory ("NG"), which is what
--               keeps is_meta's default false from passing by accident
--               ("plain"). Read factory->pool because these pools expose no
--               factory() getter. BASE_POOL() also reverts on all five.
--   deploy_block  eth_getCode non-empty at the block, empty at block-1, so each
--               value is the exact deploy height -- and therefore the lower
--               bound both registries' deploy_block COMMENTs demand.
--
-- No token rows are inserted. All eight referenced tokens already exist on
-- chain 1 with exactly the symbol/decimals measured on-chain -- sUSDS, USDT,
-- PYUSD, USDS, USDC, WETH, weETH via 20260204_110000_seed_sparklend_tokens.sql
-- and AUSD via 20260709_120000_add_er_missing_price_feeds.sql, both of which
-- sort before this file. They are asserted, not re-seeded: a missing token row
-- would otherwise make the pool and coin INSERTs below silently seed nothing.

-- ============================================================================
-- 1. Uniswap V3: AUSD/USDC 0.01%.
-- token0/token1 are the live token0()/token1() orientation, not alphabetized.
-- ============================================================================

WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea  -- AUSD
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xbAFeAd7c60Ea473758ED6c6021505E8BBd7e8E5d'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 20508739
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- ============================================================================
-- 2. Curve stableswap-NG plain pools. All five: plain_ng, 2 coins, their own
-- LP token (lp_token_address NULL), A_precise() present.
-- ============================================================================

INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, lp_token_address, deploy_block, has_a_precise)
SELECT 1, pr.id, v.pool_address, 'plain_ng', 2, NULL, v.deploy_block, TRUE
FROM protocol pr
CROSS JOIN (VALUES
    ('\x00836fe54625be242bcfa286207795405ca4fd10'::bytea, 22219093::bigint),  -- sUSDS/USDT  symbol() "sUSDSUSDT", A_precise() 2000000
    ('\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 23301123::bigint),  -- PYUSD/USDS  symbol() "PYUSDUSDS", A_precise() 1000000
    ('\xe79c1c7e24755574438a26d5e062ad2626c04662'::bytea, 20457618::bigint),  -- USDC/AUSD   symbol() "AUSDUSDC",  A_precise() 50000
    ('\x4f493b7de8aac7d55f71853688b1f7c8f0243c85'::bytea, 21702976::bigint),  -- USDC/USDT   symbol() "crv2pool",  A_precise() 1000000
    ('\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5'::bytea, 19714579::bigint)   -- WETH/weETH  symbol() "weeth-ng",  A_precise() 500000
) AS v(pool_address, deploy_block)
WHERE pr.chain_id = 1
  AND pr.address = '\x6A8cbed756804B16E05E741eDaBd5cB544AE21bf'::bytea
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- precision is derived from the token row, not typed out, so it cannot drift
-- from the decimals it normalizes; trim_scale matches the scale the rows
-- 20260521_110000 seeded through float arithmetic already carry.
INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id, precision)
SELECT p.id, v.coin_index, tk.id, trim_scale(power(10::numeric, (18 - tk.decimals)::numeric))
FROM (VALUES
    ('\x00836fe54625be242bcfa286207795405ca4fd10'::bytea, 0::smallint, '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea),  -- sUSDS/USDT coins(0) sUSDS
    ('\x00836fe54625be242bcfa286207795405ca4fd10'::bytea, 1::smallint, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),  -- sUSDS/USDT coins(1) USDT
    ('\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 0::smallint, '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea),  -- PYUSD/USDS coins(0) PYUSD
    ('\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 1::smallint, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea),  -- PYUSD/USDS coins(1) USDS
    ('\xe79c1c7e24755574438a26d5e062ad2626c04662'::bytea, 0::smallint, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),  -- USDC/AUSD  coins(0) USDC
    ('\xe79c1c7e24755574438a26d5e062ad2626c04662'::bytea, 1::smallint, '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea),  -- USDC/AUSD  coins(1) AUSD
    ('\x4f493b7de8aac7d55f71853688b1f7c8f0243c85'::bytea, 0::smallint, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),  -- USDC/USDT  coins(0) USDC
    ('\x4f493b7de8aac7d55f71853688b1f7c8f0243c85'::bytea, 1::smallint, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),  -- USDC/USDT  coins(1) USDT
    ('\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5'::bytea, 0::smallint, '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea),  -- WETH/weETH coins(0) WETH
    ('\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5'::bytea, 1::smallint, '\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee'::bytea)   -- WETH/weETH coins(1) weETH
) AS v(pool_address, coin_index, coin_address)
JOIN curve_pool p ON p.chain_id = 1 AND p.pool_address = v.pool_address
JOIN token tk ON tk.chain_id = 1 AND tk.address = v.coin_address
ON CONFLICT (curve_pool_id, coin_index) DO NOTHING;

-- ============================================================================
-- 3. Assertions. Every INSERT above resolves its FKs by natural key, so a
-- missing protocol/token row makes it a silent zero-row no-op rather than an
-- error. These turn that into a failed migration.
-- ============================================================================

DO $$
DECLARE
    token_count INT;
    pool_count  INT;
    bad         TEXT;
BEGIN
    SELECT count(*) INTO token_count
    FROM (VALUES
        ('\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea, 'AUSD',  6),
        ('\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, 'USDC',  6),
        ('\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, 'USDT',  6),
        ('\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, 'PYUSD', 6),
        ('\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea, 'sUSDS', 18),
        ('\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, 'USDS',  18),
        ('\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea, 'WETH',  18),
        ('\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee'::bytea, 'weETH', 18)
    ) AS x(address, symbol, decimals)
    JOIN token t ON t.chain_id = 1 AND t.address = x.address
                AND t.symbol = x.symbol AND t.decimals = x.decimals;
    IF token_count <> 8 THEN
        RAISE EXCEPTION 'expected all 8 ARCT-384 counterparty tokens on chain 1 with the on-chain symbol/decimals, got %', token_count;
    END IF;

    SELECT count(*) INTO pool_count
    FROM uniswap_v3_pool p
    JOIN protocol pr ON pr.id = p.protocol_id
    JOIN token t0 ON t0.id = p.token0_id
    JOIN token t1 ON t1.id = p.token1_id
    WHERE p.chain_id = 1
      AND p.pool_address = '\xbAFeAd7c60Ea473758ED6c6021505E8BBd7e8E5d'::bytea
      AND pr.chain_id = 1 AND pr.name = 'UniswapV3'
      AND t0.address = '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea
      AND t1.address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
      AND p.fee = 100
      AND p.tick_spacing = 1
      AND p.max_liquidity_per_tick = 191757530477355301479181766273477
      AND p.deploy_block = 20508739;
    IF pool_count <> 1 THEN
        RAISE EXCEPTION 'expected the AUSD/USDC UniswapV3 pool seeded with the cast-verified fields, got % matching rows', pool_count;
    END IF;

    SELECT string_agg(encode(x.pool_address, 'hex'), ', ') INTO bad
    FROM (VALUES
        ('\x00836fe54625be242bcfa286207795405ca4fd10'::bytea, 22219093::bigint),
        ('\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 23301123::bigint),
        ('\xe79c1c7e24755574438a26d5e062ad2626c04662'::bytea, 20457618::bigint),
        ('\x4f493b7de8aac7d55f71853688b1f7c8f0243c85'::bytea, 21702976::bigint),
        ('\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5'::bytea, 19714579::bigint)
    ) AS x(pool_address, deploy_block)
    WHERE NOT EXISTS (
        SELECT 1
        FROM curve_pool p
        JOIN protocol pr ON pr.id = p.protocol_id
        WHERE p.chain_id = 1
          AND p.pool_address = x.pool_address
          AND pr.chain_id = 1 AND pr.address = '\x6A8cbed756804B16E05E741eDaBd5cB544AE21bf'::bytea
          AND p.pool_kind = 'plain_ng'
          AND p.n_coins = 2
          AND p.lp_token_address IS NULL
          AND p.has_a_precise
          AND p.deploy_block = x.deploy_block
    );
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'ARCT-384 curve pools missing or seeded with wrong fields: %', bad;
    END IF;
END $$;

-- Coin rows: right token at the right index, and precision consistent with the
-- decimals it normalizes. Compared as a set so a swapped pair fails too.
DO $$
DECLARE
    bad TEXT;
BEGIN
    SELECT string_agg(format('%s[%s]', encode(x.pool_address, 'hex'), x.coin_index), ', ') INTO bad
    FROM (VALUES
        ('\x00836fe54625be242bcfa286207795405ca4fd10'::bytea, 0::smallint, '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea),
        ('\x00836fe54625be242bcfa286207795405ca4fd10'::bytea, 1::smallint, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),
        ('\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 0::smallint, '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea),
        ('\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 1::smallint, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea),
        ('\xe79c1c7e24755574438a26d5e062ad2626c04662'::bytea, 0::smallint, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),
        ('\xe79c1c7e24755574438a26d5e062ad2626c04662'::bytea, 1::smallint, '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea),
        ('\x4f493b7de8aac7d55f71853688b1f7c8f0243c85'::bytea, 0::smallint, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),
        ('\x4f493b7de8aac7d55f71853688b1f7c8f0243c85'::bytea, 1::smallint, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),
        ('\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5'::bytea, 0::smallint, '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea),
        ('\xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5'::bytea, 1::smallint, '\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee'::bytea)
    ) AS x(pool_address, coin_index, coin_address)
    WHERE NOT EXISTS (
        SELECT 1
        FROM curve_pool_coin cpc
        JOIN curve_pool p ON p.id = cpc.curve_pool_id
        JOIN token tk ON tk.id = cpc.token_id
        WHERE p.chain_id = 1
          AND p.pool_address = x.pool_address
          AND cpc.coin_index = x.coin_index
          AND tk.chain_id = 1
          AND tk.address = x.coin_address
          AND cpc.precision = trim_scale(power(10::numeric, (18 - tk.decimals)::numeric))
    );
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'ARCT-384 curve_pool_coin rows missing, pointing at the wrong token, or with an inconsistent precision: %', bad;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260831_120000_seed_prime_dex_pools.sql')
ON CONFLICT (filename) DO NOTHING;
