-- The Spark ALM's RLUSD/USDS pool (ARCT-452): PoolKey re-read from its one Initialize log on
-- Ethereum mainnet (2026-09-08); the migration integration test re-derives the PoolId.

WITH seed (pool_id, currency0, currency1, fee, tick_spacing, hooks, deploy_block) AS (
    VALUES
        ('\x9035721b23481db3888fd201b9c2b26dbc3af60258bca65e669f2ed98dc8eb4f'::bytea, '\x8292Bb45bf1Ee4d140127049757C2E0fF06317eD'::bytea, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, 5, 1, '\x0000000000000000000000000000000000000000'::bytea, 25653372::bigint)
)
INSERT INTO uniswap_v4_pool
    (chain_id, pool_id, currency0, currency1,
     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block,
     snapshot_supported)
SELECT 1, s.pool_id, s.currency0, s.currency1, t0.id, t1.id,
       s.fee, s.tick_spacing, s.hooks, s.deploy_block,
       TRUE
FROM seed s
JOIN token t0 ON t0.chain_id = 1 AND t0.address = s.currency0
JOIN token t1 ON t1.chain_id = 1 AND t1.address = s.currency1
ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING;

DO $$
DECLARE
    bad_token TEXT;
    pool_count INT;
BEGIN
    SELECT format('%s: expected (%s, %s), got (%s, %s)',
                  encode(e.address, 'hex'), e.symbol, e.decimals, t.symbol, t.decimals)
    INTO bad_token
    FROM (VALUES
        ('\x8292Bb45bf1Ee4d140127049757C2E0fF06317eD'::bytea, 'RLUSD', 18),
        ('\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, 'USDS', 18)
    ) AS e (address, symbol, decimals)
    LEFT JOIN token t ON t.chain_id = 1 AND t.address = e.address
    WHERE t.id IS NULL
       OR t.symbol IS DISTINCT FROM e.symbol
       OR t.decimals IS DISTINCT FROM e.decimals
    ORDER BY e.address
    LIMIT 1;
    IF bad_token IS NOT NULL THEN
        RAISE EXCEPTION 'ARCT-452 seed token mismatch for %', bad_token;
    END IF;

    SELECT count(*) INTO pool_count
    FROM uniswap_v4_pool p
    JOIN token t0 ON t0.id = p.currency0_token_id
    JOIN token t1 ON t1.id = p.currency1_token_id
    WHERE p.chain_id = 1
      AND p.pool_id = '\x9035721b23481db3888fd201b9c2b26dbc3af60258bca65e669f2ed98dc8eb4f'::bytea
      AND p.currency0 = '\x8292Bb45bf1Ee4d140127049757C2E0fF06317eD'::bytea
      AND p.currency1 = '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea
      AND t0.chain_id = 1 AND t0.address = p.currency0
      AND t1.chain_id = 1 AND t1.address = p.currency1
      AND p.fee = 5
      AND p.tick_spacing = 1
      AND p.hooks = '\x0000000000000000000000000000000000000000'::bytea
      AND p.deploy_block = 25653372
      AND p.snapshot_supported;
    IF pool_count <> 1 THEN
        RAISE EXCEPTION 'expected exactly 1 RLUSD/USDS UniswapV4 pool row carrying the verified PoolKey, got %', pool_count;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260908_120000_seed_uniswap_v4_rlusd_usds.sql')
ON CONFLICT (filename) DO NOTHING;
