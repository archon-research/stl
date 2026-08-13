-- Prerequisites for the VEC-79 DEX worker migrations (Curve, Uniswap V3, Balancer V2).
--
-- 1. Register the three DEX protocols so per-DEX workers can FK protocol_event.protocol_id.
-- 2. Seed CVX / FXS / BAL / STG tokens used by Curve gauges as bonus reward tokens.
-- 3. Bind those reward tokens to their Chainlink USD feeds via oracle_asset so the existing
--    oracle-price-indexer picks them up and writes prices into onchain_token_price; this
--    enables Curve gauge bonus-APY derivation at read time without any new worker code.
--
-- Feed addresses verified live against Ethereum mainnet on 2026-05-21 (see plan §11 step 1).
-- LDO has no direct USD feed and is composed at read time as LDO/ETH × ETH/USD (both wired
-- pre-VEC-79). CRV/USD is also wired pre-VEC-79; no action needed here.

-- ---------------------------------------------------------------------------
-- 1. Protocol rows (target of protocol_event.protocol_id).
-- ---------------------------------------------------------------------------

INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (
    1,
    '\x6A8cbed756804B16E05E741eDaBd5cB544AE21bf'::bytea,
    'Curve',
    'dex',
    19421686,
    NOW(),
    '{"role":"stableswap_ng_factory"}'::jsonb
)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (
    1,
    '\x1F98431c8aD98523631AE4a59f267346ea31F984'::bytea,
    'UniswapV3',
    'dex',
    12369621,
    NOW(),
    '{"role":"factory"}'::jsonb
)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (
    1,
    '\xBA12222222228d8Ba445958a75a0704d566BF2C8'::bytea,
    'BalancerV2',
    'dex',
    12272146,
    NOW(),
    '{"role":"vault"}'::jsonb
)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 2. Curve gauge bonus-reward tokens.
-- ---------------------------------------------------------------------------

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B'::bytea, 'CVX', 18),
    (1, '\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'::bytea, 'FXS', 18),
    (1, '\xba100000625a3754423978a60c9317c58a424e3D'::bytea, 'BAL', 18),
    (1, '\xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'::bytea, 'STG', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3. Chainlink USD feed bindings for the reward tokens.
-- Pattern mirrors db/migrations/20260212_120000_add_oracle_type_and_feed_columns.sql.
-- Join token by address (not symbol) so a pre-existing token row with a different
-- symbol can't cause this INSERT to silently bind zero rows.
-- ---------------------------------------------------------------------------

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xd962fC30A72A84cE50161031391756Bf2876Af5D'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink'
  AND t.chain_id = 1
  AND t.address = '\x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B'::bytea  -- CVX
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x6Ebc52C8C1089be9eB3945C4350B68B8E4C2233f'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink'
  AND t.chain_id = 1
  AND t.address = '\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'::bytea  -- FXS
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xdF2917806E30300537aEB49A7663062F4d1F2b5F'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink'
  AND t.chain_id = 1
  AND t.address = '\xba100000625a3754423978a60c9317c58a424e3D'::bytea  -- BAL
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x7A9f34a0Aa917D438e9b6E630067062B7F8f6f3d'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink'
  AND t.chain_id = 1
  AND t.address = '\xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'::bytea  -- STG
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- ---------------------------------------------------------------------------
-- 4. Assertion: every reward token must end this migration with a chainlink/USD
-- binding. A future rename of the chainlink oracle row, or a token address typo
-- above, would otherwise produce a successful migration with zero bindings.
-- Count DISTINCT token_id (not rows): a token already bound to a second chainlink
-- feed must not inflate the count past 4 (false failure), and one unbound token
-- offset by another's extra binding must not still sum to 4 (false pass).
-- ---------------------------------------------------------------------------

DO $$
DECLARE bound_count INT;
BEGIN
    SELECT count(DISTINCT t.id) INTO bound_count
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id
    JOIN token  t ON t.id = oa.token_id
    WHERE o.name = 'chainlink'
      AND t.chain_id = 1
      AND oa.quote_currency = 'USD'
      AND t.address IN (
          '\x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B'::bytea,  -- CVX
          '\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'::bytea,  -- FXS
          '\xba100000625a3754423978a60c9317c58a424e3D'::bytea,  -- BAL
          '\xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'::bytea   -- STG
      );
    IF bound_count <> 4 THEN
        RAISE EXCEPTION 'expected all 4 of CVX/FXS/BAL/STG to have a chainlink USD binding, got % distinct', bound_count;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260521_100000_create_dex_prereqs.sql')
ON CONFLICT (filename) DO NOTHING;
