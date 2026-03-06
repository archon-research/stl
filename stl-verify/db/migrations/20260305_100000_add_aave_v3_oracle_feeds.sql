-- Add Aave V3 oracle and Chainlink feeds for high/medium-impact Aave V3 tokens.

-- 0. Seed tokens referenced by this migration (idempotent — ON CONFLICT DO NOTHING)
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\x4c9EDD5852cd905f086C759E8383e09bff1E68B3'::bytea, 'USDe', 18),
    (1, '\x9D39A5DE30e57443BfF2A8307A4256c8797A3497'::bytea, 'sUSDe', 18),
    (1, '\x3De0fF76e8b528C092d47b9DAc775931CEf80f49'::bytea, 'PT-sUSDE-7MAY2026', 18),
    (1, '\x356b8d89C1E1239cbBb9DE4815c39a1474d5Ba7D'::bytea, 'syrupUSDT', 6),
    (1, '\x9bF45Ab47747F4b4dd09b3c2c73953484b4eB375'::bytea, 'PT-srUSDe-2APR2026', 18),
    (1, '\x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c'::bytea, 'EURC', 6),
    (1, '\x514910771AF9Ca656af840dff83E8264EcF986CA'::bytea, 'LINK', 18),
    (1, '\xaEBF0Bb9F57E89260D57f31AF34eB58657D96ce0'::bytea, 'PT-USDe-7MAY2026', 18),
    (1, '\xE8483517077aFA11A9b07f849CeE2552f040D7B2'::bytea, 'PT-sUSDE-5FEB2026', 18),
    (1, '\xD533a949740bb3306d119CC777fa900bA034cd52'::bytea, 'CRV', 18),
    (1, '\x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'::bytea, 'AAVE', 18),
    (1, '\x5A98FcBEA516Cf06857215779Fd812CA3beF1B32'::bytea, 'LDO', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- 1. Insert Aave V3 oracle
-- Oracle address: 0x54586be62e3c3580375ae3723c145253060ca0c2
-- Resolved from PoolAddressesProvider 0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e
-- Verified stable across blocks 16291127, 20000000, and latest.
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
VALUES (
    'aave_v3',
    'Aave V3: Oracle',
    1,
    '\x54586be62e3c3580375ae3723c145253060ca0c2',
    'aave_oracle',
    16291127,
    true,
    8
)
ON CONFLICT (name) DO NOTHING;

-- 2. Add unique constraint to protocol_oracle (prevents duplicate bindings)
ALTER TABLE protocol_oracle
  ADD CONSTRAINT protocol_oracle_unique UNIQUE (protocol_id, oracle_id, from_block);

-- 3. Bind Aave V3 oracle to Aave V3 protocol
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 16291127
FROM protocol p, oracle o
WHERE p.name = 'Aave V3' AND o.name = 'aave_v3'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- 3. Add 12 oracle_asset entries for Aave V3 oracle
-- These tokens are priced via getAssetsPrices() on the Aave V3 oracle contract.
INSERT INTO oracle_asset (oracle_id, token_id, enabled)
SELECT o.id, t.id, true
FROM oracle o
CROSS JOIN (VALUES
    ('USDe'),
    ('sUSDe'),
    ('PT-sUSDE-7MAY2026'),
    ('syrupUSDT'),
    ('PT-srUSDe-2APR2026'),
    ('EURC'),
    ('LINK'),
    ('PT-USDe-7MAY2026'),
    ('PT-sUSDE-5FEB2026'),
    ('CRV'),
    ('AAVE'),
    ('LDO')
) AS symbols(symbol)
JOIN token t ON t.symbol = symbols.symbol AND t.chain_id = 1
WHERE o.name = 'aave_v3'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

-- 4. Add 7 Chainlink feed entries (additional price source for tokens with feeds)
-- All USD-denominated except LDO which is ETH-denominated.
-- LDO/ETH conversion to USD works automatically via the existing WETH/USD reference feed
-- in the Chainlink oracle (see ConvertNonUSDPrices in oracle_unit.go).

-- USDe / USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xa569d910839Ae8865Da8F8e70FfFb0cBA869F961'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'USDe' AND t.chain_id = 1
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- sUSDe / USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xFF3BC18cCBd5999CE63E788A1c250a88626aD099'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'sUSDe' AND t.chain_id = 1
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- EURC / USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x04F84020Fdf10d9ee64D1dcC2986EDF2F556DA11'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'EURC' AND t.chain_id = 1
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- LINK / USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'LINK' AND t.chain_id = 1
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- CRV / USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xCd627aA160A6fA45Eb793D19Ef54f5062F20f33f'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'CRV' AND t.chain_id = 1
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- AAVE / USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x547a514d5e3769680Ce22B2361c10Ea13619e8a9'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'AAVE' AND t.chain_id = 1
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- LDO / ETH (18 decimals, ETH-denominated — needs WETH/USD reference feed for conversion)
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x4e844125952D32AcdF339BE976c98E22F6F318dB'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'LDO' AND t.chain_id = 1
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260305_100000_add_aave_v3_oracle_feeds.sql')
ON CONFLICT (filename) DO NOTHING;
