-- Add Aave V3 oracle and Chainlink feeds for high/medium-impact Aave V3 tokens.
-- See docs/plans/2026-03-05-aave-v3-oracle-feeds-design.md for context.

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

-- 2. Bind Aave V3 oracle to Aave V3 protocol
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 16291127
FROM protocol p, oracle o
WHERE p.name = 'Aave V3' AND o.name = 'aave_v3';

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
