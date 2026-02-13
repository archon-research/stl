-- Add oracle_type to oracle table for dispatch (aave_oracle vs chainlink_feed)
ALTER TABLE oracle ADD COLUMN IF NOT EXISTS oracle_type VARCHAR(20) NOT NULL DEFAULT 'aave_oracle';

-- Add per-feed columns to oracle_asset for chainlink-style feeds
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS feed_address BYTEA;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS feed_decimals SMALLINT;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS quote_currency VARCHAR(10) NOT NULL DEFAULT 'USD';

-- Seed Chainlink oracle (oracle_type = 'chainlink_feed', address = zero)
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('chainlink', 'Chainlink', 1, '\x0000000000000000000000000000000000000000', 'chainlink_feed', 10606501, 8, true)
ON CONFLICT (name) DO NOTHING;

-- Seed Chronicle oracle (uses DirectCaller instead of Multicall3 due to toll/whitelist)
-- deployment_block = earliest feed (WETH/USD deployed at block 18791466)
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('chronicle', 'Chronicle', 1, '\x0000000000000000000000000000000000000000', 'chronicle', 18791466, 18, true)
ON CONFLICT (name) DO NOTHING;

-- Seed Redstone oracle (same ABI as chainlink_feed)
-- deployment_block = weETH/USD feed deployed at block 19712153
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('redstone', 'RedStone', 1, '\x0000000000000000000000000000000000000000', 'chainlink_feed', 19712153, 8, true)
ON CONFLICT (name) DO NOTHING;

-- Chainlink feed assets (16 feeds)
-- Requires token table to have matching symbols
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'WETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'WBTC'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'DAI'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'USDC'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x3E7d1eAB13ad0104d2750B8863b489D65364e32D'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'USDT'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x2665701293fCbEB223D11A08D826563EDcCE423A'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'cbBTC'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8f1dF6D7F2db73eECE86a18b4381F4707b918FB1'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'PYUSD'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8350b7De6a6a2C1368E7D4Bd968190e13E354297'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'tBTC'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x29081f7aB5a644716EfcDC10D5c926c5fEE9F72B'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'sDAI'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xfF30586cD0F29eD462364C7e81375FC0C71219b1'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'USDS'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

-- ETH-denominated feeds
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x5c9C449BbC9a6075A2c061dF312a35fd1E05fF22'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'weETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x536218f9E9Eb48863970252233c8F271f554C2d0'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'rETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x86392dC19c0b719886221c78AB11eb8Cf5c52812'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'wstETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x636A000262F6aA9e1F094ABF0aD8f645C44f641C'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'ezETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x03c68933f7a3F76875C0bc670a58e69294cDFD01'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'rsETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

-- BTC-denominated feed
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x5c29868C58b6e15e2b962943278969Ab6a7D3212'::BYTEA, 8, 'BTC'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'LBTC'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

-- Chronicle feed assets (18 decimals, USD-denominated)
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x46ef0071b1E2fF6B42d36e5A177EA43Ae5917f4E'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'WETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xA770582353b573CbfdCC948751750EeB3Ccf23CF'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'wstETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

-- USDS/USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x74661a9ea74fD04975c6eBc6B155Abf8f885636c'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'USDS'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

-- sUSDS/USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x496470F4835186bF118545Bd76889F123D608E84'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'sUSDS'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

-- Redstone feed asset
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x0c2c7ded01ccdfab16f04aff82af766b23d6be0a'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'weETH'
ON CONFLICT (oracle_id, token_id) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260212_120000_add_oracle_type_and_feed_columns.sql')
ON CONFLICT (filename) DO NOTHING;
