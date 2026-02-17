-- Add oracle_type to oracle table for dispatch (aave_oracle vs chainlink_feed)
ALTER TABLE oracle ADD COLUMN IF NOT EXISTS oracle_type VARCHAR(20) NOT NULL DEFAULT 'aave_oracle';

-- Feed-based oracles (chainlink_feed, chronicle) don't have a single oracle contract;
-- each asset has its own feed address in oracle_asset.feed_address.
ALTER TABLE oracle ALTER COLUMN address DROP NOT NULL;

-- Add per-feed columns to oracle_asset for chainlink-style feeds
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS feed_address BYTEA;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS feed_decimals SMALLINT;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS quote_currency VARCHAR(10) NOT NULL DEFAULT 'USD';

-- Allow multiple feeds per token per oracle (e.g. weETH/USD + weETH/ETH).
-- Split into two partial indexes:
--   - Non-feed oracles (aave_oracle): one row per (oracle_id, token_id)
--   - Feed oracles: one row per (oracle_id, token_id, feed_address)
ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_oracle_id_token_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS oracle_asset_nonfeed_unique
  ON oracle_asset (oracle_id, token_id) WHERE feed_address IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS oracle_asset_feed_unique
  ON oracle_asset (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL;

-- Seed Chainlink oracle (no single contract; feeds stored per-asset)
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('chainlink', 'Chainlink', 1, NULL, 'chainlink_feed', 10606501, 8, true)
ON CONFLICT (name) DO NOTHING;

-- Seed Chronicle oracle (uses DirectCaller instead of Multicall3 due to toll/whitelist)
-- deployment_block = earliest feed (wstETH/USD deployed at block 18791466)
-- Note: WETH/USDC/USDT/WBTC feeds were deployed later (~block 22219400, Apr 2025)
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('chronicle', 'Chronicle', 1, NULL, 'chronicle', 18791466, 18, true)
ON CONFLICT (name) DO NOTHING;

-- Seed Redstone oracle (same ABI as chainlink_feed)
-- deployment_block = weETH/USD feed deployed at block 19712153
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('redstone', 'RedStone', 1, NULL, 'chainlink_feed', 19712153, 8, true)
ON CONFLICT (name) DO NOTHING;

-- Chainlink feed assets (16 feeds)
-- Feed addresses sourced from https://data.chain.link/feeds/ethereum/mainnet
-- Requires token table to have matching symbols
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'WETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'WBTC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'DAI'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'USDC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x3E7d1eAB13ad0104d2750B8863b489D65364e32D'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'USDT'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x2665701293fCbEB223D11A08D826563EDcCE423A'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'cbBTC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8f1dF6D7F2db73eECE86a18b4381F4707b918FB1'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'PYUSD'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8350b7De6a6a2C1368E7D4Bd968190e13E354297'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'tBTC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x29081f7aB5a644716EfcDC10D5c926c5fEE9F72B'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'sDAI'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xfF30586cD0F29eD462364C7e81375FC0C71219b1'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'USDS'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- ETH-denominated feeds
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x5c9C449BbC9a6075A2c061dF312a35fd1E05fF22'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'weETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x536218f9E9Eb48863970252233c8F271f554C2d0'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'rETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- wstETH/USD uses WstETHSynchronicityPriceAdapter (composes wstETH/stETH + stETH/ETH + ETH/USD)
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8B6851156023f4f5A66F68BEA80851c3D905Ac93'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'wstETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x636A000262F6aA9e1F094ABF0aD8f645C44f641C'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'ezETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x03c68933f7a3F76875C0bc670a58e69294cDFD01'::BYTEA, 18, 'ETH'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'rsETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- BTC-denominated feed
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x5c29868C58b6e15e2b962943278969Ab6a7D3212'::BYTEA, 8, 'BTC'
FROM oracle o, token t WHERE o.name = 'chainlink' AND t.symbol = 'LBTC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- Chronicle feed assets (18 decimals, USD-denominated)
-- Feed addresses sourced from prices_oracles_reference.md (canonical1 addresses)

-- WETH/USD (deployed ~block 22219400, Apr 2025)
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xb074EEE1F1e66650DA49A4d96e255c8337A272a9'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'WETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- wstETH/USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xA770582353b573CbfdCC948751750EeB3Ccf23CF'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'wstETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- USDS/USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x74661a9ea74fD04975c6eBc6B155Abf8f885636c'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'USDS'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- sUSDS/USD
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x496470F4835186bF118545Bd76889F123D608E84'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'sUSDS'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- USDC/USD (deployed ~block 22219400, Apr 2025)
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xCe701340261a3dc3541C5f8A6d2bE689381C8fCC'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'USDC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- USDT/USD (deployed ~block 22219400, Apr 2025)
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x7084a627a22b2de99E18733DC5aAF40993FA405C'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'USDT'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- WBTC/USD (deployed ~block 22219400, Apr 2025)
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x286204401e0C1E63043E95a8DE93236B735d4BF2'::BYTEA, 18, 'USD'
FROM oracle o, token t WHERE o.name = 'chronicle' AND t.symbol = 'WBTC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- Redstone feed assets (12 feeds)
-- Feed addresses sourced from RedStone relayer manifest (https://app.redstone.finance/app/feeds/)
-- All feeds use Chainlink AggregatorV3 interface (latestRoundData)

-- USD-denominated feeds
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xdDb6F90fFb4d3257dd666b69178e5B3c5Bf41136'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'weETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x67F6838e58859d612E4ddF04dA396d6DABB66Dc4'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'WETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xAB7f623fb2F6fea6601D4350FA0E2290663C28Fc'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'WBTC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xe4aE88743c3834d0c492eAbC47384c84BcADC6a6'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'wstETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xcE18A2Bf89Fa3c56aF5Bde8A41efF967a6d63d26'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'PYUSD'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xeeF31c7d9F2E82e8A497b140cc60cc082Be4b94e'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'USDC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x02E1F8d15762047b7a87BA0E5d94B9a0c5b54Ed2'::BYTEA, 8, 'USD'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'USDT'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- ETH-denominated feeds
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x8751F736E94F6CD167e8C5B97E245680FbD9CC36'::BYTEA, 8, 'ETH'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'weETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xA736eAe8805dDeFFba40cAB8c99bCB309dEaBd9B'::BYTEA, 8, 'ETH'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'rsETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xF4a3e183F59D2599ee3DF213ff78b1B3b1923696'::BYTEA, 8, 'ETH'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'ezETH'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- BTC-denominated feed
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xb415eAA355D8440ac7eCB602D3fb67ccC1f0bc81'::BYTEA, 8, 'BTC'
FROM oracle o, token t WHERE o.name = 'redstone' AND t.symbol = 'LBTC'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260212_120000_add_oracle_type_and_feed_columns.sql')
ON CONFLICT (filename) DO NOTHING;
