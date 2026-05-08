-- VEC-210: Wire Aave V3 Lido and Aave V3 RWA (Horizon) into protocol_oracle so
-- amount_usd flows for receipt-token positions in primes.
--
-- Aave V2 is intentionally NOT wired here. The deployed V2 mainnet oracle
-- (0xA50ba011c48153De246E5192C8f9258A2ba79Ca9, resolved from its
-- PoolAddressesProvider.getPriceOracle()) is the original V2 contract: its read
-- ABI is {WETH, getAssetPrice, getAssetsPrices, getFallbackOracle,
-- getSourceOfAsset, owner} with no BASE_CURRENCY/BASE_CURRENCY_UNIT. Prices are
-- ETH-denominated (WEI, 18 decimals), and the current oracle_price_worker path
-- for aave_oracle stores raw ScaleByDecimals output into price_usd without an
-- ETH→USD conversion step. Wiring V2 needs a follow-up that either teaches the
-- aave_oracle path to use a WETH/USD reference (mirroring ConvertNonUSDPrices
-- on the chainlink_feed path) or registers V2 reserves as per-asset feeds.

-- ───────────────────────────────────────────────────────────────────────────
-- Tokens
-- ───────────────────────────────────────────────────────────────────────────
-- GHO is a reserve on both V3 Lido and V3 Horizon but isn't yet seeded for
-- chain_id=1 (only chain_id=43114 from the avalanche migration).
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f'::bytea, 'GHO', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ───────────────────────────────────────────────────────────────────────────
-- Aave V3 Lido
-- ───────────────────────────────────────────────────────────────────────────
-- Oracle address from AaveV3EthereumLido.ORACLE
-- (https://github.com/bgd-labs/aave-address-book/blob/main/src/AaveV3EthereumLido.sol).
-- Resolved from PoolAddressesProvider 0xcfBf336fe147D643B9Cb705648500e101504B16d.
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
VALUES (
    'aave_v3_lido',
    'Aave V3 Lido: Oracle',
    1,
    '\xE3C061981870C0C7b1f3C4F4bB36B95f1F260BE6'::bytea,
    'aave_oracle',
    20262414,
    true,
    8
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 20262414
FROM protocol p, oracle o
WHERE p.chain_id = 1 AND p.name = 'Aave V3 Lido' AND o.name = 'aave_v3_lido'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- Reserve set per AaveV3EthereumLidoAssets: wstETH, WETH, USDS, USDC, ezETH,
-- sUSDe, GHO, rsETH, tETH. tETH is omitted (token row not yet seeded).
INSERT INTO oracle_asset (oracle_id, token_id, enabled)
SELECT o.id, t.id, true
FROM oracle o
CROSS JOIN (VALUES
    ('wstETH'),
    ('WETH'),
    ('USDS'),
    ('USDC'),
    ('ezETH'),
    ('sUSDe'),
    ('GHO'),
    ('rsETH')
) AS symbols(symbol)
JOIN token t ON t.symbol = symbols.symbol AND t.chain_id = 1
WHERE o.name = 'aave_v3_lido'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

-- ───────────────────────────────────────────────────────────────────────────
-- Aave V3 RWA (Horizon)
-- ───────────────────────────────────────────────────────────────────────────
-- Oracle address from AaveV3EthereumHorizon.ORACLE
-- (https://github.com/bgd-labs/aave-address-book/blob/main/src/AaveV3EthereumHorizon.sol).
-- Resolved from PoolAddressesProvider 0x5D39E06b825C1F2B80bf2756a73e28eFAA128ba0.
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
VALUES (
    'aave_v3_rwa',
    'Aave V3 RWA: Oracle',
    1,
    '\x985BcfAB7e0f4EF2606CC5b64FC1A16311880442'::bytea,
    'aave_oracle',
    23125535,
    true,
    8
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 23125535
FROM protocol p, oracle o
WHERE p.chain_id = 1 AND p.name = 'Aave V3 RWA' AND o.name = 'aave_v3_rwa'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- Reserve set per AaveV3EthereumHorizonAssets: GHO, USDC, RLUSD, USTB, USCC,
-- USYC, JTRSY, JAAA, VBILL, ACRED. Only GHO and USDC are seeded today; the
-- remaining RWA tokens are not in the token table yet and would need their own
-- seed + Chainlink feed work, so they're omitted here.
INSERT INTO oracle_asset (oracle_id, token_id, enabled)
SELECT o.id, t.id, true
FROM oracle o
CROSS JOIN (VALUES
    ('GHO'),
    ('USDC')
) AS symbols(symbol)
JOIN token t ON t.symbol = symbols.symbol AND t.chain_id = 1
WHERE o.name = 'aave_v3_rwa'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260505_135100_add_aave_lido_horizon_oracles.sql')
ON CONFLICT (filename) DO NOTHING;
