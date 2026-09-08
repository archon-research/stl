-- Register the three collateral-enabled Aave V3 Avalanche reserves that have no
-- oracle_asset row yet, so their prices flow through the aave_v3_avax oracle.
-- Addresses verified against AaveV3AvalancheAssets in the official aave-address-book:
--   sUSDe  = 0x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2
--   USDe   = 0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34
--   wrsETH = 0x7bFd4CA2a6Cf3A3fDDd645D10B323031afe47FF0

-- Seed the token rows so a fresh DB resolves the FK joins deterministically;
-- on live DBs ingestion has already created them and this is a no-op.
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (43114, '\x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2'::bytea, 'sUSDe',  18),
    (43114, '\x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34'::bytea, 'USDe',   18),
    (43114, '\x7bFd4CA2a6Cf3A3fDDd645D10B323031afe47FF0'::bytea, 'wrsETH', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, quote_currency)
SELECT o.id, t.id, true, 'USD'
FROM oracle o
JOIN token t ON t.chain_id = 43114 AND t.address IN (
    '\x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2'::bytea,
    '\x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34'::bytea,
    '\x7bFd4CA2a6Cf3A3fDDd645D10B323031afe47FF0'::bytea
)
WHERE o.name = 'aave_v3_avax'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260831_140000_add_avalanche_oracle_assets.sql')
ON CONFLICT (filename) DO NOTHING;
