-- Fluid Savings USDS (fsUSDS) ERC-4626 share pricing on Ethereum mainnet (VEC-435).
-- fsUSDS is priced per block by the oracle-price-indexer: convertToAssets(1e18)/1e18
-- (underlying USDS per share) times the USDS/USD Chainlink feed already used for USDS.
-- USDS underlying is itself priced via Chainlink + Chronicle (migration 20260212).

-- 1. Seed the fsUSDS vault token (idempotent).
--    fsUSDS: 0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11, 18 decimals.
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11'::bytea, 'fsUSDS', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- 2. Insert the erc4626_share oracle. address is the fsUSDS vault contract read for
--    convertToAssets; deployment_block is the fsUSDS vault deployment block.
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
VALUES (
    'fluid_fsusds',
    'Fluid: fsUSDS (ERC-4626)',
    1,
    '\x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11',
    'erc4626_share',
    21624401,
    true,
    8
)
ON CONFLICT (name) DO NOTHING;

-- 3. Link fsUSDS to its underlying USDS/USD Chainlink feed.
--    The asset's token_id is the fsUSDS share token (the priced asset); feed_address
--    carries the USDS/USD feed (0xfF30586cD0F29eD462364C7e81375FC0C71219b1, 8 decimals)
--    so the unit reads the underlying USD price in the same multicall batch.
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\xfF30586cD0F29eD462364C7e81375FC0C71219b1'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'fluid_fsusds'
  AND t.chain_id = 1
  AND t.address = '\x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260626_120000_add_fsusds_erc4626_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
