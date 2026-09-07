-- Fluid Savings USDS (fsUSDS) ERC-4626 share pricing on Ethereum mainnet (VEC-435).
-- fsUSDS vault (0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11) holds sUSDS as its
-- underlying asset. Price = convertToAssets(1e18)/1e18 (sUSDS per share) times
-- the sUSDS/USD Chronicle feed (0x496470F4835186bF118545Bd76889F123D608E84, 18 decimals).
-- The Chronicle sUSDS/USD feed is tollgated and reverts through Multicall3; the
-- oracle-price-indexer routes erc4626_share oracles through a DirectCaller.

-- 1. Seed the fsUSDS vault token (idempotent).
--    fsUSDS: 0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11, 18 decimals.
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (1, '\x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11'::bytea, 'fsUSDS', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- 2. Insert the erc4626_share oracle disabled. address is the fsUSDS vault contract
--    read for convertToAssets; deployment_block is the fsUSDS vault deployment block.
--    Starts disabled: an old binary that encounters an unknown oracle_type will error
--    every block and DLQ-loop the whole pipeline. Flip enabled=true after the image
--    that supports erc4626_share is rolled out.
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
VALUES (
    'fluid_fsusds',
    'Fluid: fsUSDS (ERC-4626)',
    1,
    '\x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11',
    'erc4626_share',
    21624401,
    false,
    8
)
ON CONFLICT (name) DO NOTHING;

-- 3. Link fsUSDS to its underlying sUSDS/USD Chronicle feed.
--    token_id is the fsUSDS share token (the priced asset); feed_address carries the
--    sUSDS/USD Chronicle feed (0x496470F4835186bF118545Bd76889F123D608E84, 18 decimals)
--    so the unit reads the underlying USD price via DirectCaller.
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true,
       '\x496470F4835186bF118545Bd76889F123D608E84'::bytea, 18, 'USD'
FROM oracle o, token t
WHERE o.name = 'fluid_fsusds'
  AND t.chain_id = 1
  AND t.address = '\x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- 4. Extend oracle_type and document feed_address's dual role for erc4626_share oracles.
COMMENT ON COLUMN public.oracle.oracle_type IS
  'Determines ABI method called when reading prices: aave_oracle | chainlink_feed | chronicle | redstone | erc4626_share.';
COMMENT ON COLUMN public.oracle_asset.feed_address IS
  'Specific feed contract for this token (20 bytes). Null for Aave-style oracles. '
  'For chainlink_feed / chronicle / redstone: the priced token''s own USD feed. '
  'For erc4626_share: the UNDERLYING token''s USD feed (not the share token''s own feed); '
  'the binary reads this address via DirectCaller to obtain the underlying spot price.';

INSERT INTO migrations (filename)
VALUES ('20260626_120000_add_fsusds_erc4626_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
