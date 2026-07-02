-- Expose Maple Syrup vaults as first-class allocations (VEC-372), Ethereum mainnet (chain_id = 1).
-- Three steps, ordered by dependency:
--   1. Seed the syrup share tokens + their underlying tokens.
--   2. Register each syrup vault as a receipt_token under the `maple` protocol.
--   3. Bind the `maple` protocol to the aave_v3 oracle so syrup positions can be priced.
-- All addresses + decimals verified against the live Maple GraphQL API (poolV2.asset);
-- share tokens are the ERC-4626 vault addresses (docs/maple_spec.md). FK ids are resolved
-- by natural key so this is FK-correct per environment AND deterministic on a fresh DB,
-- with no dependency on the Maple indexer having populated maple_pool.

-- 1. Share tokens (ERC-4626 vault shares) + underlyings. syrupUSDT and the common
-- underlyings may already exist (20260305_100000); re-inserts are no-ops via ON CONFLICT.
-- Underlyings are seeded so step 2 can resolve underlying_token_id by (chain_id, address).
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    -- share tokens
    (1, '\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b'::bytea, 'syrupUSDC', 6),
    (1, '\x87b65c4aaffa76881f9e96f3e7ed945ddfc3cd7a'::bytea, 'syrupUSDG', 6),
    -- underlyings
    (1, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea, 'USDC', 6),
    (1, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea, 'USDT', 6),
    (1, '\xe343167631d89b6ffc58b88d6b7fb0228795491d'::bytea, 'USDG', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

-- 2. Register the syrup vaults as receipt tokens under the `maple` protocol.
-- Static seed of a fixed, verified vault->underlying set; unique key is
-- (chain_id, receipt_token_address) (see 20260319_100000_update_receipt_token.sql).
INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol)
SELECT 1, p.id, t.id, v.vault::bytea, v.sym
FROM (VALUES
    ('\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b', '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'syrupUSDC'),
    ('\x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d', '\xdac17f958d2ee523a2206206994597c13d831ec7', 'syrupUSDT'),
    ('\x87b65c4aaffa76881f9e96f3e7ed945ddfc3cd7a', '\xe343167631d89b6ffc58b88d6b7fb0228795491d', 'syrupUSDG')
) AS v(vault, underlying, sym)
JOIN protocol p ON p.chain_id = 1 AND p.name = 'maple'
JOIN token t    ON t.chain_id = 1 AND t.address = v.underlying::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- 3. Price Maple syrup positions: bind maple -> aave_v3 oracle so the allocations USD join
-- (protocol_oracle -> onchain_token_price) resolves, and register the syrup share tokens as
-- oracle assets. syrupUSDT is already an aave_v3 oracle asset (20260305_100000). amount_usd
-- degrades to null (UI shows token amount) for any token the oracle does not price.
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 16291127
FROM protocol p, oracle o
WHERE p.chain_id = 1 AND p.name = 'maple' AND o.name = 'aave_v3'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled)
SELECT o.id, t.id, true
FROM oracle o
JOIN token t ON t.chain_id = 1 AND t.symbol IN ('syrupUSDC', 'syrupUSDG')
WHERE o.name = 'aave_v3'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260702_120000_maple_syrup_allocation_exposure.sql')
ON CONFLICT (filename) DO NOTHING;
