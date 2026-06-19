-- Seed Maple Syrup share tokens + their underlying tokens (Ethereum mainnet, chain_id = 1).
-- Share tokens: syrupUSDC, syrupUSDG (syrupUSDT already seeded by 20260305_100000).
-- Underlyings (USDC/USDT/USDG) are seeded idempotently so the static receipt_token
-- seed in 20260618_120100 can resolve underlying_token_id by (chain_id, address) on a
-- fresh DB, independent of the Maple indexer. All addresses + decimals verified against
-- the live Maple GraphQL API (poolV2.asset); share tokens are the ERC-4626 vault addresses.
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    -- share tokens (ERC-4626 vault shares)
    (1, '\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b'::bytea, 'syrupUSDC', 6),
    (1, '\x87b65c4aaffa76881f9e96f3e7ed945ddfc3cd7a'::bytea, 'syrupUSDG', 6),
    -- underlyings
    (1, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea, 'USDC', 6),
    (1, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea, 'USDT', 6),
    (1, '\xe343167631d89b6ffc58b88d6b7fb0228795491d'::bytea, 'USDG', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260618_120000_seed_maple_syrup_tokens.sql')
ON CONFLICT (filename) DO NOTHING;
