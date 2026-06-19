-- Register Maple Syrup vaults as receipt tokens (Ethereum mainnet).
-- Static seed: vault->underlying pairs are a fixed, verified set (docs/maple_spec.md +
-- live Maple GraphQL poolV2.asset). FK ids resolved by natural key (protocol.name,
-- token.(chain_id,address)) so this is FK-correct per environment AND deterministic on a
-- fresh DB (no dependency on the Maple indexer having populated maple_pool).
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

INSERT INTO migrations (filename)
VALUES ('20260618_120100_seed_maple_receipt_tokens.sql')
ON CONFLICT (filename) DO NOTHING;
