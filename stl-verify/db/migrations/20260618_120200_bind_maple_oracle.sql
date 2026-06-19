-- Price Maple syrup positions: bind the maple protocol to the aave_v3 oracle so the
-- allocations USD join (protocol_oracle -> onchain_token_price) resolves, and register
-- the syrup share tokens as oracle assets. syrupUSDT is already an aave_v3 oracle asset
-- (20260305_100000). amount_usd degrades to null (UI shows token amount) for any token
-- the oracle does not price.
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
VALUES ('20260618_120200_bind_maple_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
