-- Partial index for the morpho-indexer symbol-reconciliation sweep. Tokens whose
-- on-chain symbol() was not yet resolvable at first sighting are flagged with
-- metadata->>'symbol_pending' = 'true' and are a tiny subset, so this partial
-- index keeps ListTokensPendingSymbol off a full table scan.
CREATE INDEX IF NOT EXISTS idx_token_symbol_pending
    ON token (chain_id)
    WHERE (metadata ->> 'symbol_pending') = 'true';

INSERT INTO migrations (filename)
VALUES ('20260608_120000_index_token_symbol_pending.sql')
ON CONFLICT (filename) DO NOTHING;
