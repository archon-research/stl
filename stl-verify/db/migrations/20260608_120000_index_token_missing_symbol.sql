-- Partial index for the morpho-indexer symbol-reconciliation sweep: tokens whose
-- on-chain symbol() was not yet readable when first persisted have symbol = ''
-- and are re-read every sweep until it resolves. The empty-symbol subset is tiny,
-- so this partial index keeps ListTokensMissingSymbol off a full table scan.
-- created_at_block is included as the second key so the query's
-- ORDER BY created_at_block LIMIT comes straight off the index with no sort.
CREATE INDEX IF NOT EXISTS idx_token_missing_symbol
    ON token (chain_id, created_at_block)
    WHERE symbol = '';

INSERT INTO migrations (filename)
VALUES ('20260608_120000_index_token_missing_symbol.sql')
ON CONFLICT (filename) DO NOTHING;
