-- Catalogue metadata for debt_token, now that VEC-413 populates it.
-- The table maps each SparkLend/Aave reserve's variable and stable debt tokens
-- to their underlying asset; VEC-419 resolves the debt leg on the variable-debt
-- token address, so an index on that column is added here.
-- Follows the [Type]/Roles conventions from 20260609_120000_add_schema_comments.sql.

COMMENT ON TABLE debt_token IS
  '[Configuration] Maps each SparkLend/Aave reserve''s variable and stable debt tokens to their underlying asset. One row per (protocol, underlying token). The debt leg keys on the variable-debt token address (VEC-419).';
COMMENT ON COLUMN debt_token.id IS
  'PK. BIGSERIAL surrogate key.';
COMMENT ON COLUMN debt_token.protocol_id IS
  'FK→protocol.id. Protocol this debt token belongs to. Part of the (protocol_id, underlying_token_id) UNIQUE natural key.';
COMMENT ON COLUMN debt_token.underlying_token_id IS
  'FK→token.id. Actual borrowed asset (e.g. DAI for the variable-debt DAI token). Part of the UNIQUE natural key.';
COMMENT ON COLUMN debt_token.variable_debt_address IS
  'Variable-debt token contract address (20 bytes). Nullable. The address the debt leg is resolved on (VEC-419).';
COMMENT ON COLUMN debt_token.stable_debt_address IS
  'Stable-debt token contract address (20 bytes). Nullable; not all protocols issue stable-debt tokens.';
COMMENT ON COLUMN debt_token.variable_symbol IS
  'On-chain ERC-20 symbol of the variable-debt token (e.g. variableDebtWETH). Nullable when the symbol is absent from on-chain metadata.';
COMMENT ON COLUMN debt_token.stable_symbol IS
  'On-chain ERC-20 symbol of the stable-debt token (e.g. stableDebtWETH). Nullable when there is no stable-debt token or the symbol is absent.';
COMMENT ON COLUMN debt_token.created_at_block IS
  'Block number at which this debt token was first seen. First-seen marker, not a per-block metric; merges with LEAST() on conflict so the earliest observation is retained.';
COMMENT ON COLUMN debt_token.updated_at IS
  'Audit. Wall-clock time of the last write that changed the row.';
COMMENT ON COLUMN debt_token.metadata IS
  'Free-form JSONB for auxiliary attributes. Currently written as an empty object.';

CREATE INDEX IF NOT EXISTS idx_debt_token_variable_debt_address
  ON debt_token (variable_debt_address);

INSERT INTO migrations (filename)
VALUES ('20260715_120000_debt_token_comments.sql')
ON CONFLICT (filename) DO NOTHING;
