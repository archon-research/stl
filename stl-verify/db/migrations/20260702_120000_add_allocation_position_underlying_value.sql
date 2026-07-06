-- VEC-307: store each position's current value in underlying-asset units.
--
-- PR #216 (VEC-125) deliberately made balance == raw share count for
-- ERC4626/Curve entries, so the underlying value (convertToAssets) is no
-- longer stored anywhere. These columns restore it additively, without
-- touching the balance/scaled_balance semantics VEC-125 fixed.
--
-- allocation_position is a columnstore-enabled hypertable with a tiering
-- policy: columns are added bare (nullable, no DEFAULT) per the TigerData
-- restriction documented in 20260410_110000. The FK and index below apply
-- cleanly on the columnstore hypertable.

ALTER TABLE allocation_position ADD COLUMN IF NOT EXISTS underlying_value NUMERIC;
ALTER TABLE allocation_position ADD COLUMN IF NOT EXISTS underlying_token_id BIGINT;

-- DROP before ADD: allocation_position is a hypertable pinned to public; the
-- multi-schema test harness replays this migration against the same public
-- table once per registered test schema, so the constraint can already exist
-- on the second pass. In prod (one transactional run per DB) the DROPs are
-- no-ops.
ALTER TABLE allocation_position
  DROP CONSTRAINT IF EXISTS allocation_position_underlying_token_id_fkey;
ALTER TABLE allocation_position
  ADD CONSTRAINT allocation_position_underlying_token_id_fkey
  FOREIGN KEY (underlying_token_id) REFERENCES token (id);

-- Postgres does not auto-index the child side of an FK; index it so parent-side
-- (token) changes and joins through underlying_token_id do not seq-scan
-- allocation_position (mirrors idx_receipt_token_underlying). Plain CREATE INDEX
-- (not CONCURRENTLY): hypertables reject CONCURRENTLY and build the index per
-- chunk, new chunks inherit it (see 20260615_120000).
CREATE INDEX IF NOT EXISTS idx_allocation_position_underlying_token_id
  ON allocation_position (underlying_token_id);

-- Invariant: underlying_value and underlying_token_id are both set or both
-- NULL. This is intentionally NOT enforced with a DB CHECK constraint:
-- ALTER TABLE ... ADD CHECK on a columnstore hypertable is broken on TSDB
-- 2.26.x (fails with "stack depth limit exceeded"). It is enforced in the
-- domain instead: AllocationPosition.Underlying is a single pointer, so both
-- columns are written from it or neither is, and entity Validate() rejects an
-- incomplete valuation. No writer bypasses that path.

COMMENT ON COLUMN allocation_position.underlying_value IS
  'Current position value denominated in underlying_token_id''s asset (decimals-normalised by that asset), read on-chain at (block_number, block_version) at the same pinned block as balance. erc4626: convertToAssets(shares). atoken: balanceOf (1:1 underlying by construction). erc20: balanceOf — deliberately duplicates balance so non-NULL uniformly means "valued"; do not deduplicate. NULL: not computable (curve/uni_v3/NAV-token rows this phase, reverting or undecodable convertToAssets, missing asset_address) and every row written before this column existed. NULL is never zero exposure; consumers fall back to balance-based pricing.';

COMMENT ON COLUMN allocation_position.underlying_token_id IS
  'FK to token: the asset underlying_value is denominated in. NULL iff underlying_value is NULL (enforced in the domain: entity Validate() plus the single AllocationPosition.Underlying pointer).';

INSERT INTO migrations (filename)
VALUES ('20260702_120000_add_allocation_position_underlying_value.sql')
ON CONFLICT (filename) DO NOTHING;
