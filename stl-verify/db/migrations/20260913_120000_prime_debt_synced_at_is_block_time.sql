-- VEC-709: prime_debt.synced_at now carries the on-chain timestamp of block_number
-- instead of the sweep's wall clock. Rows written before this migration keep the old
-- processing-time value; the two differ by the sweep's read latency, not by chunks.

COMMENT ON COLUMN prime_debt.synced_at IS
  'Partition key. On-chain block-header timestamp (UTC) of block_number, not the sweep''s wall clock. Part of the natural key, so a redelivered block reproduces it and ON CONFLICT DO NOTHING dedupes (VEC-709). Rows written before 2026-09-13 hold processing time.';

INSERT INTO migrations (filename)
VALUES ('20260913_120000_prime_debt_synced_at_is_block_time.sql')
ON CONFLICT (filename) DO NOTHING;
