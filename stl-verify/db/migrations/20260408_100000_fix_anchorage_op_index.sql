-- Fix idx_anchorage_op_prime_time: remove operation_id from the index.
-- The varchar column causes "missing operator" errors on TigerData distributed
-- hypertables because the distributed query coordinator cannot find a varchar
-- comparison operator in the timestamptz operator family.
DROP INDEX IF EXISTS idx_anchorage_op_prime_time;

CREATE INDEX IF NOT EXISTS idx_anchorage_op_prime_time
    ON anchorage_operation (prime_id, created_at DESC);

INSERT INTO migrations (filename)
VALUES ('20260408_100000_fix_anchorage_op_index.sql')
ON CONFLICT (filename) DO NOTHING;
