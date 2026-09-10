-- Initial backfill of offchain_token_price_current, and the statement an
-- operator re-runs to converge it. Runs as the migrator, which owns the cache.
-- Separate from 20260910_120100 for the lock-holding reason that file and
-- 20260825_120100 document. Forward-only: raises a cached row to a newer history
-- row, never lowers or removes one; a cache AHEAD of history needs an owner-only
-- TRUNCATE first. The ORDER BY is the trigger's comparison spelled as a sort.
SET LOCAL lock_timeout = '10s';

-- Explicitly OFF: the read this cache replaces ran in a plain session, so the
-- locally readable set is what keeps every answer identical, and it avoids an S3
-- scan of the tiered chunks. Not left to the default, which a role setting could
-- silently flip.
SET LOCAL timescaledb.enable_tiered_reads = 'off';

INSERT INTO offchain_token_price_current
    (token_id, source_id, price_usd, snapshot_time, processing_version)
SELECT DISTINCT ON (op.token_id, op.source_id)
    op.token_id, op.source_id, op.price_usd, op."timestamp" AS snapshot_time, op.processing_version
FROM offchain_token_price op
ORDER BY op.token_id, op.source_id,
         op."timestamp" DESC, op.processing_version DESC
ON CONFLICT (token_id, source_id) DO UPDATE SET
    price_usd = EXCLUDED.price_usd,
    snapshot_time = EXCLUDED.snapshot_time,
    processing_version = EXCLUDED.processing_version,
    created_at = now()
WHERE (EXCLUDED.snapshot_time, EXCLUDED.processing_version)
    > (offchain_token_price_current.snapshot_time, offchain_token_price_current.processing_version);

ANALYZE offchain_token_price_current;

INSERT INTO migrations (filename)
VALUES ('20260910_120200_backfill_offchain_token_price_current.sql')
ON CONFLICT (filename) DO NOTHING;
