-- Initial backfill of offchain_token_price_current, and the statement an
-- operator re-runs to converge it. Runs as the migrator, which owns the cache —
-- the cache's only two writers are its SECURITY DEFINER trigger and this
-- statement (20260910_120100); an application role cannot run this.
--
-- Separate from 20260910_120100 for the lock-holding reason that file and
-- 20260825_120100 document: split, this scan holds only ACCESS SHARE on the
-- history while the trigger is already live, so no row can land in a gap, and
-- where the two overlap the newer-wins guard makes the second a no-op.
--
-- Re-running is a FORWARD-ONLY merge: it raises a cached row to a newer history
-- row and never lowers or removes one. A cache AHEAD of history needs an
-- owner-only TRUNCATE first.
--
-- The ORDER BY is the trigger's newer-wins comparison spelled as a sort, term for
-- term: snapshot_time first, processing_version last (20260910_120100).
SET LOCAL lock_timeout = '10s';

-- Tiered reads explicitly OFF, unlike the position-cache backfills. This cache
-- exists to replace a read that ran in a plain session — which sees local chunks
-- only — so mirroring exactly that set is what keeps every answer identical
-- (VEC-672's acceptance criterion). A price older than the 1-year tiering
-- horizon is not a "current" price in any case, and reading the ~2,000 tiered
-- chunks back from S3 would be a heavy scan on the instance this change relieves.
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

-- Fresh table, so the first reads after deploy would otherwise plan on no stats.
ANALYZE offchain_token_price_current;

INSERT INTO migrations (filename)
VALUES ('20260910_120200_backfill_offchain_token_price_current.sql')
ON CONFLICT (filename) DO NOTHING;
