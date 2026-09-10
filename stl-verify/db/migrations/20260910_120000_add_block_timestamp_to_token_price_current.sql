-- token_price_current gains the winning row's observation time (VEC-672).
--
-- Why: two reads still walked the 355-chunk onchain_token_price history because
-- the cache could not answer them — /v1/tokens/{id}/price orders by timestamp and
-- returns it as staleness_seconds, and the CORE readers' feed-liveness check
-- filters on it. A latest-row read with no time predicate cannot exclude a single
-- chunk, so each call planned over every chunk of the hypertable: measured on
-- staging on 2026-09-10 at 63 MB of planner memory and 5.6 s of planning for a
-- 40 ms read, with every pooled backend keeping the chunk metadata it loaded.
-- Under a burst of API calls that is what pushed the 8 GB staging instance into
-- oom_guard refusals (VectorDatabaseResourceErrors, 8–9 Sep 2026).
--
-- Catalog-only change, the way VEC-661 widens sparklend_reserve_data_current:
-- ALTER TABLE on the ~100-row cache, CREATE OR REPLACE of the trigger function
-- (the trigger itself is NOT recreated, so no SHARE ROW EXCLUSIVE lock is taken
-- on the busy hypertable), and a forward-only backfill.
--
-- Nullable, not NOT NULL. The history column is NOT NULL, so every row the
-- trigger sees carries one; but the backfill below cannot date a cache row whose
-- winning history row is no longer readable in a plain session (tiered to S3),
-- and a NOT NULL would abort the very statement that fills the rest. Reads that
-- need the timestamp filter `"timestamp" IS NOT NULL`, so such a key falls back to
-- its next source, exactly as the history read did for a row it could not see.
SET LOCAL lock_timeout = '10s';

-- Named block_timestamp, not "timestamp": the canonical name for on-chain block
-- time in the schema register (data_quality/schemamaster), which the transformed
-- layer also uses when it renames the raw column; a derived table declares
-- canonical names, as morpho_market_position_current does for the same copy.
ALTER TABLE token_price_current ADD COLUMN IF NOT EXISTS block_timestamp TIMESTAMPTZ;

COMMENT ON COLUMN token_price_current.block_timestamp IS 'Derived (copy of onchain_token_price.timestamp). On-chain block time of the winning row. NOT a term of the newer-wins comparison — (block_number, block_version, processing_version) already orders one (oracle, token) key totally and the indexer never varies the timestamp within a block — but it is what /v1/tokens/…/price orders by and reports as staleness, and what the CORE feed-liveness check filters on. NULL only for a row backfilled before 20260910_120000 whose winning history row is no longer readable in a plain session (tiered); reads treat such a row as absent.';

-- Same body as 20260820_120000 plus the timestamp. Deliberately still not
-- SECURITY DEFINER: the four VEC-577 caches keep their older grant form until
-- VEC-684 aligns them, and changing the security mode here would widen a
-- catalog-only change.
CREATE OR REPLACE FUNCTION upsert_token_price_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO token_price_current AS cur
        (oracle_id, token_id, price_usd,
         block_number, block_version, processing_version, block_timestamp)
    VALUES
        (NEW.oracle_id, NEW.token_id, NEW.price_usd,
         NEW.block_number, NEW.block_version, NEW.processing_version, NEW."timestamp")
    ON CONFLICT (oracle_id, token_id) DO UPDATE SET
        price_usd = EXCLUDED.price_usd,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        block_timestamp = EXCLUDED.block_timestamp
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Backfill: a forward-only merge that also fills the new column. The guard is
-- `>=`, not the strict `>` of 20260820_120000: every key already sits at its
-- newest version, so a strict guard would be a silent no-op and leave the column
-- NULL. `>=` re-states each cached row from the identical history row (same
-- tuple, hence the same price) and adds its timestamp; a key the cache has fallen
-- behind on is raised in the same pass; nothing is ever lowered.
--
-- Tiered reads stay OFF (the default), as the 20260820_120000 backfill left them:
-- the cache then mirrors what a plain session can read, which is exactly what the
-- history read this replaces could see, so the switch preserves every answer.
-- Turning them on would pull the ~900 tiered chunks of prices older than the
-- 1-year horizon back from S3 — useless as a "current" price, and a heavy scan
-- on the very instance this change relieves.
INSERT INTO token_price_current
    (oracle_id, token_id, price_usd,
     block_number, block_version, processing_version, block_timestamp)
SELECT DISTINCT ON (otp.oracle_id, otp.token_id)
    otp.oracle_id, otp.token_id, otp.price_usd,
    otp.block_number, otp.block_version, otp.processing_version, otp."timestamp"
FROM onchain_token_price otp
ORDER BY otp.oracle_id, otp.token_id,
         otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
ON CONFLICT (oracle_id, token_id) DO UPDATE SET
    price_usd = EXCLUDED.price_usd,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version,
    block_timestamp = EXCLUDED.block_timestamp
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    >= (token_price_current.block_number, token_price_current.block_version, token_price_current.processing_version);

-- The column is new, so the first reads after deploy would otherwise plan on no stats.
ANALYZE token_price_current;

INSERT INTO migrations (filename)
VALUES ('20260910_120000_add_block_timestamp_to_token_price_current.sql')
ON CONFLICT (filename) DO NOTHING;
