-- Backfill of token_total_supply_current, split from
-- 20260827_120000_create_token_total_supply_current.sql so the trigger's lock on
-- token_total_supply is released before this history scan starts (rationale in
-- that file's header).
--
-- The guard is >= where the trigger's is strict >: this statement doubles as the
-- REPAIR for corrections that re-land at an equal version tuple (a hand-fix
-- UPDATE with triggers disabled, a delete + re-sweep that re-assigns version 0),
-- which the trigger can never push into the cache. The overwrite stays
-- order-independent against live ingest: an equal tuple from this scan carries
-- the values the trigger just wrote, and a newer trigger write beats this scan's
-- older snapshot on the tuple.
--
-- Rebuild/repair procedure: run this file in ONE transaction as the table owner
-- (psql --single-transaction -f <this file>) — the migrator never re-runs a
-- recorded filename, and outside a transaction block the SET LOCALs warn and
-- silently no-op. The trailing migrations INSERT is a no-op. If history rows
-- were REMOVED (not corrected), TRUNCATE the cache as owner first: a >= guard
-- cannot regress a cache row to a lower tuple. Racing a tracker batch, either
-- side can be the deadlock victim — the migration re-runs via the migrate job,
-- a tracker batch via SQS redelivery; both are idempotent.
--
-- Fail fast on lock waits, as the create file does.
SET LOCAL lock_timeout = '10s';

-- token_total_supply has a 1-year tiering policy
-- (20260423_214929_create_token_total_supply.sql), so a key whose newest row has
-- already been tiered is invisible to the scan without this. Set explicitly in
-- either direction rather than inherited, and dependent on the migrator's
-- per-file transaction — both explained in the
-- 20260820_120000_create_current_position_tables.sql header.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

INSERT INTO token_total_supply_current
    (chain_id, token_id, total_supply, scaled_total_supply, block_timestamp,
     block_number, block_version, processing_version, created_at)
SELECT DISTINCT ON (tts.chain_id, tts.token_id)
    tts.chain_id, tts.token_id, tts.total_supply, tts.scaled_total_supply, tts.block_timestamp,
    tts.block_number, tts.block_version, tts.processing_version, tts.created_at
FROM token_total_supply tts
ORDER BY tts.chain_id, tts.token_id,
         tts.block_number DESC, tts.block_version DESC, tts.processing_version DESC,
         tts.block_timestamp DESC
ON CONFLICT (chain_id, token_id) DO UPDATE SET
    total_supply = EXCLUDED.total_supply,
    scaled_total_supply = EXCLUDED.scaled_total_supply,
    block_timestamp = EXCLUDED.block_timestamp,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version,
    created_at = EXCLUDED.created_at
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
    >= (token_total_supply_current.block_number, token_total_supply_current.block_version,
        token_total_supply_current.processing_version, token_total_supply_current.block_timestamp);

-- Fresh table, so the first reads after deploy would otherwise plan on no stats.
ANALYZE token_total_supply_current;

INSERT INTO migrations (filename)
VALUES ('20260827_120100_backfill_token_total_supply_current.sql')
ON CONFLICT (filename) DO NOTHING;
