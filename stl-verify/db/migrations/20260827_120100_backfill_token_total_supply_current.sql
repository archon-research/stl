-- Backfill of token_total_supply_current, split from
-- 20260827_120000_create_token_total_supply_current.sql so the trigger's lock on
-- token_total_supply is released before this history scan starts (rationale in
-- that file's header). Same newer-wins guard as the trigger, so it is idempotent
-- against live ingest, and re-running it is the rebuild procedure.
--
-- It can lose a deadlock to a concurrent tracker batch: cache rows are locked
-- here in (chain_id, token_id) order, by SaveSupplies in (chain_id, token_address)
-- order. The migration then aborts and rolls back with ingest unharmed, and the
-- cache stays trigger-fed but missing its pre-existing keys until the migrator is
-- re-run; the migrate job is an ArgoCD PreSync hook, so the API that reads the
-- cache does not roll out before that succeeds.
--
-- token_total_supply has a 1-year tiering policy
-- (20260423_214929_create_token_total_supply.sql), so a key whose newest row has
-- already been tiered is invisible to the scan without this. Set explicitly in
-- either direction rather than inherited, and dependent on the migrator's
-- per-file transaction — both explained in the
-- 20260820_120000_create_current_position_tables.sql header.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

INSERT INTO token_total_supply_current
    (chain_id, token_id, total_supply, scaled_total_supply, block_timestamp,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (tts.chain_id, tts.token_id)
    tts.chain_id, tts.token_id, tts.total_supply, tts.scaled_total_supply, tts.block_timestamp,
    tts.block_number, tts.block_version, tts.processing_version
FROM token_total_supply tts
ORDER BY tts.chain_id, tts.token_id,
         tts.block_number DESC, tts.block_version DESC, tts.processing_version DESC
ON CONFLICT (chain_id, token_id) DO UPDATE SET
    total_supply = EXCLUDED.total_supply,
    scaled_total_supply = EXCLUDED.scaled_total_supply,
    block_timestamp = EXCLUDED.block_timestamp,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version,
    updated_at = now()
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (token_total_supply_current.block_number, token_total_supply_current.block_version,
       token_total_supply_current.processing_version);

-- Fresh table, so the first reads after deploy would otherwise plan on no stats.
ANALYZE token_total_supply_current;

INSERT INTO migrations (filename)
VALUES ('20260827_120100_backfill_token_total_supply_current.sql')
ON CONFLICT (filename) DO NOTHING;
