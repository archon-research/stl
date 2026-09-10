-- Backfill of token_price_current.block_timestamp (20260910_120000), and the
-- statement an operator re-runs to converge the cache. Separate from 120000 so
-- the ADD COLUMN's exclusive lock is released before this scan; the trigger is
-- already writing the column, so live keys self-fill and this mops up the rest.
--
-- Forward-only merge with a `>=` guard, not 20260820_120000's strict `>`: every
-- key already sits at its newest version, so `>` would be a no-op and leave the
-- column NULL. `>=` re-states each row from the identical history row (same
-- tuple, same price) and adds its timestamp; nothing is ever lowered.
SET LOCAL lock_timeout = '10s';

-- Explicitly OFF: the read this cache replaces ran in a plain session, so the
-- locally readable set is what keeps every answer identical, and it avoids an S3
-- scan of the tiered chunks. Not left to the default, which a role setting could
-- silently flip.
SET LOCAL timescaledb.enable_tiered_reads = 'off';

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

ANALYZE token_price_current;

INSERT INTO migrations (filename)
VALUES ('20260910_120050_backfill_token_price_current_block_timestamp.sql')
ON CONFLICT (filename) DO NOTHING;
