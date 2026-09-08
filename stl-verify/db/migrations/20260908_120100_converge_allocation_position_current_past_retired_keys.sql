-- VEC-535: the statement an operator re-runs to converge allocation_position_current.
-- It SUPERSEDES 20260825_120100, which must no longer be re-run: that statement merges
-- every key allocation_position history holds, and history is append-only, so it puts
-- the retired ERC-7540 vault keys back the moment it runs (20260908_120000 explains why
-- they are retired and why deleting them alone does not hold).
--
-- Same forward-only merge as 20260825_120100 — read that file for the ORDER BY, the
-- newer-wins guard, and why re-running it repairs a cache that is BEHIND history but
-- not one that is AHEAD of it — with one addition: a candidate row whose key is retired
-- is not a candidate at all. Everything else is term for term the same, deliberately,
-- so the row this picks stays the row the trigger would have left.
--
-- Applying it here is not only the migration: allocation_position is small (tens of
-- thousands of rows), so running it at apply time costs little and is the live proof
-- that the exclusion holds on real data, immediately after 20260908_120000 emptied the
-- retired keys out of the cache.
--
-- Separate from 20260908_120000 for the reason 20260825_120100 records: that file holds
-- locks a full-history scan must not be queued behind, and the split keeps the trigger
-- live for the whole of this scan so no row can land in a gap.
SET LOCAL lock_timeout = '10s';

-- allocation_position has a 1-year tiering policy
-- (20260409_130000_convert_event_tables_to_hypertables.sql), so a key whose newest row
-- has already been tiered is computed over a PARTIAL table without this, and the cache
-- silently gets a stale row or none at all. Set explicitly in either direction rather
-- than inherited; see 20260825_120100 for the measurement.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

INSERT INTO allocation_position_current
    (proxy_address, chain_id, token_id, balance, underlying_value, underlying_token_id,
     tx_amount, direction, tx_hash, block_timestamp,
     block_number, block_version, log_index, processing_version)
SELECT DISTINCT ON (ap.proxy_address, ap.chain_id, ap.token_id)
    ap.proxy_address, ap.chain_id, ap.token_id, ap.balance, ap.underlying_value,
    ap.underlying_token_id, ap.tx_amount, ap.direction, ap.tx_hash,
    ap.created_at AS block_timestamp,
    ap.block_number, ap.block_version, ap.log_index, ap.processing_version
FROM allocation_position ap
WHERE NOT EXISTS (
    SELECT 1
    FROM allocation_position_key_retirement_current r
    WHERE r.chain_id = ap.chain_id AND r.token_id = ap.token_id AND r.retired
)
ORDER BY ap.proxy_address, ap.chain_id, ap.token_id,
         ap.block_number DESC, ap.block_version DESC, ap.created_at DESC,
         ap.log_index DESC, ap.direction DESC, ap.tx_hash DESC,
         ap.processing_version DESC
ON CONFLICT (proxy_address, chain_id, token_id) DO UPDATE SET
    balance = EXCLUDED.balance,
    underlying_value = EXCLUDED.underlying_value,
    underlying_token_id = EXCLUDED.underlying_token_id,
    tx_amount = EXCLUDED.tx_amount,
    direction = EXCLUDED.direction,
    tx_hash = EXCLUDED.tx_hash,
    block_timestamp = EXCLUDED.block_timestamp,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    log_index = EXCLUDED.log_index,
    processing_version = EXCLUDED.processing_version,
    created_at = now()
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.block_timestamp,
       EXCLUDED.log_index, EXCLUDED.direction, EXCLUDED.tx_hash, EXCLUDED.processing_version)
    > (allocation_position_current.block_number, allocation_position_current.block_version,
       allocation_position_current.block_timestamp, allocation_position_current.log_index,
       allocation_position_current.direction, allocation_position_current.tx_hash,
       allocation_position_current.processing_version);

INSERT INTO migrations (filename)
VALUES ('20260908_120100_converge_allocation_position_current_past_retired_keys.sql')
ON CONFLICT (filename) DO NOTHING;
