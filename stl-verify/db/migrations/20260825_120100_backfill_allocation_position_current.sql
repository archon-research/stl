-- Initial backfill of allocation_position_current, and the statement an operator
-- re-runs to converge it. It runs as the migrator, which owns the cache, and that
-- is the whole of its authority to write one: the cache's only two writers are its
-- SECURITY DEFINER trigger and this statement, both the owner's, and stl_readwrite
-- holds SELECT only (20260825_120000). An application role cannot run this.
--
-- Separate from 20260825_120000, which creates the table and the trigger, for the
-- reason that file's header states and 20260819_150100 (VEC-409) established: the
-- migrator runs a whole file in one transaction, so together CREATE TRIGGER's
-- SHARE ROW EXCLUSIVE on allocation_position was held for the length of the
-- full-history scan below, and that lock conflicts with the ROW EXCLUSIVE every
-- ingest INSERT takes. Split, the statement below holds ACCESS SHARE on
-- allocation_position, which conflicts with nothing ingest does. The no-gap
-- invariant survives the split: 20260825_120000 commits before this file starts,
-- so the trigger is live for the whole of this scan, and where the two overlap
-- whichever resolves second is a guarded no-op.
--
-- Re-running this statement is a FORWARD-ONLY merge, not a rebuild. The newer-wins
-- guard raises a cached row to a newer history row and never lowers or removes
-- one, so it cannot repair a row that is AHEAD of history, nor one whose key has
-- no history at all. A true rebuild is TRUNCATE then this statement, and TRUNCATE
-- is owner-only — no login role holds any write grant here, TRUNCATE included.
--
-- Forward-only is enough for the recovery cases 20260825_120000's header names — a
-- restore, a `session_replication_role = replica` load, any window with the trigger
-- disabled — because all of them leave the cache BEHIND history, never ahead.
--
-- The ORDER BY is the trigger's newer-wins comparison spelled as a sort, term for
-- term, so the row this picks is the row the trigger would have left: identity
-- first (block_number, block_version, block_timestamp, log_index, direction,
-- tx_hash), processing_version last. See 20260825_120000 for why that order and
-- not another.
SET LOCAL lock_timeout = '10s';

-- allocation_position has a 1-year tiering policy
-- (20260409_130000_convert_event_tables_to_hypertables.sql), so a key whose newest
-- row has already been tiered is computed over a PARTIAL table without this, and
-- the cache silently gets a stale row or none at all. Set explicitly in either
-- direction rather than inherited; see the sibling migration for the measurement
-- and for why a one-shot scan must not rest on a GUC default.
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

-- Fresh table, so the first reads after deploy would otherwise plan on no stats.
ANALYZE allocation_position_current;

INSERT INTO migrations (filename)
VALUES ('20260825_120100_backfill_allocation_position_current.sql')
ON CONFLICT (filename) DO NOTHING;
