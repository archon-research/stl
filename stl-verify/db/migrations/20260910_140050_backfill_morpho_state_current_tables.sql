-- Initial backfill of morpho_vault_state_current and morpho_market_state_current,
-- and the statements an operator re-runs to converge them. Runs as the migrator,
-- which owns both caches — each cache's only two writers are its SECURITY DEFINER
-- trigger and this statement (20260910_140000); an application role cannot run this.
--
-- Separate from 20260910_140000 for the lock-holding reason that file and
-- 20260825_120100 document: split, these scans hold only ACCESS SHARE on the
-- histories while the triggers are already live, so no row can land in a gap, and
-- where the two overlap the newer-wins guard makes the second a no-op.
--
-- Re-running is a FORWARD-ONLY merge: it raises a cached row to a newer history
-- row and never lowers or removes one. That repairs a cache left BEHIND history
-- (a restore, a `session_replication_role = replica` load, a window with the
-- trigger disabled). A cache AHEAD of history needs an owner-only TRUNCATE first.
--
-- The ORDER BY is the trigger's newer-wins comparison spelled as a sort, term for
-- term: identity first (block_number, block_version, timestamp),
-- processing_version last. See 20260910_140000 for why that order.
SET LOCAL lock_timeout = '10s';

-- Both histories have a 1-year tiering policy (20260224_100000), so a vault or
-- market whose newest row has already been tiered would otherwise get a stale
-- cache row or none at all. Set explicitly rather than inherited, as
-- 20260909_150100 does.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

INSERT INTO morpho_vault_state_current
    (morpho_vault_id, total_assets, total_shares, block_timestamp,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (vs.morpho_vault_id)
    vs.morpho_vault_id, vs.total_assets, vs.total_shares, vs."timestamp",
    vs.block_number, vs.block_version, vs.processing_version
FROM morpho_vault_state vs
ORDER BY vs.morpho_vault_id,
         vs.block_number DESC, vs.block_version DESC, vs."timestamp" DESC,
         vs.processing_version DESC
ON CONFLICT (morpho_vault_id) DO UPDATE SET
    total_assets = EXCLUDED.total_assets,
    total_shares = EXCLUDED.total_shares,
    block_timestamp = EXCLUDED.block_timestamp,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version,
    created_at = now()
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.block_timestamp,
       EXCLUDED.processing_version)
    > (morpho_vault_state_current.block_number,
       morpho_vault_state_current.block_version,
       morpho_vault_state_current.block_timestamp,
       morpho_vault_state_current.processing_version);

INSERT INTO morpho_market_state_current
    (morpho_market_id, total_supply_assets, total_supply_shares,
     total_borrow_assets, total_borrow_shares, last_update_at, fee, block_timestamp,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (ms.morpho_market_id)
    ms.morpho_market_id, ms.total_supply_assets, ms.total_supply_shares,
    ms.total_borrow_assets, ms.total_borrow_shares,
    CASE WHEN ms.last_update BETWEEN 1500000000 AND 4100000000
         THEN to_timestamp(ms.last_update) END,
    ms.fee, ms."timestamp", ms.block_number, ms.block_version, ms.processing_version
FROM morpho_market_state ms
ORDER BY ms.morpho_market_id,
         ms.block_number DESC, ms.block_version DESC, ms."timestamp" DESC,
         ms.processing_version DESC
ON CONFLICT (morpho_market_id) DO UPDATE SET
    total_supply_assets = EXCLUDED.total_supply_assets,
    total_supply_shares = EXCLUDED.total_supply_shares,
    total_borrow_assets = EXCLUDED.total_borrow_assets,
    total_borrow_shares = EXCLUDED.total_borrow_shares,
    last_update_at = EXCLUDED.last_update_at,
    fee = EXCLUDED.fee,
    block_timestamp = EXCLUDED.block_timestamp,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version,
    created_at = now()
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.block_timestamp,
       EXCLUDED.processing_version)
    > (morpho_market_state_current.block_number,
       morpho_market_state_current.block_version,
       morpho_market_state_current.block_timestamp,
       morpho_market_state_current.processing_version);

-- Fresh tables, so the first reads after deploy would otherwise plan on no stats.
ANALYZE morpho_vault_state_current;
ANALYZE morpho_market_state_current;

INSERT INTO migrations (filename)
VALUES ('20260910_140050_backfill_morpho_state_current_tables.sql')
ON CONFLICT (filename) DO NOTHING;
