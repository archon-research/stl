-- Initial backfill of morpho_market_position_current, and the statement an
-- operator re-runs to converge it. Runs as the migrator, which owns the cache —
-- the cache's only two writers are its SECURITY DEFINER trigger and this
-- statement (20260909_150000); an application role cannot run this.
--
-- Separate from 20260909_150000 for the lock-holding reason that file and
-- 20260825_120100 document: split, this scan holds only ACCESS SHARE on the
-- history while the trigger is already live, so no row can land in a gap, and
-- where the two overlap the newer-wins guard makes the second a no-op.
--
-- Re-running is a FORWARD-ONLY merge: it raises a cached row to a newer history
-- row and never lowers or removes one. That repairs a cache left BEHIND history
-- (a restore, a `session_replication_role = replica` load, a window with the
-- trigger disabled). A cache AHEAD of history needs an owner-only TRUNCATE first.
--
-- The ORDER BY is the trigger's newer-wins comparison spelled as a sort, term for
-- term: identity first (block_number, block_version, timestamp),
-- processing_version last. See 20260909_150000 for why that order.
SET LOCAL lock_timeout = '10s';

-- morpho_market_position has a 1-year tiering policy, so a borrower whose newest
-- row has already been tiered would otherwise get a stale cache row or none at
-- all. Set explicitly rather than inherited, as 20260825_120100 does.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

INSERT INTO morpho_market_position_current
    (user_id, morpho_market_id, supply_shares, borrow_shares, collateral,
     supply_assets, borrow_assets, block_timestamp,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (mp.user_id, mp.morpho_market_id)
    mp.user_id, mp.morpho_market_id, mp.supply_shares, mp.borrow_shares,
    mp.collateral, mp.supply_assets, mp.borrow_assets, mp."timestamp",
    mp.block_number, mp.block_version, mp.processing_version
FROM morpho_market_position mp
ORDER BY mp.user_id, mp.morpho_market_id,
         mp.block_number DESC, mp.block_version DESC, mp."timestamp" DESC,
         mp.processing_version DESC
ON CONFLICT (user_id, morpho_market_id) DO UPDATE SET
    supply_shares = EXCLUDED.supply_shares,
    borrow_shares = EXCLUDED.borrow_shares,
    collateral = EXCLUDED.collateral,
    supply_assets = EXCLUDED.supply_assets,
    borrow_assets = EXCLUDED.borrow_assets,
    block_timestamp = EXCLUDED.block_timestamp,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version,
    created_at = now()
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.block_timestamp,
       EXCLUDED.processing_version)
    > (morpho_market_position_current.block_number,
       morpho_market_position_current.block_version,
       morpho_market_position_current.block_timestamp,
       morpho_market_position_current.processing_version);

-- Fresh table, so the first reads after deploy would otherwise plan on no stats.
ANALYZE morpho_market_position_current;

INSERT INTO migrations (filename)
VALUES ('20260909_150100_backfill_morpho_market_position_current.sql')
ON CONFLICT (filename) DO NOTHING;
