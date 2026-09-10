-- Backfill of the payload columns 20260910_130000 added to
-- sparklend_reserve_data_current, and the statement an operator re-runs to
-- converge the cache. Separate from 130000 so the ADD COLUMN's exclusive lock is
-- released before this history scan; the widened trigger is already writing every
-- column, so live keys self-fill and this mops up the rest.
--
-- Forward-only merge with a `>=` guard, not 20260820_120000's strict `>`: every
-- key already sits at its newest version, so `>` would be a no-op and leave the
-- 21 columns NULL. `>=` re-states each row from the identical history row (same
-- tuple, same flag) and adds its payload; nothing is ever lowered, and a key
-- history has but the cache lacks is inserted, so this one statement is also the
-- rebuild (same shape as 20260910_120050).
--
-- Each converged key takes a row lock the ingest trigger's DO UPDATE for that key
-- waits on, and the reverse crossing can deadlock; either way the migration
-- aborts and is re-run, as 20260820_120000 states for the sibling backfills.
SET LOCAL lock_timeout = '10s';

-- sparklend_reserve_data carries no tiering policy today — compression only, set
-- outside the migrations (20260410_140000) — so this GUC is a no-op for it. It is
-- still set explicitly, in either direction, as every sibling backfill does
-- (20260820_120000 measured why the default cannot be trusted): if a policy is ever
-- added, a local-only scan would find an older row for a reserve whose newest row
-- had been tiered, fail the guard below, and leave its payload NULL.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

INSERT INTO sparklend_reserve_data_current
    (protocol_id, token_id, usage_as_collateral_enabled,
     unbacked, accrued_to_treasury_scaled, total_a_token, total_stable_debt,
     total_variable_debt, liquidity_rate, variable_borrow_rate, stable_borrow_rate,
     average_stable_borrow_rate, liquidity_index, variable_borrow_index,
     last_update_at, decimals, ltv, liquidation_threshold, liquidation_bonus,
     reserve_factor, borrowing_enabled, stable_borrow_rate_enabled, is_active, is_frozen,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (srd.protocol_id, srd.token_id)
    srd.protocol_id, srd.token_id, srd.usage_as_collateral_enabled,
    srd.unbacked, srd.accrued_to_treasury_scaled, srd.total_a_token, srd.total_stable_debt,
    srd.total_variable_debt, srd.liquidity_rate, srd.variable_borrow_rate, srd.stable_borrow_rate,
    srd.average_stable_borrow_rate, srd.liquidity_index, srd.variable_borrow_index,
    CASE WHEN srd.last_update_timestamp BETWEEN 1500000000 AND 4100000000
         THEN to_timestamp(srd.last_update_timestamp) END,
    CASE WHEN srd.decimals BETWEEN 0 AND 255 AND srd.decimals = trunc(srd.decimals)
         THEN srd.decimals::smallint END,
    srd.ltv, srd.liquidation_threshold, srd.liquidation_bonus,
    srd.reserve_factor, srd.borrowing_enabled, srd.stable_borrow_rate_enabled,
    srd.is_active, srd.is_frozen,
    srd.block_number, srd.block_version, COALESCE(srd.processing_version, -1)
FROM sparklend_reserve_data srd
ORDER BY srd.protocol_id, srd.token_id,
         srd.block_number DESC, srd.block_version DESC, COALESCE(srd.processing_version, -1) DESC
ON CONFLICT (protocol_id, token_id) DO UPDATE SET
    usage_as_collateral_enabled = EXCLUDED.usage_as_collateral_enabled,
    unbacked = EXCLUDED.unbacked,
    accrued_to_treasury_scaled = EXCLUDED.accrued_to_treasury_scaled,
    total_a_token = EXCLUDED.total_a_token,
    total_stable_debt = EXCLUDED.total_stable_debt,
    total_variable_debt = EXCLUDED.total_variable_debt,
    liquidity_rate = EXCLUDED.liquidity_rate,
    variable_borrow_rate = EXCLUDED.variable_borrow_rate,
    stable_borrow_rate = EXCLUDED.stable_borrow_rate,
    average_stable_borrow_rate = EXCLUDED.average_stable_borrow_rate,
    liquidity_index = EXCLUDED.liquidity_index,
    variable_borrow_index = EXCLUDED.variable_borrow_index,
    last_update_at = EXCLUDED.last_update_at,
    decimals = EXCLUDED.decimals,
    ltv = EXCLUDED.ltv,
    liquidation_threshold = EXCLUDED.liquidation_threshold,
    liquidation_bonus = EXCLUDED.liquidation_bonus,
    reserve_factor = EXCLUDED.reserve_factor,
    borrowing_enabled = EXCLUDED.borrowing_enabled,
    stable_borrow_rate_enabled = EXCLUDED.stable_borrow_rate_enabled,
    is_active = EXCLUDED.is_active,
    is_frozen = EXCLUDED.is_frozen,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
   >= (sparklend_reserve_data_current.block_number, sparklend_reserve_data_current.block_version,
       sparklend_reserve_data_current.processing_version);

-- 21 new columns' worth of stats the planner does not have yet.
ANALYZE sparklend_reserve_data_current;

INSERT INTO migrations (filename)
VALUES ('20260910_130050_backfill_sparklend_reserve_data_current_payload.sql')
ON CONFLICT (filename) DO NOTHING;
