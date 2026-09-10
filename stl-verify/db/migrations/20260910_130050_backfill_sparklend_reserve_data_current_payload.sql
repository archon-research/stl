-- Backfill of the payload columns 20260910_130000 added to
-- sparklend_reserve_data_current, and the statement an operator re-runs to
-- converge them. Separate from 130000 so the ADD COLUMN's ACCESS EXCLUSIVE lock on
-- the cache is released before this history scan: the widened trigger is already
-- writing every column, so live keys self-fill and this mops up the rest. This
-- statement holds ROW EXCLUSIVE on the cache and ACCESS SHARE on the history,
-- neither of which conflicts with what ingest does.
--
-- This is an UPDATE, not the original migration's INSERT … ON CONFLICT DO UPDATE,
-- and the difference is not cosmetic: that statement's guard is a strict `>`, and
-- every key here already sits at its newest version (the trigger has kept it there
-- since VEC-577). Re-running it would therefore conflict on every row and take a
-- DO UPDATE arm whose guard can never be true — a silent no-op that leaves all 21
-- new columns NULL. The guard below is `<=` so the equal case, which is the only
-- case in practice, updates. It is still forward-only: a cache row AHEAD of the
-- newest readable history row (a reserve whose newest row landed after this scan
-- took its snapshot) is left alone, never lowered.
--
-- Keys are matched on (protocol_id, token_id); the version tuple and
-- usage_as_collateral_enabled are re-asserted alongside the new columns so the
-- statement converges a row that is genuinely older, not just fills its gaps. There
-- is no INSERT arm: a key in history but absent from the cache cannot exist — the
-- trigger has fired on every insert since VEC-577 and VEC-577's own backfill covered
-- everything before it. If that ever stops holding, the original migration's INSERT
-- backfill (20260820_120000) is the repair, and it is the statement to re-run first.
SET LOCAL lock_timeout = '10s';

-- sparklend_reserve_data carries no tiering policy today — compression only, set
-- outside the migrations (20260410_140000) — so this GUC is a no-op for it. It is
-- still set explicitly, in either direction, as every sibling backfill does
-- (20260820_120000 measured why the default cannot be trusted): if a policy is ever
-- added, a local-only scan would find an older row for a reserve whose newest row
-- had been tiered, fail the guard below, and leave its payload NULL.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

UPDATE sparklend_reserve_data_current c
SET usage_as_collateral_enabled = s.usage_as_collateral_enabled,
    unbacked                    = s.unbacked,
    accrued_to_treasury_scaled  = s.accrued_to_treasury_scaled,
    total_a_token               = s.total_a_token,
    total_stable_debt           = s.total_stable_debt,
    total_variable_debt         = s.total_variable_debt,
    liquidity_rate              = s.liquidity_rate,
    variable_borrow_rate        = s.variable_borrow_rate,
    stable_borrow_rate          = s.stable_borrow_rate,
    average_stable_borrow_rate  = s.average_stable_borrow_rate,
    liquidity_index             = s.liquidity_index,
    variable_borrow_index       = s.variable_borrow_index,
    last_update_at              = s.last_update_at,
    decimals                    = s.decimals,
    ltv                         = s.ltv,
    liquidation_threshold       = s.liquidation_threshold,
    liquidation_bonus           = s.liquidation_bonus,
    reserve_factor              = s.reserve_factor,
    borrowing_enabled           = s.borrowing_enabled,
    stable_borrow_rate_enabled  = s.stable_borrow_rate_enabled,
    is_active                   = s.is_active,
    is_frozen                   = s.is_frozen,
    block_number                = s.block_number,
    block_version               = s.block_version,
    processing_version          = s.processing_version
FROM (
    SELECT DISTINCT ON (srd.protocol_id, srd.token_id)
        srd.protocol_id,
        srd.token_id,
        srd.usage_as_collateral_enabled,
        srd.unbacked,
        srd.accrued_to_treasury_scaled,
        srd.total_a_token,
        srd.total_stable_debt,
        srd.total_variable_debt,
        srd.liquidity_rate,
        srd.variable_borrow_rate,
        srd.stable_borrow_rate,
        srd.average_stable_borrow_rate,
        srd.liquidity_index,
        srd.variable_borrow_index,
        CASE WHEN srd.last_update_timestamp BETWEEN 1500000000 AND 4100000000
             THEN to_timestamp(srd.last_update_timestamp) END AS last_update_at,
        CASE WHEN srd.decimals BETWEEN 0 AND 255 THEN srd.decimals::smallint END AS decimals,
        srd.ltv,
        srd.liquidation_threshold,
        srd.liquidation_bonus,
        srd.reserve_factor,
        srd.borrowing_enabled,
        srd.stable_borrow_rate_enabled,
        srd.is_active,
        srd.is_frozen,
        srd.block_number,
        srd.block_version,
        COALESCE(srd.processing_version, -1) AS processing_version
    FROM sparklend_reserve_data srd
    ORDER BY srd.protocol_id, srd.token_id,
             srd.block_number DESC, srd.block_version DESC, COALESCE(srd.processing_version, -1) DESC
) s
WHERE c.protocol_id = s.protocol_id
  AND c.token_id    = s.token_id
  AND (c.block_number, c.block_version, c.processing_version)
   <= (s.block_number, s.block_version, s.processing_version);

-- 21 new columns' worth of stats the planner does not have yet.
ANALYZE sparklend_reserve_data_current;

INSERT INTO migrations (filename)
VALUES ('20260910_130050_backfill_sparklend_reserve_data_current_payload.sql')
ON CONFLICT (filename) DO NOTHING;
