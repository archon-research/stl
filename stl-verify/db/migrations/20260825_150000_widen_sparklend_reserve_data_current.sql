-- Widen sparklend_reserve_data_current to the full column set of a
-- sparklend_reserve_data row.
--
-- 20260820_120000_create_current_position_tables.sql created this cache carrying
-- one payload column, usage_as_collateral_enabled, because that was all the
-- backed-breakdown read needed. The next reader
-- (aave_like_liquidation_params_repository, VEC-661) needs two more, and the one
-- after that will need others. Widening one column at a time means a migration per
-- reader, each one an ALTER + a CREATE OR REPLACE + a backfill over the history
-- hypertable — so this migration takes the whole row instead. The cache is 153 rows
-- on a full-scale clone (one per protocol/token reserve ever seen), so carrying 22
-- payload columns rather than 1 costs nothing measurable in storage, write time or
-- read time, and no further reader needs a migration.
--
-- Every added column is NULL-able, for the reason the original migration gives for
-- usage_as_collateral_enabled: the source columns are all NULL-able, and the cache
-- is written by an AFTER INSERT trigger on the history table, so a NOT NULL here
-- would abort the very history insert that fires the trigger — i.e. stop ingest.
--
-- Three source columns are deliberately NOT carried:
--   * id           — a per-history-row surrogate. The cache's identity is
--                    (protocol_id, token_id); an id column would invite reading it
--                    as a pointer into history, which the newer-wins upsert does
--                    not maintain.
--   * created_at   — wall-clock time the history row was inserted. That is a fact
--                    about the history row, not about the reserve, and the cache is
--                    not an audit trail of its own writes.
--   * build_id     — audit-only ("which deployment wrote the history row"), same
--                    argument; and it is not_null in the schema_master register, so
--                    carrying it NULL-able would need a nullable_exempt entry for a
--                    column nothing reads.
--
-- Two source columns are carried under their CANONICAL name/type rather than
-- verbatim, because schema_master governs this table and the conformance check
-- keys on column name:
--   * decimals — canonical int2; the history column is numeric, a pre-register
--     retrofit that schema_master sanctions only via a declared cast transform on
--     sparklend_reserve_data itself. A table created after the register uses the
--     canonical width (same rationale as token_price_current.oracle_id in the
--     original migration). Cast under a 0..255 range test rather than bare: ERC-20
--     decimals is a uint8, so anything outside that is corrupt, and an unguarded
--     ::smallint on an out-of-range value would raise inside the trigger and abort
--     the history insert. Out of range therefore caches as NULL.
--   * last_update_at — the history column is last_update_timestamp, a Unix epoch
--     bigint whose canonical form schema_master declares as last_update_at
--     (timestamptz) with plausibility bounds 1500000000..4100000000, values outside
--     them NULLed. Those bounds are not decoration: the history column's own COMMENT
--     records that ~5.9% of its values are corrupt (some negative). The cast is
--     applied here so the cache holds the canonical value, and the guard is applied
--     with it so corruption caches as NULL rather than as a year-1969 timestamp.
--
-- The trigger function is replaced in place and the TRIGGER is NOT recreated. That
-- keeps this migration catalog-only with respect to the history hypertable: a
-- CREATE TRIGGER would take SHARE ROW EXCLUSIVE on sparklend_reserve_data and
-- propagate it to every one of its chunks, held to commit, queueing all ingest
-- behind this transaction. CREATE OR REPLACE FUNCTION touches pg_proc only.

-- Fail fast rather than convoy: the ALTER TABLE below takes ACCESS EXCLUSIVE on the
-- cache, which the /risk-capital reads query. Same rationale and value as the
-- original migration; re-run in a quieter window if it trips.
SET LOCAL lock_timeout = '10s';

-- The backfill reads the history hypertable, which has an S3 tiering policy. Without
-- this, "newest row per key" is computed over local chunks only, so a reserve whose
-- newest row has already been tiered would backfill from a stale row or none at all,
-- silently. Set explicitly rather than inherited, in either direction, exactly as the
-- original migration argues.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

ALTER TABLE sparklend_reserve_data_current
    ADD COLUMN IF NOT EXISTS unbacked                   NUMERIC,
    ADD COLUMN IF NOT EXISTS accrued_to_treasury_scaled NUMERIC,
    ADD COLUMN IF NOT EXISTS total_a_token              NUMERIC,
    ADD COLUMN IF NOT EXISTS total_stable_debt          NUMERIC,
    ADD COLUMN IF NOT EXISTS total_variable_debt        NUMERIC,
    ADD COLUMN IF NOT EXISTS liquidity_rate             NUMERIC,
    ADD COLUMN IF NOT EXISTS variable_borrow_rate       NUMERIC,
    ADD COLUMN IF NOT EXISTS stable_borrow_rate         NUMERIC,
    ADD COLUMN IF NOT EXISTS average_stable_borrow_rate NUMERIC,
    ADD COLUMN IF NOT EXISTS liquidity_index            NUMERIC,
    ADD COLUMN IF NOT EXISTS variable_borrow_index      NUMERIC,
    ADD COLUMN IF NOT EXISTS last_update_at             TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS decimals                   SMALLINT,
    ADD COLUMN IF NOT EXISTS ltv                        NUMERIC,
    ADD COLUMN IF NOT EXISTS liquidation_threshold      NUMERIC,
    ADD COLUMN IF NOT EXISTS liquidation_bonus          NUMERIC,
    ADD COLUMN IF NOT EXISTS reserve_factor             NUMERIC,
    ADD COLUMN IF NOT EXISTS borrowing_enabled          BOOLEAN,
    ADD COLUMN IF NOT EXISTS stable_borrow_rate_enabled BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_active                  BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_frozen                  BOOLEAN;

COMMENT ON TABLE sparklend_reserve_data_current IS '[Operational] Newest sparklend_reserve_data row per (protocol, token), carrying that row''s full payload. Derived cache of the sparklend_reserve_data history; rebuildable from it at any time. Not a history: it answers "what is this reserve now", never "what was it at block N".';

COMMENT ON COLUMN sparklend_reserve_data_current.unbacked IS 'Derived (copy of sparklend_reserve_data.unbacked). Raw on-chain integer in the reserve token''s native decimals. Unbacked aTokens minted against bridged liquidity.';
COMMENT ON COLUMN sparklend_reserve_data_current.accrued_to_treasury_scaled IS 'Derived (copy of sparklend_reserve_data.accrued_to_treasury_scaled). Raw on-chain integer in the reserve token''s native decimals, scaled by liquidity_index — multiply by liquidity_index/1e27 for the current amount.';
COMMENT ON COLUMN sparklend_reserve_data_current.total_a_token IS 'Derived (copy of sparklend_reserve_data.total_a_token). Raw on-chain integer in the reserve token''s native decimals. Total aToken supply = total liquidity supplied to this reserve.';
COMMENT ON COLUMN sparklend_reserve_data_current.total_stable_debt IS 'Derived (copy of sparklend_reserve_data.total_stable_debt). Raw on-chain integer in the reserve token''s native decimals. Total stable-rate debt outstanding.';
COMMENT ON COLUMN sparklend_reserve_data_current.total_variable_debt IS 'Derived (copy of sparklend_reserve_data.total_variable_debt). Raw on-chain integer in the reserve token''s native decimals. Total variable-rate debt outstanding.';
COMMENT ON COLUMN sparklend_reserve_data_current.liquidity_rate IS 'Derived (copy of sparklend_reserve_data.liquidity_rate). Ray (÷1e27). Current annual supply yield (APY).';
COMMENT ON COLUMN sparklend_reserve_data_current.variable_borrow_rate IS 'Derived (copy of sparklend_reserve_data.variable_borrow_rate). Ray (÷1e27). Current annual borrowing cost (APY).';
COMMENT ON COLUMN sparklend_reserve_data_current.stable_borrow_rate IS 'Derived (copy of sparklend_reserve_data.stable_borrow_rate). Ray (÷1e27). Current annual stable borrowing cost (APY).';
COMMENT ON COLUMN sparklend_reserve_data_current.average_stable_borrow_rate IS 'Derived (copy of sparklend_reserve_data.average_stable_borrow_rate). Ray (÷1e27). Debt-weighted average of the outstanding stable borrow rates.';
COMMENT ON COLUMN sparklend_reserve_data_current.liquidity_index IS 'Derived (copy of sparklend_reserve_data.liquidity_index). Ray (÷1e27). Cumulative interest factor since reserve creation, monotonically increasing.';
COMMENT ON COLUMN sparklend_reserve_data_current.variable_borrow_index IS 'Derived (copy of sparklend_reserve_data.variable_borrow_index). Ray (÷1e27). Cumulative variable-borrow interest factor since reserve creation, monotonically increasing.';
COMMENT ON COLUMN sparklend_reserve_data_current.last_update_at IS 'Derived (canonical cast of sparklend_reserve_data.last_update_timestamp, a Unix epoch). Protocol-reported time this reserve''s interest state was last updated — NOT the time this cache row was written. NULL when the epoch falls outside the schema_master plausibility bounds (1500000000..4100000000); ~5.9% of the history column''s values are corrupt, so a NULL here is expected.';
COMMENT ON COLUMN sparklend_reserve_data_current.decimals IS 'Derived (canonical cast of sparklend_reserve_data.decimals). Count of decimal places in the reserve token''s on-chain integer amounts — a scale, not a value. NULL when the history value falls outside the ERC-20 uint8 range 0..255.';
COMMENT ON COLUMN sparklend_reserve_data_current.ltv IS 'Derived (copy of sparklend_reserve_data.ltv). Basis points (÷10000): 7500 = 75%. Maximum loan-to-value for borrowing against this token as collateral.';
COMMENT ON COLUMN sparklend_reserve_data_current.liquidation_threshold IS 'Derived (copy of sparklend_reserve_data.liquidation_threshold). Basis points (÷10000): 8250 = 82.5%. Loan-to-value at which positions in this reserve become liquidatable.';
COMMENT ON COLUMN sparklend_reserve_data_current.liquidation_bonus IS 'Derived (copy of sparklend_reserve_data.liquidation_bonus). Basis points (÷10000) as a MULTIPLIER, not a spread: 10500 = 1.05×, i.e. a 5% liquidator bonus.';
COMMENT ON COLUMN sparklend_reserve_data_current.reserve_factor IS 'Derived (copy of sparklend_reserve_data.reserve_factor). Basis points (÷10000): 1000 = 10%. Share of borrow interest routed to the protocol treasury.';
COMMENT ON COLUMN sparklend_reserve_data_current.borrowing_enabled IS 'Derived (copy of sparklend_reserve_data.borrowing_enabled). Whether the protocol still allows borrowing this reserve. NULL when the history row left it unset.';
COMMENT ON COLUMN sparklend_reserve_data_current.stable_borrow_rate_enabled IS 'Derived (copy of sparklend_reserve_data.stable_borrow_rate_enabled). Whether stable-rate borrowing is offered for this reserve. NULL when the history row left it unset.';
COMMENT ON COLUMN sparklend_reserve_data_current.is_active IS 'Derived (copy of sparklend_reserve_data.is_active). Whether the reserve is active (a deactivated reserve blocks all interaction). NULL when the history row left it unset.';
COMMENT ON COLUMN sparklend_reserve_data_current.is_frozen IS 'Derived (copy of sparklend_reserve_data.is_frozen). Whether the reserve is frozen (existing positions live on, no new supply/borrow). NULL when the history row left it unset.';

-- Replaces the function declared in 20260820_120000_create_current_position_tables.sql,
-- extending the column lists only. The newer-wins guard and the
-- COALESCE(processing_version, -1) sentinel are unchanged and load-bearing — see that
-- migration for why -1 rather than 0, and why the flag columns stay NULL-able.
CREATE OR REPLACE FUNCTION upsert_sparklend_reserve_data_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO sparklend_reserve_data_current AS cur
        (protocol_id, token_id, usage_as_collateral_enabled,
         unbacked, accrued_to_treasury_scaled, total_a_token, total_stable_debt,
         total_variable_debt, liquidity_rate, variable_borrow_rate, stable_borrow_rate,
         average_stable_borrow_rate, liquidity_index, variable_borrow_index,
         last_update_at, decimals, ltv, liquidation_threshold, liquidation_bonus,
         reserve_factor, borrowing_enabled, stable_borrow_rate_enabled, is_active, is_frozen,
         block_number, block_version, processing_version)
    VALUES
        (NEW.protocol_id, NEW.token_id, NEW.usage_as_collateral_enabled,
         NEW.unbacked, NEW.accrued_to_treasury_scaled, NEW.total_a_token, NEW.total_stable_debt,
         NEW.total_variable_debt, NEW.liquidity_rate, NEW.variable_borrow_rate, NEW.stable_borrow_rate,
         NEW.average_stable_borrow_rate, NEW.liquidity_index, NEW.variable_borrow_index,
         CASE WHEN NEW.last_update_timestamp BETWEEN 1500000000 AND 4100000000
              THEN to_timestamp(NEW.last_update_timestamp) END,
         CASE WHEN NEW.decimals BETWEEN 0 AND 255 THEN NEW.decimals::smallint END,
         NEW.ltv, NEW.liquidation_threshold, NEW.liquidation_bonus,
         NEW.reserve_factor, NEW.borrowing_enabled, NEW.stable_borrow_rate_enabled,
         NEW.is_active, NEW.is_frozen,
         NEW.block_number, NEW.block_version, COALESCE(NEW.processing_version, -1))
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
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Backfill. This is an UPDATE, not the original migration's INSERT … ON CONFLICT
-- DO UPDATE, and the difference is not cosmetic: that statement's guard is a strict
-- `>`, and every key here already sits at its newest version (the trigger has kept
-- it there since VEC-577). Re-running it would therefore conflict on every row and
-- take the DO NOTHING path of a guard that can never be true — a silent no-op that
-- leaves all 21 new columns NULL. The guard below is `<=` so the equal case, which
-- is the only case in practice, updates.
--
-- Keys are matched on (protocol_id, token_id); the version tuple and
-- usage_as_collateral_enabled are re-asserted alongside the new columns so the
-- statement converges a row that is genuinely older, not just fills its gaps. There
-- is no INSERT arm: a key in history but absent from the cache cannot exist — the
-- trigger has fired on every insert since VEC-577 and VEC-577's own backfill covered
-- everything before it. If that ever stops holding, the original migration's INSERT
-- backfill is the repair, and it is the statement to re-run.
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
VALUES ('20260825_150000_widen_sparklend_reserve_data_current.sql')
ON CONFLICT (filename) DO NOTHING;
