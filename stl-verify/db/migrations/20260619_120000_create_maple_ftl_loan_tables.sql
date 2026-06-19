-- Maple Finance fixed-term loans (FTLs) for the GraphQL indexer (VEC-344).
-- Source: https://api.maple.finance/v2/graphql (no introspection; queries validated by execution).
-- Follow-up to VEC-320 (20260610_120000) which added the open-term-loan (OTL)
-- tables. FTLs are a distinct PoolV2 product with on-chain ERC-20 collateral and
-- amortization terms, so they get dedicated tables rather than reusing maple_loan.
--
-- Snapshot semantics mirror the OTL tables: rows are keyed by synced_at (cron
-- cycle timestamp, UTC). There is NO block_version — GraphQL data has no reorg
-- concept. The service queries live (non-terminal) states only, so a loan that
-- stops appearing at a given synced_at has gone terminal (matured/liquidated),
-- mirroring OTL absence=inactive.
--
-- Encoding (verified against the live API 2026-06-19, see plan
-- 2026-06-19-vec-344-ftl-indexer-plan.md). All numeric API values are stored as
-- raw NUMERIC integer values:
--   principal_owed / interest_paid / drawdown_amount / claimable_amount: funds-asset decimals
--   collateral_amount / collateral_required: collateral-asset decimals
--   interest_rate: annualized, 6 decimals on live PoolV2 loans (182000 = 18.2%).
--     V1-era loans (fundingPoolV1) encode this at 18 decimals, but they are all
--     terminal and excluded by the live-states filter, so they are never indexed.
--   collateral_ratio: 6 decimals (per docs/maple_spec.md); acm_ratio: 6 decimals.
-- Epoch-second timestamps (maturity_date, next_payment_due) are converted to
-- TIMESTAMPTZ; the API sentinel 0 (pre-funding / none due) maps to SQL NULL.
--
-- Compression strategy (consistent with the OTL tables):
-- - Segment by entity FK (maple_ftl_loan_id), order by synced_at DESC
-- - Compress chunks older than 2 days (2x chunk_interval)
-- - Tier to S3 after 1 year (best-effort; skipped where unavailable)
--
-- Auditability follows ADR-0002: the hypertable has processing_version +
-- build_id, PK = natural key + processing_version, and a build-aware
-- advisory-locked BEFORE INSERT trigger (prefix: mfls). The registry has no
-- hypertable, so no trigger.

-- ============================================================================
-- maple_ftl_loan: registry of fixed-term loans (one row per loan contract).
-- Unlike OTL collateral (off-chain custodied BTC/SOL stored raw by symbol),
-- FTL collateral and funds are mainnet ERC-20s, so both FK the token registry
-- (CLAUDE.md system-wide-registry rule). fundingPool is a PoolV2 (maple_pool);
-- the service fails hard on a null/unknown pool rather than inserting here.
-- maple_pool_id, borrower_user_id, collateral_token_id and funds_token_id are
-- immutable per loan (fundingPool / underlying asset are fixed at origination);
-- refinance-mutable terms (term_days, interest_rate, payments_remaining) live
-- in maple_ftl_loan_state.
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_ftl_loan
(
    id                  BIGSERIAL PRIMARY KEY,
    chain_id            INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id         BIGINT      NOT NULL REFERENCES protocol (id),
    loan_address        BYTEA       NOT NULL,                              -- loan.id (FTL contract)
    maple_pool_id       BIGINT      NOT NULL REFERENCES maple_pool (id),   -- fundingPool (PoolV2)
    borrower_user_id    BIGINT      NOT NULL REFERENCES "user" (id),       -- borrower.id
    collateral_token_id BIGINT      NOT NULL REFERENCES token (id),        -- collateralAsset
    funds_token_id      BIGINT      NOT NULL REFERENCES token (id),        -- liquidityAsset (fundsAsset)
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, loan_address)
);

CREATE INDEX IF NOT EXISTS idx_maple_ftl_loan_pool ON maple_ftl_loan (maple_pool_id);
CREATE INDEX IF NOT EXISTS idx_maple_ftl_loan_borrower ON maple_ftl_loan (borrower_user_id);
CREATE INDEX IF NOT EXISTS idx_maple_ftl_loan_collateral_token ON maple_ftl_loan (collateral_token_id);
CREATE INDEX IF NOT EXISTS idx_maple_ftl_loan_funds_token ON maple_ftl_loan (funds_token_id);

-- ============================================================================
-- maple_ftl_loan_state: per-cycle snapshot of a live fixed-term loan.
-- Amounts are NUMERIC NOT NULL: pre-funding states (WaitingForAcceptance,
-- DrawdownFunds) legitimately report 0, which is a valid value, not absence.
-- maturity_date / next_payment_due are nullable: the API sentinel 0 maps to
-- SQL NULL (not 1970-01-01). state_detail / acm_ratio are nullable upstream.
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_ftl_loan_state
(
    maple_ftl_loan_id     BIGINT      NOT NULL REFERENCES maple_ftl_loan (id),
    synced_at             TIMESTAMPTZ NOT NULL,
    state                 VARCHAR(32) NOT NULL,   -- LoanState enum
    state_detail          VARCHAR(32),            -- LoanStateDetail enum; NULL when the API reports none
    principal_owed        NUMERIC     NOT NULL,   -- funds-asset decimals
    interest_rate         NUMERIC     NOT NULL,   -- annualized, 6 decimals on live PoolV2 loans
    interest_paid         NUMERIC     NOT NULL,   -- funds-asset decimals
    payments_remaining    BIGINT      NOT NULL,
    payment_interval_days BIGINT      NOT NULL,
    term_days             BIGINT      NOT NULL,
    maturity_date         TIMESTAMPTZ,            -- from epoch secs; NULL when the API reports 0 (pre-funding)
    next_payment_due      TIMESTAMPTZ,            -- from epoch secs; NULL when the API reports 0 (none due)
    collateral_amount     NUMERIC     NOT NULL,   -- collateral-asset decimals
    collateral_required   NUMERIC     NOT NULL,   -- collateral-asset decimals
    collateral_ratio      NUMERIC     NOT NULL,   -- 6 decimals
    drawdown_amount       NUMERIC     NOT NULL,   -- funds-asset decimals
    claimable_amount      NUMERIC     NOT NULL,   -- funds-asset decimals
    acm_ratio             NUMERIC,                -- 6 decimals; NULL when the API reports none
    is_impaired           BOOLEAN     NOT NULL,
    processing_version    INT         NOT NULL DEFAULT 0,
    build_id              INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_ftl_loan_id, synced_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE maple_ftl_loan_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maple_ftl_loan_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('maple_ftl_loan_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_ftl_loan_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_ftl_loan_state';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same build_id retry → reuse version (idempotent); new build_id → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_maple_ftl_loan_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mfls|%s|%s', NEW.maple_ftl_loan_id, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM maple_ftl_loan_state
    WHERE maple_ftl_loan_id = NEW.maple_ftl_loan_id
      AND synced_at         = NEW.synced_at
      AND build_id          = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_ftl_loan_state
        WHERE maple_ftl_loan_id = NEW.maple_ftl_loan_id
          AND synced_at         = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_ftl_loan_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_maple_ftl_loan_state();

INSERT INTO migrations (filename)
VALUES ('20260619_120000_create_maple_ftl_loan_tables.sql')
ON CONFLICT (filename) DO NOTHING;
