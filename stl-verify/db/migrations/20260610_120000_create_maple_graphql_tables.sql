-- Maple Finance GraphQL indexer tables (VEC-320).
-- Source: https://api.maple.finance/v2/graphql (no introspection; queries validated by execution).
-- Snapshot semantics: rows are keyed by synced_at (cron cycle timestamp, UTC).
-- There is NO block_version here — GraphQL data has no reorg concept.
-- Closed/repaid loans simply stop appearing in snapshots (Active-only query);
-- absence at a given synced_at means "no longer active".
-- All numeric API values are stored as raw NUMERIC integer values:
--   principal_owed: pool-asset decimals (6 for USDC/USDT)
--   acm_ratio: 6 decimals; asset_value_usd: per-unit USD price, 8 decimals
--   APYs: 30 decimals; pool tvl/assets/principal_out: pool-asset decimals.
--
-- Compression strategy (consistent with morpho tables):
-- - Segment by entity FK columns (queries always filter by these)
-- - Order by synced_at DESC (time-series access pattern)
-- - Compress chunks older than 2 days (2x chunk_interval)
-- - Tier to S3 after 1 year (best-effort; skipped where unavailable)
--
-- Auditability follows ADR-0002: every hypertable has processing_version +
-- build_id, PK = natural key + processing_version, and a build-aware
-- advisory-locked BEFORE INSERT trigger (prefixes: mps, mls, mlc, mss, msg).

-- ============================================================================
-- Protocol seed row (MapleGlobals registry contract, Ethereum mainnet).
-- https://github.com/maple-labs/address-registry/blob/main/MapleAddressRegistryETH.md
-- ============================================================================
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (1, '\x804a6F5F667170F545Bf14e5DDB48C70B788390C'::bytea,
        'maple', 'lending', 11964925, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt FROM protocol WHERE chain_id = 1 AND name = 'maple';
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 maple protocol row, found %', cnt;
    END IF;
END $$;

-- ============================================================================
-- maple_pool: registry of PoolV2 lending pools (discovered dynamically per sync).
-- asset_* stored raw (no token FK): pools span assets we do not seed, and
-- Maple collateral assets (BTC, SOL) have no Ethereum token address at all.
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_pool
(
    id             BIGSERIAL PRIMARY KEY,
    chain_id       INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id    BIGINT      NOT NULL REFERENCES protocol (id),
    address        BYTEA       NOT NULL,               -- poolV2.id
    name           VARCHAR(255),
    asset_address  BYTEA,                              -- poolV2.asset.id
    asset_symbol   VARCHAR(50),
    asset_decimals SMALLINT,
    is_syrup       BOOLEAN     NOT NULL DEFAULT FALSE, -- poolV2.syrupRouter != null
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_maple_pool_chain ON maple_pool (chain_id);
CREATE INDEX IF NOT EXISTS idx_maple_pool_is_syrup ON maple_pool (is_syrup) WHERE is_syrup;

-- ============================================================================
-- maple_loan: registry of Open Term Loans (one row per loan contract).
-- loanMeta is present on most loans (internal and external alike) and every
-- field inside it is nullable; only loan_meta_type in ('amm', 'strategy')
-- marks an internal Maple position (see is_internal).
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_loan
(
    id                       BIGSERIAL PRIMARY KEY,
    chain_id                 INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id              BIGINT      NOT NULL REFERENCES protocol (id),
    loan_address             BYTEA       NOT NULL,    -- openTermLoan.id
    loan_type                VARCHAR(16) NOT NULL DEFAULT 'OTL',
    maple_pool_id            BIGINT      NOT NULL REFERENCES maple_pool (id),
    borrower_user_id         BIGINT      NOT NULL REFERENCES "user" (id),
    loan_meta_type           VARCHAR(32),             -- null | 'amm' | 'strategy' | 'tBills' | 'intercompany' | ... (open set)
    loan_meta_asset_symbol   VARCHAR(50),
    loan_meta_dex            VARCHAR(64),
    loan_meta_wallet_address VARCHAR(255),            -- may be non-EVM (BASE/SOL custody wallets)
    loan_meta_wallet_type    VARCHAR(32),
    loan_meta_location       VARCHAR(64),
    is_internal              BOOLEAN GENERATED ALWAYS AS
                             (COALESCE(loan_meta_type IN ('amm', 'strategy'), FALSE)) STORED,
    first_seen_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, loan_address)
);

CREATE INDEX IF NOT EXISTS idx_maple_loan_pool ON maple_loan (maple_pool_id);
CREATE INDEX IF NOT EXISTS idx_maple_loan_borrower ON maple_loan (borrower_user_id);
CREATE INDEX IF NOT EXISTS idx_maple_loan_internal ON maple_loan (is_internal) WHERE is_internal;

-- ============================================================================
-- maple_sky_strategy: registry of Sky strategies (internal pool-asset deployments).
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_sky_strategy
(
    id               BIGSERIAL PRIMARY KEY,
    chain_id         INT         NOT NULL REFERENCES chain (chain_id),
    strategy_address BYTEA       NOT NULL,            -- skyStrategy.id
    maple_pool_id    BIGINT      NOT NULL REFERENCES maple_pool (id),
    version          INT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, strategy_address)
);

CREATE INDEX IF NOT EXISTS idx_maple_sky_strategy_pool ON maple_sky_strategy (maple_pool_id);

-- ============================================================================
-- Hypertables (all partitioned on synced_at, 1-day chunks).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- maple_pool_state
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maple_pool_state
(
    maple_pool_id        BIGINT      NOT NULL REFERENCES maple_pool (id),
    synced_at            TIMESTAMPTZ NOT NULL,
    tvl                  NUMERIC,              -- nullable in the API schema
    liquid_assets        NUMERIC     NOT NULL, -- poolV2.assets
    collateral_value_usd NUMERIC,              -- nullable in the API schema
    principal_out        NUMERIC     NOT NULL,
    utilization          NUMERIC     NOT NULL, -- derived: principal_out / (liquid_assets + principal_out), 0 when empty
    monthly_apy          NUMERIC,              -- 30 decimals
    spot_apy             NUMERIC,              -- 30 decimals
    processing_version   INT         NOT NULL DEFAULT 0,
    build_id             INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_pool_id, synced_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE maple_pool_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maple_pool_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('maple_pool_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_pool_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_pool_state';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same build_id retry → reuse version (idempotent); new build_id → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_maple_pool_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mps|%s|%s', NEW.maple_pool_id, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM maple_pool_state
    WHERE maple_pool_id = NEW.maple_pool_id
      AND synced_at     = NEW.synced_at
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_pool_state
        WHERE maple_pool_id = NEW.maple_pool_id
          AND synced_at     = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_pool_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_maple_pool_state();

-- ----------------------------------------------------------------------------
-- maple_loan_state
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maple_loan_state
(
    maple_loan_id      BIGINT      NOT NULL REFERENCES maple_loan (id),
    synced_at          TIMESTAMPTZ NOT NULL,
    state              VARCHAR(32) NOT NULL, -- 'Active' (only Active queried for MVP)
    principal_owed     NUMERIC     NOT NULL,
    acm_ratio          NUMERIC,              -- 6 decimals; NULL when the API reports none (uncollateralized loans)
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_loan_id, synced_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE maple_loan_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maple_loan_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('maple_loan_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_loan_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_loan_state';
END $$;

CREATE OR REPLACE FUNCTION assign_processing_version_maple_loan_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mls|%s|%s', NEW.maple_loan_id, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM maple_loan_state
    WHERE maple_loan_id = NEW.maple_loan_id
      AND synced_at     = NEW.synced_at
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_loan_state
        WHERE maple_loan_id = NEW.maple_loan_id
          AND synced_at     = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_loan_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_maple_loan_state();

-- ----------------------------------------------------------------------------
-- maple_loan_collateral
-- One row per loan per snapshot; loans with null API collateral simply have no row.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maple_loan_collateral
(
    maple_loan_id      BIGINT      NOT NULL REFERENCES maple_loan (id),
    synced_at          TIMESTAMPTZ NOT NULL,
    asset_symbol       VARCHAR(50) NOT NULL,
    asset_amount       NUMERIC     NOT NULL, -- native decimals
    asset_decimals     SMALLINT    NOT NULL,
    asset_value_usd    NUMERIC     NOT NULL, -- per-unit price, 8 decimals
    state              VARCHAR(32),          -- 'Deposited' | 'DepositPending' | ...
    custodian          VARCHAR(64),          -- e.g. 'FORDEFI', 'ANCHORAGE'
    liquidation_level  NUMERIC,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_loan_id, synced_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE maple_loan_collateral SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maple_loan_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('maple_loan_collateral', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_loan_collateral', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_loan_collateral';
END $$;

CREATE OR REPLACE FUNCTION assign_processing_version_maple_loan_collateral()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mlc|%s|%s', NEW.maple_loan_id, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM maple_loan_collateral
    WHERE maple_loan_id = NEW.maple_loan_id
      AND synced_at     = NEW.synced_at
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_loan_collateral
        WHERE maple_loan_id = NEW.maple_loan_id
          AND synced_at     = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_loan_collateral
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_maple_loan_collateral();

-- ----------------------------------------------------------------------------
-- maple_sky_strategy_state
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maple_sky_strategy_state
(
    maple_sky_strategy_id BIGINT      NOT NULL REFERENCES maple_sky_strategy (id),
    synced_at             TIMESTAMPTZ NOT NULL,
    state                 VARCHAR(32) NOT NULL,
    currently_deployed    NUMERIC     NOT NULL,
    deposited_assets      NUMERIC     NOT NULL,
    withdrawn_assets      NUMERIC     NOT NULL,
    strategy_fee_rate     NUMERIC,
    total_fees_collected  NUMERIC,
    processing_version    INT         NOT NULL DEFAULT 0,
    build_id              INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_sky_strategy_id, synced_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE maple_sky_strategy_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maple_sky_strategy_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('maple_sky_strategy_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_sky_strategy_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_sky_strategy_state';
END $$;

CREATE OR REPLACE FUNCTION assign_processing_version_maple_sky_strategy_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mss|%s|%s', NEW.maple_sky_strategy_id, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM maple_sky_strategy_state
    WHERE maple_sky_strategy_id = NEW.maple_sky_strategy_id
      AND synced_at             = NEW.synced_at
      AND build_id              = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_sky_strategy_state
        WHERE maple_sky_strategy_id = NEW.maple_sky_strategy_id
          AND synced_at             = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_sky_strategy_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_maple_sky_strategy_state();

-- ----------------------------------------------------------------------------
-- maple_syrup_global_state
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maple_syrup_global_state
(
    chain_id           INT         NOT NULL REFERENCES chain (chain_id),
    synced_at          TIMESTAMPTZ NOT NULL,
    tvl                NUMERIC     NOT NULL,
    apy                NUMERIC     NOT NULL, -- 30 decimals
    collateral_apy     NUMERIC     NOT NULL,
    pool_apy           NUMERIC     NOT NULL,
    drips_yield_boost  NUMERIC,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (chain_id, synced_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE maple_syrup_global_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'chain_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('maple_syrup_global_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_syrup_global_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_syrup_global_state';
END $$;

CREATE OR REPLACE FUNCTION assign_processing_version_maple_syrup_global_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('msg|%s|%s', NEW.chain_id, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM maple_syrup_global_state
    WHERE chain_id  = NEW.chain_id
      AND synced_at = NEW.synced_at
      AND build_id  = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_syrup_global_state
        WHERE chain_id  = NEW.chain_id
          AND synced_at = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_syrup_global_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_maple_syrup_global_state();

INSERT INTO migrations (filename)
VALUES ('20260610_120000_create_maple_graphql_tables.sql')
ON CONFLICT (filename) DO NOTHING;
