-- Uniswap V3 indexer tables (VEC-261).
-- Creates the registry (uniswap_v3_pool) and 5 fact/state tables
-- (uniswap_v3_pool_state, uniswap_v3_swap, uniswap_v3_liquidity_event,
-- uniswap_v3_tick, uniswap_v3_pool_event) with full auditability (ADR-0002).
--
-- Prerequisites (already applied):
--   20260521_100000_create_dex_prereqs.sql  -- UniswapV3 protocol row
--
-- Mirrors the trigger/hypertable/compression boilerplate from
-- 20260521_110000_create_curve_dex_tables.sql. COMMENT ON statements and the
-- pool/token seed are separate follow-up migrations (VEC-261 tasks B2/B3).

-- ============================================================================
-- uniswap_v3_pool: registry of Uniswap V3 pools (one row per deployed pool).
-- Migration-seeded and read-only at runtime: the indexer only LoadPools()s this
-- table, there is no application write path. Do not add an UPDATE/DELETE path
-- (or an ON CONFLICT DO UPDATE upsert) without a deliberate design decision.
-- ============================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool
(
    id                     BIGSERIAL PRIMARY KEY,
    chain_id               INT     NOT NULL REFERENCES chain (chain_id),
    protocol_id            BIGINT  NOT NULL REFERENCES protocol (id),
    pool_address           BYTEA   NOT NULL,
    token0_id              BIGINT  NOT NULL REFERENCES token (id),
    token1_id              BIGINT  NOT NULL REFERENCES token (id),
    fee                    INT     NOT NULL,
    tick_spacing           INT     NOT NULL,
    max_liquidity_per_tick NUMERIC NOT NULL,
    deploy_block           BIGINT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, pool_address)
);

-- chain_id lookups (LoadPools) are served by the UNIQUE (chain_id, pool_address)
-- index, so no separate chain_id index is needed.
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_protocol ON uniswap_v3_pool (protocol_id);

-- ============================================================================
-- uniswap_v3_pool_state: periodic per-touched-block snapshot of pool slot0 /
-- liquidity / fee-growth / protocol-fee / balance state.
-- Hypertable partitioned on block_timestamp (1-day chunks), same as Curve.
-- ============================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool_state
(
    pool_id                          BIGINT      NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number                     BIGINT      NOT NULL,
    block_version                    INT         NOT NULL DEFAULT 0,
    block_timestamp                  TIMESTAMPTZ NOT NULL,
    sqrt_price_x96                   NUMERIC     NOT NULL,
    tick                             INT         NOT NULL,
    observation_index                INT         NOT NULL,
    observation_cardinality          INT         NOT NULL,
    observation_cardinality_next     INT         NOT NULL,
    fee_protocol                     SMALLINT    NOT NULL,
    unlocked                         BOOLEAN     NOT NULL,
    liquidity                        NUMERIC     NOT NULL,
    fee_growth_global0_x128          NUMERIC     NOT NULL,
    fee_growth_global1_x128          NUMERIC     NOT NULL,
    protocol_fees_token0             NUMERIC     NOT NULL,
    protocol_fees_token1             NUMERIC     NOT NULL,
    balance0                         NUMERIC     NOT NULL,
    balance1                         NUMERIC     NOT NULL,
    twap_tick                        INT,
    twap_window_secs                 INT,
    processing_version               INT         NOT NULL DEFAULT 0,
    build_id                         INT         NOT NULL DEFAULT 0,
    -- block_timestamp must be in the PK: TimescaleDB requires the partition
    -- column in every unique index on a hypertable.
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE uniswap_v3_pool_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v3_pool_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_pool_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_pool_state';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_state_pv_lookup
    ON uniswap_v3_pool_state (pool_id, block_number, block_version, build_id);

-- Prefix 'uvps' for uniswap_v3_pool_state.
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_pool_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('uvps|%s|%s|%s', NEW.pool_id, NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_pool_state
    WHERE pool_id       = NEW.pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_pool_state
        WHERE pool_id       = NEW.pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_pool_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_pool_state();

-- ============================================================================
-- uniswap_v3_swap: on-chain Swap events.
-- Hypertable partitioned on block_timestamp (1-day chunks), same as Curve.
-- ============================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_swap
(
    pool_id             BIGINT      NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL DEFAULT 0,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    tx_hash             BYTEA       NOT NULL,
    log_index           INT         NOT NULL,
    sender              BYTEA       NOT NULL,
    recipient           BYTEA       NOT NULL,
    amount0             NUMERIC     NOT NULL,
    amount1             NUMERIC     NOT NULL,
    sqrt_price_x96      NUMERIC     NOT NULL,
    liquidity           NUMERIC     NOT NULL,
    tick                INT         NOT NULL,
    processing_version  INT         NOT NULL DEFAULT 0,
    build_id            INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE uniswap_v3_swap SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v3_swap', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_swap', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_swap';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_swap_pv_lookup
    ON uniswap_v3_swap (pool_id, block_number, block_version, log_index, build_id);

-- Prefix 'uvs' for uniswap_v3_swap.
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_swap()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('uvs|%s|%s|%s|%s', NEW.pool_id, NEW.block_number, NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_swap
    WHERE pool_id       = NEW.pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND log_index     = NEW.log_index
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_swap
        WHERE pool_id       = NEW.pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND log_index     = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_swap
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_swap();

-- ============================================================================
-- uniswap_v3_liquidity_event: Mint / Burn / Collect events.
-- Hypertable partitioned on block_timestamp (1-day chunks), same as Curve.
-- ============================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_liquidity_event
(
    pool_id             BIGINT      NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL DEFAULT 0,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    tx_hash             BYTEA       NOT NULL,
    log_index           INT         NOT NULL,
    event_name          VARCHAR(20) NOT NULL CHECK (event_name IN ('mint', 'burn', 'collect')),
    owner               BYTEA       NOT NULL,
    sender              BYTEA,
    recipient           BYTEA,
    tick_lower          INT         NOT NULL,
    tick_upper          INT         NOT NULL,
    amount              NUMERIC,
    amount0             NUMERIC     NOT NULL,
    amount1             NUMERIC     NOT NULL,
    processing_version  INT         NOT NULL DEFAULT 0,
    build_id            INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE uniswap_v3_liquidity_event SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v3_liquidity_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_liquidity_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_liquidity_event';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_liquidity_event_pv_lookup
    ON uniswap_v3_liquidity_event (pool_id, block_number, block_version, log_index, build_id);

-- Prefix 'uvle' for uniswap_v3_liquidity_event.
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_liquidity_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('uvle|%s|%s|%s|%s', NEW.pool_id, NEW.block_number, NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_liquidity_event
    WHERE pool_id       = NEW.pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND log_index     = NEW.log_index
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_liquidity_event
        WHERE pool_id       = NEW.pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND log_index     = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_liquidity_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_liquidity_event();

-- ============================================================================
-- uniswap_v3_tick: append-on-change authoritative per-tick state (liquidity
-- gross/net, fee-growth-outside, initialized). Regular table (NOT a
-- hypertable): ticks are written on-change, not on every touched block.
-- ============================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_tick
(
    pool_id                    BIGINT      NOT NULL REFERENCES uniswap_v3_pool (id),
    tick                       INT         NOT NULL,
    block_number               BIGINT      NOT NULL,
    block_version              INT         NOT NULL DEFAULT 0,
    block_timestamp            TIMESTAMPTZ NOT NULL,
    liquidity_gross            NUMERIC     NOT NULL,
    liquidity_net              NUMERIC     NOT NULL,
    fee_growth_outside0_x128   NUMERIC     NOT NULL,
    fee_growth_outside1_x128   NUMERIC     NOT NULL,
    initialized                BOOLEAN     NOT NULL,
    processing_version         INT         NOT NULL DEFAULT 0,
    build_id                   INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, tick, block_number, block_version, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_tick_pv_lookup
    ON uniswap_v3_tick (pool_id, tick, block_number, block_version, build_id);

-- Prefix 'uvt' for uniswap_v3_tick.
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_tick()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('uvt|%s|%s|%s|%s', NEW.pool_id, NEW.tick, NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_tick
    WHERE pool_id       = NEW.pool_id
      AND tick          = NEW.tick
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_tick
        WHERE pool_id       = NEW.pool_id
          AND tick          = NEW.tick
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_tick
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_tick();

-- ============================================================================
-- uniswap_v3_pool_event: typed low-frequency pool events (Initialize, Flash,
-- SetFeeProtocol, CollectProtocol, IncreaseObservationCardinalityNext).
-- Hypertable partitioned on block_timestamp (1-day chunks), same as Curve's
-- curve_parameter_event.
-- ============================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool_event
(
    pool_id             BIGINT      NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL DEFAULT 0,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    tx_hash             BYTEA       NOT NULL,
    log_index           INT         NOT NULL,
    event_name          VARCHAR(40) NOT NULL CHECK (event_name IN (
        'initialize', 'flash', 'set_fee_protocol', 'collect_protocol',
        'increase_observation_cardinality_next'
    )),
    params              JSONB       NOT NULL,
    processing_version  INT         NOT NULL DEFAULT 0,
    build_id            INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE uniswap_v3_pool_event SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v3_pool_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_pool_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_pool_event';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_event_pv_lookup
    ON uniswap_v3_pool_event (pool_id, block_number, block_version, log_index, build_id);

-- Prefix 'uvpe' for uniswap_v3_pool_event.
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_pool_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('uvpe|%s|%s|%s|%s', NEW.pool_id, NEW.block_number, NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_pool_event
    WHERE pool_id       = NEW.pool_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND log_index     = NEW.log_index
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_pool_event
        WHERE pool_id       = NEW.pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND log_index     = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_pool_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_pool_event();

INSERT INTO migrations (filename)
VALUES ('20260701_100000_create_uniswap_v3_tables.sql')
ON CONFLICT (filename) DO NOTHING;
