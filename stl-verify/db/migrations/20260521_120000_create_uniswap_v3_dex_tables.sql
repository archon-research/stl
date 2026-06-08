-- VEC-261: Uniswap V3 DEX indexing schema.
--
-- See docs/vec-79-implementation-plan.md §5.2 + §5.2.x for design rationale.
-- Pattern follows db/migrations/20260423_214929_create_token_total_supply.sql:
--   processing_version + build_id + advisory-locked BEFORE INSERT trigger,
--   newer columnstore syntax (timescaledb.enable_columnstore + add_columnstore_policy),
--   tiering policy after 1 year. Lock prefix is unique per table (3 chars).
--
-- TWAP design choice: Uniswap V3's `observe([0])` returns cumulative values at the
-- current block. We store those cumulatives on every uniswap_v3_pool_state row
-- (tick_cumulative + secs_per_liquidity_cumulative_x128); TWAP for any window
-- [t_old, t_new] is computed at read time as a self-join differencing the two.
-- No separate observations table is needed.

-- ===========================================================================
-- Registry: indexed Uniswap V3 pools. Static after deployment.
-- Pool composition is fixed-2-token; token references are scalar columns
-- rather than parallel arrays (the Curve schema uses arrays for variable N).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool (
    id                BIGSERIAL PRIMARY KEY,
    chain_id          INT          NOT NULL REFERENCES chain (chain_id),
    address           BYTEA        NOT NULL,
    token0_id         BIGINT       NOT NULL REFERENCES token (id),
    token1_id         BIGINT       NOT NULL REFERENCES token (id),
    fee_tier          INT          NOT NULL CHECK (fee_tier IN (100, 500, 3000, 10000)),
    tick_spacing      INT          NOT NULL CHECK (tick_spacing > 0),
    label             TEXT         NOT NULL,
    deployment_block  BIGINT,
    enabled           BOOLEAN      NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address),
    CHECK (token0_id <> token1_id)
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_chain_address
    ON uniswap_v3_pool (chain_id, address);

-- Index the token FK columns (PG does not auto-index them). This is a small,
-- static registry table so the perf need is modest, but the indexes keep
-- parent-token deletes/updates cheap and match the token-FK indexing pattern
-- used by the Morpho market registry.
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_token0 ON uniswap_v3_pool (token0_id);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_token1 ON uniswap_v3_pool (token1_id);

-- ===========================================================================
-- Registry: NonfungiblePositionManager NFT positions targeting our pools.
-- One row per (chain, nfpm, token_id). Static after mint except owner, which
-- moves on ERC-721 Transfer.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_position (
    id                  BIGSERIAL PRIMARY KEY,
    chain_id            INT          NOT NULL REFERENCES chain (chain_id),
    nfpm_address        BYTEA        NOT NULL,
    token_id            NUMERIC      NOT NULL CHECK (token_id >= 0),
    uniswap_v3_pool_id  BIGINT       NOT NULL REFERENCES uniswap_v3_pool (id),
    owner               BYTEA        NOT NULL,
    -- Uniswap V3 ticks are bounded by MIN_TICK / MAX_TICK = ±887272.
    tick_lower          INT          NOT NULL CHECK (tick_lower BETWEEN -887272 AND 887272),
    tick_upper          INT          NOT NULL CHECK (tick_upper BETWEEN -887272 AND 887272),
    fee                 INT          NOT NULL CHECK (fee IN (100, 500, 3000, 10000)),
    created_at_block    BIGINT       NOT NULL CHECK (created_at_block > 0),
    burned              BOOLEAN      NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, nfpm_address, token_id),
    CHECK (tick_lower < tick_upper)
);

CREATE INDEX idx_uniswap_v3_position_pool  ON uniswap_v3_position (uniswap_v3_pool_id);
CREATE INDEX idx_uniswap_v3_position_owner ON uniswap_v3_position (owner);

-- ===========================================================================
-- Hypertable: per-block pool state. Written via the event-triggered multicall.
-- Stores Uniswap's oracle cumulatives (tick_cumulative,
-- secs_per_liquidity_cumulative_x128) — TWAP is a self-join differencing two
-- rows at chosen timestamps, no separate observation table required.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool_state (
    uniswap_v3_pool_id                 BIGINT       NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number                       BIGINT       NOT NULL,
    block_version                      INT          NOT NULL DEFAULT 0,
    timestamp                          TIMESTAMPTZ  NOT NULL,
    source                             TEXT         NOT NULL CHECK (source IN ('event', 'snapshot')),
    sqrt_price_x96                     NUMERIC      NOT NULL,
    tick                               INT          NOT NULL,
    liquidity                          NUMERIC      NOT NULL,
    observation_index                  INT,
    observation_cardinality            INT,
    observation_cardinality_next       INT,
    fee_protocol                       INT,
    unlocked                           BOOLEAN,
    tick_cumulative                    NUMERIC,
    secs_per_liquidity_cumulative_x128 NUMERIC,
    balance0                           NUMERIC,
    balance1                           NUMERIC,
    processing_version                 INT          NOT NULL DEFAULT 0,
    build_id                           INT          NOT NULL DEFAULT 0,
    created_at                         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (uniswap_v3_pool_id, block_number, block_version, processing_version, timestamp)
);

SELECT create_hypertable('uniswap_v3_pool_state', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_u3s_current
    ON uniswap_v3_pool_state (uniswap_v3_pool_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_pool_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u3s|%s|%s|%s|%s',
            NEW.uniswap_v3_pool_id,
            NEW.block_number,
            NEW.block_version,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_pool_state
    WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
      AND block_number       = NEW.block_number
      AND block_version      = NEW.block_version
      AND timestamp          = NEW.timestamp
      AND build_id           = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_pool_state
        WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
          AND block_number       = NEW.block_number
          AND block_version      = NEW.block_version
          AND timestamp          = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_pool_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_pool_state();

ALTER TABLE uniswap_v3_pool_state SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'uniswap_v3_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('uniswap_v3_pool_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_pool_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_pool_state';
END $$;

-- ---------------------------------------------------------------------------
-- Current-state companion table (last-point optimization).
--
-- "Latest state for pool X" is the dominant read, and it carries no natural
-- time bound, so against the hypertable it degrades into a last-point scan
-- across every chunk (one index probe per chunk, growing forever). This plain
-- (non-hypertable) table holds exactly one row per pool — the current state —
-- so that read becomes an O(1) primary-key lookup independent of history.
--
-- The hypertable remains the append-only source of truth; this table is a
-- mutable projection of its head. It is maintained by the AFTER INSERT trigger
-- below, NOT written directly. An upsert (ON CONFLICT DO UPDATE) is only legal
-- here because this is a regular table — TimescaleDB forbids cross-chunk
-- upserts on the hypertable itself.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uniswap_v3_pool_current (
    uniswap_v3_pool_id                 BIGINT       PRIMARY KEY REFERENCES uniswap_v3_pool (id),
    block_number                       BIGINT       NOT NULL,
    block_version                      INT          NOT NULL,
    timestamp                          TIMESTAMPTZ  NOT NULL,
    source                             TEXT         NOT NULL,
    sqrt_price_x96                     NUMERIC      NOT NULL,
    tick                               INT          NOT NULL,
    liquidity                          NUMERIC      NOT NULL,
    observation_index                  INT,
    observation_cardinality            INT,
    observation_cardinality_next       INT,
    fee_protocol                       INT,
    unlocked                           BOOLEAN,
    tick_cumulative                    NUMERIC,
    secs_per_liquidity_cumulative_x128 NUMERIC,
    balance0                           NUMERIC,
    balance1                           NUMERIC,
    processing_version                 INT          NOT NULL,
    build_id                           INT          NOT NULL,
    updated_at                         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Projects each newly inserted hypertable row onto uniswap_v3_pool_current, but
-- only when it is strictly newer than the stored head. "Newer" is the same
-- total order idx_u3s_current sorts by: (block_number, block_version,
-- processing_version) descending. The guard makes the projection correct under
-- out-of-order arrival, SQS replays, and reorgs, and keeps the result identical
-- to the equivalent ORDER BY ... DESC LIMIT 1 over the hypertable.
--
-- timestamp is a defensive final tiebreaker. Block timestamp is derived from
-- block_number and this design writes no periodic 'snapshot' rows, so
-- (block_number, block_version, processing_version) is already unique per pool
-- and the leg never changes behaviour today. If a 'snapshot' row with a
-- read-time timestamp is ever added at the same (block, version), the latest
-- timestamp then wins deterministically instead of first-writer-wins.
CREATE OR REPLACE FUNCTION refresh_uniswap_v3_pool_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO uniswap_v3_pool_current AS c (
        uniswap_v3_pool_id, block_number, block_version, timestamp, source,
        sqrt_price_x96, tick, liquidity, observation_index, observation_cardinality,
        observation_cardinality_next, fee_protocol, unlocked, tick_cumulative,
        secs_per_liquidity_cumulative_x128, balance0, balance1,
        processing_version, build_id, updated_at)
    VALUES (
        NEW.uniswap_v3_pool_id, NEW.block_number, NEW.block_version, NEW.timestamp, NEW.source,
        NEW.sqrt_price_x96, NEW.tick, NEW.liquidity, NEW.observation_index, NEW.observation_cardinality,
        NEW.observation_cardinality_next, NEW.fee_protocol, NEW.unlocked, NEW.tick_cumulative,
        NEW.secs_per_liquidity_cumulative_x128, NEW.balance0, NEW.balance1,
        NEW.processing_version, NEW.build_id, NOW())
    ON CONFLICT (uniswap_v3_pool_id) DO UPDATE SET
        block_number                       = EXCLUDED.block_number,
        block_version                      = EXCLUDED.block_version,
        timestamp                          = EXCLUDED.timestamp,
        source                             = EXCLUDED.source,
        sqrt_price_x96                     = EXCLUDED.sqrt_price_x96,
        tick                               = EXCLUDED.tick,
        liquidity                          = EXCLUDED.liquidity,
        observation_index                  = EXCLUDED.observation_index,
        observation_cardinality            = EXCLUDED.observation_cardinality,
        observation_cardinality_next       = EXCLUDED.observation_cardinality_next,
        fee_protocol                       = EXCLUDED.fee_protocol,
        unlocked                           = EXCLUDED.unlocked,
        tick_cumulative                    = EXCLUDED.tick_cumulative,
        secs_per_liquidity_cumulative_x128 = EXCLUDED.secs_per_liquidity_cumulative_x128,
        balance0                           = EXCLUDED.balance0,
        balance1                           = EXCLUDED.balance1,
        processing_version                 = EXCLUDED.processing_version,
        build_id                           = EXCLUDED.build_id,
        updated_at                         = NOW()
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.timestamp)
        > (c.block_number, c.block_version, c.processing_version, c.timestamp);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_uniswap_v3_pool_current
    AFTER INSERT ON uniswap_v3_pool_state
    FOR EACH ROW
EXECUTE FUNCTION refresh_uniswap_v3_pool_current();

-- ===========================================================================
-- Hypertable: per-Swap fact rows. amount0/amount1 are signed deltas from the
-- pool's perspective.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool_swap (
    uniswap_v3_pool_id   BIGINT       NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number         BIGINT       NOT NULL,
    block_version        INT          NOT NULL DEFAULT 0,
    timestamp            TIMESTAMPTZ  NOT NULL,
    tx_hash              BYTEA        NOT NULL,
    log_index            INT          NOT NULL,
    sender               BYTEA        NOT NULL,
    recipient            BYTEA        NOT NULL,
    amount0              NUMERIC      NOT NULL,
    amount1              NUMERIC      NOT NULL,
    sqrt_price_x96_after NUMERIC      NOT NULL,
    liquidity_after      NUMERIC      NOT NULL,
    tick_after           INT          NOT NULL,
    processing_version   INT          NOT NULL DEFAULT 0,
    build_id             INT          NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (uniswap_v3_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('uniswap_v3_pool_swap', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_uniswap_v3_pool_swap_pool_time
    ON uniswap_v3_pool_swap (uniswap_v3_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_pool_swap()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u3w|%s|%s|%s|%s|%s|%s',
            NEW.uniswap_v3_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_pool_swap
    WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
      AND block_number       = NEW.block_number
      AND block_version      = NEW.block_version
      AND tx_hash            = NEW.tx_hash
      AND log_index          = NEW.log_index
      AND timestamp          = NEW.timestamp
      AND build_id           = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_pool_swap
        WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
          AND block_number       = NEW.block_number
          AND block_version      = NEW.block_version
          AND tx_hash            = NEW.tx_hash
          AND log_index          = NEW.log_index
          AND timestamp          = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_pool_swap
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_pool_swap();

ALTER TABLE uniswap_v3_pool_swap SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'uniswap_v3_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('uniswap_v3_pool_swap', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_pool_swap', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_pool_swap';
END $$;

-- ===========================================================================
-- Hypertable: per-position liquidity + unclaimed fees over time.
-- Written on NFPM IncreaseLiquidity / DecreaseLiquidity / Collect and via
-- a positions(tokenId) multicall on each.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_position_state (
    uniswap_v3_position_id       BIGINT       NOT NULL REFERENCES uniswap_v3_position (id),
    block_number                 BIGINT       NOT NULL,
    block_version                INT          NOT NULL DEFAULT 0,
    timestamp                    TIMESTAMPTZ  NOT NULL,
    source                       TEXT         NOT NULL CHECK (source IN ('event', 'snapshot')),
    liquidity                    NUMERIC      NOT NULL,
    tokens_owed0                 NUMERIC      NOT NULL,
    tokens_owed1                 NUMERIC      NOT NULL,
    fee_growth_inside0_last_x128 NUMERIC      NOT NULL,
    fee_growth_inside1_last_x128 NUMERIC      NOT NULL,
    processing_version           INT          NOT NULL DEFAULT 0,
    build_id                     INT          NOT NULL DEFAULT 0,
    created_at                   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (uniswap_v3_position_id, block_number, block_version, processing_version, timestamp)
);

SELECT create_hypertable('uniswap_v3_position_state', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_u3i_current
    ON uniswap_v3_position_state (uniswap_v3_position_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_position_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u3i|%s|%s|%s|%s',
            NEW.uniswap_v3_position_id,
            NEW.block_number,
            NEW.block_version,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_position_state
    WHERE uniswap_v3_position_id = NEW.uniswap_v3_position_id
      AND block_number           = NEW.block_number
      AND block_version          = NEW.block_version
      AND timestamp              = NEW.timestamp
      AND build_id               = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_position_state
        WHERE uniswap_v3_position_id = NEW.uniswap_v3_position_id
          AND block_number           = NEW.block_number
          AND block_version          = NEW.block_version
          AND timestamp              = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_position_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_position_state();

ALTER TABLE uniswap_v3_position_state SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'uniswap_v3_position_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('uniswap_v3_position_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_position_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_position_state';
END $$;

-- ===========================================================================
-- Hypertable: pool-side Mint/Burn/Collect fact rows. Distinct from NFPM-side
-- events because the pool's own ticks change here; NFPM events update the
-- per-position state instead.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool_liquidity_event (
    uniswap_v3_pool_id BIGINT       NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    tx_hash            BYTEA        NOT NULL,
    log_index          INT          NOT NULL,
    event_kind         TEXT         NOT NULL CHECK (event_kind IN ('Mint', 'Burn', 'Collect')),
    owner              BYTEA        NOT NULL,
    tick_lower         INT          NOT NULL,
    tick_upper         INT          NOT NULL,
    amount             NUMERIC,
    amount0            NUMERIC,
    amount1            NUMERIC,
    sender             BYTEA,
    recipient          BYTEA,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (uniswap_v3_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp),
    CHECK (tick_lower < tick_upper),
    -- amount0/amount1 are always emitted by Mint/Burn/Collect — enforce NOT NULL.
    -- Only Mint carries a `sender` (the contract that called mint).
    -- Only Collect carries a `recipient`.
    -- `amount` (liquidity delta) is present on Mint and Burn but not on Collect.
    CHECK (
        (event_kind = 'Mint'
            AND sender    IS NOT NULL AND amount  IS NOT NULL
            AND amount0   IS NOT NULL AND amount1 IS NOT NULL)
     OR (event_kind = 'Burn'
            AND sender    IS NULL     AND amount  IS NOT NULL
            AND amount0   IS NOT NULL AND amount1 IS NOT NULL)
     OR (event_kind = 'Collect'
            AND recipient IS NOT NULL AND amount  IS NULL
            AND amount0   IS NOT NULL AND amount1 IS NOT NULL)
    )
);

SELECT create_hypertable('uniswap_v3_pool_liquidity_event', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_uniswap_v3_pool_liquidity_event_pool_time
    ON uniswap_v3_pool_liquidity_event (uniswap_v3_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_pool_liquidity_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u3l|%s|%s|%s|%s|%s|%s',
            NEW.uniswap_v3_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_pool_liquidity_event
    WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
      AND block_number       = NEW.block_number
      AND block_version      = NEW.block_version
      AND tx_hash            = NEW.tx_hash
      AND log_index          = NEW.log_index
      AND timestamp          = NEW.timestamp
      AND build_id           = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_pool_liquidity_event
        WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
          AND block_number       = NEW.block_number
          AND block_version      = NEW.block_version
          AND tx_hash            = NEW.tx_hash
          AND log_index          = NEW.log_index
          AND timestamp          = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_pool_liquidity_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_pool_liquidity_event();

ALTER TABLE uniswap_v3_pool_liquidity_event SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'uniswap_v3_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('uniswap_v3_pool_liquidity_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_pool_liquidity_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_pool_liquidity_event';
END $$;

-- ===========================================================================
-- Hypertable: Initialize / IncreaseObservationCardinalityNext / SetFeeProtocol /
-- CollectProtocol events. Wider columnstore lag (14 days) — months-rare events.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS uniswap_v3_pool_parameter_event (
    uniswap_v3_pool_id           BIGINT       NOT NULL REFERENCES uniswap_v3_pool (id),
    block_number                 BIGINT       NOT NULL,
    block_version                INT          NOT NULL DEFAULT 0,
    timestamp                    TIMESTAMPTZ  NOT NULL,
    tx_hash                      BYTEA        NOT NULL,
    log_index                    INT          NOT NULL,
    event_kind                   TEXT         NOT NULL CHECK (event_kind IN (
        'Initialize',
        'IncreaseObservationCardinalityNext',
        'SetFeeProtocol',
        'CollectProtocol'
    )),
    sqrt_price_x96               NUMERIC,
    tick                         INT,
    observation_cardinality_old  INT,
    observation_cardinality_new  INT,
    fee_protocol0_old            INT,
    fee_protocol0_new            INT,
    fee_protocol1_old            INT,
    fee_protocol1_new            INT,
    amount0                      NUMERIC,
    amount1                      NUMERIC,
    sender                       BYTEA,
    recipient                    BYTEA,
    extra                        JSONB,
    processing_version           INT          NOT NULL DEFAULT 0,
    build_id                     INT          NOT NULL DEFAULT 0,
    created_at                   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (uniswap_v3_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('uniswap_v3_pool_parameter_event', by_range('timestamp', INTERVAL '7 days'));

CREATE INDEX idx_uniswap_v3_pool_parameter_event_pool_time
    ON uniswap_v3_pool_parameter_event (uniswap_v3_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v3_pool_parameter_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u3p|%s|%s|%s|%s|%s|%s',
            NEW.uniswap_v3_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v3_pool_parameter_event
    WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
      AND block_number       = NEW.block_number
      AND block_version      = NEW.block_version
      AND tx_hash            = NEW.tx_hash
      AND log_index          = NEW.log_index
      AND timestamp          = NEW.timestamp
      AND build_id           = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v3_pool_parameter_event
        WHERE uniswap_v3_pool_id = NEW.uniswap_v3_pool_id
          AND block_number       = NEW.block_number
          AND block_version      = NEW.block_version
          AND tx_hash            = NEW.tx_hash
          AND log_index          = NEW.log_index
          AND timestamp          = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v3_pool_parameter_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v3_pool_parameter_event();

ALTER TABLE uniswap_v3_pool_parameter_event SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'uniswap_v3_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('uniswap_v3_pool_parameter_event', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v3_pool_parameter_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v3_pool_parameter_event';
END $$;

-- ===========================================================================
-- GRANTs
-- ===========================================================================
GRANT SELECT ON uniswap_v3_pool, uniswap_v3_position, uniswap_v3_pool_state,
                uniswap_v3_pool_swap, uniswap_v3_position_state,
                uniswap_v3_pool_liquidity_event, uniswap_v3_pool_parameter_event
    TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON uniswap_v3_pool, uniswap_v3_position, uniswap_v3_pool_state,
                uniswap_v3_pool_swap, uniswap_v3_position_state,
                uniswap_v3_pool_liquidity_event, uniswap_v3_pool_parameter_event
    TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE uniswap_v3_pool_id_seq     TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE uniswap_v3_position_id_seq TO stl_readwrite;

-- ===========================================================================
-- Seed: indexed Uniswap V3 wstETH/WETH pools on Ethereum mainnet.
-- Pool addresses verified live on 2026-05-20. token_id 5 = wstETH, 4 = WETH.
-- 1.00% fee tier is not deployed for this pair (factory.getPool → 0x0), so
-- only the three deployed fee tiers are seeded.
--
-- deployment_block is left NULL: the three pools were created at different
-- blocks (the 1bp fee tier was governance-enabled in late 2021, so its pool
-- post-dates the 5bp/30bp pools by months) and no authoritative value was on
-- hand at seed time. The worker can backfill these via factory PoolCreated
-- event lookups; the column is metadata only and is not in any PK or read path.
-- ===========================================================================
INSERT INTO uniswap_v3_pool (chain_id, address, token0_id, token1_id, fee_tier, tick_spacing, label)
VALUES
    (1, '\x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa'::bytea, 5, 4,   100,  1, 'wstETH-WETH-001'),
    (1, '\xD340B57AAcDD10F96FC1CF10e15921936F41E29c'::bytea, 5, 4,   500, 10, 'wstETH-WETH-005'),
    (1, '\xC12aF0C4AA39D3061c56cD3CB19f5e62dEeaeBdE'::bytea, 5, 4,  3000, 60, 'wstETH-WETH-030')
ON CONFLICT (chain_id, address) DO NOTHING;

-- ===========================================================================
-- Post-seed assertion: token0_id and token1_id must resolve to existing token
-- rows on the matching chain_id. Mirrors the Curve schema migration check.
-- ===========================================================================
DO $$
DECLARE missing_count INT;
BEGIN
    SELECT count(*) INTO missing_count
    FROM uniswap_v3_pool p
    LEFT JOIN token t0 ON t0.id = p.token0_id AND t0.chain_id = p.chain_id
    LEFT JOIN token t1 ON t1.id = p.token1_id AND t1.chain_id = p.chain_id
    WHERE t0.id IS NULL OR t1.id IS NULL;
    IF missing_count > 0 THEN
        RAISE EXCEPTION 'uniswap_v3_pool seed references % row(s) with missing token id(s)', missing_count;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260521_120000_create_uniswap_v3_dex_tables.sql')
ON CONFLICT (filename) DO NOTHING;
