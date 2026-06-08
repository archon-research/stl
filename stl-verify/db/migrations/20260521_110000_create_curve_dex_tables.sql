-- VEC-260: Curve DEX indexing schema.
--
-- See docs/vec-79-implementation-plan.md §5.1 + §5.1.x for design rationale.
-- Pattern follows db/migrations/20260423_214929_create_token_total_supply.sql:
--   - One hypertable per fact stream (pool state, swap, liquidity event, ...).
--   - Every hypertable: processing_version + build_id + advisory-locked BEFORE INSERT
--     trigger so SQS replays are idempotent within a build and reprocesses across
--     builds append a new version. Lock key uses a 3-letter table prefix and the
--     natural identity columns (TIMESTAMPTZ wrapped via EXTRACT(epoch FROM ...)).
--   - Columnstore policy after 2 days, tiering policy after 1 year.

-- ===========================================================================
-- Registry: indexed Curve pools. Static after deployment.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_pool (
    id                BIGSERIAL PRIMARY KEY,
    chain_id          INT          NOT NULL REFERENCES chain (chain_id),
    pool_kind         TEXT         NOT NULL CHECK (pool_kind IN ('v1', 'ng')),
    address           BYTEA        NOT NULL,
    -- NULL ⇒ LP token == pool.address (true for factory-NG pools only).
    -- Pre-factory pools (3pool, stETH-classic) have a separate LP contract.
    lp_token_address  BYTEA,
    label             TEXT         NOT NULL,
    n_coins           SMALLINT     NOT NULL CHECK (n_coins BETWEEN 2 AND 8),
    coin_addresses    BYTEA[]      NOT NULL,
    coin_token_ids    BIGINT[]     NOT NULL,
    coin_decimals     SMALLINT[]   NOT NULL,
    deployment_block  BIGINT,
    enabled           BOOLEAN      NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address),
    CHECK (cardinality(coin_addresses) = n_coins
       AND cardinality(coin_token_ids) = n_coins
       AND cardinality(coin_decimals) = n_coins)
);

CREATE INDEX IF NOT EXISTS idx_curve_pool_chain_address ON curve_pool (chain_id, address);

-- ===========================================================================
-- Registry: liquidity gauges for indexed pools. Discovered event-driven from
-- GaugeController NewGauge; bootstrapped at startup via MetaRegistry.get_gauge.
--
-- UNIQUE(curve_pool_id) enforces at most one gauge per pool, matching the
-- Curve product reality. If Curve ever migrates a pool to a replacement gauge
-- (kill old, deploy new) the migration path is: UPDATE existing row to point
-- at the new address and set is_killed=false on the new gauge, then insert a
-- historical row in a follow-up gauge-history table if we need both for audit.
-- That follow-up table is out of scope here.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_gauge (
    id                BIGSERIAL PRIMARY KEY,
    curve_pool_id     BIGINT       NOT NULL REFERENCES curve_pool (id),
    chain_id          INT          NOT NULL REFERENCES chain (chain_id),
    address           BYTEA        NOT NULL,
    deployment_block  BIGINT,
    is_killed         BOOLEAN      NOT NULL DEFAULT false,
    has_no_crv        BOOLEAN      NOT NULL DEFAULT false,
    enabled           BOOLEAN      NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address),
    UNIQUE (curve_pool_id)
);

-- ===========================================================================
-- Hypertable: pool state (balances, virtual_price, A, fee, oracles).
-- Written via the event-triggered multicall in the worker.
--
-- `source` is intentionally NOT part of the PK: in steady state every state row
-- comes from the 'event' path (no separate snapshot loop per plan §7). The
-- 'snapshot' value exists only for one-time bootstrap reads (e.g., at first
-- worker startup); it should not race with 'event' writes for the same block.
-- If both paths ever produce a row for the same (pool, block, version, timestamp,
-- build_id), the BEFORE INSERT trigger dedupes them to the same processing_version
-- and the second writer hits a PK violation that the worker treats as a duplicate.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_pool_state (
    curve_pool_id      BIGINT       NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    source             TEXT         NOT NULL CHECK (source IN ('event', 'snapshot')),
    balances           NUMERIC[]    NOT NULL,
    total_supply       NUMERIC,
    virtual_price      NUMERIC,
    a_factor           NUMERIC,
    fee                NUMERIC,
    price_oracle       NUMERIC[],
    last_price         NUMERIC[],
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (curve_pool_id, block_number, block_version, processing_version, timestamp)
);

SELECT create_hypertable('curve_pool_state', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_cps_current
    ON curve_pool_state (curve_pool_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_curve_pool_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cps|%s|%s|%s|%s',
            NEW.curve_pool_id,
            NEW.block_number,
            NEW.block_version,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM curve_pool_state
    WHERE curve_pool_id  = NEW.curve_pool_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_pool_state
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp     = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_pool_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_pool_state();

ALTER TABLE curve_pool_state SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'curve_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('curve_pool_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_pool_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_pool_state';
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
-- mutable projection of its head, mirroring the registry/history split already
-- used by curve_pool + curve_pool_state. It is maintained by the AFTER INSERT
-- trigger below, NOT written directly. Note: an upsert (ON CONFLICT DO UPDATE)
-- is only legal here because this is a regular table — TimescaleDB forbids
-- cross-chunk upserts on the hypertable itself.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curve_pool_current (
    curve_pool_id      BIGINT       PRIMARY KEY REFERENCES curve_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL,
    timestamp          TIMESTAMPTZ  NOT NULL,
    source             TEXT         NOT NULL,
    balances           NUMERIC[]    NOT NULL,
    total_supply       NUMERIC,
    virtual_price      NUMERIC,
    a_factor           NUMERIC,
    fee                NUMERIC,
    price_oracle       NUMERIC[],
    last_price         NUMERIC[],
    processing_version INT          NOT NULL,
    build_id           INT          NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Projects each newly inserted hypertable row onto curve_pool_current, but only
-- when it is strictly newer than the stored head. "Newer" is the same total
-- order idx_cps_current sorts by: (block_number, block_version,
-- processing_version) descending. The guard makes the projection correct under
-- out-of-order arrival, SQS replays, and reorgs (a higher block_version for the
-- same block wins), and keeps the result identical to the equivalent
-- ORDER BY ... DESC LIMIT 1 over the hypertable.
CREATE OR REPLACE FUNCTION refresh_curve_pool_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO curve_pool_current AS c (
        curve_pool_id, block_number, block_version, timestamp, source,
        balances, total_supply, virtual_price, a_factor, fee,
        price_oracle, last_price, processing_version, build_id, updated_at)
    VALUES (
        NEW.curve_pool_id, NEW.block_number, NEW.block_version, NEW.timestamp, NEW.source,
        NEW.balances, NEW.total_supply, NEW.virtual_price, NEW.a_factor, NEW.fee,
        NEW.price_oracle, NEW.last_price, NEW.processing_version, NEW.build_id, NOW())
    ON CONFLICT (curve_pool_id) DO UPDATE SET
        block_number       = EXCLUDED.block_number,
        block_version      = EXCLUDED.block_version,
        timestamp          = EXCLUDED.timestamp,
        source             = EXCLUDED.source,
        balances           = EXCLUDED.balances,
        total_supply       = EXCLUDED.total_supply,
        virtual_price      = EXCLUDED.virtual_price,
        a_factor           = EXCLUDED.a_factor,
        fee                = EXCLUDED.fee,
        price_oracle       = EXCLUDED.price_oracle,
        last_price         = EXCLUDED.last_price,
        processing_version = EXCLUDED.processing_version,
        build_id           = EXCLUDED.build_id,
        updated_at         = NOW()
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (c.block_number, c.block_version, c.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_curve_pool_current
    AFTER INSERT ON curve_pool_state
    FOR EACH ROW
EXECUTE FUNCTION refresh_curve_pool_current();

-- ===========================================================================
-- Hypertable: per-Swap fact rows (TokenExchange events).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_pool_swap (
    curve_pool_id      BIGINT       NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    tx_hash            BYTEA        NOT NULL,
    log_index          INT          NOT NULL,
    buyer              BYTEA        NOT NULL,
    sold_id            SMALLINT     NOT NULL,
    tokens_sold        NUMERIC      NOT NULL,
    bought_id          SMALLINT     NOT NULL,
    tokens_bought      NUMERIC      NOT NULL,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (curve_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('curve_pool_swap', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_curve_pool_swap_pool_time
    ON curve_pool_swap (curve_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_curve_pool_swap()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cpw|%s|%s|%s|%s|%s|%s',
            NEW.curve_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM curve_pool_swap
    WHERE curve_pool_id  = NEW.curve_pool_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND tx_hash        = NEW.tx_hash
      AND log_index      = NEW.log_index
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_pool_swap
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND tx_hash       = NEW.tx_hash
          AND log_index     = NEW.log_index
          AND timestamp     = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_pool_swap
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_pool_swap();

ALTER TABLE curve_pool_swap SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'curve_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('curve_pool_swap', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_pool_swap', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_pool_swap';
END $$;

-- ===========================================================================
-- Hypertable: per-AddLiquidity / RemoveLiquidity* fact rows.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_pool_liquidity_event (
    curve_pool_id      BIGINT       NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    tx_hash            BYTEA        NOT NULL,
    log_index          INT          NOT NULL,
    event_kind         TEXT         NOT NULL CHECK (event_kind IN (
        'AddLiquidity', 'RemoveLiquidity', 'RemoveLiquidityOne', 'RemoveLiquidityImbalance')),
    provider           BYTEA        NOT NULL,
    token_amounts      NUMERIC[]    NOT NULL,
    fees               NUMERIC[],
    invariant_d        NUMERIC,
    token_supply       NUMERIC,
    token_amount       NUMERIC,
    coin_index         SMALLINT     CHECK (coin_index IS NULL OR coin_index BETWEEN 0 AND 7),
    coin_amount        NUMERIC,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (curve_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp),
    -- RemoveLiquidityOne is the only event that fills (token_amount, coin_index, coin_amount).
    -- All three are NULL for the other three event kinds. NG emits coin_index in the
    -- event; V1 pre-NG pools (stETH-classic, 3pool) do not, so coin_index stays NULL
    -- for V1 unless the worker recovers it from tx call data.
    CHECK (
        (event_kind = 'RemoveLiquidityOne'
            AND token_amount IS NOT NULL AND coin_amount IS NOT NULL)
        OR (event_kind <> 'RemoveLiquidityOne'
            AND token_amount IS NULL AND coin_index IS NULL AND coin_amount IS NULL)
    )
);

SELECT create_hypertable('curve_pool_liquidity_event', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_curve_pool_liquidity_event_pool_time
    ON curve_pool_liquidity_event (curve_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_curve_pool_liquidity_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cpl|%s|%s|%s|%s|%s|%s',
            NEW.curve_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM curve_pool_liquidity_event
    WHERE curve_pool_id  = NEW.curve_pool_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND tx_hash        = NEW.tx_hash
      AND log_index      = NEW.log_index
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_pool_liquidity_event
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND tx_hash       = NEW.tx_hash
          AND log_index     = NEW.log_index
          AND timestamp     = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_pool_liquidity_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_pool_liquidity_event();

ALTER TABLE curve_pool_liquidity_event SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'curve_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('curve_pool_liquidity_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_pool_liquidity_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_pool_liquidity_event';
END $$;

-- ===========================================================================
-- Hypertable: RampA / StopRampA / NewFee parameter-change events.
-- Wider chunk interval (7 days) — these events are rare.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_pool_parameter_event (
    curve_pool_id      BIGINT       NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    tx_hash            BYTEA        NOT NULL,
    log_index          INT          NOT NULL,
    event_kind         TEXT         NOT NULL,
    old_a              NUMERIC,
    new_a              NUMERIC,
    initial_time       TIMESTAMPTZ,
    future_time        TIMESTAMPTZ,
    new_fee            NUMERIC,
    new_admin_fee      NUMERIC,
    extra              JSONB,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (curve_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('curve_pool_parameter_event', by_range('timestamp', INTERVAL '7 days'));

CREATE INDEX idx_curve_pool_parameter_event_pool_time
    ON curve_pool_parameter_event (curve_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_curve_pool_parameter_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cpp|%s|%s|%s|%s|%s|%s',
            NEW.curve_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM curve_pool_parameter_event
    WHERE curve_pool_id  = NEW.curve_pool_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND tx_hash        = NEW.tx_hash
      AND log_index      = NEW.log_index
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_pool_parameter_event
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND tx_hash       = NEW.tx_hash
          AND log_index     = NEW.log_index
          AND timestamp     = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_pool_parameter_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_pool_parameter_event();

ALTER TABLE curve_pool_parameter_event SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'curve_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- 14-day (vs 2-day) columnstore lag: parameter events are months-rare on
-- Curve mainnet and queries against them tend to look up specific historical
-- RampA windows. Keeping two 7-day chunks in row-store makes those lookups
-- cheap; compression savings on a sparse table are not worth the read penalty.
CALL add_columnstore_policy('curve_pool_parameter_event', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_pool_parameter_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_pool_parameter_event';
END $$;

-- ===========================================================================
-- Hypertable: periodic get_dy(i, j, 10^decimals[i]) snapshots.
-- One row per (i, j) directional pair per event-triggered multicall.
--
-- dy is nullable: get_dy can revert on degenerate pool states (paused pools,
-- empty reserves, exotic Stableswap-NG variants). On revert the worker
-- writes NULL so consumers can distinguish revert from a genuine zero with
-- IS NOT NULL — a NOT NULL column would force the worker to write 0, which
-- is indistinguishable from a real "no liquidity that direction" quote.
-- dx stays NOT NULL because it's an input parameter we control.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_pool_exchange_rate (
    curve_pool_id      BIGINT       NOT NULL REFERENCES curve_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    i                  SMALLINT     NOT NULL CHECK (i BETWEEN 0 AND 7),
    j                  SMALLINT     NOT NULL CHECK (j BETWEEN 0 AND 7),
    dx                 NUMERIC      NOT NULL,
    dy                 NUMERIC,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (curve_pool_id, block_number, block_version, i, j, processing_version, timestamp),
    CHECK (i <> j)
);

SELECT create_hypertable('curve_pool_exchange_rate', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_curve_pool_exchange_rate_pair
    ON curve_pool_exchange_rate (curve_pool_id, i, j, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_curve_pool_exchange_rate()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cpr|%s|%s|%s|%s|%s|%s',
            NEW.curve_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.i,
            NEW.j,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM curve_pool_exchange_rate
    WHERE curve_pool_id  = NEW.curve_pool_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND i              = NEW.i
      AND j              = NEW.j
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_pool_exchange_rate
        WHERE curve_pool_id = NEW.curve_pool_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND i             = NEW.i
          AND j             = NEW.j
          AND timestamp     = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_pool_exchange_rate
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_pool_exchange_rate();

ALTER TABLE curve_pool_exchange_rate SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'curve_pool_id, i, j',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('curve_pool_exchange_rate', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_pool_exchange_rate', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_pool_exchange_rate';
END $$;

-- ===========================================================================
-- Hypertable: per-block gauge metrics. Written via event-triggered multicall
-- on any pool event for a gauged pool, plus any gauge-side event.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_gauge_state (
    curve_gauge_id       BIGINT         NOT NULL REFERENCES curve_gauge (id),
    block_number         BIGINT         NOT NULL,
    block_version        INT            NOT NULL DEFAULT 0,
    timestamp            TIMESTAMPTZ    NOT NULL,
    source               TEXT           NOT NULL CHECK (source IN ('event', 'snapshot')),
    inflation_rate       NUMERIC,
    working_supply       NUMERIC,
    total_supply         NUMERIC,
    is_killed            BOOLEAN,
    reward_count         INT            CHECK (reward_count IS NULL OR reward_count >= 0),
    reward_tokens        BYTEA[],
    reward_rates         NUMERIC[],
    reward_period_finish TIMESTAMPTZ[],
    processing_version   INT            NOT NULL DEFAULT 0,
    build_id             INT            NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (curve_gauge_id, block_number, block_version, processing_version, timestamp),
    -- The three reward arrays are parallel and must match reward_count.
    CHECK (
        reward_count IS NULL
        OR (
            COALESCE(cardinality(reward_tokens), 0) = reward_count
            AND COALESCE(cardinality(reward_rates), 0) = reward_count
            AND COALESCE(cardinality(reward_period_finish), 0) = reward_count
        )
    )
);

SELECT create_hypertable('curve_gauge_state', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_curve_gauge_state_current
    ON curve_gauge_state (curve_gauge_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_curve_gauge_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('cgs|%s|%s|%s|%s',
            NEW.curve_gauge_id,
            NEW.block_number,
            NEW.block_version,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM curve_gauge_state
    WHERE curve_gauge_id = NEW.curve_gauge_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_gauge_state
        WHERE curve_gauge_id = NEW.curve_gauge_id
          AND block_number   = NEW.block_number
          AND block_version  = NEW.block_version
          AND timestamp      = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_gauge_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_gauge_state();

ALTER TABLE curve_gauge_state SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'curve_gauge_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('curve_gauge_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_gauge_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_gauge_state';
END $$;

-- ===========================================================================
-- Hypertable: per-user LP-token balance over time. Sourced from Transfer events
-- on lp_token_address (NULL means lp == pool.address for NG factory pools).
-- One INSERT per Transfer per side (from and to); zero address means mint/burn.
-- tx_hash + log_index are part of the PK so two Transfer events in the same
-- block touching the same user (e.g., A→user followed by user→B) don't collide.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS curve_user_lp_position (
    curve_pool_id      BIGINT       NOT NULL REFERENCES curve_pool (id),
    user_address       BYTEA        NOT NULL,
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    tx_hash            BYTEA        NOT NULL,
    log_index          INT          NOT NULL,
    lp_balance         NUMERIC      NOT NULL,
    delta              NUMERIC      NOT NULL,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (curve_pool_id, user_address, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('curve_user_lp_position', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_curve_user_lp_position_user
    ON curve_user_lp_position (user_address, curve_pool_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_curve_user_lp_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('clp|%s|%s|%s|%s|%s|%s|%s',
            NEW.curve_pool_id,
            NEW.user_address,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM curve_user_lp_position
    WHERE curve_pool_id = NEW.curve_pool_id
      AND user_address  = NEW.user_address
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND tx_hash       = NEW.tx_hash
      AND log_index     = NEW.log_index
      AND timestamp     = NEW.timestamp
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM curve_user_lp_position
        WHERE curve_pool_id = NEW.curve_pool_id
          AND user_address  = NEW.user_address
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND tx_hash       = NEW.tx_hash
          AND log_index     = NEW.log_index
          AND timestamp     = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON curve_user_lp_position
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_curve_user_lp_position();

ALTER TABLE curve_user_lp_position SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'curve_pool_id, user_address',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('curve_user_lp_position', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('curve_user_lp_position', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_user_lp_position';
END $$;

-- ===========================================================================
-- GRANTs
-- ===========================================================================
GRANT SELECT ON curve_pool, curve_gauge, curve_pool_state, curve_pool_swap,
                curve_pool_liquidity_event, curve_pool_parameter_event,
                curve_pool_exchange_rate, curve_gauge_state, curve_user_lp_position
    TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON curve_pool, curve_gauge, curve_pool_state, curve_pool_swap,
                curve_pool_liquidity_event, curve_pool_parameter_event,
                curve_pool_exchange_rate, curve_gauge_state, curve_user_lp_position
    TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE curve_pool_id_seq  TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE curve_gauge_id_seq TO stl_readwrite;

-- ===========================================================================
-- Seed: indexed Curve pools on Ethereum mainnet (chain_id = 1).
-- coin_token_ids reference rows in the token table:
--   WETH = 4, stETH = 227, DAI = 1, USDC = 3, USDT = 9 (verified live 2026-05-20).
-- For ETH-native Curve pools the placeholder 0xEee...EEEE is stored in
-- coin_addresses while coin_token_ids maps that slot to WETH (id=4); raw native
-- ETH amounts are 18-decimal wei and pass through to WETH accounting unchanged.
-- ===========================================================================
INSERT INTO curve_pool (chain_id, pool_kind, address, lp_token_address, label, n_coins,
                        coin_addresses, coin_token_ids, coin_decimals, deployment_block)
VALUES
    (
        1, 'v1',
        '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea,
        '\x06325440D014e39736583c165C2963BA99fAf14E'::bytea,
        'stETH-classic',
        2,
        ARRAY['\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea,
              '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea],
        ARRAY[4::bigint, 227::bigint],
        ARRAY[18::smallint, 18::smallint],
        11592551
    ),
    (
        1, 'ng',
        '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea,
        NULL,
        'stETH-ng',
        2,
        ARRAY['\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea,
              '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea],
        ARRAY[4::bigint, 227::bigint],
        ARRAY[18::smallint, 18::smallint],
        -- stETH-ng's pool-deploy block is later than the factory's; left NULL
        -- and backfilled by the worker via factory PlainPoolDeployed events.
        NULL
    ),
    (
        1, 'v1',
        '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'::bytea,
        '\x6c3F90f043a72FA612cbac8115EE7e52BDe6E490'::bytea,  -- 3CRV LP, separate from pool
        '3pool',
        3,
        ARRAY['\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea,
              '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
              '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea],
        ARRAY[1::bigint, 3::bigint, 9::bigint],
        ARRAY[18::smallint, 6::smallint, 6::smallint],
        10809473
    )
ON CONFLICT (chain_id, address) DO NOTHING;

-- ===========================================================================
-- Post-seed assertion: every coin_token_id must resolve to an existing token
-- AND coin_addresses[i] must equal the resolved token.address. The single
-- documented exception is the Curve ETH placeholder 0xEee…EEEE, which maps
-- by convention to the WETH token row (raw native ETH amounts share WETH's
-- 18-decimal accounting). Anything else is a swapped or stale mapping that
-- would only surface as wrong indexer reads in production.
-- ===========================================================================
DO $$
DECLARE mismatch_count INT;
BEGIN
    WITH pairs AS (
        SELECT cp.id   AS pool_id,
               cp.label,
               ord,
               cp.coin_addresses[ord] AS addr,
               cp.coin_token_ids[ord] AS tid
        FROM curve_pool cp,
             LATERAL generate_subscripts(cp.coin_token_ids, 1) AS ord
        WHERE cp.chain_id = 1
    )
    SELECT count(*) INTO mismatch_count
    FROM pairs p
    LEFT JOIN token t ON t.id = p.tid AND t.chain_id = 1
    WHERE t.id IS NULL
       OR (
           t.address <> p.addr
           AND p.addr <> '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea
       );
    IF mismatch_count > 0 THEN
        RAISE EXCEPTION 'curve_pool seed has % coin slot(s) with missing or address-mismatched token mapping', mismatch_count;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260521_110000_create_curve_dex_tables.sql')
ON CONFLICT (filename) DO NOTHING;
