-- VEC-262: Balancer V2 DEX indexing schema.
--
-- See docs/vec-79-implementation-plan.md §5.3 + §5.3.x for design rationale.
-- Pattern follows db/migrations/20260423_214929_create_token_total_supply.sql.
-- Lock prefixes (3-char, unique per table): bps, bsw, bpl, bpp, bup.
--
-- Balancer's variable-N-token composable pools (with phantom BPT slots and
-- per-token rate providers) need a normalized join table: balancer_pool_token.
-- The hypertables store per-token data as parallel NUMERIC[] arrays indexed
-- by balancer_pool_token.token_index for fast pool-state reads.

-- ===========================================================================
-- Registry: indexed Balancer V2 pools.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS balancer_pool (
    id                BIGSERIAL PRIMARY KEY,
    chain_id          INT          NOT NULL REFERENCES chain (chain_id),
    pool_kind         TEXT         NOT NULL CHECK (pool_kind IN ('stable', 'composable_stable', 'weighted')),
    address           BYTEA        NOT NULL,
    pool_id           BYTEA        NOT NULL CHECK (octet_length(pool_id) = 32),
    vault_address     BYTEA        NOT NULL,
    label             TEXT         NOT NULL,
    deployment_block  BIGINT,
    enabled           BOOLEAN      NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address),
    UNIQUE (chain_id, pool_id)
);

CREATE INDEX IF NOT EXISTS idx_balancer_pool_chain_address
    ON balancer_pool (chain_id, address);

-- ===========================================================================
-- Join table: pool → token slots (in Vault.getPoolTokens order).
-- token_index is the array index used by all balancer_pool_state arrays.
-- is_phantom is true for the BPT slot in composable_stable pools, where the
-- pool itself appears as a token in its own getPoolTokens output.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS balancer_pool_token (
    balancer_pool_id  BIGINT       NOT NULL REFERENCES balancer_pool (id),
    token_index       SMALLINT     NOT NULL CHECK (token_index BETWEEN 0 AND 31),
    token_id          BIGINT       NOT NULL REFERENCES token (id),
    is_phantom        BOOLEAN      NOT NULL DEFAULT false,
    rate_provider     BYTEA,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (balancer_pool_id, token_index),
    -- Composable_stable + weighted pools we currently index never repeat a
    -- token across slots, so one row per token per pool is correct here.
    -- If a future Balancer weighted pool indexes the same underlying twice,
    -- this UNIQUE will reject the second slot with a loud constraint error
    -- (not a silent drop) and the operator can relax it then.
    UNIQUE (balancer_pool_id, token_id)
);

-- token_id is the second column of the PK and UNIQUE constraints, so neither
-- supports lookups/joins keyed on token_id alone. Index it explicitly: workers
-- join balancer_pool_token on token_id, and a parent token delete/update would
-- otherwise seq-scan + take a heavier lock (PG does not auto-index FK columns).
CREATE INDEX IF NOT EXISTS idx_balancer_pool_token_token_id
    ON balancer_pool_token (token_id);

-- ===========================================================================
-- Hypertable: pool state. Written via event-triggered multicall on Vault and
-- pool contract events. `balances` is indexed by balancer_pool_token.token_index.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS balancer_pool_state (
    balancer_pool_id   BIGINT       NOT NULL REFERENCES balancer_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    source             TEXT         NOT NULL CHECK (source IN ('event', 'snapshot')),
    balances           NUMERIC[]    NOT NULL,
    amp_factor         NUMERIC,
    bpt_rate           NUMERIC,
    actual_supply      NUMERIC,
    total_supply       NUMERIC,
    token_rates        NUMERIC[],
    scaling_factors    NUMERIC[],
    last_change_block  BIGINT,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (balancer_pool_id, block_number, block_version, processing_version, timestamp)
);

SELECT create_hypertable('balancer_pool_state', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_bps_current
    ON balancer_pool_state (balancer_pool_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_balancer_pool_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bps|%s|%s|%s|%s',
            NEW.balancer_pool_id,
            NEW.block_number,
            NEW.block_version,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM balancer_pool_state
    WHERE balancer_pool_id = NEW.balancer_pool_id
      AND block_number     = NEW.block_number
      AND block_version    = NEW.block_version
      AND timestamp        = NEW.timestamp
      AND build_id         = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM balancer_pool_state
        WHERE balancer_pool_id = NEW.balancer_pool_id
          AND block_number     = NEW.block_number
          AND block_version    = NEW.block_version
          AND timestamp        = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON balancer_pool_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_balancer_pool_state();

ALTER TABLE balancer_pool_state SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'balancer_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('balancer_pool_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('balancer_pool_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for balancer_pool_state';
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
CREATE TABLE IF NOT EXISTS balancer_pool_current (
    balancer_pool_id   BIGINT       PRIMARY KEY REFERENCES balancer_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL,
    timestamp          TIMESTAMPTZ  NOT NULL,
    source             TEXT         NOT NULL,
    balances           NUMERIC[]    NOT NULL,
    amp_factor         NUMERIC,
    bpt_rate           NUMERIC,
    actual_supply      NUMERIC,
    total_supply       NUMERIC,
    token_rates        NUMERIC[],
    scaling_factors    NUMERIC[],
    last_change_block  BIGINT,
    processing_version INT          NOT NULL,
    build_id           INT          NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Projects each newly inserted hypertable row onto balancer_pool_current, but
-- only when it is strictly newer than the stored head. "Newer" is the same
-- total order idx_bps_current sorts by: (block_number, block_version,
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
CREATE OR REPLACE FUNCTION refresh_balancer_pool_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO balancer_pool_current AS c (
        balancer_pool_id, block_number, block_version, timestamp, source,
        balances, amp_factor, bpt_rate, actual_supply, total_supply,
        token_rates, scaling_factors, last_change_block,
        processing_version, build_id, updated_at)
    VALUES (
        NEW.balancer_pool_id, NEW.block_number, NEW.block_version, NEW.timestamp, NEW.source,
        NEW.balances, NEW.amp_factor, NEW.bpt_rate, NEW.actual_supply, NEW.total_supply,
        NEW.token_rates, NEW.scaling_factors, NEW.last_change_block,
        NEW.processing_version, NEW.build_id, NOW())
    ON CONFLICT (balancer_pool_id) DO UPDATE SET
        block_number       = EXCLUDED.block_number,
        block_version      = EXCLUDED.block_version,
        timestamp          = EXCLUDED.timestamp,
        source             = EXCLUDED.source,
        balances           = EXCLUDED.balances,
        amp_factor         = EXCLUDED.amp_factor,
        bpt_rate           = EXCLUDED.bpt_rate,
        actual_supply      = EXCLUDED.actual_supply,
        total_supply       = EXCLUDED.total_supply,
        token_rates        = EXCLUDED.token_rates,
        scaling_factors    = EXCLUDED.scaling_factors,
        last_change_block  = EXCLUDED.last_change_block,
        processing_version = EXCLUDED.processing_version,
        build_id           = EXCLUDED.build_id,
        updated_at         = NOW()
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.timestamp)
        > (c.block_number, c.block_version, c.processing_version, c.timestamp);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_balancer_pool_current
    AFTER INSERT ON balancer_pool_state
    FOR EACH ROW
EXECUTE FUNCTION refresh_balancer_pool_current();

-- ===========================================================================
-- Hypertable: per-Swap fact rows (Vault Swap events). token_in_idx / token_out_idx
-- reference balancer_pool_token.token_index.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS balancer_pool_swap (
    balancer_pool_id   BIGINT       NOT NULL REFERENCES balancer_pool (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    tx_hash            BYTEA        NOT NULL,
    log_index          INT          NOT NULL,
    token_in_idx       SMALLINT     NOT NULL CHECK (token_in_idx BETWEEN 0 AND 31),
    token_out_idx      SMALLINT     NOT NULL CHECK (token_out_idx BETWEEN 0 AND 31),
    amount_in          NUMERIC      NOT NULL,
    amount_out         NUMERIC      NOT NULL,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (balancer_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp),
    CHECK (token_in_idx <> token_out_idx)
);

SELECT create_hypertable('balancer_pool_swap', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_balancer_pool_swap_pool_time
    ON balancer_pool_swap (balancer_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_balancer_pool_swap()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bsw|%s|%s|%s|%s|%s|%s',
            NEW.balancer_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM balancer_pool_swap
    WHERE balancer_pool_id = NEW.balancer_pool_id
      AND block_number     = NEW.block_number
      AND block_version    = NEW.block_version
      AND tx_hash          = NEW.tx_hash
      AND log_index        = NEW.log_index
      AND timestamp        = NEW.timestamp
      AND build_id         = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM balancer_pool_swap
        WHERE balancer_pool_id = NEW.balancer_pool_id
          AND block_number     = NEW.block_number
          AND block_version    = NEW.block_version
          AND tx_hash          = NEW.tx_hash
          AND log_index        = NEW.log_index
          AND timestamp        = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON balancer_pool_swap
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_balancer_pool_swap();

ALTER TABLE balancer_pool_swap SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'balancer_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('balancer_pool_swap', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('balancer_pool_swap', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for balancer_pool_swap';
END $$;

-- ===========================================================================
-- Hypertable: Vault PoolBalanceChanged join/exit fact rows.
-- deltas[] is signed (positive = into pool, negative = out).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS balancer_pool_liquidity_event (
    balancer_pool_id     BIGINT       NOT NULL REFERENCES balancer_pool (id),
    block_number         BIGINT       NOT NULL,
    block_version        INT          NOT NULL DEFAULT 0,
    timestamp            TIMESTAMPTZ  NOT NULL,
    tx_hash              BYTEA        NOT NULL,
    log_index            INT          NOT NULL,
    liquidity_provider   BYTEA        NOT NULL,
    deltas               NUMERIC[]    NOT NULL,
    protocol_fee_amounts NUMERIC[],
    processing_version   INT          NOT NULL DEFAULT 0,
    build_id             INT          NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (balancer_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('balancer_pool_liquidity_event', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_balancer_pool_liquidity_event_pool_time
    ON balancer_pool_liquidity_event (balancer_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_balancer_pool_liquidity_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bpl|%s|%s|%s|%s|%s|%s',
            NEW.balancer_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM balancer_pool_liquidity_event
    WHERE balancer_pool_id = NEW.balancer_pool_id
      AND block_number     = NEW.block_number
      AND block_version    = NEW.block_version
      AND tx_hash          = NEW.tx_hash
      AND log_index        = NEW.log_index
      AND timestamp        = NEW.timestamp
      AND build_id         = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM balancer_pool_liquidity_event
        WHERE balancer_pool_id = NEW.balancer_pool_id
          AND block_number     = NEW.block_number
          AND block_version    = NEW.block_version
          AND tx_hash          = NEW.tx_hash
          AND log_index        = NEW.log_index
          AND timestamp        = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON balancer_pool_liquidity_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_balancer_pool_liquidity_event();

ALTER TABLE balancer_pool_liquidity_event SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'balancer_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('balancer_pool_liquidity_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('balancer_pool_liquidity_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for balancer_pool_liquidity_event';
END $$;

-- ===========================================================================
-- Hypertable: pool parameter-change events. Schema accommodates AmpUpdate*,
-- TokenRate*, SwapFeePercentageChanged, PausedStateChanged with typed columns
-- and an extra JSONB catch-all for future events.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS balancer_pool_parameter_event (
    balancer_pool_id    BIGINT       NOT NULL REFERENCES balancer_pool (id),
    block_number        BIGINT       NOT NULL,
    block_version       INT          NOT NULL DEFAULT 0,
    timestamp           TIMESTAMPTZ  NOT NULL,
    tx_hash             BYTEA        NOT NULL,
    log_index           INT          NOT NULL,
    event_kind          TEXT         NOT NULL CHECK (event_kind IN (
        'AmpUpdateStarted',
        'AmpUpdateStopped',
        'TokenRateProviderSet',
        'TokenRateCacheUpdated',
        'SwapFeePercentageChanged',
        'PausedStateChanged'
    )),
    start_value         NUMERIC,
    end_value           NUMERIC,
    start_time          TIMESTAMPTZ,
    end_time            TIMESTAMPTZ,
    current_value       NUMERIC,
    token_address       BYTEA,
    rate_provider       BYTEA,
    cache_duration      INT,
    rate                NUMERIC,
    swap_fee_percentage NUMERIC,
    extra               JSONB,
    processing_version  INT          NOT NULL DEFAULT 0,
    build_id            INT          NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (balancer_pool_id, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('balancer_pool_parameter_event', by_range('timestamp', INTERVAL '7 days'));

CREATE INDEX idx_balancer_pool_parameter_event_pool_time
    ON balancer_pool_parameter_event (balancer_pool_id, timestamp DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_balancer_pool_parameter_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bpp|%s|%s|%s|%s|%s|%s',
            NEW.balancer_pool_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM balancer_pool_parameter_event
    WHERE balancer_pool_id = NEW.balancer_pool_id
      AND block_number     = NEW.block_number
      AND block_version    = NEW.block_version
      AND tx_hash          = NEW.tx_hash
      AND log_index        = NEW.log_index
      AND timestamp        = NEW.timestamp
      AND build_id         = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM balancer_pool_parameter_event
        WHERE balancer_pool_id = NEW.balancer_pool_id
          AND block_number     = NEW.block_number
          AND block_version    = NEW.block_version
          AND tx_hash          = NEW.tx_hash
          AND log_index        = NEW.log_index
          AND timestamp        = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON balancer_pool_parameter_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_balancer_pool_parameter_event();

ALTER TABLE balancer_pool_parameter_event SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'balancer_pool_id',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- 14-day columnstore lag mirrors curve_pool_parameter_event: parameter events
-- are months-rare and queries against them tend to hit specific historical
-- windows; keeping two 7-day chunks in row-store keeps those reads cheap.
CALL add_columnstore_policy('balancer_pool_parameter_event', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('balancer_pool_parameter_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for balancer_pool_parameter_event';
END $$;

-- ===========================================================================
-- Hypertable: per-user BPT balance over time. BPT is the pool contract itself
-- on composable_stable; for legacy stable pools it's a separate contract held
-- in balancer_pool.address. Same per-event PK shape as Curve's user-LP table:
-- tx_hash + log_index disambiguate multiple Transfers in the same block.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS balancer_user_bpt_position (
    balancer_pool_id   BIGINT       NOT NULL REFERENCES balancer_pool (id),
    user_address       BYTEA        NOT NULL,
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    tx_hash            BYTEA        NOT NULL,
    log_index          INT          NOT NULL,
    bpt_balance        NUMERIC      NOT NULL,
    delta              NUMERIC      NOT NULL,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (balancer_pool_id, user_address, block_number, block_version, tx_hash, log_index, processing_version, timestamp)
);

SELECT create_hypertable('balancer_user_bpt_position', by_range('timestamp', INTERVAL '1 day'));

CREATE INDEX idx_balancer_user_bpt_position_user
    ON balancer_user_bpt_position (user_address, balancer_pool_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_balancer_user_bpt_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bup|%s|%s|%s|%s|%s|%s|%s',
            NEW.balancer_pool_id,
            NEW.user_address,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.log_index,
            EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM balancer_user_bpt_position
    WHERE balancer_pool_id = NEW.balancer_pool_id
      AND user_address     = NEW.user_address
      AND block_number     = NEW.block_number
      AND block_version    = NEW.block_version
      AND tx_hash          = NEW.tx_hash
      AND log_index        = NEW.log_index
      AND timestamp        = NEW.timestamp
      AND build_id         = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM balancer_user_bpt_position
        WHERE balancer_pool_id = NEW.balancer_pool_id
          AND user_address     = NEW.user_address
          AND block_number     = NEW.block_number
          AND block_version    = NEW.block_version
          AND tx_hash          = NEW.tx_hash
          AND log_index        = NEW.log_index
          AND timestamp        = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON balancer_user_bpt_position
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_balancer_user_bpt_position();

ALTER TABLE balancer_user_bpt_position SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'balancer_pool_id, user_address',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);

CALL add_columnstore_policy('balancer_user_bpt_position', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('balancer_user_bpt_position', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for balancer_user_bpt_position';
END $$;

-- ===========================================================================
-- GRANTs
-- ===========================================================================
GRANT SELECT ON balancer_pool, balancer_pool_token, balancer_pool_state,
                balancer_pool_swap, balancer_pool_liquidity_event,
                balancer_pool_parameter_event, balancer_user_bpt_position
    TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON balancer_pool, balancer_pool_token, balancer_pool_state,
                balancer_pool_swap, balancer_pool_liquidity_event,
                balancer_pool_parameter_event, balancer_user_bpt_position
    TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE balancer_pool_id_seq TO stl_readwrite;

-- ===========================================================================
-- Seed: indexed Balancer V2 pools on Ethereum mainnet.
-- The wstETH-WETH ComposableStable Pool is the canonical wstETH/WETH venue.
-- balancer_pool_token rows are populated by the worker at first run by calling
-- Vault.getPoolTokens(poolId) and identifying which slot is the phantom BPT.
-- deployment_block left NULL; the worker backfills it via Vault.PoolRegistered
-- event lookups (column is metadata only, not in any PK or read path).
-- ===========================================================================
INSERT INTO balancer_pool (chain_id, pool_kind, address, pool_id, vault_address, label)
VALUES (
    1,
    'composable_stable',
    '\x93d199263632a4EF4Bb438F1feB99e57b4b5f0BD'::bytea,
    '\x93d199263632a4ef4bb438f1feb99e57b4b5f0bd0000000000000000000005c2'::bytea,
    '\xBA12222222228d8Ba445958a75a0704d566BF2C8'::bytea,
    'wstETH-WETH-CSP'
)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ===========================================================================
-- Post-seed assertion: pool_id must encode the pool address in the leading 20
-- bytes (Balancer V2 convention: poolId = abi.encodePacked(pool, specialization,
-- nonce)). Walk through every row and confirm.
-- ===========================================================================
DO $$
DECLARE bad_count INT;
BEGIN
    SELECT count(*) INTO bad_count
    FROM balancer_pool
    WHERE substring(pool_id FROM 1 FOR 20) <> address;
    IF bad_count > 0 THEN
        RAISE EXCEPTION 'balancer_pool seed has % row(s) whose pool_id does not encode address in its leading 20 bytes', bad_count;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260521_130000_create_balancer_v2_dex_tables.sql')
ON CONFLICT (filename) DO NOTHING;
