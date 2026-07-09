-- Uniswap V3 indexer tables (VEC-261).
-- Creates the registry (uniswap_v3_pool) and 5 fact/state tables
-- (uniswap_v3_pool_state, uniswap_v3_swap, uniswap_v3_liquidity_event,
-- uniswap_v3_tick, uniswap_v3_pool_event) with full auditability (ADR-0002),
-- plus their column-level COMMENT metadata (consumed by the metadata catalogue).
--
-- Prerequisites (already applied):
--   20260521_100000_create_dex_prereqs.sql  -- UniswapV3 protocol row
--
-- Mirrors the trigger/hypertable/compression boilerplate from
-- 20260521_110000_create_curve_dex_tables.sql. The pool/token seed is a
-- separate follow-up migration (VEC-261 task B3).
--
-- COMMENT conventions used below (mirrors 20260521_120000_curve_column_
-- comments.sql):
--   [Type]: Dimension (seeded/read-only registry) | Configuration
--           (governance/config) | Operational (append-on-change or bookkeeping
--           state, not partitioned) | Hypertable (time-series facts)
--   Roles:  PK | FK->table.col | Partition | Audit | Derived
--   Scale:  numeric amounts state their unit explicitly. sqrt_price_x96 is
--           Q64.96 fixed point: price(token1/token0) = (sqrtPriceX96/2^96)^2,
--           then adjust by 10^(dec0-dec1). fee_growth_*_x128 columns are
--           Q128.128 fixed point. fee is hundredths of a bip (1e6 = 100%,
--           3000 = 0.30%). tick is a plain integer where price = 1.0001^tick.
--           liquidity/liquidity_gross/liquidity_net are raw on-chain L
--           (sqrt(xy) scaled by the pool's token decimals; not comparable
--           across pools with different token decimals). balance0/balance1
--           and amount0/amount1 are raw native-decimal integers (scale by
--           the relevant token's decimals); amounts may be signed (negative
--           = outflow from the pool).

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
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_token0 ON uniswap_v3_pool (token0_id);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pool_token1 ON uniswap_v3_pool (token1_id);

COMMENT ON TABLE uniswap_v3_pool IS
  '[Dimension] Registry of indexed Uniswap V3 pools, one row per deployed pool; FK target for every uniswap_v3_* fact table. Migration-seeded and read-only at runtime.';
COMMENT ON COLUMN uniswap_v3_pool.id IS
  'PK. Surrogate pool ID; FK target for all uniswap_v3_* fact tables.';
COMMENT ON COLUMN uniswap_v3_pool.chain_id IS
  'FK->chain.chain_id. Network the pool is deployed on.';
COMMENT ON COLUMN uniswap_v3_pool.protocol_id IS
  'FK->protocol.id. The UniswapV3 protocol (factory) row this pool belongs to.';
COMMENT ON COLUMN uniswap_v3_pool.pool_address IS
  'On-chain pool contract address, 20 bytes. Unique per chain.';
COMMENT ON COLUMN uniswap_v3_pool.token0_id IS
  'FK->token.id. The pool''s token0 as returned by token0(); not assumed from the pair listing (a given symbol may be token0 in one pool and token1 in another).';
COMMENT ON COLUMN uniswap_v3_pool.token1_id IS
  'FK->token.id. The pool''s token1 as returned by token1().';
COMMENT ON COLUMN uniswap_v3_pool.fee IS
  'Pool fee tier fee(), hundredths of a bip (1e6 = 100%; e.g. 3000 = 0.30%, 500 = 0.05%, 100 = 0.01%).';
COMMENT ON COLUMN uniswap_v3_pool.tick_spacing IS
  'tickSpacing(): minimum tick granularity for this fee tier (e.g. 1, 10, 60, 200); ticks usable by positions must be a multiple of this value.';
COMMENT ON COLUMN uniswap_v3_pool.max_liquidity_per_tick IS
  'maxLiquidityPerTick(): maximum raw liquidity L (see liquidity scale above) that can be attributed to a single tick, derived from tick_spacing.';
COMMENT ON COLUMN uniswap_v3_pool.deploy_block IS
  'Configuration (load-bearing). Block at which the pool contract was deployed (PoolCreated), used to gate snapshot reads. REQUIRED: LoadPools rejects a NULL deploy_block (unlike Curve), since a NULL defeats the reorg deploy-gate. Must be a lower bound of the true deploy block (<= actual deploy height): DueSet hard-errors ("registry bug") when a touched pool reports deploy_block greater than the processed block, and skips sweep scheduling before this height.';
COMMENT ON COLUMN uniswap_v3_pool.created_at IS
  'Audit. Row insertion timestamp (bookkeeping only; not an on-chain value).';

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

COMMENT ON TABLE uniswap_v3_pool_state IS
  '[Hypertable] Per-touched-block snapshot of pool slot0 / liquidity / fee-growth / protocol-fee / balance state, taken only on blocks that touch the pool (V3 state is piecewise-constant; no periodic heartbeat). Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_pool_state.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool the snapshot is for.';
COMMENT ON COLUMN uniswap_v3_pool_state.block_number IS
  'PK. Block height at which the snapshot was read (via multicall pinned to this block''s hash).';
COMMENT ON COLUMN uniswap_v3_pool_state.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_pool_state.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_pool_state.sqrt_price_x96 IS
  'slot0().sqrtPriceX96: Q64.96 fixed point. price(token1/token0) = (sqrt_price_x96/2^96)^2, then adjust by 10^(token0.decimals-token1.decimals).';
COMMENT ON COLUMN uniswap_v3_pool_state.tick IS
  'slot0().tick: current pool tick (plain integer); price(token1/token0) = 1.0001^tick before decimal adjustment.';
COMMENT ON COLUMN uniswap_v3_pool_state.observation_index IS
  'slot0().observationIndex: index of the most recently written oracle observation in the observations ring buffer.';
COMMENT ON COLUMN uniswap_v3_pool_state.observation_cardinality IS
  'slot0().observationCardinality: number of populated oracle observation slots.';
COMMENT ON COLUMN uniswap_v3_pool_state.observation_cardinality_next IS
  'slot0().observationCardinalityNext: cardinality the oracle ring buffer will grow to on its next write.';
COMMENT ON COLUMN uniswap_v3_pool_state.fee_protocol IS
  'slot0().feeProtocol: packed protocol fee, 4 bits per token (feeProtocol0 in the low nibble, feeProtocol1 in the high nibble); 0 means the protocol takes no share of that token''s swap fee, otherwise the fee is divided by this value.';
COMMENT ON COLUMN uniswap_v3_pool_state.unlocked IS
  'slot0().unlocked: reentrancy-lock flag from the contract; false only mid-callback, always true in a settled post-block read.';
COMMENT ON COLUMN uniswap_v3_pool_state.liquidity IS
  'liquidity(): in-range raw liquidity L active at the current tick (sqrt(xy) scaled by the pool''s token decimals; not comparable across pools with different token decimals).';
COMMENT ON COLUMN uniswap_v3_pool_state.fee_growth_global0_x128 IS
  'feeGrowthGlobal0X128(): cumulative token0 fees earned per unit of liquidity over the pool''s lifetime, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_pool_state.fee_growth_global1_x128 IS
  'feeGrowthGlobal1X128(): cumulative token1 fees earned per unit of liquidity over the pool''s lifetime, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_pool_state.protocol_fees_token0 IS
  'protocolFees().token0: accrued protocol-owned token0 fees awaiting collection, raw native decimals of token0.';
COMMENT ON COLUMN uniswap_v3_pool_state.protocol_fees_token1 IS
  'protocolFees().token1: accrued protocol-owned token1 fees awaiting collection, raw native decimals of token1.';
COMMENT ON COLUMN uniswap_v3_pool_state.balance0 IS
  'ERC20(token0).balanceOf(pool): actual token0 reserves held by the pool contract, raw native decimals of token0 (the pool exposes no balance getter of its own).';
COMMENT ON COLUMN uniswap_v3_pool_state.balance1 IS
  'ERC20(token1).balanceOf(pool): actual token1 reserves held by the pool contract, raw native decimals of token1.';
COMMENT ON COLUMN uniswap_v3_pool_state.twap_tick IS
  'Derived. Arithmetic-mean tick over twap_window_secs from observe([twap_window_secs,0]); NULL when the call reverts with OLD (observation cardinality cannot yet cover the requested window). Convert to price the same way as tick.';
COMMENT ON COLUMN uniswap_v3_pool_state.twap_window_secs IS
  'Window length in seconds used for the twap_tick observe() call; NULL alongside twap_tick when the read was skipped or reverted.';
COMMENT ON COLUMN uniswap_v3_pool_state.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_pool_state.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

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

COMMENT ON TABLE uniswap_v3_swap IS
  '[Hypertable] One row per on-chain Swap event. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_swap.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool that emitted the swap.';
COMMENT ON COLUMN uniswap_v3_swap.block_number IS
  'PK. Block height at which the swap was emitted.';
COMMENT ON COLUMN uniswap_v3_swap.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_swap.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_swap.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v3_swap.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v3_swap.sender IS
  'Address that initiated the swap (event field `sender`, typically a router), 20 bytes.';
COMMENT ON COLUMN uniswap_v3_swap.recipient IS
  'Address that received the swap output (event field `recipient`), 20 bytes.';
COMMENT ON COLUMN uniswap_v3_swap.amount0 IS
  'Signed change in the pool''s token0 balance from this swap, raw native decimals of token0 (positive = token0 flowed into the pool, negative = out).';
COMMENT ON COLUMN uniswap_v3_swap.amount1 IS
  'Signed change in the pool''s token1 balance from this swap, raw native decimals of token1 (positive = token1 flowed into the pool, negative = out).';
COMMENT ON COLUMN uniswap_v3_swap.sqrt_price_x96 IS
  'Pool sqrtPriceX96 immediately after the swap, Q64.96 fixed point (see uniswap_v3_pool_state.sqrt_price_x96 for the price conversion).';
COMMENT ON COLUMN uniswap_v3_swap.liquidity IS
  'Pool in-range liquidity() immediately after the swap, raw L (see uniswap_v3_pool_state.liquidity for the scale).';
COMMENT ON COLUMN uniswap_v3_swap.tick IS
  'Pool tick immediately after the swap (plain integer; price = 1.0001^tick before decimal adjustment).';
COMMENT ON COLUMN uniswap_v3_swap.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_swap.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

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
    event_name          TEXT        NOT NULL CHECK (event_name IN ('mint', 'burn', 'collect')),
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

COMMENT ON TABLE uniswap_v3_liquidity_event IS
  '[Hypertable] One row per Mint / Burn / Collect event (adding, removing, or withdrawing accrued fees from a position''s liquidity range). Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool that emitted the event.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.block_number IS
  'PK. Block height at which the event was emitted.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_liquidity_event.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.event_name IS
  'Event variant: mint, burn, or collect.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.owner IS
  'Position owner address (event field `owner`), 20 bytes.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.sender IS
  'Address that supplied the liquidity (event field `sender`), 20 bytes; populated for mint only, NULL for burn/collect.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.recipient IS
  'Address that received the collected tokens (event field `recipient`), 20 bytes; populated for collect only, NULL for mint/burn.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.tick_lower IS
  'Lower tick bound of the position''s liquidity range (plain integer, inclusive).';
COMMENT ON COLUMN uniswap_v3_liquidity_event.tick_upper IS
  'Upper tick bound of the position''s liquidity range (plain integer, exclusive).';
COMMENT ON COLUMN uniswap_v3_liquidity_event.amount IS
  'Raw liquidity delta L added (mint) or removed (burn) from the range (see uniswap_v3_pool_state.liquidity for the scale); NULL for collect, which has no liquidity field.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.amount0 IS
  'Token0 amount moved by the event, raw native decimals of token0: owed/paid for mint/burn, collected for collect.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.amount1 IS
  'Token1 amount moved by the event, raw native decimals of token1.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

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

COMMENT ON TABLE uniswap_v3_tick IS
  '[Operational] Append-on-change authoritative per-tick state from ticks(int24) reads. A new row is written only when a touched tick''s state changes (or on first-seen baseline enumeration for the pool); not a hypertable, since ticks are written on-change rather than on every touched block.';
COMMENT ON COLUMN uniswap_v3_tick.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool the tick belongs to.';
COMMENT ON COLUMN uniswap_v3_tick.tick IS
  'PK. Tick index (plain integer, a multiple of the pool''s tick_spacing); price = 1.0001^tick before decimal adjustment.';
COMMENT ON COLUMN uniswap_v3_tick.block_number IS
  'PK. Block height at which this tick state was read.';
COMMENT ON COLUMN uniswap_v3_tick.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_tick.block_timestamp IS
  'Block timestamp (UTC) at which this tick state was read.';
COMMENT ON COLUMN uniswap_v3_tick.liquidity_gross IS
  'ticks(tick).liquidityGross: total raw liquidity L referencing this tick as a boundary, regardless of direction (see uniswap_v3_pool_state.liquidity for the scale).';
COMMENT ON COLUMN uniswap_v3_tick.liquidity_net IS
  'ticks(tick).liquidityNet: signed raw liquidity L added (positive) or removed (negative) when the pool price crosses this tick left-to-right; sign flips for a right-to-left crossing.';
COMMENT ON COLUMN uniswap_v3_tick.fee_growth_outside0_x128 IS
  'ticks(tick).feeGrowthOutside0X128: token0 fee growth on the outside of this tick at the time it was last crossed, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_tick.fee_growth_outside1_x128 IS
  'ticks(tick).feeGrowthOutside1X128: token1 fee growth on the outside of this tick at the time it was last crossed, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_tick.initialized IS
  'ticks(tick).initialized: true while liquidity_gross > 0 for this tick (i.e. at least one position boundary references it).';
COMMENT ON COLUMN uniswap_v3_tick.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_tick.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

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
    event_name          TEXT        NOT NULL CHECK (event_name IN (
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

COMMENT ON TABLE uniswap_v3_pool_event IS
  '[Hypertable] Decoded low-frequency pool events (Initialize, Flash, SetFeeProtocol, CollectProtocol, IncreaseObservationCardinalityNext); typed counterpart to the raw protocol_event mirror. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_pool_event.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool that emitted the event.';
COMMENT ON COLUMN uniswap_v3_pool_event.block_number IS
  'PK. Block height at which the event was emitted.';
COMMENT ON COLUMN uniswap_v3_pool_event.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_pool_event.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_pool_event.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v3_pool_event.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v3_pool_event.event_name IS
  'Decoded event type: one of initialize, flash, set_fee_protocol, collect_protocol, increase_observation_cardinality_next.';
COMMENT ON COLUMN uniswap_v3_pool_event.params IS
  'JSONB of decoded event fields keyed by name. Keys per event_name: initialize {sqrtPriceX96,tick} (sqrtPriceX96 is Q64.96, tick a plain integer); flash {sender,recipient,amount0,amount1,paid0,paid1} (raw native decimals, signed where applicable); set_fee_protocol {feeProtocol0Old,feeProtocol1Old,feeProtocol0New,feeProtocol1New} (plain integers); collect_protocol {sender,recipient,amount0,amount1} (raw native decimals); increase_observation_cardinality_next {observationCardinalityNextOld,observationCardinalityNextNew} (plain integers).';
COMMENT ON COLUMN uniswap_v3_pool_event.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_pool_event.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ============================================================================
-- Pool + token seed: the 18 real, cast-verified, active wstETH/stETH Uniswap V3
-- pools (VEC-261 design spec §4), plus any counterparty token rows not already
-- present. Merged into this migration (was a separate seed migration) so the
-- Uniswap V3 registry is created and populated by one file, mirroring
-- 20260521_110000_create_curve_dex_tables.sql.
--
-- max_liquidity_per_tick is NOT NULL and derived purely from tick_spacing via
-- Uniswap V3's tickSpacingToMaxLiquidityPerTick(): floor((2^128-1) / numTicks),
-- where numTicks is computed from MIN_TICK/MAX_TICK (887272) truncated to each
-- tick spacing.
--
-- token0/token1 orientation below is exactly as returned by each pool's live
-- token0()/token1() (cast-verified) -- NOT alphabetized. Pools #14, #17, #18
-- have wstETH as token1 (mstETH/urLRT/boxETH are token0 respectively).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Counterparty tokens not already seeded by an earlier migration.
-- WETH/USDC/USDT/wstETH/weETH/rsETH/ezETH (20260204_110000) and stETH
-- (20260521_105000) already exist; listed here only for readability, not
-- re-inserted.
-- ----------------------------------------------------------------------------

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\xA35b1B31Ce002FBF2058D22F30f95D405200A15b'::bytea, 'ETHx', 18),
    (1, '\x8c9532a60E0e7C6Bbd2b2c1303F63aCE1c3E9811'::bytea, 'pzETH', 18),
    (1, '\xf073bAC22DAb7FaF4a3Dd6c6189a70D54110525C'::bytea, 'inETH', 18),
    (1, '\x49446A0874197839D15395B908328a74ccc96Bc0'::bytea, 'mstETH', 18),
    (1, '\xe2237a34246eeDcEB43283073Ba9adEF0450351E'::bytea, 'fstETH', 18),
    (1, '\x4f3cc6359364004b245ad5be36e6ad4e805dc961'::bytea, 'urLRT', 18),
    (1, '\x7690202e2C2297bcD03664e31116D1dFFE7e3B73'::bytea, 'boxETH', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 2. Seed the 18 pools. token0_id/token1_id/protocol_id resolved by natural
-- key (address / name), never by symbol.
-- ----------------------------------------------------------------------------

-- 1. wstETH/WETH 0.01%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 15384250
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 2. wstETH/WETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xD340B57AAcDD10F96FC1CF10e15921936F41E29c'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 12376093
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 3. wstETH/WETH 0.30%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xC12aF0C4AA39D3061c56cD3CB19f5e62dEeaeBdE'::bytea, t0.id, t1.id, 3000, 60, 11505743598341114571880798222544994, 14943576
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 4. wstETH/USDC 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x4622Df6fB2d9Bee0DCDaCF545aCDB6a2b2f4f863'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 16065412
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 5. wstETH/USDC 0.30%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x173821f6aD4c5324cd35753A9FD12D92f2eaAB29'::bytea, t0.id, t1.id, 3000, 60, 11505743598341114571880798222544994, 14420259
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 6. wstETH/USDT 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea  -- USDT
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xeC5055067d60292Ab2c514A1090Bc8E014e4aBAA'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 18461625
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 7. stETH/WETH 1.00%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea  -- stETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea  -- WETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x63818BbDd21E69bE108A23aC1E84cBf66399Bd7D'::bytea, t0.id, t1.id, 10000, 200, 38350317471085141830651933667504588, 14937573
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 8. wstETH/weETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee'::bytea  -- weETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xf47F04a8605bE181E525D6391233cbA1f7474182'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 19629516
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 9. wstETH/ETHx 0.01%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA35b1B31Ce002FBF2058D22F30f95D405200A15b'::bytea  -- ETHx
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xC9eD5de354D4BE9fd576D3108C7637a71C01faA1'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 20525114
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 10. wstETH/rsETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7'::bytea  -- rsETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x7a27c7b7E2536e452c57d3E8b909d9ecba2e2eee'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 19970144
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 11. wstETH/ezETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xbf5495Efe5DB9ce00f80364C8B423567e58d2110'::bytea  -- ezETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x1b9d58bEa5eD5d935cc2E818Dde1D796abFf0bc0'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 20011672
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 12. wstETH/pzETH 0.01%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x8c9532a60E0e7C6Bbd2b2c1303F63aCE1c3E9811'::bytea  -- pzETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\xfc354f5cf57125a7d85E1165f4FCDfd3006db61a'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 20192842
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 13. wstETH/inETH 0.05%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xf073bAC22DAb7FaF4a3Dd6c6189a70D54110525C'::bytea  -- inETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x3c0a1a9e0E22b9Acc9248D9f358286e9e9205b0a'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 19232685
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 14. mstETH/wstETH 0.05% -- wstETH is token1, NOT token0 here.
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x49446A0874197839D15395B908328a74ccc96Bc0'::bytea  -- mstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x7f13847459450236d2D233f1c08D742Ed69D2997'::bytea, t0.id, t1.id, 500, 10, 1917569901783203986719870431555990, 20011699
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 15. wstETH/fstETH 1.00%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xe2237a34246eeDcEB43283073Ba9adEF0450351E'::bytea  -- fstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x526389df2DCc8c5F7af69E93aD9E0d8FC21799f6'::bytea, t0.id, t1.id, 10000, 200, 38350317471085141830651933667504588, 16478531
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 16. wstETH/fstETH 0.30%
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\xe2237a34246eeDcEB43283073Ba9adEF0450351E'::bytea  -- fstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x39683566C148851464781f0673112eF0746B9578'::bytea, t0.id, t1.id, 3000, 60, 11505743598341114571880798222544994, 16486285
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 17. urLRT/wstETH 0.01% -- wstETH is token1, NOT token0 here.
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x4f3cc6359364004b245ad5be36e6ad4e805dc961'::bytea  -- urLRT
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x104b3e3aCd2396a7292223b5778EA1CACdb68eC9'::bytea, t0.id, t1.id, 100, 1, 191757530477355301479181766273477, 20832240
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- 18. boxETH/wstETH 1.00% -- wstETH is token1, NOT token0 here.
WITH proto AS (
    SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3' LIMIT 1
),
t0 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7690202e2C2297bcD03664e31116D1dFFE7e3B73'::bytea  -- boxETH
),
t1 AS (
    SELECT id FROM token WHERE chain_id = 1 AND address = '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea  -- wstETH
)
INSERT INTO uniswap_v3_pool (chain_id, protocol_id, pool_address, token0_id, token1_id, fee, tick_spacing, max_liquidity_per_tick, deploy_block)
SELECT 1, proto.id, '\x703A177Fcb4dEf281d180d3619a5edbAE67Ec7b5'::bytea, t0.id, t1.id, 10000, 200, 38350317471085141830651933667504588, 23506288
FROM proto, t0, t1
ON CONFLICT (chain_id, pool_address) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 3. Post-seed assertions.
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    pool_count       INT;
    null_deploy_count INT;
BEGIN
    SELECT count(*) INTO pool_count
    FROM uniswap_v3_pool p
    JOIN protocol pr ON pr.id = p.protocol_id
    WHERE pr.name = 'UniswapV3' AND pr.chain_id = 1;
    IF pool_count <> 18 THEN
        RAISE EXCEPTION 'expected exactly 18 UniswapV3 pools, got %', pool_count;
    END IF;

    SELECT count(*) INTO null_deploy_count
    FROM uniswap_v3_pool p
    JOIN protocol pr ON pr.id = p.protocol_id
    WHERE pr.name = 'UniswapV3' AND pr.chain_id = 1 AND p.deploy_block IS NULL;
    IF null_deploy_count <> 0 THEN
        RAISE EXCEPTION 'expected 0 UniswapV3 pools with NULL deploy_block, got %', null_deploy_count;
    END IF;
END $$;

-- Address-equality spot checks: pool #1 (wstETH/WETH 0.01%), pool #7
-- (stETH/WETH 1%), and pool #14 (mstETH/wstETH, wstETH is token1) --
-- confirms the orientation from the live cast scan was not "normalized".
DO $$
DECLARE
    got_token0 BYTEA;
    got_token1 BYTEA;
BEGIN
    SELECT t0.address, t1.address INTO got_token0, got_token1
    FROM uniswap_v3_pool p
    JOIN token t0 ON t0.id = p.token0_id
    JOIN token t1 ON t1.id = p.token1_id
    WHERE p.chain_id = 1 AND p.pool_address = '\x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa'::bytea;
    IF got_token0 <> '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea
        OR got_token1 <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea THEN
        RAISE EXCEPTION 'pool #1 wstETH/WETH token0/token1 mismatch: token0=%, token1=%', got_token0, got_token1;
    END IF;

    SELECT t0.address, t1.address INTO got_token0, got_token1
    FROM uniswap_v3_pool p
    JOIN token t0 ON t0.id = p.token0_id
    JOIN token t1 ON t1.id = p.token1_id
    WHERE p.chain_id = 1 AND p.pool_address = '\x63818BbDd21E69bE108A23aC1E84cBf66399Bd7D'::bytea;
    IF got_token0 <> '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea
        OR got_token1 <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea THEN
        RAISE EXCEPTION 'pool #7 stETH/WETH token0/token1 mismatch: token0=%, token1=%', got_token0, got_token1;
    END IF;

    SELECT t0.address, t1.address INTO got_token0, got_token1
    FROM uniswap_v3_pool p
    JOIN token t0 ON t0.id = p.token0_id
    JOIN token t1 ON t1.id = p.token1_id
    WHERE p.chain_id = 1 AND p.pool_address = '\x7f13847459450236d2D233f1c08D742Ed69D2997'::bytea;
    IF got_token0 <> '\x49446A0874197839D15395B908328a74ccc96Bc0'::bytea
        OR got_token1 <> '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea THEN
        RAISE EXCEPTION 'pool #14 mstETH/wstETH token0/token1 mismatch (wstETH must be token1): token0=%, token1=%', got_token0, got_token1;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260701_100000_create_uniswap_v3_tables.sql')
ON CONFLICT (filename) DO NOTHING;
