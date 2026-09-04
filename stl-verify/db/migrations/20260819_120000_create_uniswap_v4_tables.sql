-- Uniswap V4 indexer tables (VEC-475).

CREATE TABLE IF NOT EXISTS uniswap_v4_pool_manager
(
    id                   BIGSERIAL PRIMARY KEY,
    chain_id             INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id          BIGINT      NOT NULL REFERENCES protocol (id),
    state_view_address   BYTEA       NOT NULL CHECK (octet_length(state_view_address) = 20),
    deploy_block         BIGINT      NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version   INT         NOT NULL DEFAULT 0,
    build_id             INT         NOT NULL DEFAULT 0,
    UNIQUE (chain_id, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_pool_manager_protocol ON uniswap_v4_pool_manager (protocol_id);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_pool_manager_pv_lookup
    ON uniswap_v4_pool_manager (chain_id, build_id);

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_pool_manager()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4pm|%s', NEW.chain_id), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_pool_manager
    WHERE chain_id = NEW.chain_id
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v4_pool_manager
        WHERE chain_id = NEW.chain_id;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_pool_manager
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_pool_manager();

COMMENT ON TABLE uniswap_v4_pool_manager IS
  '[Dimension] Append-only, versioned registry of the singleton Uniswap V4 PoolManager deployed on a chain, plus the StateView lens used to read pool state. The current row for a chain is the one with the highest processing_version; there is never an UPDATE. A later migration corrects a row by inserting a new version with a build_id different from the row it supersedes (seeds use 0): the build-aware trigger reuses the existing processing_version for an identical build_id, so a same-build re-insert is an idempotent no-op, not a correction.';
COMMENT ON COLUMN uniswap_v4_pool_manager.id IS
  'PK. Surrogate ID of this PoolManager VERSION. uniswap_v4_pool does not FK it: a pool joins the PoolManager by chain_id and picks the highest processing_version, so a corrected manager row does not orphan or re-pin the pools.';
COMMENT ON COLUMN uniswap_v4_pool_manager.chain_id IS
  'FK->chain.chain_id. Network the PoolManager is deployed on. V4 is a singleton, so a chain has exactly one live PoolManager: UNIQUE (chain_id, processing_version) allows only superseding versions of it, never two concurrent managers.';
COMMENT ON COLUMN uniswap_v4_pool_manager.protocol_id IS
  'FK->protocol.id. The UniswapV4 protocol row this deployment belongs to, and the SOLE source of the on-chain PoolManager address (protocol.address): every indexed V4 log is emitted by it, and a log from any other address is not a V4 pool event. Deliberately not duplicated as a column here -- a second copy could disagree with the row it FKs.';
COMMENT ON COLUMN uniswap_v4_pool_manager.state_view_address IS
  'On-chain StateView contract address, 20 bytes. Read-only lens over PoolManager''s transient/packed storage: getSlot0, getLiquidity, getFeeGrowthGlobals, getTickInfo, getTickBitmap. Deployed after the PoolManager, so its own deploy height is later than deploy_block.';
COMMENT ON COLUMN uniswap_v4_pool_manager.deploy_block IS
  'Documentation only. Block at which the PoolManager was deployed, hence a lower bound for every pool under it; no V4 state exists before this height. The indexer never reads it: the deploy gate runs off uniswap_v4_pool.deploy_block, which is the load-bearing one.';
COMMENT ON COLUMN uniswap_v4_pool_manager.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_pool_manager.processing_version IS
  'Audit. Correction version (ADR-0002): 0 for the first write of a chain under a build_id, bumped only when a later build rewrites the same chain; prior versions are retained. Order by processing_version DESC for the current row.';
COMMENT ON COLUMN uniswap_v4_pool_manager.build_id IS
  'Audit. ID of the build (code+config) that wrote this row; 0 for the migration seed. Never use it to pick the latest row.';

CREATE TABLE IF NOT EXISTS uniswap_v4_pool
(
    id                 BIGSERIAL PRIMARY KEY,
    chain_id           INT         NOT NULL REFERENCES chain (chain_id),
    pool_id            BYTEA       NOT NULL CHECK (octet_length(pool_id) = 32),
    currency0          BYTEA       NOT NULL CHECK (octet_length(currency0) = 20),
    currency1          BYTEA       NOT NULL CHECK (octet_length(currency1) = 20),
    currency0_token_id BIGINT      NOT NULL REFERENCES token (id),
    currency1_token_id BIGINT      NOT NULL REFERENCES token (id),
    fee                INT         NOT NULL CHECK (fee >= 0 AND (fee <= 1000000 OR fee = 8388608)),
    tick_spacing       INT         NOT NULL CHECK (tick_spacing BETWEEN 1 AND 32767),
    hooks              BYTEA       NOT NULL CHECK (octet_length(hooks) = 20),
    deploy_block       BIGINT      NOT NULL,
    snapshot_supported BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    UNIQUE (chain_id, pool_id, processing_version),
    CHECK (currency0 < currency1),
    CHECK (fee <> 8388608 OR NOT snapshot_supported)
);

-- No chain_id index: LoadPools' lookup is served by UNIQUE (chain_id, pool_id, processing_version).
CREATE INDEX IF NOT EXISTS idx_uniswap_v4_pool_currency0 ON uniswap_v4_pool (currency0_token_id);
CREATE INDEX IF NOT EXISTS idx_uniswap_v4_pool_currency1 ON uniswap_v4_pool (currency1_token_id);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_pool_pv_lookup
    ON uniswap_v4_pool (chain_id, pool_id, build_id);

-- pool_id is hex-encoded into the lock key so it cannot shift with bytea_output.
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_pool()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4p|%s|%s', NEW.chain_id, encode(NEW.pool_id, 'hex')), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_pool
    WHERE chain_id = NEW.chain_id
      AND pool_id  = NEW.pool_id
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v4_pool
        WHERE chain_id = NEW.chain_id
          AND pool_id  = NEW.pool_id;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_pool
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_pool();

COMMENT ON TABLE uniswap_v4_pool IS
  '[Dimension] Append-only, versioned registry of indexed Uniswap V4 pools, one row per PoolKey version; FK target for every uniswap_v4_* fact table. V4 pools have no contract of their own (the singleton PoolManager holds them all), so identity is the 32-byte PoolId, not an address. The current row for a PoolKey is the one with the highest processing_version; there is never an UPDATE. A later migration corrects a row by inserting a new version with a build_id different from the row it supersedes (seeds use 0): the build-aware trigger reuses the existing processing_version for an identical build_id, so a same-build re-insert is an idempotent no-op, not a correction. To aggregate a pool''s history across registry corrections, join uniswap_v4_pool and group by (chain_id, pool_id) -- never by uniswap_v4_pool.id, which changes on every correction.';
COMMENT ON COLUMN uniswap_v4_pool.id IS
  'PK. Surrogate ID of this pool VERSION; FK target for all uniswap_v4_* fact tables (whose pool_id column is this surrogate, NOT the 32-byte on-chain PoolId below). A corrected registry row is a NEW version with a NEW id, so every fact row keeps pointing at the exact registry version that was in force when it was written.';
COMMENT ON COLUMN uniswap_v4_pool.chain_id IS
  'FK->chain.chain_id. Network the pool lives on. The PoolManager and StateView addresses are resolved by joining the highest-processing_version uniswap_v4_pool_manager row for this chain, rather than FKing one manager version.';
COMMENT ON COLUMN uniswap_v4_pool.pool_id IS
  'On-chain PoolId, 32 bytes: keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks)), the topics[1] of every pool-keyed PoolManager event. Identical across chains for an identical PoolKey, hence the natural key is (chain_id, pool_id).';
COMMENT ON COLUMN uniswap_v4_pool.currency0 IS
  'PoolKey.currency0 verbatim as it appears on chain, 20 bytes: address(0) means native ETH, which has no ERC-20 contract. Always the lower of the two addresses (CHECK currency0 < currency1), as PoolKey requires.';
COMMENT ON COLUMN uniswap_v4_pool.currency1 IS
  'PoolKey.currency1 verbatim as it appears on chain, 20 bytes; always the higher of the two addresses.';
COMMENT ON COLUMN uniswap_v4_pool.currency0_token_id IS
  'FK->token.id. Resolves currency0''s symbol/decimals. address(0) (native ETH) maps to the 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE ETH placeholder token, the same convention curve_pool_coin uses; for every other currency the token''s address equals currency0 byte for byte. Never resolved by symbol.';
COMMENT ON COLUMN uniswap_v4_pool.currency1_token_id IS
  'FK->token.id. Resolves currency1''s symbol/decimals, with the same address(0) -> 0xEeee... native-ETH mapping as currency0_token_id.';
COMMENT ON COLUMN uniswap_v4_pool.fee IS
  'PoolKey.fee, hundredths of a bip (1e6 = 100%; e.g. 3000 = 0.30%, 100 = 0.01%). Static fees are <= 1000000. The sentinel 8388608 (0x800000) is not a fee: it flags a DYNAMIC-LP-fee pool whose effective fee the hook sets at runtime through updateDynamicLPFee, which emits no event -- so such a pool is registered with snapshot_supported = FALSE and only its per-swap fee is observable, never a uniswap_v4_pool_state.lp_fee snapshot. The fee is hashed into pool_id, so it is immutable for a PoolKey: a superseding registry version can never change it.';
COMMENT ON COLUMN uniswap_v4_pool.tick_spacing IS
  'PoolKey.tickSpacing: minimum tick granularity for this pool (1..32767); ticks usable by positions must be a multiple of this value. Chosen freely per pool in V4, not derived from the fee tier as in V3.';
COMMENT ON COLUMN uniswap_v4_pool.hooks IS
  'PoolKey.hooks, 20 bytes: the hook contract attached to this pool, or the zero address when the pool has no hooks. The hook''s PERMISSION FLAGS are encoded in the low 14 bits of the address itself (beforeSwap, afterSwap, delta-returning, ...), so the address doubles as the capability set. A dynamic fee is not among them: it is signalled by PoolKey.fee = 8388608 (0x800000).';
COMMENT ON COLUMN uniswap_v4_pool.snapshot_supported IS
  'Configuration (curated, load-bearing). Gates the STATE and TICK snapshot path only: TRUE = the indexer reads uniswap_v4_pool_state and uniswap_v4_tick rows for this pool; FALSE = the pool stays registered and its logs are still decoded into uniswap_v4_swap / uniswap_v4_liquidity_event / uniswap_v4_pool_event and mirrored into protocol_event, but no state or tick row is ever written for it. Curated the way curve_pool.has_a_precise is, rather than derived: set FALSE for a dynamic-LP-fee pool (fee = 8388608), whose lp_fee updateDynamicLPFee rewrites emitting no event, so a snapshot would silently go stale between touches -- until VEC-573 gives lp_fee a refresh path. A change is a new version row, never an UPDATE. A CHECK enforces the one direction that is always wrong (fee = 8388608 with snapshot_supported = TRUE); VEC-573''s lp_fee refresh path will have to DROP that constraint before it can flip such a pool back on.';
COMMENT ON COLUMN uniswap_v4_pool.deploy_block IS
  'Configuration (load-bearing). Block at which the pool was created (its Initialize event; a V4 pool has no contract deployment of its own), used to gate snapshot reads. NOT NULL: a NULL would defeat the reorg deploy-gate, so the column makes one unrepresentable. Must be a lower bound of the true initialize block (<= actual height): DueSet hard-errors ("registry bug") when a touched pool reports deploy_block greater than the processed block, and skips sweep scheduling before this height.';
COMMENT ON COLUMN uniswap_v4_pool.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_pool.processing_version IS
  'Audit. Correction version (ADR-0002): 0 for the first write of a (chain_id, pool_id) under a build_id, bumped only when a later build rewrites the same key; prior versions are retained. Order by processing_version DESC for the current row.';
COMMENT ON COLUMN uniswap_v4_pool.build_id IS
  'Audit. ID of the build (code+config) that wrote this row; 0 for the migration seed. Never use it to pick the latest row.';

CREATE TABLE IF NOT EXISTS uniswap_v4_pool_state
(
    pool_id                 BIGINT      NOT NULL REFERENCES uniswap_v4_pool (id),
    block_number            BIGINT      NOT NULL,
    block_version           INT         NOT NULL DEFAULT 0,
    block_timestamp         TIMESTAMPTZ NOT NULL,
    sqrt_price_x96          NUMERIC     NOT NULL,
    tick                    INT         NOT NULL CHECK (tick BETWEEN -887272 AND 887272),
    protocol_fee            INT         NOT NULL CHECK (protocol_fee BETWEEN 0 AND 16777215
                                                    AND (protocol_fee & 4095) <= 1000
                                                    AND (protocol_fee >> 12) <= 1000),
    lp_fee                  INT         NOT NULL CHECK (lp_fee BETWEEN 0 AND 1000000),
    liquidity               NUMERIC     NOT NULL,
    fee_growth_global0_x128 NUMERIC     NOT NULL,
    fee_growth_global1_x128 NUMERIC     NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version      INT         NOT NULL DEFAULT 0,
    build_id                INT         NOT NULL DEFAULT 0,
    CHECK (sqrt_price_x96 > 0 OR (sqrt_price_x96 = 0 AND block_version > 0)),
    -- TimescaleDB requires the partition column in every unique index on a hypertable.
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day',
    -- Without columnstore = false, tsdb.hypertable installs its own 1-day policy
    -- and add_compression_policy below returns -1 rather than replacing it.
    tsdb.columnstore = false
);

ALTER TABLE uniswap_v4_pool_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v4_pool_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v4_pool_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v4_pool_state';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_pool_state_pv_lookup
    ON uniswap_v4_pool_state (pool_id, block_number, block_version, build_id);

-- force_custom_plan (VEC-541): once plpgsql caches a generic plan the per-row
-- lookups stop pruning chunks and fan out over every one.
-- The version rule lives in a function the INSERT calls in its VALUES list and the
-- trigger delegates to, so both share one rule and one advisory-lock key (ADR-0002 §3).
-- The trigger alone is not enough: once a chunk is columnstored, TimescaleDB resolves
-- ON CONFLICT before row triggers fire, so a version left to the trigger reaches the
-- arbiter as DEFAULT 0, matches the pv=0 row already there, and the correction row is
-- silently discarded (VEC-615; measured in 20260821_120000_morpho_adapter_state_version_function.sql).
-- VOLATILE is load-bearing: a STABLE function would read the calling statement's
-- snapshot, and a writer released from the lock would recompute the version the writer
-- it waited for just used. The same three sibling tables below follow this one.
CREATE OR REPLACE FUNCTION next_processing_version_uniswap_v4_pool_state(
    p_pool_id       BIGINT,
    p_block_number  BIGINT,
    p_block_version INT,
    p_build_id      INT)
RETURNS INT
VOLATILE
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4ps|%s|%s|%s', p_pool_id, p_block_number, p_block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_pool_state
    WHERE pool_id       = p_pool_id
      AND block_number  = p_block_number
      AND block_version = p_block_version
      AND build_id      = p_build_id
    LIMIT 1;

    IF FOUND THEN
        RETURN existing_ver;
    END IF;

    SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
    FROM uniswap_v4_pool_state
    WHERE pool_id       = p_pool_id
          AND block_number  = p_block_number
          AND block_version = p_block_version;
    RETURN max_ver + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION next_processing_version_uniswap_v4_pool_state(BIGINT, BIGINT, INT, INT) IS
  'Returns the processing_version a uniswap_v4_pool_state row at (pool, block, block_version) must carry for build_id: the version that build already wrote there, else MAX+1. Takes the key''s advisory lock (ADR-0002 §3) for the transaction. Call it in the INSERT''s VALUES list: on a columnstored chunk ON CONFLICT is resolved before row triggers fire, so a version left to the trigger is DEFAULT 0 there and the correction row is silently discarded (VEC-615).';

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_pool_state()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
BEGIN
    NEW.processing_version := next_processing_version_uniswap_v4_pool_state(
        NEW.pool_id, NEW.block_number, NEW.block_version, NEW.build_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_pool_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_pool_state();

COMMENT ON TABLE uniswap_v4_pool_state IS
  '[Hypertable] Per-touched-block snapshot of a pool''s slot0 / liquidity / fee-growth state read through StateView, taken only on blocks that touch the pool (V4 state is piecewise-constant; no periodic heartbeat). No balance columns: the singleton PoolManager holds every pool''s currencies in one pot, so a per-pool ERC-20 balance does not exist. No TWAP columns: V4 core has no oracle (a hook may provide one). Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v4_pool_state.pool_id IS
  'PK, FK->uniswap_v4_pool.id. Surrogate pool ID the snapshot is for (not the 32-byte on-chain PoolId). To aggregate a pool''s history across registry corrections, join uniswap_v4_pool and group by (chain_id, pool_id) -- never by uniswap_v4_pool.id, which changes on every correction.';
COMMENT ON COLUMN uniswap_v4_pool_state.block_number IS
  'PK. Block height at which the snapshot was read (via multicall pinned to this block''s hash).';
COMMENT ON COLUMN uniswap_v4_pool_state.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v4_pool_state.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v4_pool_state.sqrt_price_x96 IS
  'StateView.getSlot0().sqrtPriceX96: Q64.96 fixed point. price(currency1/currency0) = (sqrt_price_x96/2^96)^2, then adjust by 10^(currency0.decimals-currency1.decimals). 0 means the PoolManager does not know the PoolId -- StateView returns all zeros for an unknown one rather than reverting -- and the CHECK allows it only at block_version > 0, where it is the legitimate tombstone a reorg re-read of a pool whose Initialize was orphaned writes to supersede the orphaned fork''s snapshot. A 0 at block_version 0 means the registry row is wrong and is refused.';
COMMENT ON COLUMN uniswap_v4_pool_state.tick IS
  'StateView.getSlot0().tick: current pool tick (plain integer, -887272..887272); price(currency1/currency0) = 1.0001^tick before decimal adjustment.';
COMMENT ON COLUMN uniswap_v4_pool_state.protocol_fee IS
  'StateView.getSlot0().protocolFee: the PACKED uint24 protocol fee, not a single rate. Low 12 bits = the zeroForOne fee, high 12 bits = the oneForZero fee, each in hundredths of a bip and each capped at 1000 (0.1%). Taken off the swap input before the LP fee.';
COMMENT ON COLUMN uniswap_v4_pool_state.lp_fee IS
  'StateView.getSlot0().lpFee: the LP fee currently in force, hundredths of a bip (<= 1000000). For a static-fee pool this equals uniswap_v4_pool.fee. A dynamic-fee pool (uniswap_v4_pool.fee = 8388608) has no such guarantee -- the hook sets the fee via updateDynamicLPFee, which emits NO event, so this column would only be refreshed on blocks that otherwise touch the pool -- which is why such a pool is registered with uniswap_v4_pool.snapshot_supported = FALSE and produces no row here at all until lp_fee has a refresh path.';
COMMENT ON COLUMN uniswap_v4_pool_state.liquidity IS
  'StateView.getLiquidity(): in-range raw liquidity L active at the current tick (sqrt(xy) scaled by the pool''s currency decimals; not comparable across pools with different decimals).';
COMMENT ON COLUMN uniswap_v4_pool_state.fee_growth_global0_x128 IS
  'StateView.getFeeGrowthGlobals().feeGrowthGlobal0: cumulative currency0 fees earned per unit of liquidity over the pool''s lifetime, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v4_pool_state.fee_growth_global1_x128 IS
  'StateView.getFeeGrowthGlobals().feeGrowthGlobal1: cumulative currency1 fees earned per unit of liquidity over the pool''s lifetime, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v4_pool_state.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_pool_state.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v4_pool_state.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

CREATE TABLE IF NOT EXISTS uniswap_v4_swap
(
    pool_id            BIGINT      NOT NULL REFERENCES uniswap_v4_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    tx_hash            BYTEA       NOT NULL,
    log_index          INT         NOT NULL,
    sender             BYTEA       NOT NULL,
    amount0            NUMERIC     NOT NULL,
    amount1            NUMERIC     NOT NULL,
    sqrt_price_x96     NUMERIC     NOT NULL CHECK (sqrt_price_x96 > 0),
    liquidity          NUMERIC     NOT NULL,
    tick               INT         NOT NULL CHECK (tick BETWEEN -887272 AND 887272),
    fee                INT         NOT NULL CHECK (fee BETWEEN 0 AND 1000000),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day',
    tsdb.columnstore = false
);

ALTER TABLE uniswap_v4_swap SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v4_swap', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v4_swap', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v4_swap';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_swap_pv_lookup
    ON uniswap_v4_swap (pool_id, block_number, block_version, log_index, build_id);

-- Same rule as uniswap_v4_pool_state's (see the note there): the INSERT calls it, the
-- trigger delegates to it, and a columnstored chunk cannot drop a correction (VEC-615).
CREATE OR REPLACE FUNCTION next_processing_version_uniswap_v4_swap(
    p_pool_id       BIGINT,
    p_block_number  BIGINT,
    p_block_version INT,
    p_log_index     INT,
    p_build_id      INT)
RETURNS INT
VOLATILE
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4s|%s|%s|%s|%s', p_pool_id, p_block_number, p_block_version, p_log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_swap
    WHERE pool_id       = p_pool_id
      AND block_number  = p_block_number
      AND block_version = p_block_version
      AND log_index     = p_log_index
      AND build_id      = p_build_id
    LIMIT 1;

    IF FOUND THEN
        RETURN existing_ver;
    END IF;

    SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
    FROM uniswap_v4_swap
    WHERE pool_id       = p_pool_id
          AND block_number  = p_block_number
          AND block_version = p_block_version
          AND log_index     = p_log_index;
    RETURN max_ver + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION next_processing_version_uniswap_v4_swap(BIGINT, BIGINT, INT, INT, INT) IS
  'Returns the processing_version a uniswap_v4_swap row at (pool, block, block_version, log_index) must carry for build_id: the version that build already wrote there, else MAX+1. Takes the key''s advisory lock (ADR-0002 §3) for the transaction. Call it in the INSERT''s VALUES list: on a columnstored chunk ON CONFLICT is resolved before row triggers fire, so a version left to the trigger is DEFAULT 0 there and the correction row is silently discarded (VEC-615).';

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_swap()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
BEGIN
    NEW.processing_version := next_processing_version_uniswap_v4_swap(
        NEW.pool_id, NEW.block_number, NEW.block_version, NEW.log_index, NEW.build_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_swap
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_swap();

COMMENT ON TABLE uniswap_v4_swap IS
  '[Hypertable] One row per on-chain PoolManager Swap event. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v4_swap.pool_id IS
  'PK, FK->uniswap_v4_pool.id. Surrogate pool ID the swap belongs to (resolved from the event''s topics[1] PoolId). To aggregate a pool''s history across registry corrections, join uniswap_v4_pool and group by (chain_id, pool_id) -- never by uniswap_v4_pool.id, which changes on every correction.';
COMMENT ON COLUMN uniswap_v4_swap.block_number IS
  'PK. Block height at which the swap was emitted.';
COMMENT ON COLUMN uniswap_v4_swap.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v4_swap.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v4_swap.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v4_swap.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v4_swap.sender IS
  'Event field `sender`: msg.sender of the PoolManager.swap call -- a router, or the hook contract itself when a hook initiates the swap, 20 bytes. Not the end user.';
COMMENT ON COLUMN uniswap_v4_swap.amount0 IS
  'Swap BalanceDelta for currency0 from the SWAPPER''s perspective, as emitted, raw native decimals of currency0: v4-core applies swapDelta to msg.sender, so NEGATIVE = the swapper owes the PoolManager, POSITIVE = the PoolManager owes the swapper. PoolManager emits this delta BEFORE afterSwap applies any hook delta, so it equals what the swapper actually settled only for pools whose hooks address carries no *_RETURNS_DELTA permission. This is the INVERSE of uniswap_v3_swap.amount0, which is signed from the pool''s side; negate to compare the two.';
COMMENT ON COLUMN uniswap_v4_swap.amount1 IS
  'Swap BalanceDelta for currency1 from the SWAPPER''s perspective, as emitted, raw native decimals of currency1 (negative = the swapper owes the PoolManager, positive = the PoolManager owes the swapper); same pre-hook-delta caveat and same inverted convention as amount0.';
COMMENT ON COLUMN uniswap_v4_swap.sqrt_price_x96 IS
  'Pool sqrtPriceX96 immediately after the swap, Q64.96 fixed point (see uniswap_v4_pool_state.sqrt_price_x96 for the price conversion).';
COMMENT ON COLUMN uniswap_v4_swap.liquidity IS
  'Pool in-range liquidity immediately after the swap, raw L (see uniswap_v4_pool_state.liquidity for the scale).';
COMMENT ON COLUMN uniswap_v4_swap.tick IS
  'Pool tick immediately after the swap (plain integer; price = 1.0001^tick before decimal adjustment).';
COMMENT ON COLUMN uniswap_v4_swap.fee IS
  'Event field `fee`: the swap fee actually charged on this swap, hundredths of a bip (<= 1000000). It is the LP fee in force combined with the direction''s protocol fee when one is set (protocolFee + lpFee - protocolFee*lpFee/1e6), so it is >= the LP fee in force for this swap. It is NOT comparable to uniswap_v4_pool_state.lp_fee: a hook can override the fee for a single swap, and a dynamic fee moves between snapshots.';
COMMENT ON COLUMN uniswap_v4_swap.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_swap.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v4_swap.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

CREATE TABLE IF NOT EXISTS uniswap_v4_liquidity_event
(
    pool_id            BIGINT      NOT NULL REFERENCES uniswap_v4_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    tx_hash            BYTEA       NOT NULL,
    log_index          INT         NOT NULL,
    sender             BYTEA       NOT NULL,
    tick_lower         INT         NOT NULL CHECK (tick_lower BETWEEN -887272 AND 887272),
    tick_upper         INT         NOT NULL CHECK (tick_upper BETWEEN -887272 AND 887272),
    liquidity_delta    NUMERIC     NOT NULL,
    salt               BYTEA       NOT NULL CHECK (octet_length(salt) = 32),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    CHECK (tick_lower < tick_upper),
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day',
    tsdb.columnstore = false
);

ALTER TABLE uniswap_v4_liquidity_event SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v4_liquidity_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v4_liquidity_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v4_liquidity_event';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_liquidity_event_pv_lookup
    ON uniswap_v4_liquidity_event (pool_id, block_number, block_version, log_index, build_id);

-- Same rule as uniswap_v4_pool_state's (see the note there): the INSERT calls it, the
-- trigger delegates to it, and a columnstored chunk cannot drop a correction (VEC-615).
CREATE OR REPLACE FUNCTION next_processing_version_uniswap_v4_liquidity_event(
    p_pool_id       BIGINT,
    p_block_number  BIGINT,
    p_block_version INT,
    p_log_index     INT,
    p_build_id      INT)
RETURNS INT
VOLATILE
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4le|%s|%s|%s|%s', p_pool_id, p_block_number, p_block_version, p_log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_liquidity_event
    WHERE pool_id       = p_pool_id
      AND block_number  = p_block_number
      AND block_version = p_block_version
      AND log_index     = p_log_index
      AND build_id      = p_build_id
    LIMIT 1;

    IF FOUND THEN
        RETURN existing_ver;
    END IF;

    SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
    FROM uniswap_v4_liquidity_event
    WHERE pool_id       = p_pool_id
          AND block_number  = p_block_number
          AND block_version = p_block_version
          AND log_index     = p_log_index;
    RETURN max_ver + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION next_processing_version_uniswap_v4_liquidity_event(BIGINT, BIGINT, INT, INT, INT) IS
  'Returns the processing_version a uniswap_v4_liquidity_event row at (pool, block, block_version, log_index) must carry for build_id: the version that build already wrote there, else MAX+1. Takes the key''s advisory lock (ADR-0002 §3) for the transaction. Call it in the INSERT''s VALUES list: on a columnstored chunk ON CONFLICT is resolved before row triggers fire, so a version left to the trigger is DEFAULT 0 there and the correction row is silently discarded (VEC-615).';

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_liquidity_event()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
BEGIN
    NEW.processing_version := next_processing_version_uniswap_v4_liquidity_event(
        NEW.pool_id, NEW.block_number, NEW.block_version, NEW.log_index, NEW.build_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_liquidity_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_liquidity_event();

COMMENT ON TABLE uniswap_v4_liquidity_event IS
  '[Hypertable] One row per PoolManager ModifyLiquidity event (V4''s single add/remove/poke primitive; there is no Mint/Burn/Collect split). Carries no token amounts: V4 settles through flash accounting, so the moved amounts are not in the event. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.pool_id IS
  'PK, FK->uniswap_v4_pool.id. Surrogate pool ID the event belongs to (resolved from the event''s topics[1] PoolId). To aggregate a pool''s history across registry corrections, join uniswap_v4_pool and group by (chain_id, pool_id) -- never by uniswap_v4_pool.id, which changes on every correction.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.block_number IS
  'PK. Block height at which the event was emitted.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v4_liquidity_event.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.sender IS
  'Event field `sender`: the address that called PoolManager.modifyLiquidity (typically the PositionManager), 20 bytes. Part of the position identity (sender, tick_lower, tick_upper, salt), not necessarily the beneficial owner.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.tick_lower IS
  'Lower tick bound of the position''s liquidity range (plain integer, inclusive; a multiple of the pool''s tick_spacing).';
COMMENT ON COLUMN uniswap_v4_liquidity_event.tick_upper IS
  'Upper tick bound of the position''s liquidity range (plain integer, exclusive; a multiple of the pool''s tick_spacing).';
COMMENT ON COLUMN uniswap_v4_liquidity_event.liquidity_delta IS
  'Signed raw liquidity L added (positive) or removed (negative) from the range (see uniswap_v4_pool_state.liquidity for the scale). Legitimately 0 for a "poke": a call made only to settle accrued fees on an unchanged position.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.salt IS
  'Event field `salt`, 32 bytes: the caller-chosen discriminator that lets one sender hold several independent positions over the same tick range. All zeros when the caller supplied none.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v4_liquidity_event.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

CREATE TABLE IF NOT EXISTS uniswap_v4_tick
(
    pool_id                  BIGINT      NOT NULL REFERENCES uniswap_v4_pool (id),
    tick                     INT         NOT NULL CHECK (tick BETWEEN -887272 AND 887272),
    block_number             BIGINT      NOT NULL,
    block_version            INT         NOT NULL DEFAULT 0,
    block_timestamp          TIMESTAMPTZ NOT NULL,
    liquidity_gross          NUMERIC     NOT NULL,
    liquidity_net            NUMERIC     NOT NULL,
    fee_growth_outside0_x128 NUMERIC     NOT NULL,
    fee_growth_outside1_x128 NUMERIC     NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version       INT         NOT NULL DEFAULT 0,
    build_id                 INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, tick, block_number, block_version, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_tick_pv_lookup
    ON uniswap_v4_tick (pool_id, tick, block_number, block_version, build_id);

-- Serves TicksForPoolAtBlock (reorg path): in the PK and the pv-lookup index
-- tick sits between the filtered columns, so block_number rechecks every entry.
CREATE INDEX IF NOT EXISTS idx_uniswap_v4_tick_block_lookup
    ON uniswap_v4_tick (pool_id, block_number, tick);

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_tick()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4t|%s|%s|%s|%s', NEW.pool_id, NEW.tick, NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_tick
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
        FROM uniswap_v4_tick
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
    BEFORE INSERT ON uniswap_v4_tick
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_tick();

COMMENT ON TABLE uniswap_v4_tick IS
  '[Operational] Append-on-change authoritative per-tick state from StateView.getTickInfo(poolId, tick) reads. A new row is written only when a touched tick''s state changes (or on first-seen baseline enumeration for the pool). Deliberately a plain table, not a hypertable, under the append-on-change exception in db/migrations/AGENTS.md: every write first reads the latest row per (pool_id, tick) at or below a block height, and that read admits no block_timestamp bound -- the height bound is load-bearing for out-of-order backfill writes, and a tick can sit untouched arbitrarily long -- so a block_timestamp-partitioned hypertable would prune no chunks on the per-block path (VEC-541), and S3 tiering would go further and hide the prior row from the change comparison (timescaledb.enable_tiered_reads defaults off), making the writer append a change that never happened. Unbounded row growth is the price, covered by a row-growth alert and a documented conversion path rather than by compression. There is no `initialized` column: V4''s TickInfo struct has no such field, so a tick is initialized exactly while liquidity_gross > 0, and an all-zero row records a tick that is uninitialized or has been cleared. fee_growth_outside0_x128/fee_growth_outside1_x128 are STALE after swap crossings -- see their own comments.';
COMMENT ON COLUMN uniswap_v4_tick.pool_id IS
  'PK, FK->uniswap_v4_pool.id. Surrogate pool ID the tick belongs to (not the 32-byte on-chain PoolId). To aggregate a pool''s history across registry corrections, join uniswap_v4_pool and group by (chain_id, pool_id) -- never by uniswap_v4_pool.id, which changes on every correction.';
COMMENT ON COLUMN uniswap_v4_tick.tick IS
  'PK. Tick index (plain integer, a multiple of the pool''s tick_spacing); price = 1.0001^tick before decimal adjustment.';
COMMENT ON COLUMN uniswap_v4_tick.block_number IS
  'PK. Block height at which this tick state was read.';
COMMENT ON COLUMN uniswap_v4_tick.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v4_tick.block_timestamp IS
  'Block timestamp (UTC) at which this tick state was read.';
COMMENT ON COLUMN uniswap_v4_tick.liquidity_gross IS
  'TickInfo.liquidityGross: total raw liquidity L referencing this tick as a boundary, regardless of direction (see uniswap_v4_pool_state.liquidity for the scale). Doubles as the initialized flag: > 0 means initialized, 0 means uninitialized or cleared.';
COMMENT ON COLUMN uniswap_v4_tick.liquidity_net IS
  'TickInfo.liquidityNet: signed raw liquidity L added (positive) or removed (negative) when the pool price crosses this tick left-to-right; sign flips for a right-to-left crossing.';
COMMENT ON COLUMN uniswap_v4_tick.fee_growth_outside0_x128 IS
  'TickInfo.feeGrowthOutside0X128: currency0 fee growth on the outside of this tick at the time it was last crossed, Q128.128 fixed point. Only meaningful relative to fee_growth_global0_x128, never absolute. STALE after swap crossings: the row is refreshed only when a ModifyLiquidity touches this tick or on the pool''s first-seen baseline enumeration, never on a swap that crosses it, so do not derive feeGrowthInside from it without an independent read at the block of interest.';
COMMENT ON COLUMN uniswap_v4_tick.fee_growth_outside1_x128 IS
  'TickInfo.feeGrowthOutside1X128: currency1 fee growth on the outside of this tick at the time it was last crossed, Q128.128 fixed point. STALE after swap crossings for the same reason as fee_growth_outside0_x128; do not derive feeGrowthInside from it without an independent read.';
COMMENT ON COLUMN uniswap_v4_tick.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_tick.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v4_tick.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

CREATE TABLE IF NOT EXISTS uniswap_v4_pool_event
(
    pool_id            BIGINT      NOT NULL REFERENCES uniswap_v4_pool (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    tx_hash            BYTEA       NOT NULL,
    log_index          INT         NOT NULL,
    event_name         TEXT        NOT NULL CHECK (event_name IN (
        'initialize', 'donate', 'protocol_fee_updated'
    )),
    params             JSONB       NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, block_timestamp, block_number, block_version, log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day',
    tsdb.columnstore = false
);

ALTER TABLE uniswap_v4_pool_event SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v4_pool_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v4_pool_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v4_pool_event';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_pool_event_pv_lookup
    ON uniswap_v4_pool_event (pool_id, block_number, block_version, log_index, build_id);

-- Same rule as uniswap_v4_pool_state's (see the note there): the INSERT calls it, the
-- trigger delegates to it, and a columnstored chunk cannot drop a correction (VEC-615).
CREATE OR REPLACE FUNCTION next_processing_version_uniswap_v4_pool_event(
    p_pool_id       BIGINT,
    p_block_number  BIGINT,
    p_block_version INT,
    p_log_index     INT,
    p_build_id      INT)
RETURNS INT
VOLATILE
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4pe|%s|%s|%s|%s', p_pool_id, p_block_number, p_block_version, p_log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_pool_event
    WHERE pool_id       = p_pool_id
      AND block_number  = p_block_number
      AND block_version = p_block_version
      AND log_index     = p_log_index
      AND build_id      = p_build_id
    LIMIT 1;

    IF FOUND THEN
        RETURN existing_ver;
    END IF;

    SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
    FROM uniswap_v4_pool_event
    WHERE pool_id       = p_pool_id
          AND block_number  = p_block_number
          AND block_version = p_block_version
          AND log_index     = p_log_index;
    RETURN max_ver + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION next_processing_version_uniswap_v4_pool_event(BIGINT, BIGINT, INT, INT, INT) IS
  'Returns the processing_version a uniswap_v4_pool_event row at (pool, block, block_version, log_index) must carry for build_id: the version that build already wrote there, else MAX+1. Takes the key''s advisory lock (ADR-0002 §3) for the transaction. Call it in the INSERT''s VALUES list: on a columnstored chunk ON CONFLICT is resolved before row triggers fire, so a version left to the trigger is DEFAULT 0 there and the correction row is silently discarded (VEC-615).';

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_pool_event()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
BEGIN
    NEW.processing_version := next_processing_version_uniswap_v4_pool_event(
        NEW.pool_id, NEW.block_number, NEW.block_version, NEW.log_index, NEW.build_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_pool_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_pool_event();

COMMENT ON TABLE uniswap_v4_pool_event IS
  '[Hypertable] Decoded low-frequency pool-keyed PoolManager events (Initialize, Donate, ProtocolFeeUpdated); typed counterpart to the raw protocol_event mirror. Singleton-wide governance events (OwnershipTransferred, ProtocolFeeControllerUpdated) are not pool-keyed and live only in protocol_event. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v4_pool_event.pool_id IS
  'PK, FK->uniswap_v4_pool.id. Surrogate pool ID the event belongs to (resolved from the event''s topics[1] PoolId). To aggregate a pool''s history across registry corrections, join uniswap_v4_pool and group by (chain_id, pool_id) -- never by uniswap_v4_pool.id, which changes on every correction.';
COMMENT ON COLUMN uniswap_v4_pool_event.block_number IS
  'PK. Block height at which the event was emitted.';
COMMENT ON COLUMN uniswap_v4_pool_event.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v4_pool_event.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v4_pool_event.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v4_pool_event.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v4_pool_event.event_name IS
  'Decoded event type: one of initialize, donate, protocol_fee_updated.';
COMMENT ON COLUMN uniswap_v4_pool_event.params IS
  'JSONB of decoded event fields keyed by name, indexed arguments included. Every key set starts with id, the 32-byte on-chain PoolId as lowercase 0x hex (joinable against uniswap_v4_pool.pool_id via decode(substring(params->>''id'' from 3), ''hex'')). Keys per event_name: initialize {id,currency0,currency1,fee,tickSpacing,hooks,sqrtPriceX96,tick} (addresses as lowercase 0x hex, fee in hundredths of a bip, sqrtPriceX96 Q64.96, tick a plain integer); donate {id,sender,amount0,amount1} (raw native decimals of the respective currency, always non-negative -- the pool receives them); protocol_fee_updated {id,protocolFee} (the packed uint24, see uniswap_v4_pool_state.protocol_fee for the bit layout).';
COMMENT ON COLUMN uniswap_v4_pool_event.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_pool_event.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v4_pool_event.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_pool_manager FROM stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_pool FROM stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_pool_state FROM stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_swap FROM stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_liquidity_event FROM stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_tick FROM stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_pool_event FROM stl_readwrite;

-- Every PoolKey re-read from its own Initialize log on Ethereum mainnet
-- (2026-08-19); the migration integration test re-derives each PoolId.

INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (
    1,
    '\x000000000004444c5dc75cB358380D2e3dE08A90'::bytea,
    'UniswapV4',
    'dex',
    21688329,
    NOW(),
    '{"role":"pool_manager"}'::jsonb
)
ON CONFLICT (chain_id, address) DO NOTHING;

-- Symbols and decimals read from each contract on mainnet. Native ETH is absent:
-- address(0) is already another worker's "no token" sentinel (symbol '', decimals
-- 0), so V4 maps it to the 0xEeee... placeholder the Curve migration seeds.
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, 'wstETH', 18),
    (1, '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea, 'stETH', 18),
    (1, '\xBe9895146f7AF43049ca1c1AE358B0541Ea49704'::bytea, 'cbETH', 18),
    (1, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea, 'WBTC', 8),
    (1, '\x111111111117dC0aa78b770fA6A738034120C302'::bytea, '1INCH', 18),
    (1, '\xf951E335afb289353dc249e82926178EaC7DEd78'::bytea, 'swETH', 18),
    (1, '\x93ED3FBe21207Ec2E8f2d3c3de6e058Cb73Bc04d'::bytea, 'PNK', 18),
    (1, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, 'USDC', 6),
    (1, '\xae78736Cd615f374D3085123A210448E74Fc6393'::bytea, 'rETH', 18),
    (1, '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea, 'sUSDS', 18),
    (1, '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, 'PYUSD', 6),
    (1, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, 'USDS', 18),
    (1, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, 'USDT', 6),
    (1, '\x56072C95FAA701256059aa122697B133aDEd9279'::bytea, 'SKY', 18),
    (1, '\x68749665FF8D2d112Fa859AA293F07A622782F38'::bytea, 'XAUt', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO uniswap_v4_pool_manager (chain_id, protocol_id, state_view_address, deploy_block)
SELECT 1, pr.id,
       '\x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227'::bytea,
       21688329
FROM protocol pr
WHERE pr.chain_id = 1 AND pr.address = '\x000000000004444c5dc75cB358380D2e3dE08A90'::bytea
ON CONFLICT (chain_id, processing_version) DO NOTHING;

WITH seed (pool_id, currency0, currency1, fee, tick_spacing, hooks, deploy_block) AS (
    VALUES
        ('\x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76'::bytea, '\x0000000000000000000000000000000000000000'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,   100,   1, '\x0000000000000000000000000000000000000000'::bytea, 21743144::bigint),
        ('\xbc21dd4a44766fadfd6447f4b222a6185dcc2e6a3b15eb79e0cc637e30e7e97f'::bytea, '\x0000000000000000000000000000000000000000'::bytea, '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea,   800,  16, '\x0000000000000000000000000000000000000000'::bytea, 25199556),
        ('\x056c3c5d8aceeb400b674c27db54e4a90d2f468d786582571ee9394b4c5e3a11'::bytea, '\x0000000000000000000000000000000000000000'::bytea, '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea,  1000,  20, '\x0000000000000000000000000000000000000000'::bytea, 25199299),
        ('\x9e0032112d580d8f45a0e356c48148846a3306a991da398dde4f92071e853d09'::bytea, '\x0000000000000000000000000000000000000000'::bytea, '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea,  2500,  50, '\x0000000000000000000000000000000000000000'::bytea, 24857024),
        ('\xaea49399167b73015a01e9ca9754c2b438e8aaf42d911468443540eea235735e'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, '\xBe9895146f7AF43049ca1c1AE358B0541Ea49704'::bytea,   500,  10, '\x0000000000000000000000000000000000000000'::bytea, 25494004),
        ('\x58299b9ad89104f189f5efcdf4910615cb9e3296afb0c5a1d1d3befdd1bf7ae4'::bytea, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,  2500,  50, '\x0000000000000000000000000000000000000000'::bytea, 23188451),
        ('\x1d6ebf506eacf0e98a8c4566687380ddf1601192acd9bce29feeaf0c0245ea6f'::bytea, '\x111111111117dC0aa78b770fA6A738034120C302'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, 10000, 200, '\x0000000000000000000000000000000000000000'::bytea, 24363425),
        ('\xe7c7bbac1cb017812f5129246ba1ace4aeaadb96fed67cc43d94ac2c6c5048d8'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, '\xf951E335afb289353dc249e82926178EaC7DEd78'::bytea,  3000,  60, '\x0000000000000000000000000000000000000000'::bytea, 23796248),
        ('\xbb78d828ded564d7dfcf041eb1316200e4ec5380dc601c7b4872c0a2727a580e'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, '\x93ED3FBe21207Ec2E8f2d3c3de6e058Cb73Bc04d'::bytea,  3000,  60, '\x0000000000000000000000000000000000000000'::bytea, 24284325),
        ('\x84a2753546221b6aedf1b96098235f8eb5494b1ddd7d57583d99b2d174cd2103'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,  3000,  60, '\x0000000000000000000000000000000000000000'::bytea, 22962297),
        ('\xef3a1d51982c20ee2f125e6d6d1f9d3a10c1e94391b828040943005a1ea27e14'::bytea, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,  3000,  60, '\x0000000000000000000000000000000000000000'::bytea, 22552041),
        ('\x904e8ad11c6f8abb44ea77c507355900e7f9d2907ab0a29353cb1ef0f06b0852'::bytea, '\x0000000000000000000000000000000000000000'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea,    50,   1, '\x4440854B2d02C57A0Dc5c58b7A884562D875c0c4'::bytea, 23326185),
        ('\xa068c5ab2de0c5fed15f8c187d911915437ed55e6a47d2e42710f9174e6db9a2'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, '\xae78736Cd615f374D3085123A210448E74Fc6393'::bytea,   500,  10, '\x0000000000000000000000000000000000000000'::bytea, 22240740),
        ('\x4d9cc597ec7d8848af463fca5f4c750279f0d02d2745844c1e9f52a7930cc4d7'::bytea, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea, 10000, 200, '\x0000000000000000000000000000000000000000'::bytea, 25492487),
        ('\xe63e32b2ae40601662f760d6bf5d771057324fbd97784fe1d3717069f7b75d45'::bytea, '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea,     5,   1, '\x0000000000000000000000000000000000000000'::bytea, 24229945),
        ('\x3b1b1f2e775a6db1664f8e7d59ad568605ea2406312c11aef03146c0cf89d5b9'::bytea, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea,     5,   1, '\x0000000000000000000000000000000000000000'::bytea, 24230047),
        ('\xb54ece65cc2ddd3eaec0ad18657470fb043097220273d87368a062c7d4e59180'::bytea, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea,   100,   1, '\x0000000000000000000000000000000000000000'::bytea, 23153381),
        ('\xa2a5a544a8cbd9c24557b8393fd909360779cf0f48a0b88895a7d9d83ce9d437'::bytea, '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,   100,   1, '\x0000000000000000000000000000000000000000'::bytea, 22268982),
        ('\x2d04d518afae8b57a702a6f679edf49f39593d818f9342cc57b457ea738a7460'::bytea, '\x56072C95FAA701256059aa122697B133aDEd9279'::bytea, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea,   500,  10, '\x0000000000000000000000000000000000000000'::bytea, 25036987),
        ('\x51ccd46db78d6988ab156c9b0d023e14b2e848240bc719718e63c4cc5c258bcf'::bytea, '\x0000000000000000000000000000000000000000'::bytea, '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea,  3000,  60, '\x0000000000000000000000000000000000000000'::bytea, 22989795),
        ('\x2f5dff74b96e2df0fa8a5695318d59839c3ce5d058b19024fbfe276100b676ff'::bytea, '\x68749665FF8D2d112Fa859AA293F07A622782F38'::bytea, '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea, 10000, 200, '\x0000000000000000000000000000000000000000'::bytea, 24363921)
)
INSERT INTO uniswap_v4_pool
    (chain_id, pool_id, currency0, currency1,
     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks, deploy_block,
     snapshot_supported)
SELECT 1, s.pool_id, s.currency0, s.currency1, t0.id, t1.id,
       s.fee, s.tick_spacing, s.hooks, s.deploy_block,
       TRUE
FROM seed s
JOIN token t0 ON t0.chain_id = 1 AND t0.address = CASE
        WHEN s.currency0 = '\x0000000000000000000000000000000000000000'::bytea
            THEN '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea
        ELSE s.currency0 END
JOIN token t1 ON t1.chain_id = 1 AND t1.address = CASE
        WHEN s.currency1 = '\x0000000000000000000000000000000000000000'::bytea
            THEN '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea
        ELSE s.currency1 END
ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING;

DO $$
DECLARE
    manager_count      INT;
    pool_count         INT;
    unsupported_count  INT;
    bad_mapping_count  INT;
    null_decimal_count INT;
    bad_token          TEXT;
BEGIN
    SELECT count(*) INTO manager_count FROM uniswap_v4_pool_manager WHERE chain_id = 1;
    IF manager_count <> 1 THEN
        RAISE EXCEPTION 'expected exactly 1 UniswapV4 PoolManager row on chain 1, got %', manager_count;
    END IF;

    SELECT count(*) INTO pool_count
    FROM uniswap_v4_pool p
    WHERE p.chain_id = 1;
    IF pool_count <> 21 THEN
        RAISE EXCEPTION 'expected exactly 21 UniswapV4 pools, got %', pool_count;
    END IF;

    SELECT count(*) INTO unsupported_count
    FROM uniswap_v4_pool
    WHERE chain_id = 1 AND NOT snapshot_supported;
    IF unsupported_count <> 0 THEN
        RAISE EXCEPTION 'expected every seeded UniswapV4 pool to be snapshot_supported, got % excluded', unsupported_count;
    END IF;

    SELECT count(*) INTO bad_mapping_count
    FROM uniswap_v4_pool p
    JOIN token t0 ON t0.id = p.currency0_token_id
    JOIN token t1 ON t1.id = p.currency1_token_id
    WHERE NOT (
        ((p.currency0 <> '\x0000000000000000000000000000000000000000'::bytea
                AND t0.address = p.currency0)
            OR (p.currency0 = '\x0000000000000000000000000000000000000000'::bytea
                AND t0.address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea))
        AND
        ((p.currency1 <> '\x0000000000000000000000000000000000000000'::bytea
                AND t1.address = p.currency1)
            OR (p.currency1 = '\x0000000000000000000000000000000000000000'::bytea
                AND t1.address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea))
    );
    IF bad_mapping_count <> 0 THEN
        RAISE EXCEPTION 'expected every UniswapV4 pool currency to resolve to its own token row (or the ETH placeholder for address(0)), got % mismatched', bad_mapping_count;
    END IF;

    SELECT count(*) INTO null_decimal_count
    FROM uniswap_v4_pool p
    JOIN token t ON t.id IN (p.currency0_token_id, p.currency1_token_id)
    WHERE t.decimals IS NULL;
    IF null_decimal_count <> 0 THEN
        RAISE EXCEPTION 'expected every token referenced by a UniswapV4 pool to have decimals, got % with NULL', null_decimal_count;
    END IF;

    -- A pre-existing token row with the wrong decimals silently rescales every V4 amount.
    SELECT format('%s: expected (%s, %s), got (%s, %s)',
                  encode(e.address, 'hex'), e.symbol, e.decimals, t.symbol, t.decimals)
    INTO bad_token
    FROM (VALUES
        ('\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea, 'ETH', 18),
        ('\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, 'wstETH', 18),
        ('\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea, 'stETH', 18),
        ('\xBe9895146f7AF43049ca1c1AE358B0541Ea49704'::bytea, 'cbETH', 18),
        ('\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea, 'WBTC', 8),
        ('\x111111111117dC0aa78b770fA6A738034120C302'::bytea, '1INCH', 18),
        ('\xf951E335afb289353dc249e82926178EaC7DEd78'::bytea, 'swETH', 18),
        ('\x93ED3FBe21207Ec2E8f2d3c3de6e058Cb73Bc04d'::bytea, 'PNK', 18),
        ('\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, 'USDC', 6),
        ('\xae78736Cd615f374D3085123A210448E74Fc6393'::bytea, 'rETH', 18),
        ('\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea, 'sUSDS', 18),
        ('\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, 'PYUSD', 6),
        ('\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, 'USDS', 18),
        ('\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, 'USDT', 6),
        ('\x56072C95FAA701256059aa122697B133aDEd9279'::bytea, 'SKY', 18),
        ('\x68749665FF8D2d112Fa859AA293F07A622782F38'::bytea, 'XAUt', 6)
    ) AS e (address, symbol, decimals)
    LEFT JOIN token t ON t.chain_id = 1 AND t.address = e.address
    WHERE t.id IS NULL
       OR t.symbol IS DISTINCT FROM e.symbol
       OR t.decimals IS DISTINCT FROM e.decimals
    ORDER BY e.address
    LIMIT 1;
    IF bad_token IS NOT NULL THEN
        RAISE EXCEPTION 'UniswapV4 seed token mismatch for %', bad_token;
    END IF;
END $$;

-- The three shapes that differ: #1 native ETH (address(0) mapping), #12 the only
-- hooked pool, #21 a 6-decimal currency0.
DO $$
DECLARE
    got_currency0 BYTEA;
    got_currency1 BYTEA;
    got_token0    BYTEA;
    got_hooks     BYTEA;
BEGIN
    SELECT p.currency0, p.currency1, t0.address
    INTO got_currency0, got_currency1, got_token0
    FROM uniswap_v4_pool p
    JOIN token t0 ON t0.id = p.currency0_token_id
    WHERE p.pool_id = '\x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76'::bytea;
    IF got_currency0 <> '\x0000000000000000000000000000000000000000'::bytea
        OR got_currency1 <> '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea
        OR got_token0 <> '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea THEN
        RAISE EXCEPTION 'pool #1 ETH/wstETH mismatch: currency0=%, currency1=%, currency0 token=%',
            got_currency0, got_currency1, got_token0;
    END IF;

    SELECT p.currency0, p.currency1, p.hooks
    INTO got_currency0, got_currency1, got_hooks
    FROM uniswap_v4_pool p
    WHERE p.pool_id = '\x904e8ad11c6f8abb44ea77c507355900e7f9d2907ab0a29353cb1ef0f06b0852'::bytea;
    IF got_currency0 <> '\x0000000000000000000000000000000000000000'::bytea
        OR got_currency1 <> '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea
        OR got_hooks <> '\x4440854B2d02C57A0Dc5c58b7A884562D875c0c4'::bytea THEN
        RAISE EXCEPTION 'pool #12 hooked ETH/wstETH mismatch: currency0=%, currency1=%, hooks=%',
            got_currency0, got_currency1, got_hooks;
    END IF;

    SELECT p.currency0, p.currency1
    INTO got_currency0, got_currency1
    FROM uniswap_v4_pool p
    WHERE p.pool_id = '\x2f5dff74b96e2df0fa8a5695318d59839c3ce5d058b19024fbfe276100b676ff'::bytea;
    IF got_currency0 <> '\x68749665FF8D2d112Fa859AA293F07A622782F38'::bytea
        OR got_currency1 <> '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea THEN
        RAISE EXCEPTION 'pool #21 XAUt/sUSDS mismatch: currency0=%, currency1=%',
            got_currency0, got_currency1;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260819_120000_create_uniswap_v4_tables.sql')
ON CONFLICT (filename) DO NOTHING;
