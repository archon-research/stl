CREATE TABLE IF NOT EXISTS uniswap_v4_position
(
    pool_id                      BIGINT      NOT NULL REFERENCES uniswap_v4_pool (id),
    owner                        BYTEA       NOT NULL CHECK (octet_length(owner) = 20),
    tick_lower                   INT         NOT NULL CHECK (tick_lower BETWEEN -887272 AND 887272),
    tick_upper                   INT         NOT NULL CHECK (tick_upper BETWEEN -887272 AND 887272),
    salt                         BYTEA       NOT NULL CHECK (octet_length(salt) = 32),
    block_number                 BIGINT      NOT NULL,
    block_version                INT         NOT NULL DEFAULT 0,
    block_timestamp              TIMESTAMPTZ NOT NULL,
    liquidity                    NUMERIC     NOT NULL CHECK (liquidity >= 0),
    fee_growth_inside0_last_x128 NUMERIC     NOT NULL CHECK (fee_growth_inside0_last_x128 >= 0),
    fee_growth_inside1_last_x128 NUMERIC     NOT NULL CHECK (fee_growth_inside1_last_x128 >= 0),
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version           INT         NOT NULL DEFAULT 0,
    build_id                     INT         NOT NULL DEFAULT 0,
    CHECK (tick_lower < tick_upper),
    PRIMARY KEY (pool_id, owner, tick_lower, tick_upper, salt,
                 block_number, block_version, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_pv_lookup
    ON uniswap_v4_position (pool_id, owner, tick_lower, tick_upper, salt,
                            block_number, block_version, build_id);

-- The reorg re-read asks which positions a pool wrote at height N; the PK leads
-- with the whole natural key, so it cannot serve that prefix.
CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_pool_block
    ON uniswap_v4_position (pool_id, block_number);

-- owner and salt are hex-encoded into the lock key so it cannot shift with the
-- session's bytea_output setting. force_custom_plan per VEC-541.
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_position()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4pos|%s|%s|%s|%s|%s|%s|%s',
               NEW.pool_id, encode(NEW.owner, 'hex'), NEW.tick_lower, NEW.tick_upper,
               encode(NEW.salt, 'hex'), NEW.block_number, NEW.block_version), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_position
    WHERE pool_id       = NEW.pool_id
      AND owner         = NEW.owner
      AND tick_lower    = NEW.tick_lower
      AND tick_upper    = NEW.tick_upper
      AND salt          = NEW.salt
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v4_position
        WHERE pool_id       = NEW.pool_id
          AND owner         = NEW.owner
          AND tick_lower    = NEW.tick_lower
          AND tick_upper    = NEW.tick_upper
          AND salt          = NEW.salt
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_position
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_position();

COMMENT ON TABLE uniswap_v4_position IS
  '[Operational] Append-on-change authoritative per-position state from StateView.getPositionInfo(poolId, owner, tickLower, tickUpper, salt) reads, taken for every position a block''s ModifyLiquidity events touched. A new row is written only when the read-back state differs from the latest stored row for the natural key (pool_id, owner, tick_lower, tick_upper, salt), or when block_version differs (a reorg re-observation always appends). Coverage is event-driven and therefore incomplete by construction: V4 exposes no way to enumerate a pool''s positions, so a position appears here only from the first block this indexer saw a ModifyLiquidity for it, and its state before that block is unknown. Carries no token amounts -- V4 stores only liquidity and the two fee-growth checkpoints, so amounts are a downstream derivation from liquidity, the pool''s sqrt_price_x96 and the tick bounds. Append-only via the processing_version trigger. NOT a hypertable, deliberately, for the same reason as its sibling uniswap_v4_tick: rows are written on-change rather than on every touched block, and each write first reads the latest row per natural key with no time bound the planner could use, so partitioning would turn one bounded index lookup into a fan-out over every chunk (the cost profile ADR-0002/VEC-541 describes) while buying nothing a plain index does not already give.';
COMMENT ON COLUMN uniswap_v4_position.pool_id IS
  'PK, FK->uniswap_v4_pool.id. Surrogate pool ID the position belongs to (not the 32-byte on-chain PoolId). To aggregate a position''s history across registry corrections, join uniswap_v4_pool and group by (chain_id, pool_id) -- never by uniswap_v4_pool.id, which changes on every correction.';
COMMENT ON COLUMN uniswap_v4_position.owner IS
  'PK. PoolManager-level position owner, 20 bytes: the address that called PoolManager.modifyLiquidity, which is the getPositionInfo `owner` argument and the ModifyLiquidity event''s `sender`. For a PositionManager-managed NFT this is the PositionManager contract, NOT the NFT holder -- the holder is resolved off the ERC-721 (out of scope here). A hook or a bespoke router that unlocks the PoolManager itself owns its positions directly.';
COMMENT ON COLUMN uniswap_v4_position.tick_lower IS
  'PK. Lower tick bound of the position''s range (plain integer, inclusive; a multiple of the pool''s tick_spacing). price = 1.0001^tick before decimal adjustment.';
COMMENT ON COLUMN uniswap_v4_position.tick_upper IS
  'PK. Upper tick bound of the position''s range (plain integer, exclusive; a multiple of the pool''s tick_spacing).';
COMMENT ON COLUMN uniswap_v4_position.salt IS
  'PK. Caller-chosen position discriminator, 32 bytes: it is what lets one owner hold several independent positions over the same tick range, so it is part of the natural key rather than a payload column. All zeros when the caller supplied none. PositionManager sets it to bytes32(tokenId), so for a posm NFT the salt is the token id as a big-endian 32-byte integer.';
COMMENT ON COLUMN uniswap_v4_position.block_number IS
  'PK. Block height at which this position state was read (via multicall pinned to this block''s hash).';
COMMENT ON COLUMN uniswap_v4_position.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v4_position.block_timestamp IS
  'Block timestamp (UTC) at which this position state was read.';
COMMENT ON COLUMN uniswap_v4_position.liquidity IS
  'getPositionInfo().liquidity: the position''s raw on-chain L (uint128), i.e. sqrt(xy) scaled by the pool''s currency decimals; not comparable across pools with different decimals, and not a token amount. 0 records a position that was fully burned or never opened -- getPositionInfo answers an unknown key with zeros rather than reverting.';
COMMENT ON COLUMN uniswap_v4_position.fee_growth_inside0_last_x128 IS
  'getPositionInfo().feeGrowthInside0LastX128: the currency0 fee-growth-inside-range checkpoint v4-core wrote at the position''s last touch, Q128.128 fixed point. NOT fees owed: fees accrued since that touch are (feeGrowthInside0 at the block of interest - this) * liquidity / 2^128. Refreshed on every ModifyLiquidity including a zero-delta poke, because v4-core''s Position.update writes both checkpoints unconditionally.';
COMMENT ON COLUMN uniswap_v4_position.fee_growth_inside1_last_x128 IS
  'getPositionInfo().feeGrowthInside1LastX128: the currency1 checkpoint, Q128.128 fixed point; same not-fees-owed semantics and same poke-refresh behaviour as fee_growth_inside0_last_x128.';
COMMENT ON COLUMN uniswap_v4_position.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_position.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v4_position.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

COMMENT ON COLUMN uniswap_v4_pool.snapshot_supported IS
  'Configuration (curated, load-bearing). Gates the STATE, TICK and POSITION snapshot path only: TRUE = the indexer reads uniswap_v4_pool_state, uniswap_v4_tick and uniswap_v4_position rows for this pool; FALSE = the pool stays registered and its logs are still decoded into uniswap_v4_swap / uniswap_v4_liquidity_event / uniswap_v4_pool_event and mirrored into protocol_event, but no state, tick or position row is ever written for it. Curated the way curve_pool.has_a_precise is, rather than derived: set FALSE for a dynamic-LP-fee pool (fee = 8388608), whose lp_fee updateDynamicLPFee rewrites emitting no event, so a snapshot would silently go stale between touches -- until VEC-573 gives lp_fee a refresh path. A change is a new version row, never an UPDATE.';

REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_position FROM stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260820_120000_create_uniswap_v4_positions.sql')
ON CONFLICT (filename) DO NOTHING;
