-- Uniswap V4 PositionManager NFT transfers (ARCT-385).

CREATE TABLE IF NOT EXISTS uniswap_v4_position_manager
(
    id                 BIGSERIAL PRIMARY KEY,
    chain_id           INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id        BIGINT      NOT NULL REFERENCES protocol (id),
    deploy_block       BIGINT      NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    UNIQUE (chain_id, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_manager_protocol
    ON uniswap_v4_position_manager (protocol_id);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_manager_pv_lookup
    ON uniswap_v4_position_manager (chain_id, build_id);

-- Prefix 'u4posm' for uniswap_v4_position_manager. force_custom_plan per
-- VEC-541 (see assign_processing_version_uniswap_v4_pool_state).
CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_position_manager()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4posm|%s', NEW.chain_id), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_position_manager
    WHERE chain_id = NEW.chain_id
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM uniswap_v4_position_manager
        WHERE chain_id = NEW.chain_id;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_position_manager
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_position_manager();

COMMENT ON TABLE uniswap_v4_position_manager IS
  '[Dimension] Append-only, versioned registry of the singleton Uniswap V4 PositionManager (the ERC-721 that wraps LP positions) deployed on a chain. The current row for a chain is the one with the highest processing_version; there is never an UPDATE. A later migration corrects a row by inserting a new version with a build_id different from the row it supersedes (seeds use 0): the build-aware trigger reuses the existing processing_version for an identical build_id, so a same-build re-insert is an idempotent no-op, not a correction.';
COMMENT ON COLUMN uniswap_v4_position_manager.id IS
  'PK. Surrogate ID of this PositionManager VERSION. uniswap_v4_position_nft_transfer FKs it, so a corrected registry row starts a new surrogate id; aggregate a chain''s transfers across corrections by joining this table and grouping by chain_id, never by this id.';
COMMENT ON COLUMN uniswap_v4_position_manager.chain_id IS
  'FK->chain.chain_id. Network the PositionManager is deployed on. V4 ships one canonical PositionManager per chain, so UNIQUE (chain_id, processing_version) allows only superseding versions of it, never two concurrent deployments.';
COMMENT ON COLUMN uniswap_v4_position_manager.protocol_id IS
  'FK->protocol.id. The UniswapV4PositionManager protocol row (metadata.role = position_manager), and the SOLE source of the on-chain PositionManager address (protocol.address): the ERC-721 Transfer topic0 is byte-identical to ERC-20''s, so this address is the only thing that tells a posm transfer from any other token''s. Deliberately not duplicated as a column here -- a second copy could disagree with the row it FKs.';
COMMENT ON COLUMN uniswap_v4_position_manager.deploy_block IS
  'Block at which the PositionManager was deployed, hence the lower bound for any posm token: no Transfer log exists before this height. Load-bearing for the historical transfer backfill, which starts its log scan here.';
COMMENT ON COLUMN uniswap_v4_position_manager.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_position_manager.processing_version IS
  'Audit. Correction version (ADR-0002): 0 for the first write of a chain under a build_id, bumped only when a later build rewrites the same chain; prior versions are retained. Order by processing_version DESC for the current row.';
COMMENT ON COLUMN uniswap_v4_position_manager.build_id IS
  'Audit. ID of the build (code+config) that wrote this row; 0 for the migration seed. Never use it to pick the latest row.';

CREATE TABLE IF NOT EXISTS uniswap_v4_position_nft_transfer
(
    position_manager_id BIGINT      NOT NULL REFERENCES uniswap_v4_position_manager (id),
    token_id            NUMERIC     NOT NULL CHECK (token_id >= 0),
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL DEFAULT 0,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    tx_hash             BYTEA       NOT NULL CHECK (octet_length(tx_hash) = 32),
    log_index           INT         NOT NULL,
    from_address        BYTEA       NOT NULL CHECK (octet_length(from_address) = 20),
    to_address          BYTEA       NOT NULL CHECK (octet_length(to_address) = 20),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_version  INT         NOT NULL DEFAULT 0,
    build_id            INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (position_manager_id, block_timestamp, block_number, block_version,
                 log_index, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '1 day',
    tsdb.columnstore = false
);

ALTER TABLE uniswap_v4_position_nft_transfer SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'position_manager_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('uniswap_v4_position_nft_transfer', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('uniswap_v4_position_nft_transfer', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_v4_position_nft_transfer';
END $$;

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_nft_transfer_pv_lookup
    ON uniswap_v4_position_nft_transfer
       (position_manager_id, block_number, block_version, log_index, build_id);

-- The holder query walks one token's history downwards, a prefix the PK cannot
-- serve: it leads with block_timestamp, which the question does not bound.
CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_nft_transfer_token_block
    ON uniswap_v4_position_nft_transfer
       (position_manager_id, token_id, block_number DESC, block_version DESC, log_index DESC);

-- Prefix 'u4pnt' for uniswap_v4_position_nft_transfer. Same VEC-615 shape as
-- uniswap_v4_pool_state's (see the note there): the INSERT calls the version
-- function, the trigger delegates to it, force_custom_plan per VEC-541.
CREATE OR REPLACE FUNCTION next_processing_version_uniswap_v4_position_nft_transfer(
    p_position_manager_id BIGINT,
    p_block_number        BIGINT,
    p_block_version       INT,
    p_log_index           INT,
    p_build_id            INT)
RETURNS INT
VOLATILE
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('u4pnt|%s|%s|%s|%s', p_position_manager_id, p_block_number,
               p_block_version, p_log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM uniswap_v4_position_nft_transfer
    WHERE position_manager_id = p_position_manager_id
      AND block_number        = p_block_number
      AND block_version       = p_block_version
      AND log_index           = p_log_index
      AND build_id            = p_build_id
    LIMIT 1;

    IF FOUND THEN
        RETURN existing_ver;
    END IF;

    SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
    FROM uniswap_v4_position_nft_transfer
    WHERE position_manager_id = p_position_manager_id
      AND block_number        = p_block_number
      AND block_version       = p_block_version
      AND log_index           = p_log_index;
    RETURN max_ver + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION next_processing_version_uniswap_v4_position_nft_transfer(BIGINT, BIGINT, INT, INT, INT) IS
  'Returns the processing_version a uniswap_v4_position_nft_transfer row at (position_manager, block, block_version, log_index) must carry for build_id: the version that build already wrote there, else MAX+1. Takes the key''s advisory lock (ADR-0002 §3) for the transaction. Call it in the INSERT''s VALUES list: on a columnstored chunk ON CONFLICT is resolved before row triggers fire, so a version left to the trigger is DEFAULT 0 there and the correction row is silently discarded (VEC-615).';

CREATE OR REPLACE FUNCTION assign_processing_version_uniswap_v4_position_nft_transfer()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
BEGIN
    NEW.processing_version := next_processing_version_uniswap_v4_position_nft_transfer(
        NEW.position_manager_id, NEW.block_number, NEW.block_version, NEW.log_index, NEW.build_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON uniswap_v4_position_nft_transfer
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_uniswap_v4_position_nft_transfer();

COMMENT ON TABLE uniswap_v4_position_nft_transfer IS
  '[Hypertable] One row per ERC-721 Transfer log emitted by the Uniswap V4 PositionManager: a posm position NFT being minted, moved or burned. The holder of token_id at block N is the newest row at or below it -- WHERE position_manager_id = M AND token_id = T AND block_number <= N ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC LIMIT 1 -- and its to_address is the answer; log_index is part of that order because a token can change hands twice in one block. Every field is carried by the log itself, so a reorg redelivery re-decodes the new fork''s logs and appends them at the new block_version; nothing is ever re-read from chain state. Partitioned on block_timestamp (1-day chunks) rather than left plain: mainnet mints alone run to hundreds of thousands of tokens, transfers grow without bound, and no write path here reads an earlier row. Append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.position_manager_id IS
  'PK, FK->uniswap_v4_position_manager.id. Surrogate id of the PositionManager version that emitted the log. There is no chain_id column: the chain comes from this FK, exactly as it does for uniswap_v4_swap through uniswap_v4_pool.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.token_id IS
  'ERC-721 token id, the raw uint256 from topics[3] -- NUMERIC because a uint256 does not fit BIGINT. Not an amount and not scaled by any decimals. Equal to the position''s uniswap_v4_position.salt read as a big-endian integer, which is how a holder is joined to the pool position the NFT owns. Not part of the PK: a (position_manager_id, block, log_index) site already identifies one log.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.block_number IS
  'PK. Block height at which the transfer was emitted.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.log_index IS
  'PK. Index of the event log within the block. Part of the holder-at-block ordering, not just the key: one token can be transferred more than once in a single block.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.from_address IS
  'Event field `from`, 20 bytes: the holder the token left. All zeros on a mint, which is how a token''s first row is recognised.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.to_address IS
  'Event field `to`, 20 bytes: the holder the token arrived at, i.e. the answer to "who holds this token". All zeros on a burn, which records the token ceasing to exist rather than a holder of address(0).';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.created_at IS
  'Audit. Row insertion time as an instant (timestamptz, so it denotes the same moment under any session TimeZone, and stores UTC internally); bookkeeping only, not an on-chain value.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v4_position_nft_transfer.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- Append-only enforcement: the application role may SELECT and INSERT but never
-- mutate or delete.
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_position_manager FROM stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON uniswap_v4_position_nft_transfer FROM stl_readwrite;

-- Address and deploy height read from mainnet (2026-08-31): the contract's code
-- first appears at block 21689089, poolManager() returns the seeded V4
-- PoolManager, and symbol() is UNI-V4-POSM.
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (
    1,
    '\xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e'::bytea,
    'UniswapV4PositionManager',
    'dex',
    21689089,
    NOW(),
    '{"role":"position_manager"}'::jsonb
)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO uniswap_v4_position_manager (chain_id, protocol_id, deploy_block)
SELECT 1, pr.id, 21689089
FROM protocol pr
WHERE pr.chain_id = 1 AND pr.address = '\xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e'::bytea
ON CONFLICT (chain_id, processing_version) DO NOTHING;

DO $$
DECLARE
    manager_count INT;
    got_address   BYTEA;
    got_role      TEXT;
    got_deploy    BIGINT;
BEGIN
    SELECT count(*) INTO manager_count FROM uniswap_v4_position_manager WHERE chain_id = 1;
    IF manager_count <> 1 THEN
        RAISE EXCEPTION 'expected exactly 1 UniswapV4 PositionManager row on chain 1, got %', manager_count;
    END IF;

    SELECT pr.address, pr.metadata->>'role', m.deploy_block
    INTO got_address, got_role, got_deploy
    FROM uniswap_v4_position_manager m
    JOIN protocol pr ON pr.id = m.protocol_id AND pr.chain_id = m.chain_id
    WHERE m.chain_id = 1;
    IF got_address IS DISTINCT FROM '\xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e'::bytea THEN
        RAISE EXCEPTION 'UniswapV4 PositionManager protocol address is %, want the mainnet posm', got_address;
    END IF;
    IF got_role IS DISTINCT FROM 'position_manager' THEN
        RAISE EXCEPTION 'UniswapV4 PositionManager protocol row has metadata.role %, want position_manager', got_role;
    END IF;
    IF got_deploy <> 21689089 THEN
        RAISE EXCEPTION 'UniswapV4 PositionManager deploy_block is %, want 21689089', got_deploy;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260831_130000_create_uniswap_v4_position_nft_transfer.sql')
ON CONFLICT (filename) DO NOTHING;
