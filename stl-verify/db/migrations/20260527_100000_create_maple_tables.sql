-- Maple Finance: Syrup ERC-4626 vaults (on-chain side; GraphQL side in PR 2)
-- All time-series tables use TimescaleDB hypertables partitioned on `timestamp` (block timestamp).
-- Compression segments by entity FK, orders by block_number DESC + block_version DESC, chunks > 2 days.
-- Tier to S3 after 1 year (best-effort via add_tiering_policy).
--
-- Auditability columns (processing_version, build_id) and triggers are included from inception,
-- consistent with the post-20260410_150000 convention. PK ordering matches morpho_vault_state:
--   (entity_id, block_number, block_version, processing_version, timestamp)

-- ============================================================================
-- Protocol seed rows
-- ============================================================================
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES
    -- SyrupUSDC vault contract address used as the Syrup-v1 protocol address.
    (1, '\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b'::bytea,
        'maple-syrup-v1', 'lending', 0, NOW(), '{}'::jsonb),
    -- PoolV2 SyrupUSDC address as the Pool-V2 protocol address.
    (1, '\x80226fc0Ee2b096224EeAc085Bb9a8cba1146f7D'::bytea,
        'maple-poolv2', 'lending', 0, NOW(), '{}'::jsonb),
    -- OTL / FTL placeholder protocol rows (loan-type-level, not per-loan-contract).
    (1, '\x0000000000000000000000000000000000000001'::bytea,
        'maple-otl', 'lending', 0, NOW(), '{}'::jsonb),
    (1, '\x0000000000000000000000000000000000000002'::bytea,
        'maple-ftl', 'lending', 0, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- Token seed rows for SyrupUSDC. syrupUSDT (mainnet 0x356b…Ba7D) is already
-- seeded by 20260305_100000_add_aave_v3_oracle_feeds.sql with symbol 'syrupUSDT'.
-- USDC + USDT mainnet rows are seeded by 20260204_110000_seed_sparklend_tokens.sql.
-- ============================================================================
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b'::bytea, 'syrupUSDC', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- receipt_token rows: Syrup* → underlying USDC/USDT.
-- receipt_token schema (post-20260319): includes chain_id; UNIQUE (chain_id, receipt_token_address).
-- Looked up by address (not symbol) to remain immune to symbol-casing drift.
-- ============================================================================
INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block)
SELECT p.chain_id, p.id, u.id, s.address, s.symbol, 0
FROM protocol p
JOIN token s ON s.chain_id = p.chain_id
JOIN token u ON u.chain_id = p.chain_id
WHERE p.chain_id = 1 AND p.name = 'maple-syrup-v1'
  AND (
        (s.address = '\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b'::bytea
         AND u.address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea) -- syrupUSDC → USDC
     OR (s.address = '\x356b8d89C1E1239cbBb9DE4815c39a1474d5Ba7D'::bytea
         AND u.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea) -- syrupUSDT → USDT
      )
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- ============================================================================
-- maple_vault: registry of Syrup ERC-4626 vaults
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_vault
(
    id                   BIGSERIAL PRIMARY KEY,
    chain_id             INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id          BIGINT      NOT NULL REFERENCES protocol (id),
    address              BYTEA       NOT NULL,
    name                 VARCHAR(255),
    symbol               VARCHAR(50),
    asset_token_id       BIGINT      NOT NULL REFERENCES token (id),
    pool_address         BYTEA       NOT NULL,
    vault_version        SMALLINT    NOT NULL DEFAULT 1,
    created_at_block     BIGINT      NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_maple_vault_chain_id    ON maple_vault (chain_id);
CREATE INDEX IF NOT EXISTS idx_maple_vault_address     ON maple_vault (address);
CREATE INDEX IF NOT EXISTS idx_maple_vault_asset_token ON maple_vault (asset_token_id);
CREATE INDEX IF NOT EXISTS idx_maple_vault_protocol_id ON maple_vault (protocol_id);

-- Seed SyrupUSDC + SyrupUSDT vault rows.
-- Deploy blocks (Ethereum mainnet): SyrupUSDC=20231245, SyrupUSDT=21063245 — confirm
-- via etherscan during smoke test; if values shift, update seed via a follow-on
-- migration (NEVER modify this file once merged).
INSERT INTO maple_vault (chain_id, protocol_id, address, name, symbol,
                         asset_token_id, pool_address, vault_version, created_at_block)
SELECT 1,
       (SELECT id FROM protocol WHERE chain_id = 1 AND name = 'maple-syrup-v1'),
       '\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b'::bytea,
       'Syrup USDC', 'syrupUSDC',
       (SELECT id FROM token
        WHERE chain_id = 1
          AND address = '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),
       '\x80226fc0Ee2b096224EeAc085Bb9a8cba1146f7D'::bytea,
       1, 20231245
WHERE NOT EXISTS (
    SELECT 1 FROM maple_vault
    WHERE chain_id = 1
      AND address = '\x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b'::bytea);

INSERT INTO maple_vault (chain_id, protocol_id, address, name, symbol,
                         asset_token_id, pool_address, vault_version, created_at_block)
SELECT 1,
       (SELECT id FROM protocol WHERE chain_id = 1 AND name = 'maple-syrup-v1'),
       '\x356b8d89C1E1239cbBb9DE4815c39a1474d5Ba7D'::bytea,
       'Syrup USDT', 'syrupUSDT',
       (SELECT id FROM token
        WHERE chain_id = 1
          AND address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),
       -- Pool address for SyrupUSDT — confirm during smoke test; placeholder
       -- zero address until then. Follow-on migration updates it (NEVER edit this file).
       '\x0000000000000000000000000000000000000000'::bytea,
       1, 21063245
WHERE NOT EXISTS (
    SELECT 1 FROM maple_vault
    WHERE chain_id = 1
      AND address = '\x356b8d89C1E1239cbBb9DE4815c39a1474d5Ba7D'::bytea);

-- ============================================================================
-- maple_vault_state: per-block vault snapshot (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_vault_state
(
    maple_vault_id        BIGINT      NOT NULL REFERENCES maple_vault (id),
    block_number          BIGINT      NOT NULL,
    block_version         INT         NOT NULL DEFAULT 0,
    timestamp             TIMESTAMPTZ NOT NULL,
    total_assets          NUMERIC     NOT NULL,
    total_supply          NUMERIC     NOT NULL,
    share_price           NUMERIC     NOT NULL,
    underlying_price_usd  NUMERIC,
    syrup_price_usd       NUMERIC,
    processing_version    INT         NOT NULL DEFAULT 0,
    build_id              INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_vault_id, block_number, block_version, processing_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval   = '1 day'
);

CREATE INDEX IF NOT EXISTS idx_maple_vault_state_vault ON maple_vault_state (maple_vault_id);
CREATE INDEX IF NOT EXISTS idx_maple_vault_state_block ON maple_vault_state (block_number);

ALTER TABLE maple_vault_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maple_vault_id',
    timescaledb.compress_orderby   = 'block_number DESC, block_version DESC'
);

SELECT add_compression_policy('maple_vault_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_vault_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_vault_state';
END $$;

-- ============================================================================
-- maple_vault_position: per-block, per-user position (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS maple_vault_position
(
    user_id            BIGINT      NOT NULL REFERENCES "user" (id),
    maple_vault_id     BIGINT      NOT NULL REFERENCES maple_vault (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ NOT NULL,
    shares             NUMERIC     NOT NULL,
    assets             NUMERIC     NOT NULL,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, maple_vault_id, block_number, block_version, processing_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval   = '1 day'
);

CREATE INDEX IF NOT EXISTS idx_maple_vault_position_user       ON maple_vault_position (user_id);
CREATE INDEX IF NOT EXISTS idx_maple_vault_position_vault      ON maple_vault_position (maple_vault_id);
CREATE INDEX IF NOT EXISTS idx_maple_vault_position_block      ON maple_vault_position (block_number);
CREATE INDEX IF NOT EXISTS idx_maple_vault_position_user_vault ON maple_vault_position (user_id, maple_vault_id);

ALTER TABLE maple_vault_position SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maple_vault_id, user_id',
    timescaledb.compress_orderby   = 'block_number DESC, block_version DESC'
);

SELECT add_compression_policy('maple_vault_position', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('maple_vault_position', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for maple_vault_position';
END $$;

-- ============================================================================
-- Processing-version triggers
-- (model: 20260410_150000_create_processing_version_triggers.sql)
-- Build-aware: same build_id retry → reuse existing version (idempotent);
-- different build_id → assign MAX + 1. Concurrent inserts serialize via PK locks.
-- ============================================================================
CREATE OR REPLACE FUNCTION assign_processing_version_maple_vault_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    SELECT processing_version INTO existing_ver
    FROM maple_vault_state
    WHERE maple_vault_id = NEW.maple_vault_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_vault_state
        WHERE maple_vault_id = NEW.maple_vault_id
          AND block_number   = NEW.block_number
          AND block_version  = NEW.block_version
          AND timestamp      = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_vault_state
    FOR EACH ROW EXECUTE FUNCTION assign_processing_version_maple_vault_state();

CREATE OR REPLACE FUNCTION assign_processing_version_maple_vault_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    SELECT processing_version INTO existing_ver
    FROM maple_vault_position
    WHERE user_id          = NEW.user_id
      AND maple_vault_id   = NEW.maple_vault_id
      AND block_number     = NEW.block_number
      AND block_version    = NEW.block_version
      AND timestamp        = NEW.timestamp
      AND build_id         = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_vault_position
        WHERE user_id          = NEW.user_id
          AND maple_vault_id   = NEW.maple_vault_id
          AND block_number     = NEW.block_number
          AND block_version    = NEW.block_version
          AND timestamp        = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON maple_vault_position
    FOR EACH ROW EXECUTE FUNCTION assign_processing_version_maple_vault_position();

INSERT INTO migrations (filename)
VALUES ('20260527_100000_create_maple_tables.sql')
ON CONFLICT (filename) DO NOTHING;
