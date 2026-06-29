-- Fluid (Instadapp) vault indexer tables (VEC-436).
-- Granularity is per-vault aggregate: one fluid_vault_state row per vault per
-- block capturing the vault's total collateral + total debt across all
-- borrowers. Per-borrower positions are deliberately not modelled — the
-- fsUSDS backed-breakdown only needs pool-level aggregates.
--
-- Numeric values are stored as raw on-chain integers (the vault's own token
-- decimals), unscaled. Exchange prices and rates are read from Fluid's
-- VaultResolver and are nullable: a snapshot that did not capture them stores
-- NULL rather than a fabricated zero.
--
-- Auditability follows ADR-0002: fluid_vault_state carries block_version,
-- processing_version + build_id, PK = natural key + processing_version, and a
-- build-aware advisory-locked BEFORE INSERT trigger (prefix: fvs). block_version
-- increments on reorgs, so re-indexed blocks insert cleanly rather than
-- overwriting; same-build retries reuse processing_version and dedupe via
-- ON CONFLICT DO NOTHING.

-- ============================================================================
-- Protocol seed row (Fluid Liquidity contract, Ethereum mainnet).
-- Verified on Etherscan: "Fluid: Liquidity" (FluidLiquidityProxy).
-- ============================================================================
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (1, '\x52Aa899454998Be5b000Ad077a46Bbe360F4e497'::bytea,
        'fluid', 'lending', 19239106, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt FROM protocol WHERE chain_id = 1 AND name = 'fluid';
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 fluid protocol row, found %', cnt;
    END IF;
END $$;

-- ============================================================================
-- fluid_vault: registry of discovered Fluid vaults (one row per vault).
-- collateral_token_id and debt_token_id FK token by surrogate id (resolved
-- from each token's natural key (chain_id, address)). Both are mainnet ERC-20s
-- for the tracked vaults.
-- ============================================================================
CREATE TABLE IF NOT EXISTS fluid_vault
(
    id                  BIGSERIAL PRIMARY KEY,
    chain_id            INT         NOT NULL REFERENCES chain (chain_id),
    protocol_id         BIGINT      NOT NULL REFERENCES protocol (id),
    address             BYTEA       NOT NULL,
    vault_type          VARCHAR(16) NOT NULL,
    collateral_token_id BIGINT      NOT NULL REFERENCES token (id),
    debt_token_id       BIGINT      NOT NULL REFERENCES token (id),
    created_at_block    BIGINT      NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_fluid_vault_protocol ON fluid_vault (protocol_id);
CREATE INDEX IF NOT EXISTS idx_fluid_vault_collateral_token ON fluid_vault (collateral_token_id);
CREATE INDEX IF NOT EXISTS idx_fluid_vault_debt_token ON fluid_vault (debt_token_id);

-- ============================================================================
-- fluid_vault_state: per-vault aggregate snapshot (hypertable on timestamp).
-- ============================================================================
CREATE TABLE IF NOT EXISTS fluid_vault_state
(
    fluid_vault_id        BIGINT      NOT NULL REFERENCES fluid_vault (id),
    block_number          BIGINT      NOT NULL,
    block_version         INT         NOT NULL DEFAULT 0,
    timestamp             TIMESTAMPTZ NOT NULL,
    total_collateral      NUMERIC     NOT NULL,
    total_debt            NUMERIC     NOT NULL,
    supply_exchange_price NUMERIC,
    borrow_exchange_price NUMERIC,
    supply_rate           NUMERIC,
    borrow_rate           NUMERIC,
    processing_version    INT         NOT NULL DEFAULT 0,
    build_id              INT         NOT NULL DEFAULT 0,
    -- processing_version last so the trigger's (fluid_vault_id, block_number,
    -- block_version, timestamp) lookup is a contiguous PK-index prefix (ADR-0002,
    -- matching maple_*_state).
    PRIMARY KEY (fluid_vault_id, block_number, block_version, timestamp, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '7 days'
);

ALTER TABLE fluid_vault_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'fluid_vault_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('fluid_vault_state', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('fluid_vault_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for fluid_vault_state';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same (vault, block, block_version, timestamp, build_id) retry → reuse version
-- (idempotent); a new build_id at the same key → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_fluid_vault_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('fvs|%s|%s|%s|%s', NEW.fluid_vault_id, NEW.block_number, NEW.block_version,
               EXTRACT(epoch FROM NEW.timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM fluid_vault_state
    WHERE fluid_vault_id = NEW.fluid_vault_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM fluid_vault_state
        WHERE fluid_vault_id = NEW.fluid_vault_id
          AND block_number   = NEW.block_number
          AND block_version  = NEW.block_version
          AND timestamp      = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON fluid_vault_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_fluid_vault_state();

INSERT INTO migrations (filename)
VALUES ('20260626_120000_create_fluid_vault_tables.sql')
ON CONFLICT (filename) DO NOTHING;
