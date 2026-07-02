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
-- fluid_vault_state: per-vault aggregate snapshot (hypertable on block_timestamp).
-- ============================================================================
CREATE TABLE IF NOT EXISTS fluid_vault_state
(
    fluid_vault_id        BIGINT      NOT NULL REFERENCES fluid_vault (id),
    block_number          BIGINT      NOT NULL,
    block_version         INT         NOT NULL DEFAULT 0,
    block_timestamp       TIMESTAMPTZ NOT NULL,
    total_collateral      NUMERIC     NOT NULL,
    total_debt            NUMERIC     NOT NULL,
    supply_exchange_price NUMERIC,
    borrow_exchange_price NUMERIC,
    supply_rate           NUMERIC,
    borrow_rate           NUMERIC,
    processing_version    INT         NOT NULL DEFAULT 0,
    build_id              INT         NOT NULL DEFAULT 0,
    -- processing_version last so the trigger's (fluid_vault_id, block_number,
    -- block_version, block_timestamp) lookup is a contiguous PK-index prefix
    -- (ADR-0002, matching maple_*_state).
    PRIMARY KEY (fluid_vault_id, block_number, block_version, block_timestamp, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '7 days'
);

ALTER TABLE fluid_vault_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'fluid_vault_id',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('fluid_vault_state', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('fluid_vault_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for fluid_vault_state';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same (vault, block, block_version, block_timestamp, build_id) retry → reuse
-- version (idempotent); a new build_id at the same key → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_fluid_vault_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('fvs|%s|%s|%s|%s', NEW.fluid_vault_id, NEW.block_number, NEW.block_version,
               EXTRACT(epoch FROM NEW.block_timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM fluid_vault_state
    WHERE fluid_vault_id  = NEW.fluid_vault_id
      AND block_number    = NEW.block_number
      AND block_version   = NEW.block_version
      AND block_timestamp = NEW.block_timestamp
      AND build_id        = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM fluid_vault_state
        WHERE fluid_vault_id  = NEW.fluid_vault_id
          AND block_number    = NEW.block_number
          AND block_version   = NEW.block_version
          AND block_timestamp = NEW.block_timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON fluid_vault_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_fluid_vault_state();

-- ============================================================================
-- Catalogue metadata (source of truth for column units/scale; see the
-- "Interpreting numeric columns" convention). Conventions match
-- 20260609_120000_add_schema_comments / 20260630_120000_add_maple_schema_comments.
-- ============================================================================
COMMENT ON TABLE fluid_vault IS
  '[Dimension] Registry of discovered Fluid (Instadapp) vaults, one row per vault. Registry fields are immutable per vault; per-block metrics live in the fluid_vault_state hypertable.';
COMMENT ON COLUMN fluid_vault.id IS 'PK. Surrogate id referenced by fluid_vault_state.';
COMMENT ON COLUMN fluid_vault.chain_id IS 'FK→chain.chain_id.';
COMMENT ON COLUMN fluid_vault.protocol_id IS 'FK→protocol.id. The Fluid protocol row.';
COMMENT ON COLUMN fluid_vault.address IS 'Vault contract address (20 bytes). Unique per (chain_id, address).';
COMMENT ON COLUMN fluid_vault.vault_type IS 'Fluid vault type discriminator (e.g. ''T1'').';
COMMENT ON COLUMN fluid_vault.collateral_token_id IS 'FK→token.id. The vault''s collateral ERC-20.';
COMMENT ON COLUMN fluid_vault.debt_token_id IS 'FK→token.id. The vault''s debt (borrow) ERC-20.';
COMMENT ON COLUMN fluid_vault.created_at_block IS 'Block at which the vault was first discovered (first-seen); not a per-block metric.';
COMMENT ON COLUMN fluid_vault.created_at IS 'Audit. Wall-clock time the registry row was first inserted.';

COMMENT ON TABLE fluid_vault_state IS
  '[Hypertable] Per-vault aggregate snapshot (total collateral + total debt across all borrowers) at a single block, partitioned on block_timestamp. Per-borrower positions are not modelled (VEC-436). Append-only, ADR-0002 versioned.';
COMMENT ON COLUMN fluid_vault_state.fluid_vault_id IS 'FK→fluid_vault.id. Part of PK.';
COMMENT ON COLUMN fluid_vault_state.block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN fluid_vault_state.block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK; a new version inserts cleanly rather than overwriting.';
COMMENT ON COLUMN fluid_vault_state.block_timestamp IS 'Partition. Block timestamp (UTC). Part of PK.';
COMMENT ON COLUMN fluid_vault_state.total_collateral IS 'Vault total collateral across all borrowers, raw integer in the collateral token''s native decimals (unscaled).';
COMMENT ON COLUMN fluid_vault_state.total_debt IS 'Vault total debt across all borrowers, raw integer in the debt token''s native decimals (unscaled).';
COMMENT ON COLUMN fluid_vault_state.supply_exchange_price IS 'Fluid VaultResolver supply exchange price, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN fluid_vault_state.borrow_exchange_price IS 'Fluid VaultResolver borrow exchange price, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN fluid_vault_state.supply_rate IS 'Fluid VaultResolver supply rate, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN fluid_vault_state.borrow_rate IS 'Fluid VaultResolver borrow rate, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN fluid_vault_state.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN fluid_vault_state.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

INSERT INTO migrations (filename)
VALUES ('20260626_120000_create_fluid_vault_tables.sql')
ON CONFLICT (filename) DO NOTHING;
