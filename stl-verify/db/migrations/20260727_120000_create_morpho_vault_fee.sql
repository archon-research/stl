-- Morpho VaultV2 structured tracking (VEC-218): vault fee-config snapshots.
--
-- A VaultV2 carries a single fee configuration: a performance fee (a WAD
-- fraction of accrued interest), a management fee (a WAD per-second rate), and
-- the recipient address for each. The config is set on-chain by four separate
-- Set* events, each carrying only the one field it changed. Rather than mutate
-- morpho_vault columns in place (last-write-wins, no block dimension, not
-- queryable "as of block N"), each fee event is snapshotted here exactly like
-- caps: when any Set* fee event fires, the indexer reads the vault's FULL fee
-- config on-chain (performanceFee, managementFee, performanceFeeRecipient,
-- managementFeeRecipient), pinned to the event's block hash, and writes all four
-- together — so the latest row per vault is always the full current fee state,
-- readable without decoding protocol_event JSON. A fee read is a pure function
-- of the block, so sibling fee events in the same block read identical values
-- and dedupe (trigger + ON CONFLICT) to one row.
--
-- A plain table, not a hypertable, by deliberate maintainer carve-out: fee
-- changes are governance events writing on the order of rows per day at most
-- (measured: 2 rows for sparkUSDTbc's entire VaultV2 life), so chunking,
-- compression and S3 tiering are pure overhead. Revisit if the write rate ever
-- grows orders of magnitude. Auditability follows ADR-0002: block_version,
-- processing_version + build_id, PK = natural key + processing_version
-- (processing_version LAST for a contiguous PK-index prefix), and a
-- build-aware advisory-locked BEFORE INSERT trigger (prefix: mvf).

-- ============================================================================
-- morpho_vault_fee: full VaultV2 fee-config snapshot. One row per (vault,
-- block) capturing both fees and both recipients read on-chain.
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault_fee
(
    morpho_vault_id           BIGINT         NOT NULL REFERENCES morpho_vault (id),
    performance_fee           NUMERIC(30, 0) NOT NULL,
    management_fee            NUMERIC(30, 0) NOT NULL,
    performance_fee_recipient BYTEA          NOT NULL,
    management_fee_recipient  BYTEA          NOT NULL,
    block_number              BIGINT         NOT NULL,
    block_version             INT            NOT NULL DEFAULT 0,
    timestamp                 TIMESTAMPTZ    NOT NULL,
    processing_version        INT            NOT NULL DEFAULT 0,
    build_id                  INT            NOT NULL DEFAULT 0,
    created_at                TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    -- processing_version last so the trigger's (morpho_vault_id, block_number,
    -- block_version, timestamp) lookup is a contiguous PK-index prefix
    -- (ADR-0002, matching morpho_vault_state / morpho_vault_cap).
    PRIMARY KEY (morpho_vault_id, block_number, block_version, timestamp, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_fee_block ON morpho_vault_fee (block_number);

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- timestamp is wrapped in EXTRACT(epoch FROM …) so the key is
-- TimeZone/DateStyle-stable.
-- Pinned to force_custom_plan, required of every assign_processing_version_* function
-- whether or not its table is a hypertable (VEC-541).
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_vault_fee()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mvf|%s|%s|%s|%s', NEW.morpho_vault_id, NEW.block_number,
               NEW.block_version, EXTRACT(epoch FROM NEW.timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM morpho_vault_fee
    WHERE morpho_vault_id = NEW.morpho_vault_id
      AND block_number    = NEW.block_number
      AND block_version   = NEW.block_version
      AND timestamp       = NEW.timestamp
      AND build_id        = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_vault_fee
        WHERE morpho_vault_id = NEW.morpho_vault_id
          AND block_number    = NEW.block_number
          AND block_version   = NEW.block_version
          AND timestamp       = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_assign_processing_version ON morpho_vault_fee;
CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_vault_fee
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_vault_fee();

-- ============================================================================
-- Catalogue metadata.
-- ============================================================================
COMMENT ON TABLE morpho_vault_fee IS
  '[Configuration] Full VaultV2 fee-config snapshot. Each row is an end-of-block snapshot read on-chain (hash-pinned) when any Set* fee event fires: performanceFee, managementFee and both recipients are read together off the vault, so the latest row per morpho_vault_id is the full current fee config. Same-block sibling fee events read identical values and dedupe to one row. Append-only, ADR-0002 versioned; replaces the mutated-in-place morpho_vault fee columns. Plain table by deliberate maintainer carve-out: the governance-event write rate (order rows per day at most) makes hypertable chunking, compression and tiering pure overhead; revisit if the write rate ever grows orders of magnitude.';
COMMENT ON COLUMN morpho_vault_fee.morpho_vault_id IS 'FK→morpho_vault.id. The VaultV2 the fee config belongs to. Part of PK.';
COMMENT ON COLUMN morpho_vault_fee.performance_fee IS 'Performance fee: raw on-chain uint96 WAD fraction of accrued interest (1e18 = 100%; contract default 0). Stored unscaled (raw WAD, like lltv/relative_cap elsewhere), not as a NUMERIC(38,18) decimal. Non-negative.';
COMMENT ON COLUMN morpho_vault_fee.management_fee IS 'Management fee: raw on-chain uint96 WAD per-second rate (not bps, not annualised; contract default 0). Stored unscaled (raw WAD). Non-negative.';
COMMENT ON COLUMN morpho_vault_fee.performance_fee_recipient IS 'Performance-fee recipient address (20 bytes). The zero address is the contract default (no recipient set).';
COMMENT ON COLUMN morpho_vault_fee.management_fee_recipient IS 'Management-fee recipient address (20 bytes). The zero address is the contract default (no recipient set).';
COMMENT ON COLUMN morpho_vault_fee.block_number IS 'Block height at which the fee config was read. Part of PK.';
COMMENT ON COLUMN morpho_vault_fee.block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK; a new version inserts cleanly rather than overwriting.';
COMMENT ON COLUMN morpho_vault_fee.timestamp IS 'Block timestamp (UTC). Part of PK.';
COMMENT ON COLUMN morpho_vault_fee.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_number DESC, block_version DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN morpho_vault_fee.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';
COMMENT ON COLUMN morpho_vault_fee.created_at IS 'Audit. Processing time: wall-clock the row was inserted (DEFAULT NOW()), per the schema_master canonical semantics; NOT the block timestamp (`timestamp`). Never part of any key or latest-row ordering.';

-- ============================================================================
-- Append-only enforcement: the application role may SELECT and INSERT but never
-- mutate or delete. (Table owner stl_migrator is unaffected, as Postgres owners
-- bypass these grants — that is the operator escape hatch for repairing a bad
-- row. TRUNCATE is never granted by ALTER DEFAULT PRIVILEGES, so it needs no
-- REVOKE.)
-- ============================================================================

REVOKE UPDATE, DELETE ON morpho_vault_fee FROM stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260727_120000_create_morpho_vault_fee.sql')
ON CONFLICT (filename) DO NOTHING;
