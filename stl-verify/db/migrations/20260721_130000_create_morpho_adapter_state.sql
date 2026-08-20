-- Morpho VaultV2 structured tracking (VEC-218): adapter state hypertable.
--
-- Sibling migrations: morpho_adapter (20260721_120000, identity),
-- morpho_adapter_membership (20260721_125000, the membership log),
-- morpho_vault_cap (20260721_140000) and morpho_vault_fee (20260727_120000).
--
-- morpho_adapter_state is a per-adapter snapshot: the assets a VaultV2 liquidity
-- adapter reports it holds in its downstream venue (adapter.realAssets()) at a
-- single block. One row per adapter per block, partitioned on timestamp,
-- mirroring morpho_vault_state's shape and policies (1-day chunks, compress
-- after 2 days, tier to S3 after 1 year).
--
-- Auditability follows ADR-0002: block_version, processing_version + build_id,
-- PK = natural key + processing_version (processing_version LAST so the
-- trigger's (morpho_adapter_id, block_number, block_version, timestamp) lookup
-- is a contiguous PK-index prefix), and a build-aware advisory-locked
-- BEFORE INSERT trigger (prefix: mas). block_version increments on reorgs, so a
-- re-indexed block inserts cleanly rather than overwriting; a same-build retry
-- reuses processing_version and dedupes via the caller's ON CONFLICT DO NOTHING.

-- ============================================================================
-- morpho_adapter_state: per-adapter realAssets() snapshot (hypertable).
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_adapter_state
(
    morpho_adapter_id  BIGINT       NOT NULL REFERENCES morpho_adapter (id),
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    real_assets        NUMERIC(78, 0) NOT NULL,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- processing_version last so the trigger's (morpho_adapter_id, block_number,
    -- block_version, timestamp) lookup is a contiguous PK-index prefix
    -- (ADR-0002, matching morpho_*_state / fluid_vault_state).
    PRIMARY KEY (morpho_adapter_id, block_number, block_version, timestamp, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

CREATE INDEX IF NOT EXISTS idx_morpho_adapter_state_adapter ON morpho_adapter_state (morpho_adapter_id);
CREATE INDEX IF NOT EXISTS idx_morpho_adapter_state_block ON morpho_adapter_state (block_number);

ALTER TABLE morpho_adapter_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'morpho_adapter_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC'
);

SELECT add_compression_policy('morpho_adapter_state', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('morpho_adapter_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for morpho_adapter_state';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same (adapter, block, block_version, timestamp, build_id) retry → reuse
-- version (idempotent); a new build_id at the same key → MAX+1. Timestamp is
-- wrapped in EXTRACT(epoch FROM …) so the lock key is TimeZone/DateStyle-stable.
-- Pinned to force_custom_plan so the per-row lookups keep pruning chunks instead of
-- fanning out over every chunk once plpgsql caches a generic plan (VEC-541).
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_adapter_state()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mas|%s|%s|%s|%s', NEW.morpho_adapter_id, NEW.block_number, NEW.block_version,
               EXTRACT(epoch FROM NEW.timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM morpho_adapter_state
    WHERE morpho_adapter_id = NEW.morpho_adapter_id
      AND block_number      = NEW.block_number
      AND block_version     = NEW.block_version
      AND timestamp         = NEW.timestamp
      AND build_id          = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_adapter_state
        WHERE morpho_adapter_id = NEW.morpho_adapter_id
          AND block_number      = NEW.block_number
          AND block_version     = NEW.block_version
          AND timestamp         = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_assign_processing_version ON morpho_adapter_state;
CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_adapter_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_adapter_state();

-- ============================================================================
-- Catalogue metadata.
-- ============================================================================
COMMENT ON TABLE morpho_adapter_state IS
  '[Hypertable] Per-adapter realAssets() snapshot at a single block, partitioned on timestamp. One row per VaultV2 liquidity adapter per block. Append-only, ADR-0002 versioned.';
COMMENT ON COLUMN morpho_adapter_state.morpho_adapter_id IS 'FK→morpho_adapter.id. Part of PK.';
COMMENT ON COLUMN morpho_adapter_state.block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN morpho_adapter_state.block_version IS 'Block payload version the row was indexed from. Live rows: the reorg version carried by the block event. Replayed rows: the S3 object version, routinely 1 straight from the bulk downloader with no reorg behind it. A higher version is therefore never evidence of a reorg, only the tie-break that picks the canonical row. Part of PK; a new version inserts cleanly rather than overwriting.';
COMMENT ON COLUMN morpho_adapter_state.timestamp IS 'Partition. Block timestamp (UTC). Part of PK.';
COMMENT ON COLUMN morpho_adapter_state.real_assets IS 'Adapter realAssets() reading: raw on-chain uint256 in the vault''s underlying asset base units (unscaled). Non-negative.';
COMMENT ON COLUMN morpho_adapter_state.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_number DESC, block_version DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN morpho_adapter_state.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';
COMMENT ON COLUMN morpho_adapter_state.created_at IS 'Audit. Processing time: wall-clock the row was inserted (DEFAULT NOW()), per the schema_master canonical semantics; NOT the block timestamp (`timestamp`). Never part of any key or latest-row ordering.';

-- ============================================================================
-- Append-only enforcement: the application role may SELECT and INSERT but never
-- mutate or delete. (Table owner stl_migrator is unaffected, as Postgres owners
-- bypass these grants — that is the operator escape hatch for repairing a bad
-- row. TRUNCATE is never granted by ALTER DEFAULT PRIVILEGES, so it needs no
-- REVOKE.)
-- ============================================================================

-- One statement covers every chunk: TimescaleDB propagates a hypertable's
-- GRANT/REVOKE to its chunks, and chunks created later inherit the ACL in force
-- at creation (verified on 2.25.1-pg17, including the compressed hypertable).
REVOKE UPDATE, DELETE ON morpho_adapter_state FROM stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260721_130000_create_morpho_adapter_state.sql')
ON CONFLICT (filename) DO NOTHING;
