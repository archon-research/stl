-- Morpho VaultV2 structured tracking, part 3 of 3 (VEC-218): vault allocation
-- caps.
--
-- A VaultV2 limits how much it will allocate against each "id" — an opaque
-- bytes32 key (cap_id = keccak256(id_data)) identifying a market, collateral
-- token, or (adapter, marketParams) tuple. Every id carries two limits: an
-- absolute cap (an asset-amount ceiling) and a relative cap (a WAD fraction of
-- total assets), set on-chain by separate events. Each row here is an
-- end-of-block cap snapshot: when a cap event fires, the indexer reads BOTH
-- limits (absoluteCap, relativeCap) off the vault on-chain, pinned to the
-- event's block hash, and writes them together — so the latest row per
-- (vault, cap_id) is always the full current cap state, readable without
-- joining across event rows. Sibling cap events in the same block read
-- identical values and dedupe (trigger + ON CONFLICT) to one row.
--
-- A plain table, not a hypertable, by deliberate maintainer carve-out: cap
-- changes are governance events writing on the order of rows per day at most
-- (measured: 9 rows for sparkUSDTbc's entire VaultV2 life), so chunking,
-- compression and S3 tiering are pure overhead. Revisit if the write rate ever
-- grows orders of magnitude. Auditability follows ADR-0002: block_version,
-- processing_version + build_id, PK = natural key + processing_version
-- (processing_version LAST for a contiguous PK-index prefix), and a
-- build-aware advisory-locked BEFORE INSERT trigger (prefix: mvc).

-- ============================================================================
-- morpho_vault_cap: per-id allocation-cap snapshot.
-- id_data is the decodable pre-image of cap_id; it decodes to one of
-- ("this", adapter) | ("collateralToken", token) |
-- ("this/marketParams", adapter, marketParams).
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_vault_cap
(
    morpho_vault_id    BIGINT       NOT NULL REFERENCES morpho_vault (id),
    cap_id             BYTEA        NOT NULL,
    id_data            BYTEA        NOT NULL,
    absolute_cap       NUMERIC(39, 0) NOT NULL,
    relative_cap       NUMERIC(39, 0) NOT NULL,
    block_number       BIGINT       NOT NULL,
    block_version      INT          NOT NULL DEFAULT 0,
    timestamp          TIMESTAMPTZ  NOT NULL,
    processing_version INT          NOT NULL DEFAULT 0,
    build_id           INT          NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- processing_version last so the trigger's (morpho_vault_id, cap_id,
    -- block_number, block_version, timestamp) lookup is a contiguous PK-index
    -- prefix (ADR-0002, matching morpho_*_state / fluid_vault_state).
    PRIMARY KEY (morpho_vault_id, cap_id, block_number, block_version, timestamp, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_cap_block ON morpho_vault_cap (block_number);

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- cap_id is a BYTEA, so it is hex-encoded in the lock key; timestamp is wrapped
-- in EXTRACT(epoch FROM …) so the key is TimeZone/DateStyle-stable.
-- Pinned to force_custom_plan, required of every assign_processing_version_* function
-- whether or not its table is a hypertable (VEC-541).
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_vault_cap()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mvc|%s|%s|%s|%s|%s', NEW.morpho_vault_id, encode(NEW.cap_id, 'hex'),
               NEW.block_number, NEW.block_version, EXTRACT(epoch FROM NEW.timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM morpho_vault_cap
    WHERE morpho_vault_id = NEW.morpho_vault_id
      AND cap_id          = NEW.cap_id
      AND block_number    = NEW.block_number
      AND block_version   = NEW.block_version
      AND timestamp       = NEW.timestamp
      AND build_id        = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_vault_cap
        WHERE morpho_vault_id = NEW.morpho_vault_id
          AND cap_id          = NEW.cap_id
          AND block_number    = NEW.block_number
          AND block_version   = NEW.block_version
          AND timestamp       = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_assign_processing_version ON morpho_vault_cap;
CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_vault_cap
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_vault_cap();

-- ============================================================================
-- Catalogue metadata.
-- ============================================================================
COMMENT ON TABLE morpho_vault_cap IS
  '[Configuration] Per-id VaultV2 allocation-cap snapshot. Each row is an end-of-block snapshot read on-chain (hash-pinned) when a cap event fires: both absolute_cap and relative_cap are read together off the vault, so the latest row per (morpho_vault_id, cap_id) is the full current cap state. Same-block sibling cap events read identical values and dedupe to one row. Append-only, ADR-0002 versioned. Plain table by deliberate maintainer carve-out: the governance-event write rate (order rows per day at most) makes hypertable chunking, compression and tiering pure overhead; revisit if the write rate ever grows orders of magnitude.';
COMMENT ON COLUMN morpho_vault_cap.morpho_vault_id IS 'FK→morpho_vault.id. The VaultV2 the cap belongs to. Part of PK.';
COMMENT ON COLUMN morpho_vault_cap.cap_id IS 'The on-chain cap key (bytes32, 32 bytes) = keccak256(id_data). Part of PK.';
COMMENT ON COLUMN morpho_vault_cap.id_data IS 'ABI-encoded pre-image of cap_id (decodes to ("this",adapter) | ("collateralToken",token) | ("this/marketParams",adapter,marketParams)). Attribute, not part of the key.';
COMMENT ON COLUMN morpho_vault_cap.absolute_cap IS 'Absolute allocation ceiling: raw on-chain uint128 in the vault''s underlying asset base units (unscaled). Non-negative.';
COMMENT ON COLUMN morpho_vault_cap.relative_cap IS 'Relative allocation cap: raw on-chain uint128 WAD fraction of total assets (1e18 = 100%; contract default 0). Stored unscaled (raw WAD, like lltv elsewhere), not as a NUMERIC(38,18) decimal.';
COMMENT ON COLUMN morpho_vault_cap.block_number IS 'Block height at which the cap was set. Part of PK.';
COMMENT ON COLUMN morpho_vault_cap.block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK; a new version inserts cleanly rather than overwriting.';
COMMENT ON COLUMN morpho_vault_cap.timestamp IS 'Block timestamp (UTC). Part of PK.';
COMMENT ON COLUMN morpho_vault_cap.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_number DESC, block_version DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN morpho_vault_cap.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';
COMMENT ON COLUMN morpho_vault_cap.created_at IS 'Audit. Processing time: wall-clock the row was inserted (DEFAULT NOW()), per the schema_master canonical semantics; NOT the block timestamp (`timestamp`). Never part of any key or latest-row ordering.';

INSERT INTO migrations (filename)
VALUES ('20260721_140000_create_morpho_vault_cap.sql')
ON CONFLICT (filename) DO NOTHING;
