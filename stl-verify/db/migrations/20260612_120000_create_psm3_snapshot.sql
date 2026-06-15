-- Spark PSM3 reserve snapshots, one row per (chain, block) sweep.
--
-- Populated by the psm3-indexer: every N blocks it reads the PSM3 deployment's
-- reserve composition (USDS/sUSDS balances at the PSM3, USDC balance at the
-- governance-settable pocket), totalAssets (par valuation) and the sUSDS
-- conversion rate, all pinned to one block. Balances are stored raw (USDS and
-- sUSDS 1e18, USDC 1e6, total_assets 1e18, conversion_rate 1e27); all USD math
-- happens in the Python API at read time.
--
-- source is 'sweep'-only; the future event-driven path (ERC-20 Transfer deltas
-- for exact time-weighted averages) will widen this constraint in its own
-- migration, mirroring token_total_supply's event/sweep split.
--
-- Auditability follows the repo+trigger pattern used by token_total_supply:
--   - Repository supplies build_id at construction.
--   - assign_processing_version_psm3_snapshot BEFORE INSERT trigger assigns
--     processing_version so same-build replays are idempotent and cross-build
--     reprocesses append a new version.

CREATE TABLE psm3_snapshot (
    chain_id           INT         NOT NULL REFERENCES chain (chain_id),
    address            BYTEA       NOT NULL,            -- PSM3 contract
    usds_balance       NUMERIC     NOT NULL,            -- raw 1e18
    susds_balance      NUMERIC     NOT NULL,            -- raw 1e18
    usdc_balance       NUMERIC     NOT NULL,            -- raw 1e6, read at pocket()
    total_assets       NUMERIC     NOT NULL,            -- raw 1e18, PSM3.totalAssets()
    conversion_rate    NUMERIC     NOT NULL,            -- raw 1e27, rateProvider().getConversionRate()
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    source             TEXT        NOT NULL CHECK (source = 'sweep'),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, block_number, block_version, processing_version, block_timestamp)
);

SELECT create_hypertable('psm3_snapshot', by_range('block_timestamp', INTERVAL '7 days'));

CREATE INDEX idx_psm3_snapshot_current
    ON psm3_snapshot (chain_id, block_number DESC, block_version DESC, processing_version DESC);

CREATE OR REPLACE FUNCTION assign_processing_version_psm3_snapshot()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('psm3|%s|%s|%s|%s',
            NEW.chain_id,
            NEW.block_number,
            NEW.block_version,
            NEW.block_timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM psm3_snapshot
    WHERE chain_id        = NEW.chain_id
      AND block_number    = NEW.block_number
      AND block_version   = NEW.block_version
      AND block_timestamp = NEW.block_timestamp
      AND build_id        = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM psm3_snapshot
        WHERE chain_id        = NEW.chain_id
          AND block_number    = NEW.block_number
          AND block_version   = NEW.block_version
          AND block_timestamp = NEW.block_timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON psm3_snapshot
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_psm3_snapshot();

GRANT SELECT ON psm3_snapshot TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON psm3_snapshot TO stl_readwrite;

-- Columnstore/tiering deliberately skipped: ~600 rows/day across 4 chains is not worth the policy overhead.

INSERT INTO migrations (filename)
VALUES ('20260612_120000_create_psm3_snapshot.sql')
ON CONFLICT (filename) DO NOTHING;
