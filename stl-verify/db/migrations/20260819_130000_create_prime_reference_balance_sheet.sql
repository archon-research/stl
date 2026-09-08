-- Daily per-prime balance sheet as published by Sky, the only source of
-- reference history that predates STL's own observation of the Star monitor.
-- Source: https://sky.data.blockanalitica.com/internal/primes/historic/?days_ago=N
--
-- Why this is a separate table from prime_capital_stack, despite both carrying
-- reference data: the two feeds report different record types. The Star monitor
-- publishes risk capital (RRC, junior/senior, utilizations) and no history; this
-- feed publishes a balance sheet (treasury, assets, debt, backstop) and no risk
-- capital at all. Folding them together would mean making thirteen risk-capital
-- columns nullable so this feed could omit them, which would stop a Star row
-- missing a figure from failing. They share one *provenance* — the API serves
-- both as source="reference" — not one shape.
--
-- treasury_balance is the same measurement as the Star monitor's total_rc:
-- verified by back-to-back fetch on 2026-08-19, spark 48,142,491.09 and grove
-- 26,124,170.36 on both feeds. That is what lets a total-capital series splice
-- this history onto the forward snapshots without a step at the join.
--
-- allocated_assets is deliberately NOT the exposure series' history: measured
-- against the Star monitor's total_exposure at the same instant it is +32% for
-- spark and +0.5% for grove, so the two are different definitions and the
-- divergence is prime-dependent. It is stored because it is part of the balance
-- sheet, and must not be read as exposure.
--
-- Cadence is one row per prime per UTC day; observed_at is that day's midnight.
-- The feed reports no intraday detail, so the timestamp is a day marker, not an
-- observation instant.
--
-- Compression: segment by prime_id, order by observed_at DESC, compress after
-- 30 days (daily rows, so a 30-day chunk is ~30 rows per prime), tier after
-- 1 year. Auditability follows ADR-0002 (prefix: prbs).

CREATE TABLE IF NOT EXISTS prime_reference_balance_sheet
(
    prime_id             BIGINT      NOT NULL REFERENCES prime (id),
    observed_at          TIMESTAMPTZ NOT NULL,
    treasury_balance_usd NUMERIC     NOT NULL,
    assets_usd           NUMERIC     NOT NULL,
    allocated_assets_usd NUMERIC     NOT NULL,
    idle_assets_usd      NUMERIC     NOT NULL,
    debt_usd             NUMERIC     NOT NULL,
    backstop_capital_usd NUMERIC     NOT NULL,
    source               TEXT        NOT NULL,
    processing_version   INT         NOT NULL DEFAULT 0,
    build_id             INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (prime_id, observed_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'observed_at',
    tsdb.chunk_interval = '30 days'
);

ALTER TABLE prime_reference_balance_sheet SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'prime_id',
    timescaledb.compress_orderby = 'observed_at DESC'
);

SELECT add_compression_policy('prime_reference_balance_sheet', INTERVAL '60 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('prime_reference_balance_sheet', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for prime_reference_balance_sheet';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same build_id retry → reuse version (idempotent); new build_id → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_prime_reference_balance_sheet()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('prbs|%s|%s', NEW.prime_id, EXTRACT(epoch FROM NEW.observed_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM prime_reference_balance_sheet
    WHERE prime_id    = NEW.prime_id
      AND observed_at = NEW.observed_at
      AND build_id    = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM prime_reference_balance_sheet
        WHERE prime_id    = NEW.prime_id
          AND observed_at = NEW.observed_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET plan_cache_mode = 'force_custom_plan';

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON prime_reference_balance_sheet
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_prime_reference_balance_sheet();

-- ============================================================================
-- Catalogue metadata (COMMENT ON), consistent with 20260609 add_schema_comments.
--   [Type]: Hypertable
--   Roles:  PK | FK→table.col | Partition | Audit
--   Scale:  every *_usd column is an already-normalized USD decimal, never a
--           raw native-decimal integer.
-- ============================================================================
COMMENT ON TABLE prime_reference_balance_sheet IS
  '[Hypertable] Daily per-prime balance sheet as published by Sky, partitioned on observed_at. The only source of reference history predating STL''s own observation of the Star monitor, which publishes none. Reference data sharing one provenance with prime_capital_stack (the API serves both as source="reference") but a different record type: balance sheet here, risk capital there. One row per prime per UTC day.';
COMMENT ON COLUMN prime_reference_balance_sheet.prime_id IS 'FK→prime.id. Part of PK.';
COMMENT ON COLUMN prime_reference_balance_sheet.observed_at IS 'Partition. Midnight UTC of the reported day. A day marker, not an observation instant — the feed publishes no intraday detail. Part of PK.';
COMMENT ON COLUMN prime_reference_balance_sheet.treasury_balance_usd IS 'Normalized USD decimal. Upstream treasury_balance. The same measurement as the Star monitor''s total_rc (verified equal by back-to-back fetch), which is what lets a total-capital series splice this history onto the forward snapshots in prime_capital_stack.';
COMMENT ON COLUMN prime_reference_balance_sheet.assets_usd IS 'Normalized USD decimal. Upstream assets: the prime''s total assets, allocated plus idle.';
COMMENT ON COLUMN prime_reference_balance_sheet.allocated_assets_usd IS 'Normalized USD decimal. Upstream allocated_assets. NOT the history of the exposure series: measured against the Star monitor''s total_exposure at the same instant it runs +32% for spark and +0.5% for grove, so the two are different definitions and the gap is prime-dependent. Never serve this as exposure.';
COMMENT ON COLUMN prime_reference_balance_sheet.idle_assets_usd IS 'Normalized USD decimal. Upstream idle_assets: assets not currently allocated. Can be slightly negative in the feed.';
COMMENT ON COLUMN prime_reference_balance_sheet.debt_usd IS 'Normalized USD decimal. Upstream debt.';
COMMENT ON COLUMN prime_reference_balance_sheet.backstop_capital_usd IS 'Normalized USD decimal. Upstream backstop_capital.';
COMMENT ON COLUMN prime_reference_balance_sheet.source IS 'Provenance slug of the upstream feed that reported the row.';
COMMENT ON COLUMN prime_reference_balance_sheet.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by observed_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN prime_reference_balance_sheet.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

INSERT INTO migrations (filename)
VALUES ('20260819_130000_create_prime_reference_balance_sheet.sql')
ON CONFLICT (filename) DO NOTHING;
