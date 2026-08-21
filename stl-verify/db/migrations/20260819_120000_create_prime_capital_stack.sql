-- Per-cycle snapshots of a prime's risk capital as reported by Sky's Star Agents
-- Risk Capital & Requirements Monitor.
-- Source: https://info-sky.blockanalitica.com/star-monitoring/risk-capital/primes/{star}/
--
-- Why this table exists: the monitor publishes no history at any granularity
-- (verified by probing — ?days_ago=/?date= are accepted and silently ignored,
-- and there is no per-prime historic route). A reference figure therefore can
-- never be reconstructed for a past instant, only observed going forward, so
-- the syncer accumulates them here.
--
-- Snapshot semantics mirror the Maple GraphQL tables: rows are keyed by
-- synced_at (cron cycle timestamp, UTC). There is NO block_version — the
-- monitor is an off-chain API with no reorg concept. A prime that stops
-- appearing at a given synced_at has left the monitor's coverage, which is a
-- real fact about coverage and must never be read as zero exposure.
--
-- Encoding: every amount here is an already-normalized USD decimal, NOT a raw
-- native-decimal integer — upstream reports e.g. "2098090654.811942249063867795"
-- meaning $2.098bn. Do not scale by token.decimals. Ratio columns are plain
-- fractions (0-1), not fixed-point and not percentages: upstream's crr/ratio
-- vocabulary is fractional throughout (confirmed: crr == rrc / exposure).
--
-- Compression strategy:
-- - Segment by entity FK (prime_id), order by synced_at DESC
-- - Compress chunks older than 2 days (2x chunk_interval)
-- - Tier to S3 after 1 year (best-effort; skipped where unavailable)
--
-- Auditability follows ADR-0002: processing_version + build_id, PK = natural
-- key + processing_version, and a build-aware advisory-locked BEFORE INSERT
-- trigger (prefix: pcs).

CREATE TABLE IF NOT EXISTS prime_capital_stack
(
    prime_id                          BIGINT      NOT NULL REFERENCES prime (id),
    synced_at                         TIMESTAMPTZ NOT NULL,
    exposure_usd                      NUMERIC     NOT NULL,
    required_risk_capital_usd         NUMERIC     NOT NULL,
    total_risk_capital_usd            NUMERIC     NOT NULL,
    junior_risk_capital_usd           NUMERIC     NOT NULL,
    senior_risk_capital_usd           NUMERIC     NOT NULL,
    internal_junior_risk_capital_usd  NUMERIC     NOT NULL,
    external_junior_risk_capital_usd  NUMERIC     NOT NULL,
    tokenized_junior_risk_capital_usd NUMERIC     NOT NULL,
    internal_senior_risk_capital_usd  NUMERIC     NOT NULL,
    external_senior_risk_capital_usd  NUMERIC     NOT NULL,
    encumbrance_ratio                 NUMERIC,
    exposure_share                    NUMERIC     NOT NULL,
    epi_utilization                   NUMERIC     NOT NULL,
    spj_utilization                   NUMERIC     NOT NULL,
    source                            TEXT        NOT NULL,
    processing_version                INT         NOT NULL DEFAULT 0,
    build_id                          INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (prime_id, synced_at, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE prime_capital_stack SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'prime_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('prime_capital_stack', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('prime_capital_stack', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for prime_capital_stack';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same build_id retry → reuse version (idempotent); new build_id → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_prime_capital_stack()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('pcs|%s|%s', NEW.prime_id, EXTRACT(epoch FROM NEW.synced_at)), 0));

    SELECT processing_version INTO existing_ver
    FROM prime_capital_stack
    WHERE prime_id  = NEW.prime_id
      AND synced_at = NEW.synced_at
      AND build_id  = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM prime_capital_stack
        WHERE prime_id  = NEW.prime_id
          AND synced_at = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET plan_cache_mode = 'force_custom_plan';

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON prime_capital_stack
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_prime_capital_stack();

-- ============================================================================
-- Catalogue metadata (COMMENT ON), consistent with 20260609 add_schema_comments.
--   [Type]: Hypertable
--   Roles:  PK | FK→table.col | Partition | Audit
--   Scale:  every *_usd column is an already-normalized USD decimal, never a
--           raw native-decimal integer; every ratio is a plain 0-1 fraction.
-- ============================================================================
COMMENT ON TABLE prime_capital_stack IS
  '[Hypertable] Per-cycle snapshot of a prime''s risk capital as reported by Sky''s Star Agents Risk Capital & Requirements Monitor, partitioned on synced_at. Reference data, not STL''s own model output — the two disagree by design and must not be mixed. The monitor publishes no history, so rows here can only be accumulated forward, never backfilled. A prime absent at a given synced_at is outside the monitor''s coverage, which is not zero exposure.';
COMMENT ON COLUMN prime_capital_stack.prime_id IS 'FK→prime.id. Part of PK.';
COMMENT ON COLUMN prime_capital_stack.synced_at IS 'Partition. Cron-cycle timestamp (UTC), shared by every row of one sync cycle. Part of PK.';
COMMENT ON COLUMN prime_capital_stack.exposure_usd IS 'Normalized USD decimal. Upstream total_exposure: the prime''s total exposure across all its allocations.';
COMMENT ON COLUMN prime_capital_stack.required_risk_capital_usd IS 'Normalized USD decimal. Upstream total_rrc (Required Risk Capital).';
COMMENT ON COLUMN prime_capital_stack.total_risk_capital_usd IS 'Normalized USD decimal. Upstream total_rc (Total Risk Capital); equals junior + senior.';
COMMENT ON COLUMN prime_capital_stack.junior_risk_capital_usd IS 'Normalized USD decimal. Upstream total_jrc (junior/first-loss capital). The measured split STL''s own model cannot produce; it can only approximate a buffer as total_rc - rrc.';
COMMENT ON COLUMN prime_capital_stack.senior_risk_capital_usd IS 'Normalized USD decimal. Upstream total_src (senior capital).';
COMMENT ON COLUMN prime_capital_stack.internal_junior_risk_capital_usd IS 'Normalized USD decimal. Upstream internal_jrc; component of junior_risk_capital_usd.';
COMMENT ON COLUMN prime_capital_stack.external_junior_risk_capital_usd IS 'Normalized USD decimal. Upstream external_jrc; component of junior_risk_capital_usd.';
COMMENT ON COLUMN prime_capital_stack.tokenized_junior_risk_capital_usd IS 'Normalized USD decimal. Upstream tokenized_jrc; component of junior_risk_capital_usd.';
COMMENT ON COLUMN prime_capital_stack.internal_senior_risk_capital_usd IS 'Normalized USD decimal. Upstream internal_src; component of senior_risk_capital_usd.';
COMMENT ON COLUMN prime_capital_stack.external_senior_risk_capital_usd IS 'Normalized USD decimal. Upstream external_src; component of senior_risk_capital_usd.';
COMMENT ON COLUMN prime_capital_stack.encumbrance_ratio IS 'Plain 0-1 fraction (not a percentage). Upstream encumbrance_ratio: required over total risk capital. NULL when the monitor omits it.';
COMMENT ON COLUMN prime_capital_stack.exposure_share IS 'Plain 0-1 fraction. Upstream total_exposure_share: the prime''s share of total protocol exposure.';
COMMENT ON COLUMN prime_capital_stack.epi_utilization IS 'Plain 0-1 fraction. Upstream epi_utilization.';
COMMENT ON COLUMN prime_capital_stack.spj_utilization IS 'Plain 0-1 fraction. Upstream spj_utilization.';
COMMENT ON COLUMN prime_capital_stack.source IS 'Provenance slug of the upstream route that produced the row, so a figure can be traced to the feed that reported it.';
COMMENT ON COLUMN prime_capital_stack.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN prime_capital_stack.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

INSERT INTO migrations (filename)
VALUES ('20260819_120000_create_prime_capital_stack.sql')
ON CONFLICT (filename) DO NOTHING;
