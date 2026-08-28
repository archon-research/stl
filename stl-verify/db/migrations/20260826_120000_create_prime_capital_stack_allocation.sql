-- Per-cycle snapshots of a prime's per-allocation risk-capital breakdown as
-- reported by Sky's Star Agents Risk Capital & Requirements Monitor.
-- Source: https://info-sky.blockanalitica.com/star-monitoring/risk-capital/primes/{star}/allocations/
--
-- Why this table exists: the monitor publishes no history at any granularity,
-- so a per-allocation reference figure can never be reconstructed for a past
-- instant, only observed going forward. The prime-level totals of the same
-- feed accumulate in prime_capital_stack; this table carries the breakdown
-- behind them, which until now was only fetched live at API request time.
-- One cycle writes both tables with the same synced_at, so they join exactly.
--
-- Row identity is (prime, cycle, network, token_address): the feed reports one
-- row per position token per network (verified live), and the client rejects a
-- fetch containing duplicate identities, so a conflicting insert is a replay of
-- the same cycle rather than a silently lost row.
--
-- Identity fields (protocol, symbol, name, addresses) are upstream's claims
-- recorded verbatim, deliberately NOT foreign keys into STL's registries:
-- reference data must stay traceable to what the feed said, including tokens
-- STL does not index. Registry resolution happens at read time.
--
-- Encoding: every *_usd amount is an already-normalized USD decimal, NOT a raw
-- native-decimal integer. crr is a plain 0-1 fraction, not a percentage —
-- upstream's crr vocabulary is fractional throughout (crr == rrc / exposure
-- by definition); consumers that need percent rescale at their boundary.
--
-- Compression strategy:
-- - Segment by entity FK (prime_id), order by synced_at DESC
-- - 7-day chunks: at the 15-minute cadence the reference row tables collect
--   only ~1.1k-7.7k rows/day (~0.5-3 MB), far below where daily chunks pay
--   off, and chunk COUNT (not volume) drives planner/executor memory (VEC-663 / #808).
-- - Compress chunks older than 14 days (2x chunk_interval)
-- - Tier to S3 after 1 year (best-effort; skipped where unavailable)
--
-- Auditability follows ADR-0002: processing_version + build_id, PK = natural
-- key + processing_version, and a build-aware advisory-locked BEFORE INSERT
-- trigger (prefix: pcsa).

CREATE TABLE IF NOT EXISTS prime_capital_stack_allocation
(
    prime_id                  BIGINT      NOT NULL REFERENCES prime (id),
    synced_at                 TIMESTAMPTZ NOT NULL,
    network                   TEXT        NOT NULL,
    chain_id                  INTEGER,
    protocol_name             TEXT        NOT NULL,
    symbol                    TEXT        NOT NULL,
    name                      TEXT,
    token_address             TEXT        NOT NULL,
    loan_token_address        TEXT,
    loan_token_symbol         TEXT,
    exposure_usd              NUMERIC     NOT NULL,
    required_risk_capital_usd NUMERIC     NOT NULL,
    crr                       NUMERIC     NOT NULL,
    source                    TEXT        NOT NULL,
    processing_version        INT         NOT NULL DEFAULT 0,
    build_id                  INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (prime_id, synced_at, network, token_address, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '7 days'
);

ALTER TABLE prime_capital_stack_allocation SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'prime_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('prime_capital_stack_allocation', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('prime_capital_stack_allocation', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for prime_capital_stack_allocation';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same build_id retry → reuse version (idempotent); new build_id → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_prime_capital_stack_allocation()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('pcsa|%s|%s|%s|%s',
            NEW.prime_id, EXTRACT(epoch FROM NEW.synced_at), NEW.network, NEW.token_address), 0));

    SELECT processing_version INTO existing_ver
    FROM prime_capital_stack_allocation
    WHERE prime_id      = NEW.prime_id
      AND synced_at     = NEW.synced_at
      AND network       = NEW.network
      AND token_address = NEW.token_address
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM prime_capital_stack_allocation
        WHERE prime_id      = NEW.prime_id
          AND synced_at     = NEW.synced_at
          AND network       = NEW.network
          AND token_address = NEW.token_address;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET plan_cache_mode = 'force_custom_plan';

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON prime_capital_stack_allocation
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_prime_capital_stack_allocation();

-- ============================================================================
-- Catalogue metadata (COMMENT ON), consistent with 20260609 add_schema_comments.
--   [Type]: Hypertable
--   Roles:  PK | FK→table.col | Partition | Audit
--   Scale:  every *_usd column is an already-normalized USD decimal, never a
--           raw native-decimal integer; crr is a plain 0-1 fraction.
-- ============================================================================
COMMENT ON TABLE prime_capital_stack_allocation IS
  '[Hypertable] Per-cycle snapshot of a prime''s per-allocation risk-capital breakdown as reported by Sky''s Star Agents Risk Capital & Requirements Monitor, partitioned on synced_at. Reference data, not STL''s own model output. The breakdown behind prime_capital_stack''s totals, written by the same cycle with the same synced_at so the two join exactly. The monitor publishes no history, so rows can only be accumulated forward, never backfilled. Identity fields are upstream''s claims verbatim, not registry FKs — registry resolution happens at read time.';
COMMENT ON COLUMN prime_capital_stack_allocation.prime_id IS 'FK→prime.id. Part of PK.';
COMMENT ON COLUMN prime_capital_stack_allocation.synced_at IS 'Partition. Cron-cycle timestamp (UTC), shared by every row of one sync cycle and equal to the cycle''s prime_capital_stack.synced_at. Part of PK.';
COMMENT ON COLUMN prime_capital_stack_allocation.network IS 'Upstream''s network label verbatim (e.g. ''ethereum'' where STL says ''mainnet''). Part of PK: the same token address may exist on two networks.';
COMMENT ON COLUMN prime_capital_stack_allocation.chain_id IS 'EVM chain id mapped from network at write time. NULL for a network STL has no id for, which is a fact about the mapping, not missing data.';
COMMENT ON COLUMN prime_capital_stack_allocation.protocol_name IS 'Upstream protocol label verbatim (e.g. ''sparklend''), not an FK into protocol.';
COMMENT ON COLUMN prime_capital_stack_allocation.symbol IS 'Position token symbol as upstream reports it.';
COMMENT ON COLUMN prime_capital_stack_allocation.name IS 'Position token name as upstream reports it. NULL when omitted.';
COMMENT ON COLUMN prime_capital_stack_allocation.token_address IS '0x-prefixed address of the token that is the position, as upstream reports it. Part of PK.';
COMMENT ON COLUMN prime_capital_stack_allocation.loan_token_address IS '0x-prefixed address of the loan/underlying token upstream pairs with the position. NULL when omitted.';
COMMENT ON COLUMN prime_capital_stack_allocation.loan_token_symbol IS 'Symbol of the loan/underlying token. NULL when omitted.';
COMMENT ON COLUMN prime_capital_stack_allocation.exposure_usd IS 'Normalized USD decimal. Upstream exposure: this allocation''s exposure.';
COMMENT ON COLUMN prime_capital_stack_allocation.required_risk_capital_usd IS 'Normalized USD decimal. Upstream rrc (Required Risk Capital) for this allocation.';
COMMENT ON COLUMN prime_capital_stack_allocation.crr IS 'Plain 0-1 fraction (not a percentage). Upstream crr: capital-risk ratio, equal to rrc / exposure. Consumers needing percent rescale at their own boundary.';
COMMENT ON COLUMN prime_capital_stack_allocation.source IS 'Provenance slug of the upstream route that produced the row, so a figure can be traced to the feed that reported it.';
COMMENT ON COLUMN prime_capital_stack_allocation.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN prime_capital_stack_allocation.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

INSERT INTO migrations (filename)
VALUES ('20260826_120000_create_prime_capital_stack_allocation.sql')
ON CONFLICT (filename) DO NOTHING;
