-- Per-cycle snapshots of a prime's balance-sheet positions as reported by Sky's
-- internal feed.
-- Source: https://sky.data.blockanalitica.com/internal/allocations/?prime={star}
--
-- Why this table exists: the feed publishes only a current snapshot — the same
-- host's /primes/historic/ route (→ prime_reference_balance_sheet) carries
-- per-prime daily AGGREGATES, not positions — so a position-level reference
-- figure can never be reconstructed for a past instant, only observed forward.
--
-- This is a different question from prime_capital_stack_allocation: that table
-- carries the risk-capital breakdown, this one the balance sheet. The two are
-- not interchangeable.
--
-- Row identity is (prime, cycle, network, token_address): the client rejects a
-- fetch containing duplicate identities, so a conflicting insert is a replay
-- rather than a lost row. The feed also serves wallet_address and
-- allocation_type; both are deliberately not recorded, mirroring the serving
-- layer's decision — each is one field to reinstate if a consumer appears.
--
-- Identity fields (protocol, symbol, name, address) are upstream's claims
-- recorded verbatim, deliberately NOT foreign keys into STL's registries:
-- reference data must stay traceable to what the feed said, including tokens
-- STL does not index. Registry resolution happens at read time.
--
-- Encoding: every *_usd amount is an already-normalized USD decimal, NOT a raw
-- native-decimal integer.
--
-- Compression strategy:
-- - Segment by entity FK (prime_id), order by synced_at DESC
-- - Compress chunks older than 2 days (2x chunk_interval)
-- - Tier to S3 after 1 year (best-effort; skipped where unavailable)
--
-- Auditability follows ADR-0002: processing_version + build_id, PK = natural
-- key + processing_version, and a build-aware advisory-locked BEFORE INSERT
-- trigger (prefix: prp).

CREATE TABLE IF NOT EXISTS prime_reference_position
(
    prime_id             BIGINT      NOT NULL REFERENCES prime (id),
    synced_at            TIMESTAMPTZ NOT NULL,
    network              TEXT        NOT NULL,
    chain_id             INTEGER,
    protocol_name        TEXT        NOT NULL,
    token_symbol         TEXT        NOT NULL,
    token_name           TEXT,
    token_address        TEXT        NOT NULL,
    assets_usd           NUMERIC     NOT NULL,
    allocated_assets_usd NUMERIC,
    idle_assets_usd      NUMERIC,
    source               TEXT        NOT NULL,
    processing_version   INT         NOT NULL DEFAULT 0,
    build_id             INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (prime_id, synced_at, network, token_address, processing_version)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'synced_at',
    tsdb.chunk_interval = '1 day'
);

ALTER TABLE prime_reference_position SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'prime_id',
    timescaledb.compress_orderby = 'synced_at DESC'
);

SELECT add_compression_policy('prime_reference_position', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('prime_reference_position', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for prime_reference_position';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same build_id retry → reuse version (idempotent); new build_id → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_prime_reference_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('prp|%s|%s|%s|%s',
            NEW.prime_id, EXTRACT(epoch FROM NEW.synced_at), NEW.network, NEW.token_address), 0));

    SELECT processing_version INTO existing_ver
    FROM prime_reference_position
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
        FROM prime_reference_position
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
    BEFORE INSERT ON prime_reference_position
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_prime_reference_position();

-- ============================================================================
-- Catalogue metadata (COMMENT ON), consistent with 20260609 add_schema_comments.
--   [Type]: Hypertable
--   Roles:  PK | FK→table.col | Partition | Audit
--   Scale:  every *_usd column is an already-normalized USD decimal, never a
--           raw native-decimal integer.
-- ============================================================================
COMMENT ON TABLE prime_reference_position IS
  '[Hypertable] Per-cycle snapshot of a prime''s balance-sheet positions as reported by Sky''s internal feed, partitioned on synced_at. Reference data, not STL''s own indexing. Position-level counterpart of prime_reference_balance_sheet''s daily aggregates, and a different question from prime_capital_stack_allocation (balance sheet vs risk-capital breakdown). The feed publishes no history, so rows can only be accumulated forward, never backfilled. Identity fields are upstream''s claims verbatim, not registry FKs — registry resolution happens at read time.';
COMMENT ON COLUMN prime_reference_position.prime_id IS 'FK→prime.id. Part of PK.';
COMMENT ON COLUMN prime_reference_position.synced_at IS 'Partition. Cron-cycle timestamp (UTC), shared by every row of one sync cycle and equal to the cycle''s prime_capital_stack.synced_at. Part of PK.';
COMMENT ON COLUMN prime_reference_position.network IS 'Upstream''s network label verbatim (e.g. ''ethereum'' where STL says ''mainnet''). Part of PK: the same token address may exist on two networks.';
COMMENT ON COLUMN prime_reference_position.chain_id IS 'EVM chain id mapped from network at write time. NULL for a network STL has no id for, which is a fact about the mapping, not missing data.';
COMMENT ON COLUMN prime_reference_position.protocol_name IS 'Upstream protocol label verbatim, not an FK into protocol.';
COMMENT ON COLUMN prime_reference_position.token_symbol IS 'Position token symbol as upstream reports it (upstream field token_symbol).';
COMMENT ON COLUMN prime_reference_position.token_name IS 'Position token name as upstream reports it (upstream field token_name). NULL when omitted.';
COMMENT ON COLUMN prime_reference_position.token_address IS '0x-prefixed address of the token that is the position, as upstream reports it (upstream field address). Part of PK.';
COMMENT ON COLUMN prime_reference_position.assets_usd IS 'Normalized USD decimal. Upstream assets: the position''s balance-sheet value.';
COMMENT ON COLUMN prime_reference_position.allocated_assets_usd IS 'Normalized USD decimal. Upstream allocated_assets. NULL when upstream omits it, which is distinct from zero.';
COMMENT ON COLUMN prime_reference_position.idle_assets_usd IS 'Normalized USD decimal. Upstream idle_assets. NULL when upstream omits it, which is distinct from zero.';
COMMENT ON COLUMN prime_reference_position.source IS 'Provenance slug of the upstream route that produced the row, so a figure can be traced to the feed that reported it.';
COMMENT ON COLUMN prime_reference_position.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN prime_reference_position.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

INSERT INTO migrations (filename)
VALUES ('20260826_121000_create_prime_reference_position.sql')
ON CONFLICT (filename) DO NOTHING;
