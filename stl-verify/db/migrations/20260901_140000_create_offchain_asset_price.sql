-- Off-chain prices for assets with no token row (VEC-539 design, driven by VEC-652).
--
-- offchain_token_price is keyed by token_id NOT NULL, so it can only hold prices
-- for assets that exist as ERC-20 contracts on an indexed chain. Assets that are
-- native to other ecosystems (XRP, HYPE, native BTC/SOL) have no address to make
-- a token row from, and the registry rules forbid inventing one. Their prices
-- land here instead, keyed by the offchain_price_asset catalog row — the curated
-- identity AGENTS.md prescribes for off-chain-only assets (token_id stays NULL).
-- Keyed by asset_id rather than the raw symbol (VEC-539's sketch): a symbol is a
-- display label, not an identifier, and the catalog row already carries source
-- and symbol.
--
-- No foreign keys, mirroring offchain_token_price: incompatible with distributed
-- hypertables. asset_id is an application-enforced reference to
-- offchain_price_asset.id.
--
-- Chunk interval 30 days, not the usual 1 day: this table holds a handful of
-- curated assets at hourly resolution (~24 rows/asset/day), and 1-day chunks
-- would create thousands of ~50-row chunks (the over-chunking VEC-663 repairs).
-- Compression follows the 2x-chunk-interval convention; tiering best-effort.
--
-- Auditability follows ADR-0002: processing_version + build_id, PK = natural key
-- + processing_version, build-aware advisory-locked BEFORE INSERT trigger
-- (prefix: oap) with plan_cache_mode pinned (VEC-541).

CREATE TABLE IF NOT EXISTS offchain_asset_price (
    asset_id           BIGINT      NOT NULL,
    source_id          SMALLINT    NOT NULL,
    timestamp          TIMESTAMPTZ NOT NULL,
    price_usd          NUMERIC(30, 18) NOT NULL,
    market_cap_usd     NUMERIC(30, 2),
    volume_usd         NUMERIC(30, 2),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (asset_id, source_id, processing_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '30 days'
);

ALTER TABLE offchain_asset_price SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_id',
    timescaledb.compress_orderby = 'timestamp DESC, processing_version DESC'
);

SELECT add_compression_policy('offchain_asset_price', INTERVAL '60 days', if_not_exists => TRUE);

-- Covering index for the version rule's per-key lookups, matching
-- idx_offchain_token_price_pv_lookup (20260424_120000).
CREATE INDEX IF NOT EXISTS idx_offchain_asset_price_pv_lookup
    ON offchain_asset_price (asset_id, source_id, timestamp, processing_version DESC);

DO $$ BEGIN
    PERFORM add_tiering_policy('offchain_asset_price', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for offchain_asset_price';
END $$;

-- The version rule, callable from both the INSERT and the trigger. Same
-- build_id retry → reuse version (idempotent); new build_id → MAX+1.
--
-- The INSERT must call this in its VALUES list rather than leave the version to
-- the trigger: on a columnstored chunk TimescaleDB resolves ON CONFLICT before
-- row triggers fire, so a trigger-assigned version reaches the arbiter as
-- DEFAULT 0 and the correction row is silently discarded (see
-- 20260821_120000_morpho_adapter_state_version_function.sql and ADR-0002 §3).
--
-- VOLATILE is spelled out because correctness rests on it: a STABLE function
-- would read the calling statement's snapshot, so a writer released from the
-- advisory lock would recompute the version the previous writer already used.
CREATE OR REPLACE FUNCTION next_processing_version_offchain_asset_price(
    p_asset_id  BIGINT,
    p_source_id SMALLINT,
    p_timestamp TIMESTAMPTZ,
    p_build_id  INT)
RETURNS INT
VOLATILE
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    -- Timestamp wrapped in EXTRACT(epoch FROM …) so the key is TimeZone/DateStyle-stable.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('oap|%s|%s|%s', p_asset_id, p_source_id, EXTRACT(epoch FROM p_timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM offchain_asset_price
    WHERE asset_id  = p_asset_id
      AND source_id = p_source_id
      AND timestamp = p_timestamp
      AND build_id  = p_build_id
    LIMIT 1;

    IF FOUND THEN
        RETURN existing_ver;
    END IF;

    SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
    FROM offchain_asset_price
    WHERE asset_id  = p_asset_id
      AND source_id = p_source_id
      AND timestamp = p_timestamp;
    RETURN max_ver + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION next_processing_version_offchain_asset_price(BIGINT, SMALLINT, TIMESTAMPTZ, INT) IS
  'Returns the processing_version an offchain_asset_price row at (asset_id, source_id, timestamp) must carry for build_id: the version that build already wrote there, else MAX+1. Takes the row''s advisory lock (ADR-0002 §3) and holds it for the transaction. Call it in the INSERT''s VALUES list: on a columnstored chunk TimescaleDB resolves ON CONFLICT before row triggers fire, so a version left to the trigger reaches the arbiter as DEFAULT 0 and the correction row is silently discarded.';

-- The trigger delegates to the same rule, as the floor for writers that do not
-- call the function (psql, ad-hoc scripts) on rowstore chunks.
CREATE OR REPLACE FUNCTION assign_processing_version_offchain_asset_price()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
BEGIN
    NEW.processing_version := next_processing_version_offchain_asset_price(
        NEW.asset_id, NEW.source_id, NEW.timestamp, NEW.build_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON offchain_asset_price
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_offchain_asset_price();

COMMENT ON TABLE offchain_asset_price IS
  '[Hypertable] API-polled USD prices for assets with NO token row (offchain_price_asset.token_id IS NULL): natives of non-indexed ecosystems such as XRP, HYPE, BTC, SOL. Companion to offchain_token_price, which requires a token_id and therefore cannot hold these. Chunk interval is 30 days, not 1 day: a handful of curated assets at hourly resolution would otherwise produce thousands of tiny chunks. Partition key: timestamp.';
COMMENT ON COLUMN offchain_asset_price.asset_id IS
  'FK→offchain_price_asset.id (app-only; hypertables take no FK). The catalog row is the asset''s identity — these assets have no token row by design (registry rules: never invent addresses).';
COMMENT ON COLUMN offchain_asset_price.source_id IS
  'FK→offchain_price_source.id (app-only). SMALLINT to match offchain_token_price. Redundant with asset_id''s own source, kept so the natural key and trigger mirror offchain_token_price exactly.';
COMMENT ON COLUMN offchain_asset_price.timestamp IS
  'Partition key. API observation time.';
COMMENT ON COLUMN offchain_asset_price.price_usd IS
  'USD price from the off-chain provider, already normalized (not fixed-point).';
COMMENT ON COLUMN offchain_asset_price.market_cap_usd IS
  'Market capitalization in USD as reported by the provider; NULL when not reported.';
COMMENT ON COLUMN offchain_asset_price.volume_usd IS
  '24h trading volume in USD as reported by the provider; NULL when not reported.';
COMMENT ON COLUMN offchain_asset_price.processing_version IS
  'Roles: PK. Correction version: 0=original, N=Nth reprocess (ADR-0002).';
COMMENT ON COLUMN offchain_asset_price.build_id IS
  'Roles: Audit. Which deployment wrote the row; never used to pick latest.';

-- Append-only from birth: ingest INSERTs only, corrections are new
-- processing_version rows.
REVOKE UPDATE, DELETE ON offchain_asset_price FROM stl_readwrite;

-- offchain_only declares, rather than infers, that an asset's prices belong in
-- offchain_asset_price. token_id NULL alone cannot carry that meaning: the
-- original seed resolved token_id by symbol match, so a mismatch or an asset
-- registered before its token row leaves token_id NULL by ACCIDENT, and routing
-- on the absence would silently bury such an asset's prices in a table nothing
-- reads. The fetcher refuses assets that are neither token-linked nor declared
-- offchain-only.
ALTER TABLE offchain_price_asset
    ADD COLUMN IF NOT EXISTS offchain_only BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE offchain_price_asset
    ADD CONSTRAINT offchain_price_asset_identity_check
    CHECK (NOT (offchain_only AND token_id IS NOT NULL));
COMMENT ON COLUMN offchain_price_asset.offchain_only IS
  'Roles: Configuration. TRUE = the asset deliberately has no token row (native of a non-indexed ecosystem: XRP, HYPE, native BTC/SOL) and its prices are stored in offchain_asset_price. FALSE with token_id NULL is a configuration defect the fetcher refuses.';

-- XRP and HYPE back >$150M of Syrup debt the CORE model simulates (VEC-652),
-- and neither exists as a mainnet ERC-20, so token_id stays NULL and their
-- prices land in offchain_asset_price. IDs verified against the CoinGecko API.
-- Enabled: the scheduled offchain-price sweep keeps the series current.
-- ON CONFLICT DO NOTHING: a pre-existing catalog row (operator-configured)
-- deliberately wins over this seed.
DO $$ BEGIN
    -- The seed below joins on this row; without the guard a missing source row
    -- would insert zero rows silently, and a once-applied migration never re-runs.
    IF NOT EXISTS (SELECT 1 FROM offchain_price_source WHERE name = 'coingecko') THEN
        RAISE EXCEPTION 'offchain_price_source has no coingecko row; the XRP/HYPE seed would be a silent no-op';
    END IF;
END $$;

INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, name, symbol, enabled, offchain_only)
SELECT ps.id, pa.source_asset_id, NULL, pa.name, pa.symbol, true, true
FROM offchain_price_source ps,
     (VALUES
         ('ripple', 'XRP', 'XRP'),
         ('hyperliquid', 'Hyperliquid', 'HYPE')
     ) AS pa(source_asset_id, name, symbol)
WHERE ps.name = 'coingecko'
ON CONFLICT (source_id, source_asset_id) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260901_140000_create_offchain_asset_price.sql')
ON CONFLICT (filename) DO NOTHING;
