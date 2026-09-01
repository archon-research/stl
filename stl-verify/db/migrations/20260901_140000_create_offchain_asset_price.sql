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
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('offchain_asset_price', INTERVAL '60 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('offchain_asset_price', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for offchain_asset_price';
END $$;

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same build_id retry → reuse version (idempotent); new build_id → MAX+1.
CREATE OR REPLACE FUNCTION assign_processing_version_offchain_asset_price()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('oap|%s|%s|%s', NEW.asset_id, NEW.source_id, EXTRACT(epoch FROM NEW.timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM offchain_asset_price
    WHERE asset_id  = NEW.asset_id
      AND source_id = NEW.source_id
      AND timestamp = NEW.timestamp
      AND build_id  = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM offchain_asset_price
        WHERE asset_id  = NEW.asset_id
          AND source_id = NEW.source_id
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;
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

-- XRP and HYPE back >$150M of Syrup debt the CORE model simulates (VEC-652),
-- and neither exists as a mainnet ERC-20, so token_id stays NULL and their
-- prices land in offchain_asset_price. IDs verified against the CoinGecko API.
-- Enabled: the scheduled offchain-price sweep keeps the series current.
INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, name, symbol, enabled)
SELECT ps.id, pa.source_asset_id, NULL, pa.name, pa.symbol, true
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
