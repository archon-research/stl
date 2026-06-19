-- CEX L2 order book snapshots (VEC-3d): one row per (exchange, symbol) per tick,
-- written by the cex-orderbook-indexer (one pod per exchange).
--
-- SNAPSHOT-PER-ROW, not row-per-level: each row carries the full top-N book for a
-- symbol at a moment in time. bids/asks are JSONB arrays of ["price","size"]
-- string tuples, e.g. [["65000.1","0.5"],["65000.0","1.2"]]. Prices and sizes are
-- the EXACT decimal strings the venue published (never float64), so the stored
-- value byte-matches the feed and loses no precision. Each side is pre-trimmed to
-- the configured depth and ordered best first (bids: highest price first; asks:
-- lowest price first) by the writer.
--
-- Append-only: the indexer only ever INSERTs; there is no UPDATE/DELETE and no
-- reorg/version concept (a CEX feed has no chain reorg, unlike on-chain tables).
-- A later fingerprint-dedup-with-heartbeat (write only on change, plus a periodic
-- heartbeat row) would still be pure inserts, so this schema needs no change for
-- it.
--
-- Timestamps:
--   event_time   venue event time copied from the feed; NULL when the feed
--                carried none (it is never fabricated from a local clock), which
--                is why it is nullable and NOT the partition column.
--   ingested_at  when the provider produced the source update (always set).
--   persisted_at when the snapshot was captured for writing (the tick); this is
--                the hypertable partition column because it is always present.
--
-- Compression strategy (consistent with the maple/morpho hypertables):
--   segment by (exchange, symbol)  -- queries always filter by these
--   order by persisted_at DESC     -- time-series access pattern
--   compress chunks older than 2 days (2x chunk_interval)
--   tier to S3 after 1 year (best-effort; skipped where unavailable)

CREATE TABLE IF NOT EXISTS cex_orderbook_snapshots
(
    exchange     TEXT        NOT NULL,
    symbol       TEXT        NOT NULL,
    event_time   TIMESTAMPTZ,           -- nullable: NULL when the feed carried no event time
    ingested_at  TIMESTAMPTZ NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL,
    bids         JSONB       NOT NULL,   -- [["price","size"], ...] best (highest) first
    asks         JSONB       NOT NULL    -- [["price","size"], ...] best (lowest) first
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'persisted_at',
    tsdb.chunk_interval = '1 day'
);

-- Latest-book and time-range lookups for a symbol on an exchange.
CREATE INDEX IF NOT EXISTS idx_cex_orderbook_snapshots_lookup
    ON cex_orderbook_snapshots (exchange, symbol, persisted_at DESC);

ALTER TABLE cex_orderbook_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'persisted_at DESC'
);

SELECT add_compression_policy('cex_orderbook_snapshots', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('cex_orderbook_snapshots', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for cex_orderbook_snapshots';
END $$;

GRANT SELECT ON cex_orderbook_snapshots TO stl_readonly;
GRANT SELECT, INSERT ON cex_orderbook_snapshots TO stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260619_120000_create_cex_orderbook_snapshots.sql')
ON CONFLICT (filename) DO NOTHING;
