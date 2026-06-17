-- Migration: capital metrics snapshots (time series)
--
-- capital_metrics_snapshot accumulates periodic per-prime capital metrics sourced
-- from the upstream Star risk-capital monitor. The live /v1/capital-metrics
-- endpoint fetches that upstream on every request and keeps no history, so there
-- is nothing to aggregate into a trend. This table gives the API a real time
-- series, ingested by the capital-metrics-indexer cronjob.
--
-- Amounts are stored as the upstream decimal strings in NUMERIC columns.
-- capital_buffer is intentionally not stored; it is derived on read as
-- max(total_capital - first_loss_capital, 0), matching the live endpoint, so the
-- buffer can never drift out of sync with its inputs.
-- Partitioned as a TimescaleDB hypertable on synced_at (mirrors prime_debt).
CREATE TABLE IF NOT EXISTS capital_metrics_snapshot
(
    prime_id              BIGINT      NOT NULL REFERENCES prime (id),
    risk_capital          NUMERIC     NOT NULL,
    total_capital         NUMERIC     NOT NULL,
    first_loss_capital    NUMERIC     NOT NULL,
    risk_to_capital_ratio NUMERIC,
    benchmark_source      TEXT        NOT NULL,
    synced_at             TIMESTAMPTZ NOT NULL,
    UNIQUE (prime_id, synced_at)
) WITH ( tsdb.hypertable, tsdb.partition_column = 'synced_at', tsdb.chunk_interval = '1 day' );

CREATE INDEX IF NOT EXISTS idx_capital_metrics_prime_synced
    ON capital_metrics_snapshot (prime_id, synced_at DESC);

CREATE INDEX IF NOT EXISTS idx_capital_metrics_synced_at
    ON capital_metrics_snapshot (synced_at DESC);

INSERT INTO migrations (filename)
VALUES ('20260617_120000_create_capital_metrics_snapshots.sql')
ON CONFLICT (filename) DO NOTHING;
