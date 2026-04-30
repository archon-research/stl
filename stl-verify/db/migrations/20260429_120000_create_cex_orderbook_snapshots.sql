-- CEX orderbook snapshot storage for the risk model's liquidation slippage calculations.
-- Stores raw per-exchange orderbook snapshots. Aggregation across exchanges happens at read time.

CREATE TABLE IF NOT EXISTS cex_orderbook_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    exchange        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    captured_at     TIMESTAMPTZ NOT NULL,
    bid_data        JSONB NOT NULL,
    ask_data        JSONB NOT NULL,
    bid_levels      INT NOT NULL,
    ask_levels      INT NOT NULL,
    latency_ms      INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cex_ob_exchange_symbol_captured
    ON cex_orderbook_snapshots (exchange, symbol, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_cex_ob_symbol_captured
    ON cex_orderbook_snapshots (symbol, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_cex_ob_created_at
    ON cex_orderbook_snapshots (created_at);

INSERT INTO migrations (filename)
VALUES ('20260429_120000_create_cex_orderbook_snapshots.sql')
ON CONFLICT (filename) DO NOTHING;
