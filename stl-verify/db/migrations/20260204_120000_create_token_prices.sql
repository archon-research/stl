-- Price source table (metadata about each price provider)
CREATE TABLE IF NOT EXISTS price_source (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(100) NOT NULL,
    base_url VARCHAR(255),
    rate_limit_per_min INT,
    supports_historical BOOLEAN NOT NULL DEFAULT false,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed initial source
INSERT INTO price_source (name, display_name, base_url, rate_limit_per_min, supports_historical, enabled)
VALUES ('coingecko', 'CoinGecko', 'https://pro-api.coingecko.com/api/v3', 500, true, true)
ON CONFLICT (name) DO NOTHING;

-- Price asset mapping table
CREATE TABLE IF NOT EXISTS price_asset (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES price_source(id),
    source_asset_id VARCHAR(255) NOT NULL,
    token_id BIGINT REFERENCES token(id),
    name VARCHAR(255) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT price_asset_source_asset_unique UNIQUE (source_id, source_asset_id)
);

CREATE INDEX IF NOT EXISTS idx_price_asset_source_enabled
    ON price_asset (source_id) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_price_asset_token
    ON price_asset (token_id) WHERE token_id IS NOT NULL;

-- Token prices table (on-chain tokens only)
--
-- TimescaleDB chunk interval rationale (1 day):
-- Worst case (Base, 250ms blocks): 10 oracles × 20 assets × 345,600 blocks/day
-- = ~69M rows/day (~10 GB uncompressed per chunk).
-- 1-day chunks balance memory footprint against chunk count (~1,825 over 5 years).
--
-- Distributed TimescaleDB compatibility:
-- No foreign keys (incompatible with distributed hypertables).
-- No synthetic ID column (centralized sequences are a bottleneck across data nodes).
-- Natural PK (token_id, source, timestamp) serves both uniqueness and the primary
-- query pattern (token_id + time range).

CREATE TABLE IF NOT EXISTS token_price (
    token_id BIGINT NOT NULL,
    chain_id INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    source VARCHAR(50) NOT NULL,
    source_asset_id VARCHAR(255) NOT NULL,
    price_usd NUMERIC(30, 18) NOT NULL,
    market_cap_usd NUMERIC(30, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_id, source, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

-- Enable compression on token_price hypertable
-- Segment by token_id for efficient queries filtering by token + time range
-- Order by timestamp descending for time-series query patterns
ALTER TABLE token_price SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'token_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- Compress chunks older than 2 days (2x chunk_interval)
-- Compress aggressively to limit uncompressed working set. Queries over compressed
-- chunks decompress only the relevant token_id segments (aligned with compress_segmentby).
SELECT add_compression_policy('token_price', INTERVAL '2 days', if_not_exists => TRUE);

-- Token volume table (hourly granularity, on-chain tokens only)
-- Same design rationale as token_price - see comments above.

CREATE TABLE IF NOT EXISTS token_volume (
    token_id BIGINT NOT NULL,
    chain_id INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    source VARCHAR(50) NOT NULL,
    source_asset_id VARCHAR(255) NOT NULL,
    volume_usd NUMERIC(30, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_id, source, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

-- Enable compression on token_volume hypertable
-- Segment by token_id for efficient queries filtering by token + time range
-- Order by timestamp descending for time-series query patterns
ALTER TABLE token_volume SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'token_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- Compress chunks older than 2 days (2x chunk_interval)
-- Compress aggressively to limit uncompressed working set. Queries over compressed
-- chunks decompress only the relevant token_id segments (aligned with compress_segmentby).
SELECT add_compression_policy('token_volume', INTERVAL '2 days', if_not_exists => TRUE);

-- Seed SparkLend reserve token mappings for CoinGecko
-- Links to tokens seeded in previous migration via symbol match
INSERT INTO price_asset (source_id, source_asset_id, token_id, name, symbol, enabled)
SELECT ps.id, pa.source_asset_id, t.id, pa.name, pa.symbol, true
FROM price_source ps
CROSS JOIN (VALUES
    ('dai', 'Dai', 'DAI'),
    ('savings-dai', 'Savings Dai', 'sDAI'),
    ('usd-coin', 'USD Coin', 'USDC'),
    ('weth', 'Wrapped Ether', 'WETH'),
    ('wrapped-steth', 'Wrapped stETH', 'wstETH'),
    ('wrapped-bitcoin', 'Wrapped Bitcoin', 'WBTC'),
    ('gnosis', 'Gnosis', 'GNO'),
    ('rocket-pool-eth', 'Rocket Pool ETH', 'rETH'),
    ('tether', 'Tether', 'USDT'),
    ('wrapped-eeth', 'Wrapped eETH', 'weETH'),
    ('coinbase-wrapped-btc', 'Coinbase Wrapped BTC', 'cbBTC'),
    ('susds', 'Savings USDS', 'sUSDS'),
    ('usds', 'USDS', 'USDS'),
    ('lombard-staked-btc', 'Lombard Staked BTC', 'LBTC'),
    ('tbtc', 'tBTC', 'tBTC'),
    ('renzo-restaked-eth', 'Renzo Restaked ETH', 'ezETH'),
    ('kelp-dao-restaked-eth', 'Kelp DAO Restaked ETH', 'rsETH'),
    ('paypal-usd', 'PayPal USD', 'PYUSD')
) AS pa(source_asset_id, name, symbol)
LEFT JOIN token t ON t.symbol = pa.symbol AND t.chain_id = 1
WHERE ps.name = 'coingecko'
ON CONFLICT (source_id, source_asset_id) DO UPDATE SET
    token_id = EXCLUDED.token_id,
    updated_at = NOW();

INSERT INTO migrations (filename)
VALUES ('20260204_120000_create_token_prices.sql')
ON CONFLICT (filename) DO NOTHING;
