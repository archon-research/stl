-- Oracle source table (metadata about each onchain oracle provider)
CREATE TABLE IF NOT EXISTS oracle_source (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(100) NOT NULL,
    chain_id INT NOT NULL,
    pool_address_provider BYTEA,
    deployment_block BIGINT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed SparkLend oracle source
-- PoolAddressProvider: 0x02c3eA4e34C0cBd694D2adFa2c690EECbC1793eE
-- Deployment block: 16664447 (SparkLend mainnet deployment)
INSERT INTO oracle_source (name, display_name, chain_id, pool_address_provider, deployment_block, enabled)
VALUES (
    'sparklend',
    'SparkLend Oracle',
    1,
    '\x02c3eA4e34C0cBd694D2adFa2c690EECbC1793eE',
    16664447,
    true
)
ON CONFLICT (name) DO NOTHING;

-- Oracle asset mapping table (which tokens to fetch oracle prices for)
CREATE TABLE IF NOT EXISTS oracle_asset (
    id BIGSERIAL PRIMARY KEY,
    oracle_source_id BIGINT NOT NULL REFERENCES oracle_source(id),
    token_id BIGINT NOT NULL REFERENCES token(id),
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oracle_asset_unique UNIQUE (oracle_source_id, token_id)
);

-- Seed SparkLend reserve tokens for oracle price fetching
-- Links oracle_source to tokens seeded in the token table via symbol match
INSERT INTO oracle_asset (oracle_source_id, token_id, enabled)
SELECT os.id, t.id, true
FROM oracle_source os
CROSS JOIN (VALUES
    ('DAI'),
    ('sDAI'),
    ('USDC'),
    ('WETH'),
    ('wstETH'),
    ('WBTC'),
    ('GNO'),
    ('rETH'),
    ('USDT'),
    ('weETH'),
    ('cbBTC'),
    ('sUSDS'),
    ('USDS'),
    ('LBTC'),
    ('tBTC'),
    ('ezETH'),
    ('rsETH'),
    ('PYUSD')
) AS symbols(symbol)
JOIN token t ON t.symbol = symbols.symbol AND t.chain_id = 1
WHERE os.name = 'sparklend'
ON CONFLICT (oracle_source_id, token_id) DO NOTHING;

-- Onchain token price table (oracle prices per block)
--
-- Stores only price changes (not every block). Chainlink oracles update on
-- heartbeat (~1 hour) or deviation threshold (~0.5-1%), so most blocks have
-- no price change. This reduces storage by ~200x vs storing every block.
--
-- Query pattern for "price at block N" (block may not have a row):
--   SELECT * FROM onchain_token_price
--   WHERE token_id = $1 AND oracle_source_id = $2 AND block_number <= $3
--   ORDER BY block_number DESC, block_version DESC
--   LIMIT 1
--
-- TimescaleDB chunk interval rationale (1 day):
-- Per TimescaleDB guidance, active chunks should fit in ~25% of shared_buffers.
-- - Ethereum: 1 oracle × 18 assets × 7,200 blocks/day = ~130K rows/day (~13 MB)
-- - Future (10 oracles × 20 assets): ~1.4M rows/day (~144 MB)
-- - With change-only storage: ~7,200 rows/day (trivially small)
-- 1-day chunks consistent with offchain tables, enable granular S3 offload.

CREATE TABLE IF NOT EXISTS onchain_token_price (
    token_id BIGINT NOT NULL,
    oracle_source_id SMALLINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_version SMALLINT NOT NULL DEFAULT 0,
    timestamp TIMESTAMPTZ NOT NULL,
    oracle_address BYTEA NOT NULL,
    price_usd NUMERIC(30, 18) NOT NULL,
    PRIMARY KEY (token_id, oracle_source_id, block_number, block_version, timestamp)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'timestamp',
    tsdb.chunk_interval = '1 day'
);

-- Enable compression on onchain_token_price hypertable
-- Segment by (oracle_source_id, token_id) — queries filter by both;
-- ~200 segments per chunk at 10-oracle scale, ~7,200 rows each = good compression.
-- Order by block_number descending for time-series query patterns.
ALTER TABLE onchain_token_price SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'oracle_source_id, token_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC'
);

-- Compress chunks older than 2 days (2x chunk_interval)
SELECT add_compression_policy('onchain_token_price', INTERVAL '2 days', if_not_exists => TRUE);

-- Tier data older than 1 year to S3-backed object storage (Tiger Cloud Scale Plan)
-- Data stays queryable via standard SQL, just stored cheaper and accessed slower
-- $0.021/GB-month vs $0.212/GB-month (10x cheaper)
-- Only available on Timescale Cloud; skipped gracefully on self-hosted.
DO $$ BEGIN
    PERFORM add_tiering_policy('onchain_token_price', INTERVAL '1 year');
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for onchain_token_price';
END $$;

INSERT INTO migrations (filename)
VALUES ('20260206_100000_create_onchain_prices.sql')
ON CONFLICT (filename) DO NOTHING;
