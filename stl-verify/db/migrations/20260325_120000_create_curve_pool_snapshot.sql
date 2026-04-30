-- Curve Stableswap-NG pool snapshots (periodic state, polled every N minutes).
-- Stores composition, TVL, prices, params for tracked Curve pools.
--
-- Fee APY is NOT stored — it is computed at query time from virtual_price
-- deltas between any two snapshots:
--   fee_apy = (vp_new - vp_old) / vp_old * (365 / days_elapsed)
--
-- block_version is always 0 because this worker reads finalized blocks only.
-- snapshot_time is derived from the block header timestamp (deterministic).
--
-- Compression strategy (consistent with anchorage/morpho tables):
-- - Segment by pool_address, chain_id (queries always filter by these)
-- - Order by snapshot_time DESC (time-series access pattern)
-- - Compress chunks older than 2 days (2x chunk_interval)
-- - Tier to S3 after 1 year
CREATE TABLE IF NOT EXISTS curve_pool_snapshot
(
    pool_address   BYTEA       NOT NULL,
    chain_id       INT         NOT NULL REFERENCES chain (chain_id),
    block_number   BIGINT      NOT NULL,
    block_version  INT         NOT NULL DEFAULT 0,

    -- Pool composition (per-coin balances stored as JSON array)
    -- e.g. [{"coin": "0xa393...", "token_id": 12, "balance": "11732289.24", "decimals": 18}]
    coin_balances  JSONB       NOT NULL,
    n_coins        INT         NOT NULL,

    -- Pool metrics
    total_supply   NUMERIC     NOT NULL,
    virtual_price  NUMERIC     NOT NULL, -- used to derive fee APY between snapshots
    tvl_usd        NUMERIC,

    -- Pool parameters
    amp_factor     INT         NOT NULL,
    fee            NUMERIC     NOT NULL,

    -- Oracle prices (JSON array, coin i+1 relative to coin 0)
    -- e.g. [{"index": 0, "price": "0.999915871162500393"}]
    oracle_prices  JSONB,                -- EMA prices (manipulation-resistant)
    last_prices    JSONB,                -- spot prices (most recent trade)
    exchange_rates JSONB,                -- get_dy results (1-unit swap outputs)

    -- Fee APY (computed from virtual_price delta vs previous snapshot)
    -- NULL on first snapshot for a pool (no prior data to compare)
    fee_apy        NUMERIC,

    snapshot_time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pool_address, chain_id, block_number, block_version, snapshot_time)
) WITH (
      tsdb.hypertable,
      tsdb.partition_column = 'snapshot_time',
      tsdb.chunk_interval = '1 day'
      );

CREATE INDEX IF NOT EXISTS idx_curve_snap_pool_time
    ON curve_pool_snapshot (pool_address, chain_id, snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_curve_snap_time
    ON curve_pool_snapshot (snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_curve_snap_chain
    ON curve_pool_snapshot (chain_id);

GRANT SELECT ON curve_pool_snapshot TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON curve_pool_snapshot TO stl_readwrite;

ALTER TABLE curve_pool_snapshot
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pool_address, chain_id',
        timescaledb.compress_orderby = 'snapshot_time DESC'
        );

SELECT add_compression_policy('curve_pool_snapshot', INTERVAL '2 days', if_not_exists => TRUE);

DO
$$
    BEGIN
        PERFORM add_tiering_policy('curve_pool_snapshot', INTERVAL '1 year', if_not_exists => TRUE);
    EXCEPTION
        WHEN undefined_function THEN
            RAISE NOTICE 'add_tiering_policy not available, skipping tiering for curve_pool_snapshot';
    END
$$;

INSERT INTO migrations (filename)
VALUES ('20260325_120000_create_curve_pool_snapshot.sql')
ON CONFLICT (filename) DO NOTHING;