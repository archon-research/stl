-- Uniswap V3 pool snapshots (periodic state, polled every N minutes).
-- Stores composition, TVL, price, liquidity, and TWAP for tracked Uniswap V3 pools.
--
-- block_version is always 0 because this worker reads finalized blocks only.
-- snapshot_time is derived from the block header timestamp (deterministic).
CREATE TABLE IF NOT EXISTS uniswap_pool_snapshot
(
    pool_address            BYTEA       NOT NULL,
    chain_id                INT         NOT NULL REFERENCES chain (chain_id),
    block_number            BIGINT      NOT NULL,
    block_version           INT         NOT NULL DEFAULT 0,

    -- Token composition (JSONB with address, token_id, balance, decimals, symbol)
    token0                  JSONB       NOT NULL,
    token1                  JSONB       NOT NULL,

    -- Pool parameters
    fee                     INT         NOT NULL, -- fee tier in hundredths of a bip (100 = 0.01%)

    -- Price state
    sqrt_price_x96          NUMERIC     NOT NULL, -- raw Q64.96 value
    current_tick            INT         NOT NULL,
    price                   NUMERIC     NOT NULL, -- token1/token0 human-readable

    -- Liquidity
    active_liquidity        NUMERIC     NOT NULL, -- liquidity at current tick

    -- TVL
    tvl_usd                 NUMERIC,

    -- TWAP (30-min, nullable — observe() can fail if cardinality too low)
    twap_tick               INT,
    twap_price              NUMERIC,

    -- Fee growth accumulators (for APY computation between snapshots)
    fee_growth_global0_x128 NUMERIC     NOT NULL DEFAULT 0,
    fee_growth_global1_x128 NUMERIC     NOT NULL DEFAULT 0,

    snapshot_time           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pool_address, chain_id, block_number, block_version, snapshot_time)
) WITH (
      tsdb.hypertable,
      tsdb.partition_column = 'snapshot_time',
      tsdb.chunk_interval = '1 day'
      );

CREATE INDEX IF NOT EXISTS idx_uni_snap_pool_time
    ON uniswap_pool_snapshot (pool_address, chain_id, snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_uni_snap_time
    ON uniswap_pool_snapshot (snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_uni_snap_chain
    ON uniswap_pool_snapshot (chain_id);

GRANT SELECT ON uniswap_pool_snapshot TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON uniswap_pool_snapshot TO stl_readwrite;

ALTER TABLE uniswap_pool_snapshot
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pool_address, chain_id',
        timescaledb.compress_orderby = 'snapshot_time DESC'
        );

SELECT add_compression_policy('uniswap_pool_snapshot', INTERVAL '2 days', if_not_exists => TRUE);

DO
$$
    BEGIN
        PERFORM add_tiering_policy('uniswap_pool_snapshot', INTERVAL '1 year', if_not_exists => TRUE);
    EXCEPTION
        WHEN undefined_function THEN
            RAISE NOTICE 'add_tiering_policy not available, skipping tiering for uniswap_pool_snapshot';
    END
$$;

INSERT INTO migrations (filename)
VALUES ('20260326_120000_create_uniswap_pool_snapshot.sql')
ON CONFLICT (filename) DO NOTHING;