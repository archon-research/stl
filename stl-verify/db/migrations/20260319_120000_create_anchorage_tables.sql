-- Anchorage package snapshots (current state, polled every N minutes)
-- Partitioned as a TimescaleDB hypertable on snapshot_time.
--
-- Compression strategy (consistent with morpho tables):
-- - Segment by prime_id, package_id (queries always filter by these)
-- - Order by snapshot_time DESC (time-series access pattern)
-- - Compress chunks older than 2 days (2x chunk_interval)
-- - Tier to S3 after 1 year
CREATE TABLE IF NOT EXISTS anchorage_package_snapshot
(
    prime_id             BIGINT      NOT NULL REFERENCES prime (id),
    package_id           VARCHAR(64) NOT NULL,
    pledgor_id           VARCHAR(64) NOT NULL,
    secured_party_id     VARCHAR(64) NOT NULL,
    active               BOOLEAN     NOT NULL,
    state                VARCHAR(32) NOT NULL,

    current_ltv          NUMERIC     NOT NULL,
    exposure_value       NUMERIC     NOT NULL,
    package_value        NUMERIC     NOT NULL,

    margin_call_ltv      NUMERIC     NOT NULL,
    critical_ltv         NUMERIC     NOT NULL,
    margin_return_ltv    NUMERIC     NOT NULL,

    asset_type           VARCHAR(32) NOT NULL,
    custody_type         VARCHAR(32) NOT NULL,
    asset_price          NUMERIC     NOT NULL,
    asset_quantity       NUMERIC     NOT NULL,
    asset_weighted_value NUMERIC     NOT NULL,

    ltv_timestamp        TIMESTAMPTZ NOT NULL,
    snapshot_time        TIMESTAMPTZ NOT NULL,
    UNIQUE (prime_id, package_id, asset_type, custody_type, snapshot_time)
) WITH ( tsdb.hypertable, tsdb.partition_column = 'snapshot_time', tsdb.chunk_interval = '1 day' );

CREATE INDEX IF NOT EXISTS idx_anchorage_pkg_snap_package_time
    ON anchorage_package_snapshot (prime_id, package_id, snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_anchorage_pkg_snap_state
    ON anchorage_package_snapshot (state, snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_anchorage_pkg_snap_time
    ON anchorage_package_snapshot (snapshot_time DESC);

ALTER TABLE anchorage_package_snapshot
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'prime_id, package_id',
        timescaledb.compress_orderby = 'snapshot_time DESC'
        );

SELECT add_compression_policy('anchorage_package_snapshot', INTERVAL '2 days', if_not_exists => TRUE);

DO
$$
    BEGIN
        PERFORM add_tiering_policy('anchorage_package_snapshot', INTERVAL '1 year', if_not_exists => TRUE);
    EXCEPTION
        WHEN undefined_function THEN
            RAISE NOTICE 'add_tiering_policy not available, skipping tiering for anchorage_package_snapshot';
    END
$$;

-- ============================================================================
-- Anchorage operations (event history: deposits, paydowns, margin returns, etc.)
-- Partitioned as a TimescaleDB hypertable on created_at.
--
-- Compression strategy:
-- - Segment by prime_id, type_id (queries filter by package/exposure)
-- - Order by created_at DESC, operation_id DESC
-- - Compress chunks older than 2 days
-- - Tier to S3 after 1 year
-- ============================================================================
CREATE TABLE IF NOT EXISTS anchorage_operation
(
    prime_id       BIGINT      NOT NULL REFERENCES prime (id),
    operation_id   VARCHAR(64) NOT NULL,
    action         VARCHAR(32) NOT NULL,
    operation_type VARCHAR(32) NOT NULL,
    type_id        VARCHAR(64) NOT NULL,
    asset_type     VARCHAR(32) NOT NULL,
    custody_type   VARCHAR(32) NOT NULL,
    quantity       NUMERIC     NOT NULL,
    notes          TEXT,
    created_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (operation_id, created_at)
) WITH ( tsdb.hypertable, tsdb.partition_column = 'created_at', tsdb.chunk_interval = '1 day' );

CREATE INDEX IF NOT EXISTS idx_anchorage_op_type_id
    ON anchorage_operation (type_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_anchorage_op_prime_time
    ON anchorage_operation (prime_id, created_at DESC, operation_id DESC);

CREATE INDEX IF NOT EXISTS idx_anchorage_op_action
    ON anchorage_operation (action, created_at DESC);

ALTER TABLE anchorage_operation
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'prime_id, type_id',
        timescaledb.compress_orderby = 'created_at DESC, operation_id DESC'
        );

SELECT add_compression_policy('anchorage_operation', INTERVAL '2 days', if_not_exists => TRUE);

DO
$$
    BEGIN
        PERFORM add_tiering_policy('anchorage_operation', INTERVAL '1 year', if_not_exists => TRUE);
    EXCEPTION
        WHEN undefined_function THEN
            RAISE NOTICE 'add_tiering_policy not available, skipping tiering for anchorage_operation';
    END
$$;

INSERT INTO migrations (filename)
VALUES ('20260319_120000_create_anchorage_tables.sql')
ON CONFLICT (filename) DO NOTHING;