-- Anchorage package snapshots (current state, polled every N minutes)
CREATE TABLE IF NOT EXISTS anchorage_package_snapshot
(
    id                   BIGSERIAL PRIMARY KEY,
    prime_id             BIGINT      NOT NULL REFERENCES prime (id),
    package_id           TEXT        NOT NULL,
    pledgor_id           TEXT        NOT NULL,
    secured_party_id     TEXT        NOT NULL,
    active               BOOLEAN     NOT NULL,
    state                TEXT        NOT NULL,

    current_ltv          NUMERIC     NOT NULL,
    exposure_value       NUMERIC     NOT NULL,
    package_value        NUMERIC     NOT NULL,

    margin_call_ltv      NUMERIC     NOT NULL,
    critical_ltv         NUMERIC     NOT NULL,
    margin_return_ltv    NUMERIC     NOT NULL,

    asset_type           TEXT        NOT NULL DEFAULT '',
    custody_type         TEXT        NOT NULL DEFAULT '',
    asset_price          NUMERIC     NOT NULL DEFAULT 0,
    asset_quantity       NUMERIC     NOT NULL DEFAULT 0,
    asset_weighted_value NUMERIC     NOT NULL DEFAULT 0,

    ltv_timestamp        TIMESTAMPTZ NOT NULL,
    snapshot_time        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_anchorage_pkg_snap_package_time
    ON anchorage_package_snapshot (prime_id, package_id, snapshot_time DESC);

CREATE INDEX idx_anchorage_pkg_snap_state
    ON anchorage_package_snapshot (state, snapshot_time DESC);

CREATE INDEX idx_anchorage_pkg_snap_time
    ON anchorage_package_snapshot (snapshot_time DESC);

-- Anchorage operations (event history: deposits, paydowns, margin returns, etc.)
CREATE TABLE IF NOT EXISTS anchorage_operation
(
    id           BIGSERIAL PRIMARY KEY,
    prime_id     BIGINT      NOT NULL REFERENCES prime (id),
    operation_id TEXT        NOT NULL,
    action       TEXT        NOT NULL,
    type         TEXT        NOT NULL,
    type_id      TEXT        NOT NULL,
    asset_type   TEXT        NOT NULL,
    custody_type TEXT        NOT NULL,
    quantity     NUMERIC     NOT NULL,
    notes        TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_anchorage_op_operation_id
    ON anchorage_operation (operation_id);

CREATE INDEX idx_anchorage_op_type_id
    ON anchorage_operation (type_id, created_at DESC);

CREATE INDEX idx_anchorage_op_prime_time
    ON anchorage_operation (prime_id, created_at DESC);

CREATE INDEX idx_anchorage_op_action
    ON anchorage_operation (action, created_at DESC);

INSERT INTO migrations (filename)
VALUES ('20260317_120000_create_anchorage_tables.sql')
ON CONFLICT (filename) DO NOTHING;