-- Migration: durable prime capital stack snapshots for capital metrics

CREATE TABLE IF NOT EXISTS prime_capital_stack
(
    id                      BIGSERIAL PRIMARY KEY,
    prime_id                BIGINT      NOT NULL REFERENCES prime (id) ON DELETE CASCADE,
    capital_buffer          NUMERIC(38, 18) NOT NULL,
    first_loss_capital      NUMERIC(38, 18) NOT NULL,
    timestamp               TIMESTAMPTZ NOT NULL DEFAULT now(),
    source                  TEXT        NOT NULL,
    version                 INT         NOT NULL DEFAULT 1,
    benchmark_source        TEXT,
    reconciliation_status   TEXT,
    reconciliation_delta_pct NUMERIC(10, 4),
    created_by              TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by              TEXT,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_prime_capital_stack_nonnegative
        CHECK (capital_buffer >= 0 AND first_loss_capital >= 0),
    CONSTRAINT chk_prime_capital_stack_reconciliation_status
        CHECK (reconciliation_status IS NULL OR reconciliation_status IN ('valid', 'pending', 'divergent')),
    CONSTRAINT uq_prime_capital_stack_prime_timestamp
        UNIQUE (prime_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_prime_capital_stack_prime_id
    ON prime_capital_stack (prime_id);

CREATE INDEX IF NOT EXISTS idx_prime_capital_stack_timestamp_desc
    ON prime_capital_stack (timestamp DESC);

INSERT INTO migrations (filename)
VALUES ('20260506_193000_create_prime_capital_stack.sql')
ON CONFLICT (filename) DO NOTHING;
