-- Create the core_model_results hypertable.
-- One row per (market_key, computed_at) produced by the core-model-runner cronjob.
-- The service reads the latest row per market_key at request time.

CREATE TABLE IF NOT EXISTS core_model_results (
    id             BIGSERIAL,
    market_key     TEXT        NOT NULL,
    crr_el_pct     NUMERIC     NOT NULL,
    crr_es_pct     NUMERIC     NOT NULL,
    crr_var_pct    NUMERIC     NOT NULL,
    hhi            NUMERIC,
    protocol       TEXT        NOT NULL,
    forecast_step  INT         NOT NULL,
    n_mc           INT         NOT NULL,
    copula_type    TEXT        NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, computed_at)
);

SELECT create_hypertable('core_model_results', 'computed_at');

ALTER TABLE core_model_results SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'market_key'
);

SELECT add_compression_policy('core_model_results', INTERVAL '7 days');

SELECT add_tiering_policy('core_model_results', INTERVAL '30 days');

-- Track this migration (filename must match exactly!)
INSERT INTO migrations (filename)
VALUES ('20260601_120000_create_core_model_results.sql')
ON CONFLICT (filename) DO NOTHING;
