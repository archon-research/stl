-- Create the core_model_results hypertable.
-- One row per (market_key, computed_at) produced by the core-model runner.
-- The API service reads the latest row per market_key at request time.

CREATE TABLE IF NOT EXISTS core_model_results (
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
    params         JSONB       NOT NULL,
    PRIMARY KEY (market_key, computed_at)
);

SELECT create_hypertable('core_model_results', 'computed_at', if_not_exists => TRUE);

ALTER TABLE core_model_results SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'market_key'
);

SELECT add_compression_policy('core_model_results', INTERVAL '7 days', if_not_exists => TRUE);

DO $$
BEGIN
    PERFORM add_tiering_policy('core_model_results', INTERVAL '30 days', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for core_model_results';
END $$;

COMMENT ON TABLE core_model_results IS
  '[Hypertable] CORE model output: one CRR result per (market_key, computed_at) run. Append-only; the API serves the latest row per market_key. Model output, not ingest -- registered as model_output in the schema master.';
COMMENT ON COLUMN core_model_results.market_key IS
  'Roles: PK (with computed_at). CORE market identifier (e.g. sparklend_usdt) = one protocol + one loan token. Matches inputs/market_configs.json keys and the asset_to_market_key.json mapping values; not an FK -- markets live in config, not in a table. A market run in parameter variants needs one key per variant (Galaxy''s WITH CLASS A / NO CLASS A are the known case): variants sharing a key collide on the primary key and only the first is kept.';
COMMENT ON COLUMN core_model_results.crr_el_pct IS
  'Roles: Derived. Headline CRR: expected loss as a 0-100 percentage of total market exposure (mean of net bad debt / exposure across Monte Carlo scenarios).';
COMMENT ON COLUMN core_model_results.crr_es_pct IS
  'Roles: Derived. Expected-shortfall CRR as a 0-100 percentage, at the run''s PERC confidence level (in params).';
COMMENT ON COLUMN core_model_results.crr_var_pct IS
  'Roles: Derived. Value-at-Risk CRR as a 0-100 percentage, at the run''s PERC confidence level (in params).';
COMMENT ON COLUMN core_model_results.hhi IS
  'Roles: Derived. Herfindahl-Hirschman index of borrower concentration, scaled 0-100. NULL when the run computed no concentration metrics.';
COMMENT ON COLUMN core_model_results.protocol IS
  'Protocol label as used by the model (e.g. SPARKLEND). Display value from market config, not an FK to protocol.';
COMMENT ON COLUMN core_model_results.forecast_step IS
  'Forecast horizon of the run, in calendar days.';
COMMENT ON COLUMN core_model_results.n_mc IS
  'Number of Monte Carlo scenarios of the run.';
COMMENT ON COLUMN core_model_results.copula_type IS
  'Cross-asset dependence structure of the run: GAUSSIAN or T-COPULA.';
COMMENT ON COLUMN core_model_results.computed_at IS
  'Roles: PK, Partition. UTC time the run finished; hypertable partition column. Latest row per market_key is the served result.';
COMMENT ON COLUMN core_model_results.params IS
  'Full resolved parameter set of the run (defaults -> market config -> env overrides) for auditability: every result row states exactly what produced it.';

-- Track this migration (filename must match exactly!)
INSERT INTO migrations (filename)
VALUES ('20260818_120000_create_core_model_results.sql')
ON CONFLICT (filename) DO NOTHING;
