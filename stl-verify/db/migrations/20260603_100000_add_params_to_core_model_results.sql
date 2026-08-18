-- Add params JSONB column to core_model_results for auditability.
-- Every row will carry the exact parameter set used to produce the CRR result,
-- making it possible to reproduce or compare runs.

ALTER TABLE core_model_results ADD COLUMN IF NOT EXISTS params JSONB NOT NULL DEFAULT '{}';

INSERT INTO migrations (filename)
VALUES ('20260603_100000_add_params_to_core_model_results.sql')
ON CONFLICT (filename) DO NOTHING;
