-- core_model_results.params now also carries the Monte Carlo diagnostics of the
-- run under the lower-case key mc_diagnostics; the column COMMENT is the
-- catalogue's source of truth, so it has to say so.

COMMENT ON COLUMN core_model_results.params IS
  'Full resolved parameter set of the run (defaults -> market config -> env overrides) for auditability: every result row states exactly what produced it. Model params are UPPER_CASE keys. One lower-case key, mc_diagnostics, holds the run''s Monte Carlo sampling error: crr_el_se_pct (standard error of crr_el_pct, same 0-100 % units, NULL below two scenarios), crr_el_rel_se (that SE over the EL, unit-free, NULL when the SE is NULL or the EL is 0), n_scenarios, n_loss_scenarios (scenarios with any bad debt), n_catastrophic_scenarios (scenarios losing more than catastrophic_loss_pct % of total debt) and catastrophic_loss_pct.';

INSERT INTO migrations (filename)
VALUES ('20260907_120000_comment_core_model_results_params_diagnostics.sql')
ON CONFLICT (filename) DO NOTHING;
