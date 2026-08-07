-- VEC-541: force per-execution planning in every processing_version BEFORE INSERT trigger.
--
-- Each of these plpgsql functions looks up the latest processing_version for the incoming key on
-- every row. plpgsql caches that statement and switches it to a generic plan after ~5 executions, and
-- a generic plan prunes chunks only where TimescaleDB's ChunkAppend can still exclude them at runtime:
-- the MAX(processing_version) lookup plans as a Merge Append instead and fans out over every chunk of
-- the table, so per-row insert cost grows with the table's total chunk count. Measured on a
-- 2,071-chunk reproduction: 6.12 ms/row under the production config vs 0.21 ms/row with
-- force_custom_plan, which stays flat as chunks accumulate. force_custom_plan re-plans on each
-- execution, so the constant key predicates prune to the one chunk that can hold the row. Plan
-- choice only; no semantics change.
--
-- Excludes assign_processing_version_offchain_token_price: 20260806_120000_processing_version_force_custom_plan.sql
-- already pins it.

ALTER FUNCTION assign_processing_version_allocation_position() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_anchorage_operation() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_anchorage_package_snapshot() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_borrower() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_borrower_collateral() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_cryptoswap_config() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_cryptoswap_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_liquidity_event() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_lp_token_event() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_parameter_event() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_stableswap_config() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_stableswap_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_curve_swap() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_fluid_vault_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_maple_ftl_loan_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_maple_loan_collateral() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_maple_loan_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_maple_pool_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_maple_sky_strategy_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_maple_syrup_global_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_morpho_market_position() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_morpho_market_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_morpho_vault_position() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_morpho_vault_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_onchain_token_price() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_prime_debt() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_protocol_event() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_psm3_reserves() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_sparklend_reserve_data() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_token_total_supply() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_uniswap_v3_liquidity_event() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_uniswap_v3_pool_event() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_uniswap_v3_pool_state() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_uniswap_v3_swap() SET plan_cache_mode = 'force_custom_plan';
ALTER FUNCTION assign_processing_version_uniswap_v3_tick() SET plan_cache_mode = 'force_custom_plan';

INSERT INTO migrations (filename) VALUES ('20260806_130000_set_plan_cache_mode_on_processing_version_triggers.sql') ON CONFLICT (filename) DO NOTHING;
