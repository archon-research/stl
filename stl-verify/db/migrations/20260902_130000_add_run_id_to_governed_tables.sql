-- VEC-598: governed rows name the writer run that wrote them (ADR-0006 §2).
--
-- run_id -> writer_run.id on every governed table — schemamaster type raw_pipeline, dimension or
-- config — so a row resolves to its artefact (git hash, service, image digest) and to the
-- reference data its writer ran with. NULL means the row predates tracking; build_id stays and
-- keeps being written alongside. Nullable with no default, so on a compressed hypertable this is
-- a catalogue-only change (no chunk rewrite), and no FK, matching build_id: an FK probe on every
-- governed insert would lock a writer_run row per batch for no reproducibility gain.
--
-- The list is the governed set of data_quality/schemamaster/schema_master.json at this
-- migration's date; TestGovernedTablesCarryRunID (db/migrator) re-derives it from the register on
-- every run, so a governed table added later without the column fails there.
--
-- Each ADD COLUMN takes a brief ACCESS EXCLUSIVE lock; queueing behind a long reader would hold
-- every reader of that table out, so fail fast and re-run instead. Scoped to this transaction
-- via SET LOCAL.
SET LOCAL lock_timeout = '10s';

DO $$
DECLARE tbl text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'allocation_position',
        'anchorage_operation',
        'anchorage_package_snapshot',
        'borrower',
        'borrower_collateral',
        'cex_orderbook_snapshots',
        'chain',
        'debt_token',
        'fluid_vault',
        'fluid_vault_state',
        'maple_loan',
        'maple_loan_collateral',
        'maple_loan_state',
        'maple_pool',
        'maple_pool_state',
        'maple_sky_strategy',
        'maple_sky_strategy_state',
        'maple_syrup_global_state',
        'morpho_adapter',
        'morpho_adapter_membership',
        'morpho_adapter_state',
        'morpho_market',
        'morpho_market_position',
        'morpho_market_state',
        'morpho_vault',
        'morpho_vault_cap',
        'morpho_vault_fee',
        'morpho_vault_position',
        'morpho_vault_state',
        'offchain_price_asset',
        'offchain_price_source',
        'offchain_token_price',
        'onchain_token_price',
        'oracle',
        'oracle_asset',
        'prime',
        'prime_capital_stack',
        'prime_capital_stack_allocation',
        'prime_debt',
        'prime_proxy',
        'prime_reference_balance_sheet',
        'prime_reference_position',
        'protocol',
        'protocol_event',
        'protocol_oracle',
        'psm3_alm_shares',
        'psm3_reserves',
        'receipt_token',
        'sparklend_reserve_data',
        'token',
        'token_total_supply',
        'user'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS run_id BIGINT', tbl);
        EXECUTE format(
            'COMMENT ON COLUMN %I.run_id IS %L', tbl,
            'Audit. writer_run.id of the process start that wrote this row (ADR-0006 §2): resolves to the artefact (build_registry) and the reference snapshot/effective instant the writer ran with. NULL = written before run tracking (VEC-598). Not an FK, like build_id; never used for ordering or to pick the latest row.');
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260902_130000_add_run_id_to_governed_tables.sql') ON CONFLICT (filename) DO NOTHING;
