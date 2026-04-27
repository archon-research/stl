-- Add processing_version and build_id columns to all state tables.
--
-- processing_version: internal correction version (0 = original, N = Nth reprocessing)
-- build_id:           pointer to build_registry.id (which code produced this row)
--
-- Columns are added as bare nullable (no DEFAULT, no NOT NULL) to avoid
-- TigerData's restriction on tiered tables. A follow-up migration
-- (20260410_115000) sets the default and NOT NULL constraint.
--
-- See ADR-0002: Data Auditability and Processing Versioning.

ALTER TABLE borrower ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE borrower ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE borrower_collateral ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE borrower_collateral ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE onchain_token_price ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE onchain_token_price ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE morpho_market_state ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE morpho_market_state ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE morpho_market_position ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE morpho_market_position ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE morpho_vault_state ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE morpho_vault_state ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE morpho_vault_position ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE morpho_vault_position ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE prime_debt ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE prime_debt ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE allocation_position ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE allocation_position ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE protocol_event ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE protocol_event ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE anchorage_package_snapshot ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE anchorage_package_snapshot ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE anchorage_operation ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE anchorage_operation ADD COLUMN IF NOT EXISTS build_id INT;

ALTER TABLE offchain_token_price ADD COLUMN IF NOT EXISTS processing_version INT;
ALTER TABLE offchain_token_price ADD COLUMN IF NOT EXISTS build_id INT;

INSERT INTO migrations (filename)
VALUES ('20260410_110000_add_auditability_columns.sql')
ON CONFLICT (filename) DO NOTHING;
