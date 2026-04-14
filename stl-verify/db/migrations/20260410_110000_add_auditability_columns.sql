-- Add processing_version and build_id columns to all state tables.
--
-- processing_version: internal correction version (0 = original, N = Nth reprocessing)
-- build_id:           pointer to build_registry.id (which code produced this row)
--
-- Adding NOT NULL columns with DEFAULT is supported on compressed hypertables
-- since TimescaleDB 2.6 — no decompression needed for this step.
--
-- See ADR-0002: Data Auditability and Processing Versioning.

-- ============================================================================
-- Blockchain-derived tables
-- ============================================================================

ALTER TABLE borrower
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE borrower_collateral
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE sparklend_reserve_data
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE onchain_token_price
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE morpho_market_state
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE morpho_market_position
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE morpho_vault_state
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE morpho_vault_position
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE prime_debt
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE allocation_position
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE protocol_event
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

-- ============================================================================
-- Off-chain / polled tables
-- ============================================================================

ALTER TABLE anchorage_package_snapshot
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE anchorage_operation
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

ALTER TABLE offchain_token_price
    ADD COLUMN IF NOT EXISTS processing_version INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS build_id INT NOT NULL DEFAULT 0;

INSERT INTO migrations (filename)
VALUES ('20260410_110000_add_auditability_columns.sql')
ON CONFLICT (filename) DO NOTHING;
