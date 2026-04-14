-- Set DEFAULT 0 on processing_version and build_id columns.
-- This is metadata-only (no data rewrite) so it works on tiered/compressed tables.
-- The backfill of existing NULL rows and NOT NULL constraint are deferred to
-- 20260410_125000 which runs after decompression (20260410_120000).
--
-- See ADR-0002: Data Auditability and Processing Versioning.

ALTER TABLE borrower ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE borrower ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE borrower_collateral ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE borrower_collateral ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE sparklend_reserve_data ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE sparklend_reserve_data ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE onchain_token_price ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE onchain_token_price ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE morpho_market_state ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE morpho_market_state ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE morpho_market_position ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE morpho_market_position ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE morpho_vault_state ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE morpho_vault_state ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE morpho_vault_position ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE morpho_vault_position ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE prime_debt ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE prime_debt ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE allocation_position ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE allocation_position ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE protocol_event ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE protocol_event ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE anchorage_package_snapshot ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE anchorage_package_snapshot ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE anchorage_operation ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE anchorage_operation ALTER COLUMN build_id SET DEFAULT 0;

ALTER TABLE offchain_token_price ALTER COLUMN processing_version SET DEFAULT 0;
ALTER TABLE offchain_token_price ALTER COLUMN build_id SET DEFAULT 0;

INSERT INTO migrations (filename)
VALUES ('20260410_115000_set_auditability_defaults.sql')
ON CONFLICT (filename) DO NOTHING;
