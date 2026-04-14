-- Backfill processing_version and build_id NULLs to 0.
-- NOT NULL is enforced implicitly by the PK/UNIQUE constraints added in 130000.
--
-- See ADR-0002: Data Auditability and Processing Versioning.

UPDATE borrower SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE borrower_collateral SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE sparklend_reserve_data SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE onchain_token_price SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE morpho_market_state SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE morpho_market_position SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE morpho_vault_state SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE morpho_vault_position SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE prime_debt SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE allocation_position SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE protocol_event SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE anchorage_package_snapshot SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE anchorage_operation SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;
UPDATE offchain_token_price SET processing_version = 0, build_id = 0 WHERE processing_version IS NULL;

INSERT INTO migrations (filename)
VALUES ('20260410_125000_backfill_auditability_not_null.sql')
ON CONFLICT (filename) DO NOTHING;
