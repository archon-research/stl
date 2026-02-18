-- Migration: Fix schema discrepancies between migrations and staging database
--
-- The staging database has drifted from the schema defined in migrations:
--
-- 1. Extra tables exist that are not defined in migrations (all empty):
--    - borrowers (duplicate of borrower, missing event_type/tx_hash columns)
--    - users (duplicate of "user")
--    - debt_tokens (duplicate of debt_token)
--    - receipt_tokens (duplicate of receipt_token)
--
-- 2. borrower_collateral is missing columns: event_type, tx_hash, collateral_enabled
--    and has a FK referencing "users" instead of "user"
--
-- 3. sparklend_reserve_data is missing columns: decimals, ltv, liquidation_threshold,
--    liquidation_bonus, reserve_factor, usage_as_collateral_enabled, borrowing_enabled,
--    stable_borrow_rate_enabled, is_active, is_frozen
--
-- All affected tables are empty (0 rows), so changes are safe.

-- =============================================================================
-- 1. Fix borrower_collateral FK (must happen before dropping "users" table)
-- =============================================================================

-- Drop the incorrect FK that references the "users" table
ALTER TABLE borrower_collateral DROP CONSTRAINT IF EXISTS borrower_collateral_user_id_fkey;

-- Add the correct FK that references the "user" table (as defined in initial_schema)
ALTER TABLE borrower_collateral ADD CONSTRAINT borrower_collateral_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES "user" (id);

-- =============================================================================
-- 2. Drop extra tables (not defined in migrations, all empty)
-- =============================================================================

DROP TABLE IF EXISTS borrowers;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS debt_tokens;
DROP TABLE IF EXISTS receipt_tokens;

-- =============================================================================
-- 3. Add missing columns to borrower_collateral (table is empty)
-- =============================================================================

ALTER TABLE borrower_collateral ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL;
ALTER TABLE borrower_collateral ADD COLUMN IF NOT EXISTS tx_hash BYTEA NOT NULL;
ALTER TABLE borrower_collateral ADD COLUMN IF NOT EXISTS collateral_enabled BOOLEAN NOT NULL;

-- =============================================================================
-- 4. Add missing columns to sparklend_reserve_data (table is empty)
-- =============================================================================

ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS decimals NUMERIC;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS ltv NUMERIC;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS liquidation_threshold NUMERIC;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS liquidation_bonus NUMERIC;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS reserve_factor NUMERIC;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS usage_as_collateral_enabled BOOLEAN;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS borrowing_enabled BOOLEAN;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS stable_borrow_rate_enabled BOOLEAN;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS is_active BOOLEAN;
ALTER TABLE sparklend_reserve_data ADD COLUMN IF NOT EXISTS is_frozen BOOLEAN;

-- =============================================================================
-- Record this migration
-- =============================================================================
INSERT INTO migrations (filename)
VALUES ('20260218_100000_fix_schema_discrepancies.sql')
ON CONFLICT (filename) DO NOTHING;
