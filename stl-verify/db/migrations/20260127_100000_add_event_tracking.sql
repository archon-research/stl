-- Add event tracking columns to borrower table
ALTER TABLE borrower ADD COLUMN event_type TEXT NOT NULL;
ALTER TABLE borrower ADD COLUMN tx_hash TEXT NOT NULL;

-- Add event tracking columns to borrower_collateral table
ALTER TABLE borrower_collateral ADD COLUMN event_type TEXT NOT NULL;
ALTER TABLE borrower_collateral ADD COLUMN tx_hash TEXT NOT NULL;
ALTER TABLE borrower_collateral ADD COLUMN collateral_enabled BOOLEAN NOT NULL;


-- Track migration
INSERT INTO migrations (filename) VALUES ('20260127_100000_add_event_tracking.sql')
ON CONFLICT (filename) DO NOTHING;
