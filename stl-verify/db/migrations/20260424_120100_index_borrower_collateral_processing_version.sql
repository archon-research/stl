-- VEC-185: Cover assign_processing_version_borrower_collateral() lookup.
CREATE INDEX IF NOT EXISTS idx_borrower_collateral_pv_lookup
    ON borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, created_at, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120100_index_borrower_collateral_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
