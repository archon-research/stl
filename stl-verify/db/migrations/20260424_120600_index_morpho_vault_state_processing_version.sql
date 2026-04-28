-- VEC-185: Cover assign_processing_version_morpho_vault_state() lookup.
CREATE INDEX IF NOT EXISTS idx_morpho_vault_state_pv_lookup
    ON morpho_vault_state (morpho_vault_id, block_number, block_version, timestamp, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120600_index_morpho_vault_state_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
