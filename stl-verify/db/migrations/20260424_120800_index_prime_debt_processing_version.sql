-- VEC-185: Cover assign_processing_version_prime_debt() lookup.
CREATE INDEX IF NOT EXISTS idx_prime_debt_pv_lookup
    ON prime_debt (prime_id, block_number, block_version, synced_at, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120800_index_prime_debt_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
