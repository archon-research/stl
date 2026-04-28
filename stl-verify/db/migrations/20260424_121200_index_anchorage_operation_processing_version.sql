-- VEC-185: Cover assign_processing_version_anchorage_operation() lookup.
CREATE INDEX IF NOT EXISTS idx_anchorage_operation_pv_lookup
    ON anchorage_operation (operation_id, created_at, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_121200_index_anchorage_operation_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
