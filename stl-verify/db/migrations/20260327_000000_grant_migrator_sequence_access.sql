-- Grant missing permissions to stl_migrator on the migrations tracking table.
-- The original 20260319_000000_create_migrator_role.sql granted SELECT and INSERT
-- but missed UPDATE (needed for checksum tracking) and sequence access (needed
-- for INSERT to generate the serial id).

GRANT UPDATE ON migrations TO stl_migrator;
GRANT USAGE, SELECT ON SEQUENCE migrations_id_seq TO stl_migrator;

INSERT INTO migrations (filename)
VALUES ('20260327_000000_grant_migrator_sequence_access.sql')
ON CONFLICT (filename) DO NOTHING;
