-- Migration: Set default privileges for objects created by stl_migrator
--
-- Any table/sequence stl_migrator creates will automatically be accessible to
-- the application roles — no manual grants needed after new migrations run.
--
-- This runs after 20260319_000000_create_migrator_role.sql is validated end-to-end.

DO $$
BEGIN
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stl_migrator IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stl_readwrite';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stl_migrator IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO stl_readwrite';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stl_migrator IN SCHEMA public GRANT SELECT ON TABLES TO stl_readonly';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stl_migrator IN SCHEMA public GRANT SELECT ON SEQUENCES TO stl_readonly';
    INSERT INTO migrations (filename)
    VALUES ('20260319_000001_migrator_default_privileges.sql')
    ON CONFLICT (filename) DO NOTHING;
END
$$;
