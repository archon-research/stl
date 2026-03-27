-- Migration: Create dedicated migration role with DDL access
--
-- This migration creates one PostgreSQL role:
--   stl_migrator - Owns objects it creates, no application data access
--
-- stl_migrator can:
--   - CREATE new tables/sequences/indexes (via GRANT CREATE ON SCHEMA public)
--   - ALTER/DROP objects it owns (PostgreSQL grants full DDL to the object owner)
--   - CREATEROLE (needed to create stl_readonly/stl_readwrite on fresh envs)
--   - SELECT/INSERT on the migrations tracking table
--
-- stl_migrator cannot:
--   - Read or write application data (SELECT/INSERT/UPDATE/DELETE on app tables)
--   - ALTER/DROP objects it does not own (e.g., tables created by other roles)
--
-- Default privileges (granting SELECT/DML to stl_readonly/stl_readwrite on objects
-- created by stl_migrator) are set in 20260319_000001_migrator_default_privileges.sql,
-- applied after this approach is validated end-to-end.
--
-- Password is set via a separate script after Terraform generates it (same pattern
-- as stl_read_write and stl_read_only in create_app_roles.sql).
--
-- Note: This migration uses dynamic SQL to work with any database name.
-- In TigerData, the database is 'tsdb' and the admin user is 'tsdbadmin'.
-- In tests, the database name and admin user may differ.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'stl_migrator') THEN
        CREATE USER stl_migrator WITH PASSWORD 'PLACEHOLDER_SET_VIA_TERRAFORM' CREATEROLE;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stl_migrator', current_database());
    EXECUTE 'GRANT CREATE, USAGE ON SCHEMA public TO stl_migrator';
    -- Allow stl_migrator to track which migrations have run.
    -- Objects created by stl_migrator are owned by it (full DDL access).
    -- App tables are NOT accessible — no blanket grants on existing tables.
    EXECUTE 'GRANT SELECT, INSERT ON migrations TO stl_migrator';
    INSERT INTO migrations (filename)
    VALUES ('20260319_000000_create_migrator_role.sql')
    ON CONFLICT (filename) DO NOTHING;
END
$$;