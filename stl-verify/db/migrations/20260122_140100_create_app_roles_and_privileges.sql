-- Migration: Create application roles and set default privileges
--
-- This migration creates two PostgreSQL role groups and their login users:
--   stl_readonly   - SELECT-only access (reporting, monitoring)
--   stl_readwrite  - SELECT/INSERT/UPDATE/DELETE access (application use)
--
-- Default privileges ensure that any table/sequence created by the migration
-- role (stl_migrator) is automatically accessible to these app roles.
-- No explicit GRANTs are needed in future migrations.
--
-- Note: This migration does NOT use GRANT ON ALL TABLES because that would
-- include extension-owned views (pg_buffercache, etc.) that stl_migrator
-- cannot grant on. ALTER DEFAULT PRIVILEGES is sufficient and avoids this.
--
-- Passwords are set via a separate script after Terraform generates them.
-- This migration uses dynamic SQL to work with any database name.

-- =============================================================================
-- Read-Only Role (permission group, no login)
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'stl_readonly') THEN
        CREATE ROLE stl_readonly NOLOGIN;
    END IF;
END
$$;

DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stl_readonly', current_database());
END
$$;

GRANT USAGE ON SCHEMA public TO stl_readonly;

-- =============================================================================
-- Read-Write Role (permission group, no login)
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
        CREATE ROLE stl_readwrite NOLOGIN;
    END IF;
END
$$;

DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stl_readwrite', current_database());
END
$$;

GRANT USAGE ON SCHEMA public TO stl_readwrite;

-- =============================================================================
-- Default Privileges
-- =============================================================================
-- Since stl_migrator runs this migration, ALTER DEFAULT PRIVILEGES applies to
-- objects created by stl_migrator (equivalent to FOR ROLE stl_migrator).

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO stl_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO stl_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stl_readwrite;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO stl_readwrite;

-- =============================================================================
-- Login Users (inherit permissions from roles)
-- =============================================================================
-- Passwords are placeholders — set via Terraform after provisioning.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'stl_read_write') THEN
        CREATE USER stl_read_write WITH PASSWORD 'PLACEHOLDER_SET_VIA_TERRAFORM';
    END IF;
END
$$;
GRANT stl_readwrite TO stl_read_write;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'stl_read_only') THEN
        CREATE USER stl_read_only WITH PASSWORD 'PLACEHOLDER_SET_VIA_TERRAFORM';
    END IF;
END
$$;
GRANT stl_readonly TO stl_read_only;

-- =============================================================================
-- Record this migration
-- =============================================================================
INSERT INTO migrations (filename)
VALUES ('20260122_140100_create_app_roles_and_privileges.sql')
ON CONFLICT (filename) DO NOTHING;
