-- Migration: Create application roles with least privilege access
-- 
-- This migration creates two PostgreSQL roles:
-- 1. stl_readonly  - Can only SELECT from all tables (for reporting, monitoring)
-- 2. stl_readwrite - Can SELECT, INSERT, UPDATE, DELETE (for application use)
--
-- Neither role can:
-- - CREATE/DROP/ALTER tables or other schema objects
-- - CREATE/DROP indexes
-- - TRUNCATE tables
-- - Execute DDL commands
--
-- We use ALTER DEFAULT PRIVILEGES so new tables automatically get the right grants.
-- Passwords are set via a separate script after Terraform generates them.
--
-- Note: This migration uses dynamic SQL to work with any database name.
-- In TigerData, the database is 'tsdb' and the admin user is 'tsdbadmin'.
-- In tests, the database name and admin user may differ.

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

-- Grant CONNECT to the current database (dynamic)
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stl_readonly', current_database());
END
$$;

-- Grant USAGE on schema (required to see objects in schema)
GRANT USAGE ON SCHEMA public TO stl_readonly;

-- Grant SELECT on all existing tables in public schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO stl_readonly;

-- Grant SELECT on all existing sequences (needed for some queries)
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO stl_readonly;

-- Ensure future tables created by the current admin user also grant SELECT to stl_readonly
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT ON TABLES TO stl_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT ON SEQUENCES TO stl_readonly;

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

-- Grant CONNECT to the current database (dynamic)
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stl_readwrite', current_database());
END
$$;

-- Grant USAGE on schema (required to see objects in schema)
GRANT USAGE ON SCHEMA public TO stl_readwrite;

-- Grant DML permissions on all existing tables (NO TRUNCATE - that's quasi-DDL)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO stl_readwrite;

-- Grant USAGE and SELECT on sequences (needed for INSERT with SERIAL/BIGSERIAL columns)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO stl_readwrite;

-- Ensure future tables created by the current admin user also grant DML to stl_readwrite
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stl_readwrite;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT USAGE, SELECT ON SEQUENCES TO stl_readwrite;

-- =============================================================================
-- Login Users (inherit permissions from roles)
-- =============================================================================
-- Passwords are placeholders - run `make db-set-passwords` after Terraform apply.

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
