-- Migration: Create om_read_only login user for OpenMetadata ingestion
--
-- Creates a login user that inherits SELECT-only access from the stl_readonly
-- role group (established in 20260122_140100_create_app_roles_and_privileges).
--
-- Password is not set here: it requires superuser, which the migration role
-- (stl_migrator, CREATEROLE only) does not have. It is applied by the
-- admin-privileged bootstrap step (Terraform bootstrap-db.sh) after apply.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'om_read_only') THEN
        CREATE USER om_read_only WITH PASSWORD NULL;
    END IF;
END
$$;

GRANT stl_readonly TO om_read_only;

INSERT INTO migrations (filename)
VALUES ('20260618_120000_create_om_read_only_user.sql')
ON CONFLICT (filename) DO NOTHING;
