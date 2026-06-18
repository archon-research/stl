-- Migration: Create om_read_only login user for OpenMetadata ingestion
--
-- Creates a login user that inherits SELECT-only access from the stl_readonly
-- role group (established in 20260122_140100_create_app_roles_and_privileges).
--
-- idle_in_transaction_session_timeout is set to 5 minutes to prevent
-- OpenMetadata alert/ingestion workers from holding idle transactions open.
--
-- Password is a placeholder — set via Terraform (bootstrap-db.sh) after apply.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'om_read_only') THEN
        CREATE USER om_read_only WITH PASSWORD 'PLACEHOLDER_SET_VIA_TERRAFORM';
    END IF;
END
$$;

GRANT stl_readonly TO om_read_only;
ALTER ROLE om_read_only SET idle_in_transaction_session_timeout = '5min';

INSERT INTO migrations (filename)
VALUES ('20260618_120000_create_om_read_only_user.sql')
ON CONFLICT (filename) DO NOTHING;
