-- Fix (Simon review on #574): reference-table immutability broke FK inserts under the prod roles.
--
-- The FK integrity probe Postgres runs on a child INSERT is
--   SELECT 1 FROM ONLY <parent> WHERE <key> = $1 FOR KEY SHARE OF x
-- executed as the PARENT's owner, and FOR KEY SHARE requires UPDATE on the parent. Migration
-- 20260630_130000 revoked UPDATE from the owner (stl_migrator) on all 13 reference tables, so every
-- INSERT into a child that FKs the reference layer fails with "permission denied for table
-- <parent>" under the non-superuser prod roles. security_master (VEC-411), entity_master (VEC-410),
-- and position_entity_link (VEC-415) all FK the reference layer. The integration tests run as
-- superuser, which bypasses the ACL check, so CI never caught it.
--
-- Fix: restore UPDATE to the owner only (the RI row-lock probe needs it), keep DELETE/TRUNCATE
-- revoked, and re-enforce append-only with a BEFORE UPDATE OR DELETE trigger that raises. Row-level
-- locks (FOR KEY SHARE) do not fire row triggers, so the RI probe passes while a real UPDATE/DELETE
-- still fails hard. stl_readwrite keeps all mutation revoked (unchanged). Adding rows stays an
-- INSERT via a deliberate migration, which the trigger does not touch.

CREATE OR REPLACE FUNCTION reference_table_immutable() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'reference table %.% is append-only; % is not allowed (add rows via a migration)',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END $$;
COMMENT ON FUNCTION reference_table_immutable() IS 'Raises on UPDATE/DELETE of a controlled-vocabulary reference table. Paired with owner UPDATE restored so the FK RI row-lock probe still works (see 20260714_160000_fix_reference_table_fk_inserts.sql).';

DO $$
DECLARE tbl text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'asset_class_ref','security_type_ref','security_subtype_ref','credit_rating_ref',
        'sector_ref','industry_group_ref','currency_ref','country_ref','deal_type_ref',
        'entity_type_ref','counterparty_role_ref','origination_type_ref','corporate_action_type_ref']
    LOOP
        -- Restore UPDATE to the owner only, so the FK RI probe (run as the parent owner, FOR KEY
        -- SHARE) can lock parent rows. Guarded by role existence (absent in unit-test databases).
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_migrator') THEN
            EXECUTE format('GRANT UPDATE ON %I TO stl_migrator', tbl);
        END IF;
        -- Re-enforce append-only via a trigger. Row locks do not fire row triggers, so this does not
        -- affect the RI probe; a real UPDATE or DELETE raises. DELETE/TRUNCATE also stay ACL-revoked.
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', tbl || '_immutable', tbl);
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reference_table_immutable()', tbl || '_immutable', tbl);
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260714_160000_fix_reference_table_fk_inserts.sql') ON CONFLICT (filename) DO NOTHING;
