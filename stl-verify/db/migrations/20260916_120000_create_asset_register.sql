-- VEC-812 (epic VEC-808). The asset register: one row per SECURITY node that has a price,
-- giving it the bigint key the price layer will reference.
--
-- sec_node cannot be a FK target: its id repeats across versions and validity windows, and
-- ADR-0007 keeps prices outside the graph, joined at read time. Per-block hypertables want an
-- integer key. This table is the one place the graph's text id and the fast layer's integer
-- meet, and it holds nothing else — class, issuer, names and currency live on the node and its
-- edges, so there is nothing here for two systems to disagree about.

CREATE TABLE asset (
    id            BIGSERIAL   PRIMARY KEY,
    security_id   TEXT        NOT NULL,
    source_system TEXT        NOT NULL,
    run_id        BIGINT      REFERENCES writer_run (id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT asset_security_id_key        UNIQUE (security_id),
    CONSTRAINT asset_security_id_prefix_chk CHECK (security_id LIKE 'sec-_%'),
    CONSTRAINT asset_source_system_chk      CHECK (source_system <> '')
);

COMMENT ON TABLE asset IS '[Dimension] The asset register: one row per SECURITY node (sec_node, ADR-0007) that has a price, minting the bigint key the price layer references. Identity only, written once, never re-pointed: a superseded security keeps its row, its successor gets a new one and the price bindings move. Carries no attribute of the security — class, issuer, names and currency live on the node and its edges (VEC-808 rule 2). Append-only through reference_table_immutable() on UPDATE, DELETE and TRUNCATE; the owner keeps UPDATE for the FK integrity probe (20260714_160000). Plain table: one row per priced security at governance rate — the sparse-table exception, so no growth alert and no conversion path. Seeded by migration once the sec-* ids are agreed (VEC-808 gate 0).';
COMMENT ON COLUMN asset.id IS 'Roles: PK. The key the price layer will reference (VEC-808). A surrogate by design: an integer compresses and indexes far better than the node id in per-block hypertables.';
COMMENT ON COLUMN asset.security_id IS 'Roles: FK→sec_node.id (soft; SECURITY only — the sec- prefix is the governed half of sec_node_id_prefix_chk, enforced here by asset_security_id_prefix_chk). UNIQUE: one register row per security. The node store is versioned, so its id repeats across rows: resolve through sec_node_current or sec_node_as_of(). May be written before the node exists where the priced universe leads the register.';
COMMENT ON COLUMN asset.source_system IS 'Roles: Audit. The migration or process that minted the row.';
COMMENT ON COLUMN asset.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id and to the reference data the writer saw through writer_run.reference_snapshot / reference_effective_at. NULL when a migration wrote the row rather than a tracked writer run.';
COMMENT ON COLUMN asset.created_at IS 'Roles: Audit. Processing time the row was minted; not a validity instant — validity lives on the node.';

DO $$
DECLARE
    owner_role     text;
    owner_is_super boolean;
BEGIN
    SELECT pg_get_userbyid(c.relowner) INTO owner_role FROM pg_class c WHERE c.oid = 'asset'::regclass;
    SELECT rolsuper INTO owner_is_super FROM pg_roles WHERE rolname = owner_role;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readonly') THEN
        GRANT SELECT ON asset TO stl_readonly;
    END IF;
    -- ALTER DEFAULT PRIVILEGES (20260122_140100) hands the app role full DML on every
    -- migrator-owned table, so the REVOKE is what makes the grant SELECT + INSERT.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
        GRANT SELECT, INSERT ON asset TO stl_readwrite;
        REVOKE UPDATE, DELETE, TRUNCATE ON asset FROM stl_readwrite;
        IF has_table_privilege('stl_readwrite', 'asset', 'UPDATE')
           OR has_table_privilege('stl_readwrite', 'asset', 'DELETE')
           OR has_table_privilege('stl_readwrite', 'asset', 'TRUNCATE') THEN
            RAISE EXCEPTION 'append-only not enforced: stl_readwrite still holds a mutation privilege on asset after the revoke';
        END IF;
    END IF;
    EXECUTE format('REVOKE DELETE, TRUNCATE ON asset FROM %I', owner_role);
    -- A superuser owner (the test harness) reports every privilege as held, so the check
    -- means something only for the deployed owner role.
    IF NOT owner_is_super AND (has_table_privilege(owner_role, 'asset', 'DELETE')
                               OR has_table_privilege(owner_role, 'asset', 'TRUNCATE')) THEN
        RAISE EXCEPTION 'append-only not enforced: owner % still holds DELETE or TRUNCATE on asset after the revoke', owner_role;
    END IF;
END $$;

CREATE TRIGGER asset_immutable
    BEFORE UPDATE OR DELETE ON asset
    FOR EACH ROW EXECUTE FUNCTION reference_table_immutable();
-- TRUNCATE fires no row trigger, so it gets its own statement-level one (the writer_run form).
CREATE TRIGGER asset_truncate_immutable
    BEFORE TRUNCATE ON asset
    FOR EACH STATEMENT EXECUTE FUNCTION reference_table_immutable();

INSERT INTO migrations (filename)
VALUES ('20260916_120000_create_asset_register.sql')
ON CONFLICT (filename) DO NOTHING;
