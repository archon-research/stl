-- VEC-812 (epic VEC-808). The asset register: one row per SECURITY node that has a price,
-- giving it the bigint key the price layer will reference.
--
-- sec_node cannot be a FK target: its id repeats across versions and validity windows, and
-- ADR-0007 keeps prices outside the graph, joined at read time. Per-block hypertables want an
-- integer key. This table is the one place the graph's text id and the fast layer's integer
-- meet, and it holds nothing else — class, issuer, names and currency live on the node and its
-- edges, so there is nothing here for two systems to disagree about.
--
-- Created empty. Rows arrive by seed migration once the sec-* ids of the priced universe are
-- agreed with the reference-data owners (VEC-808 gate 0), and afterwards by the same INSERT-only
-- path: a security that is superseded keeps its row, its successor gets a new one, and the
-- price bindings move.
--
-- Form: the reference-table form (20260714_160000, 20260904_120000 vocabularies). Child tables
-- will FK this one, and the FK integrity probe on a child INSERT runs as the parent's owner with
-- FOR KEY SHARE, which needs UPDATE — so the owner keeps UPDATE and append-only is the
-- reference_table_immutable() trigger, which row locks do not fire. Plain table: governance-rate
-- writes, the sparse-table exception.

CREATE TABLE IF NOT EXISTS asset (
    id            BIGSERIAL   PRIMARY KEY,
    security_id   TEXT        NOT NULL,
    source_system TEXT        NOT NULL,
    run_id        BIGINT      REFERENCES writer_run (id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT asset_security_id_key        UNIQUE (security_id),
    -- The prefix is the governed half of sec_node's id contract (sec_node_id_prefix_chk): a
    -- register row may point only at a SECURITY node, never at an entity, concept or source.
    CONSTRAINT asset_security_id_prefix_chk CHECK (security_id LIKE 'sec-%'),
    CONSTRAINT asset_source_system_chk      CHECK (source_system <> '')
);

COMMENT ON TABLE asset IS '[Dimension] The asset register: one row per SECURITY node (sec_node, ADR-0007) that has a price, minting the bigint key the price hypertables reference. Identity only, written once, never re-pointed: a superseded security keeps its row, its successor gets a new one and the price bindings move. Carries no attribute of the security — class, issuer, names and currency live on the node and its edges (VEC-808 rule 2). Append-only through reference_table_immutable(); the owner keeps UPDATE for the FK integrity probe (20260714_160000). Plain table: governance-rate writes, per the sparse-table exception. Seeded by migration once the sec-* ids are agreed (VEC-808 gate 0).';
COMMENT ON COLUMN asset.id IS 'Roles: PK. The key price_source_asset, asset_price_chain and asset_price_feed reference. A surrogate by design: an integer compresses and indexes far better than the node id in per-block hypertables.';
COMMENT ON COLUMN asset.security_id IS 'Roles: FK→sec_node.id (soft; SECURITY, sec- prefix). UNIQUE: one register row per security. The node store is versioned, so its id repeats across rows: resolve through sec_node_current or sec_node_as_of(). Written before the node exists where the priced universe leads the register (VEC-616 uses the same soft form).';
COMMENT ON COLUMN asset.source_system IS 'Roles: Audit. The migration or process that minted the row.';
COMMENT ON COLUMN asset.run_id IS 'Roles: FK→writer_run.id, Audit. The process start that wrote this row (ADR-0006 §2); resolves to the build artefact through writer_run.build_id and to the reference data the writer saw through writer_run.reference_snapshot / reference_effective_at. NULL means written before run tracking, which is what seed rows are.';
COMMENT ON COLUMN asset.created_at IS 'Roles: Audit. Processing time the row was minted; not a validity instant — validity lives on the node.';

DO $$
DECLARE
    owner_role text;
BEGIN
    SELECT pg_get_userbyid(c.relowner) INTO owner_role FROM pg_class c WHERE c.oid = 'asset'::regclass;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readonly') THEN
        GRANT SELECT ON asset TO stl_readonly;
    END IF;
    -- ALTER DEFAULT PRIVILEGES (20260122_140100) hands the app role full DML on every
    -- migrator-owned table, so the REVOKE is what makes the grant SELECT + INSERT.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
        GRANT SELECT, INSERT ON asset TO stl_readwrite;
        REVOKE UPDATE, DELETE, TRUNCATE ON asset FROM stl_readwrite;
    END IF;
    EXECUTE format('REVOKE DELETE, TRUNCATE ON asset FROM %I', owner_role);
END $$;

CREATE TRIGGER asset_immutable
    BEFORE UPDATE OR DELETE ON asset
    FOR EACH ROW EXECUTE FUNCTION reference_table_immutable();

INSERT INTO migrations (filename)
VALUES ('20260916_120000_create_asset_register.sql')
ON CONFLICT (filename) DO NOTHING;
