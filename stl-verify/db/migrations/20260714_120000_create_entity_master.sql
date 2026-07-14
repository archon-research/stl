-- VEC-410: entity_master (append-only SCD2 legal-entity master) + current/versioned views.
--
-- The canonical record for every legal entity in the book: issuers, holders, counterparties,
-- operators. Same shape as security_master (VEC-411): pure classification/attribution of what an
-- entity IS; only valid_from is stored, valid_to / is_current are derived in entity_master_versions,
-- and UPDATE / DELETE / TRUNCATE are revoked so the record is append-only.
--
-- Holder unification (VEC-400): a position's holder resolves to a single entity_id here regardless
-- of whether it originated as an on-chain wallet ("user".id) or a prime (prime.id). The two bridge
-- columns below (pipeline_user_id, pipeline_prime_id) map each pipeline source onto one entity, so
-- prime-vs-wallet becomes an attribute of the entity rather than part of the position id.
--
-- Classification is FK-validated against the merged reference vocabulary (20260630_130000):
--   entity_type       -> entity_type_ref
--   counterparty_role -> counterparty_role_ref   (nullable; per-position roles live elsewhere)
--   origination_type  -> origination_type_ref
--   domicile_country / country_of_risk -> country_ref
--   sector            -> sector_ref
-- parent_entity_id / ultimate_parent_id are soft refs to entity_master.entity_id (an SCD2 natural
-- key is non-unique, so it cannot be an FK target); resolve them via entity_master_current.
--
-- Phase 1 is a shell: pipeline_* bridges and the mandatory fields are populated; external attributes
-- (legal_name, LEI, domicile, the real entity_type) are curated later. Data load is VEC-418.
-- No `SET search_path` here: like every other table migration, objects land in the connection's
-- search_path (public in prod; the per-test schema under the integration harness). A session-level
-- SET would leak onto the migrator's pooled connection and desync the schema-isolated tests.

CREATE TABLE IF NOT EXISTS entity_master (
    entity_sk            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- immutable surrogate, stamped onto positions
    entity_id            text NOT NULL,                 -- natural key (em-...), stable across all versions
    processing_version   integer NOT NULL DEFAULT 1,    -- monotonic per entity_id; SCD2 dedup key
    valid_from           date NOT NULL DEFAULT CURRENT_DATE,  -- when this version became effective (only temporal field stored)
    change_reason        text NOT NULL,                 -- mandatory: why this version exists
    legal_name           text,
    short_name           text,
    entity_type          text NOT NULL,                 -- FK entity_type_ref
    counterparty_role    text,                          -- FK counterparty_role_ref (nullable)
    is_internal          boolean NOT NULL DEFAULT false, -- within the Sky/Prime group
    origination_type     text,                          -- FK origination_type_ref
    domicile_country     text,                          -- FK country_ref
    country_of_risk      text,                          -- FK country_ref
    sector               text,                          -- FK sector_ref (issuer GICS sector)
    lei                  text,
    swift_bic            text,
    cik                  text,
    parent_entity_id     text,                          -- soft ref to entity_master.entity_id (SCD2, not unique)
    ultimate_parent_id   text,                          -- soft ref to entity_master.entity_id (SCD2, not unique)
    entity_status        text NOT NULL DEFAULT 'ACTIVE',
    pipeline_user_id     bigint,                        -- bridge: "user".id (on-chain wallet holder)
    pipeline_prime_id    bigint,                        -- bridge: prime.id
    pipeline_protocol_id bigint,                        -- bridge: protocol.id
    source_system        text,
    created_at           timestamp with time zone NOT NULL DEFAULT now(),
    created_by           text NOT NULL DEFAULT 'system',
    approved_by          text,                           -- 4-eyes approver
    CONSTRAINT em_type_fkey FOREIGN KEY (entity_type) REFERENCES entity_type_ref(entity_type),
    CONSTRAINT em_role_fkey FOREIGN KEY (counterparty_role) REFERENCES counterparty_role_ref(counterparty_role),
    CONSTRAINT em_origination_fkey FOREIGN KEY (origination_type) REFERENCES origination_type_ref(origination_type),
    CONSTRAINT em_domicile_fkey FOREIGN KEY (domicile_country) REFERENCES country_ref(country_code),
    CONSTRAINT em_risk_fkey FOREIGN KEY (country_of_risk) REFERENCES country_ref(country_code),
    CONSTRAINT em_sector_fkey FOREIGN KEY (sector) REFERENCES sector_ref(sector),
    CONSTRAINT em_processing_version_chk CHECK (processing_version >= 1),
    CONSTRAINT em_status_chk CHECK (entity_status IN ('ACTIVE','DISSOLVED','MERGED','SUSPENDED'))
);

-- Catalog metadata (downstream data-dictionary / schema_master tooling reads pg_catalog comments).
COMMENT ON TABLE entity_master IS 'Append-only SCD2 legal-entity master (table_type=master). One row per (entity_id, processing_version); the latest per entity_id is the current record (entity_master_current). Positions resolve their holder to a single entity_id via the pipeline_* bridges.';
COMMENT ON COLUMN entity_master.entity_sk IS 'Immutable surrogate key; the value stamped onto positions.';
COMMENT ON COLUMN entity_master.entity_id IS 'Natural key (em-<code>); stable across all SCD2 versions.';
COMMENT ON COLUMN entity_master.processing_version IS 'Monotonic version per entity_id (>=1); SCD2 dedup key, loader-assigned.';
COMMENT ON COLUMN entity_master.valid_from IS 'Date this version became effective; only temporal field stored (valid_to derived in entity_master_versions).';
COMMENT ON COLUMN entity_master.change_reason IS 'Mandatory: why this version exists.';
COMMENT ON COLUMN entity_master.legal_name IS 'Full legal name.';
COMMENT ON COLUMN entity_master.short_name IS 'Short display name.';
COMMENT ON COLUMN entity_master.entity_type IS 'FK entity_type_ref; what kind of entity this is.';
COMMENT ON COLUMN entity_master.counterparty_role IS 'Nullable; FK counterparty_role_ref. Per-position roles live in the position layer, not here.';
COMMENT ON COLUMN entity_master.is_internal IS 'TRUE = within the Sky/Prime group.';
COMMENT ON COLUMN entity_master.origination_type IS 'FK origination_type_ref.';
COMMENT ON COLUMN entity_master.domicile_country IS 'FK country_ref; country of domicile.';
COMMENT ON COLUMN entity_master.country_of_risk IS 'FK country_ref; underlying economic-exposure country.';
COMMENT ON COLUMN entity_master.sector IS 'FK sector_ref; issuer GICS sector.';
COMMENT ON COLUMN entity_master.lei IS 'Legal Entity Identifier (ISO 17442).';
COMMENT ON COLUMN entity_master.swift_bic IS 'SWIFT/BIC code.';
COMMENT ON COLUMN entity_master.cik IS 'SEC Central Index Key.';
COMMENT ON COLUMN entity_master.parent_entity_id IS 'Soft ref to entity_master.entity_id (resolve via entity_master_current; not an FK).';
COMMENT ON COLUMN entity_master.ultimate_parent_id IS 'Soft ref to entity_master.entity_id (resolve via entity_master_current; not an FK).';
COMMENT ON COLUMN entity_master.entity_status IS 'ACTIVE / DISSOLVED / MERGED / SUSPENDED.';
COMMENT ON COLUMN entity_master.pipeline_user_id IS 'Bridge: "user".id. Maps an on-chain wallet holder onto this entity for holder unification (VEC-400).';
COMMENT ON COLUMN entity_master.pipeline_prime_id IS 'Bridge: prime.id. Maps a prime holder onto this entity for holder unification (VEC-400).';
COMMENT ON COLUMN entity_master.pipeline_protocol_id IS 'Bridge: protocol.id.';
COMMENT ON COLUMN entity_master.source_system IS 'System of record.';
COMMENT ON COLUMN entity_master.created_at IS 'Write timestamp.';
COMMENT ON COLUMN entity_master.created_by IS 'User or service that wrote the row.';
COMMENT ON COLUMN entity_master.approved_by IS '4-eyes approver.';

-- One version per (entity_id, processing_version): the SCD2 dedup key. Two same-day corrections are
-- distinguished by processing_version, and the current view breaks ties on it. The loader (VEC-418)
-- owns monotonic processing_version assignment per entity_id; a collision fails hard on this unique
-- index rather than silently merging.
CREATE UNIQUE INDEX IF NOT EXISTS em_id_version_uidx ON entity_master (entity_id, processing_version);
-- Current-version lookup (the ORDER BY of entity_master_current).
CREATE INDEX IF NOT EXISTS em_current_idx ON entity_master (entity_id, valid_from DESC, processing_version DESC);
-- FK-support and bridge-resolution indexes not covered by a leading PK column.
CREATE INDEX IF NOT EXISTS em_type_idx ON entity_master (entity_type);
CREATE INDEX IF NOT EXISTS em_user_idx ON entity_master (pipeline_user_id) WHERE pipeline_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS em_prime_idx ON entity_master (pipeline_prime_id) WHERE pipeline_prime_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS em_protocol_idx ON entity_master (pipeline_protocol_id) WHERE pipeline_protocol_id IS NOT NULL;

-- Current view: the latest version per entity_id.
CREATE OR REPLACE VIEW entity_master_current AS
SELECT DISTINCT ON (entity_id) *
FROM entity_master
ORDER BY entity_id, valid_from DESC, processing_version DESC;

-- Versioned view: derive valid_to (exclusive) and is_current for full-history reads. The validity
-- window is half-open [valid_from, valid_to_exclusive): point-in-time reads must use a strict upper
-- bound. A same-day correction (two versions sharing valid_from) yields a zero-width window on the
-- superseded row, so it is correctly never selected for any date.
CREATE OR REPLACE VIEW entity_master_versions AS
SELECT *,
    lead(valid_from) OVER (PARTITION BY entity_id ORDER BY valid_from, processing_version) AS valid_to_exclusive,
    (row_number() OVER (PARTITION BY entity_id ORDER BY valid_from DESC, processing_version DESC) = 1) AS is_current
FROM entity_master;

-- Reads for both roles; append-only writes for the indexer role (INSERT, never UPDATE/DELETE).
GRANT SELECT ON entity_master, entity_master_current, entity_master_versions TO stl_readonly;
GRANT SELECT, INSERT ON entity_master TO stl_readwrite;
GRANT SELECT ON entity_master_current, entity_master_versions TO stl_readwrite;

-- Append-only guard: revoke mutation on both the indexer role and the table owner so any
-- UPDATE / DELETE / TRUNCATE errors out. Guarded by role existence; mirrors the security_master guard.
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON entity_master FROM %I', role);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260714_120000_create_entity_master.sql') ON CONFLICT (filename) DO NOTHING;
