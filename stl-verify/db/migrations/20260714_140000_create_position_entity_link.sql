-- position_entity_link (VEC-415): the entities attached to a position that are NOT the issuer,
-- one row per (position_id, entity_role). Roles: counterparty, custodian, protocol operator, etc.
--
-- The issuer is resolved on the position itself; this table holds the other roles, so a position can
-- carry several linked entities without widening the position row. Written by VEC-421 using the
-- holder/counterparty resolver (VEC-417 -> entity_ref_codes / entity_master).
--
-- Keys and refs:
--   position_id  bytea(32) from position_id() (VEC-400). No FK to a positions table (positions are
--                materialized per protocol; this row is written alongside them, as position_classification).
--   entity_role  FK -> counterparty_role_ref(counterparty_role); the role this entity plays.
--   entity_id    the linked entity, by NATURAL key. Soft ref to entity_master.entity_id (SCD2,
--                non-unique: no FK). The current entity is resolved live by joining
--                entity_master_current on entity_id; we deliberately do NOT freeze a per-version
--                surrogate here, because a frozen surrogate pins to one SCD2 version and goes stale
--                on the next (Simon review on #574).
-- Operational (mutable), like position_classification, and NOT historised: one row per
-- (position_id, entity_role), so re-pointing a role overwrites the previous linkage in place (the
-- prior entity is not retained). valid_from dates the CURRENT linkage only; it is not a
-- point-in-time history axis. Because DEFAULT CURRENT_DATE fires only on INSERT, the VEC-421 writer
-- must set valid_from (and change_reason) explicitly when it re-points an existing row.
-- No `SET search_path`: a session-level SET leaks onto the migrator's pooled connection and desyncs
-- the per-test-schema integration harness (see VEC-411).

CREATE TABLE IF NOT EXISTS position_entity_link (
    position_id   bytea       NOT NULL,   -- position identity from position_id() (VEC-400); no FK (materialized per protocol)
    entity_role   text        NOT NULL,   -- FK counterparty_role_ref; the role the entity plays on this position
    entity_id     text        NOT NULL,   -- linked entity by natural key; soft ref to entity_master.entity_id (resolve via entity_master_current)
    valid_from    date        NOT NULL DEFAULT CURRENT_DATE,
    change_reason text,                    -- why the linkage was set or last changed (provenance)
    created_at    timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT position_entity_link_pkey PRIMARY KEY (position_id, entity_role),
    CONSTRAINT position_entity_link_role_fkey
        FOREIGN KEY (entity_role) REFERENCES counterparty_role_ref (counterparty_role),
    -- Non-issuer only: the issuer is resolved on the position itself (via security_master), so an
    -- ISSUER row here would open a second, conflicting issuer channel. counterparty_role_ref seeds
    -- ISSUER for other uses, so the role FK alone does not exclude it (Simon review on #581).
    CONSTRAINT position_entity_link_not_issuer_chk CHECK (entity_role <> 'ISSUER')
);

COMMENT ON TABLE position_entity_link IS
  '[Operational] Non-issuer entities linked to a position (VEC-415). One row per (position_id, entity_role): counterparty / custodian / operator / etc. Links by natural key entity_id; the current entity is resolved live via entity_master_current (no frozen surrogate). Populated by VEC-421 via the holder/counterparty resolver (VEC-417).';
COMMENT ON COLUMN position_entity_link.position_id IS
  'PK. The bytea(32) position identity from position_id() (VEC-400). No FK to a positions table (positions are materialized per protocol).';
COMMENT ON COLUMN position_entity_link.entity_role IS
  'PK. FK->counterparty_role_ref.counterparty_role. The role this entity plays on the position. Excludes ISSUER (CHECK): the issuer is resolved on the position itself, not linked here.';
COMMENT ON COLUMN position_entity_link.entity_id IS
  'Linked entity by natural key. Soft ref to entity_master.entity_id (resolve the current entity/surrogate via entity_master_current; not an FK). No per-version surrogate is frozen here, so the link never goes stale.';
COMMENT ON COLUMN position_entity_link.valid_from IS
  'Date the current linkage took effect (provenance). Not a history axis: one row per (position_id, entity_role), so a re-point overwrites in place. DEFAULT fires only on INSERT; the writer sets this explicitly on re-point.';
COMMENT ON COLUMN position_entity_link.change_reason IS
  'Why the linkage was set or last changed (provenance).';
COMMENT ON COLUMN position_entity_link.created_at IS
  'Audit. Row insert time.';

-- Reverse lookup: all positions linked to a given entity.
CREATE INDEX IF NOT EXISTS position_entity_link_entity_id_idx ON position_entity_link (entity_id);

GRANT SELECT ON position_entity_link TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON position_entity_link TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260714_140000_create_position_entity_link.sql') ON CONFLICT (filename) DO NOTHING;
