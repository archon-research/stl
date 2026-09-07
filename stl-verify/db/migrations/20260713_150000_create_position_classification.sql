-- position_classification (VEC-401): the per-position deal-type classification, one row per
-- position_id. deal_type is a mutable classification, so it is NOT part of the position_id hash
-- (VEC-400: identity is holder + instrument); it is recorded here as a looked-up attribute of the
-- position, alongside the frozen direction + collateral_status. Written by the per-protocol
-- materializers (VEC-402..408): each computes position_id from its identity fields, then writes
-- this row keyed by that position_id, assigning deal_type_code from the raw leg's semantics
-- (e.g. morpho_market legs -> LOAN/BORROW/COLLATERAL) and direction from deal_type_ref.
--
-- deal_type_code is FK'd to deal_type_ref(deal_type) (the seeded reference codes, VEC-390); the
-- column is named deal_type_code to match the canonical vocabulary, while the FK target column in
-- deal_type_ref is named deal_type.
CREATE TABLE IF NOT EXISTS position_classification (
    position_id       bytea       NOT NULL,
    deal_type_code    text        NOT NULL,
    direction         text,
    collateral_status text,
    valid_from        date        NOT NULL DEFAULT CURRENT_DATE,
    change_reason     text,
    created_at        timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT position_classification_pkey PRIMARY KEY (position_id),
    CONSTRAINT position_classification_deal_type_fkey
        FOREIGN KEY (deal_type_code) REFERENCES deal_type_ref (deal_type),
    CONSTRAINT position_classification_direction_chk
        CHECK (direction IS NULL OR direction = ANY (ARRAY['LONG'::text, 'SHORT'::text])),
    -- position_id is sha256() output: enforce the 32-byte width rather than assume it (bytea is
    -- unlength-modified in Postgres), so a mis-sized id can never be stored (Simon review on #572).
    CONSTRAINT position_classification_id_len_chk CHECK (octet_length(position_id) = 32)
);

COMMENT ON TABLE position_classification IS
  '[Operational] Per-position deal-type classification (VEC-401). One row per position_id: the deal_type_code, the frozen direction, and collateral_status, all looked-up attributes of the position (not part of the position_id hash). Populated by the per-protocol position materializers.';
COMMENT ON COLUMN position_classification.position_id IS
  'PK. The bytea(32) position identity from position_id() (VEC-400). No FK to a positions table (positions are materialized per protocol; this row is written alongside them).';
COMMENT ON COLUMN position_classification.deal_type_code IS
  'FK->deal_type_ref.deal_type. The position''s deal type (LOAN/BORROW/COLLATERAL/CUSTODY/CUSTODY_COLLATERAL/ALLOCATION/...); a looked-up attribute of the position, not part of the position_id.';
COMMENT ON COLUMN position_classification.direction IS
  'LONG or SHORT, frozen from deal_type_ref.direction at classification time. NULL if the deal type carries no direction.';
COMMENT ON COLUMN position_classification.collateral_status IS
  'Collateral state of the position, set by the materializer where the deal type distinguishes it (e.g. anchorage CUSTODY vs CUSTODY_COLLATERAL). NULL where not applicable.';
COMMENT ON COLUMN position_classification.valid_from IS
  'Date this classification became effective (provenance). One current row per position_id; not SCD2 history.';
COMMENT ON COLUMN position_classification.change_reason IS
  'Why the classification was set or last changed (provenance).';
COMMENT ON COLUMN position_classification.created_at IS
  'Audit. Row insert time.';

GRANT SELECT ON position_classification TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON position_classification TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260713_150000_create_position_classification.sql') ON CONFLICT (filename) DO NOTHING;
