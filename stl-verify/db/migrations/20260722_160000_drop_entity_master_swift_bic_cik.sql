-- Drop entity_master.swift_bic and entity_master.cik.
--
-- Rationale: neither is a resolution key (positions resolve to an entity via on-chain codes in
-- entity_ref_codes and via pipeline_prime_id/pipeline_protocol_id -- never via a BIC or CIK), and
-- neither has a consumer. swift_bic identifies banks for fiat/SWIFT routing, which is out of scope for
-- an on-chain-position enrichment layer (no bank entities, no fiat settlement tracked); it would be
-- NULL for every row. cik applies only to SEC filers (~Coinbase, Circle) with no view reading it. Both
-- are empty on live, so there is no data loss.
--
-- Both views must be recreated: entity_master_current is SELECT * (its stored column list includes the
-- dropped columns) and entity_master_versions lists columns explicitly, so both depend on the columns.
-- DROP VIEW also drops the views' grants + comments, restored below. entity_ref_codes' SWIFT_BIC
-- code_type (in-review #580) is an unused-but-allowed enum value and is left as-is.
--
-- Rollback: schema-only, no data to restore (both columns are empty on live). A down migration
-- re-adds them nullable -- ALTER TABLE entity_master ADD COLUMN swift_bic text; ADD COLUMN cik text; --
-- then recreates the two views as below with swift_bic/cik restored to entity_master_versions'
-- explicit column list (entity_master_current is SELECT * and picks them up automatically).

DROP VIEW entity_master_current;
DROP VIEW entity_master_versions;

ALTER TABLE entity_master DROP COLUMN swift_bic;
ALTER TABLE entity_master DROP COLUMN cik;

-- entity_master_current: latest effective version per entity_id (unchanged except the dropped columns
-- no longer expand into SELECT *).
CREATE VIEW entity_master_current AS
SELECT DISTINCT ON (entity_id) *
FROM entity_master
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
ORDER BY entity_id, valid_from DESC, processing_version DESC;

-- entity_master_versions: full SCD2 history with derived valid_to_exclusive + is_current. Column list
-- is explicit (per the original, so a later ADD COLUMN does not shift the trailing computed columns);
-- swift_bic and cik removed from the list.
CREATE VIEW entity_master_versions AS
SELECT
    v.entity_id,
    v.processing_version,
    v.valid_from,
    v.change_reason,
    v.legal_name,
    v.short_name,
    v.entity_type,
    v.counterparty_role,
    v.is_internal,
    v.origination_type,
    v.domicile_country,
    v.country_of_risk,
    v.sector,
    v.lei,
    v.parent_entity_id,
    v.ultimate_parent_id,
    v.entity_status,
    v.pipeline_prime_id,
    v.pipeline_protocol_id,
    v.source_system,
    v.created_at,
    v.created_by,
    v.approved_by,
    v.valid_to_exclusive,
    (v.valid_from <= (now() AT TIME ZONE 'utc')::date
        AND (v.valid_to_exclusive IS NULL OR (now() AT TIME ZONE 'utc')::date < v.valid_to_exclusive)) AS is_current
FROM (
    SELECT entity_master.*,
        lead(valid_from) OVER (PARTITION BY entity_id ORDER BY valid_from, processing_version) AS valid_to_exclusive
    FROM entity_master
) v;

COMMENT ON VIEW entity_master_current IS '[Dimension] Latest effective version per entity_id (valid_from <= UTC today). The record positions resolve their holder/issuer to, via entity_id.';
COMMENT ON VIEW entity_master_versions IS '[Dimension] Full SCD2 history per entity_id with derived valid_to_exclusive (half-open [valid_from, valid_to_exclusive)) and is_current (effective as of UTC today).';

GRANT SELECT ON entity_master_current, entity_master_versions TO stl_readonly;
GRANT SELECT ON entity_master_current, entity_master_versions TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260722_160000_drop_entity_master_swift_bic_cik.sql') ON CONFLICT (filename) DO NOTHING;
