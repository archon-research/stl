-- Follow-up to the VEC-411 (#574) re-review: business "today" must be UTC. Written as CURRENT_DATE,
-- a valid_from default and an SCD2 "current" view bound resolve against the writer's / reader's session
-- TimeZone GUC, so a non-UTC session can record or resolve an effective date a day off, and two current
-- views can disagree for the same reader around local midnight. This normalizes the already-merged
-- enrichment / position tables to (now() AT TIME ZONE 'utc')::date, matching the fix applied to
-- security_master in #574. These migrations are immutable once applied, so the fix lands here rather
-- than by editing the originals. Idempotent: SET DEFAULT and CREATE OR REPLACE VIEW re-run cleanly.
-- No SET search_path (leaks onto the migrator's pooled connection; see 20260713_140000).

-- security_instrument_bridge (VEC-412): the one Simon flagged. Its current view could return a
-- different mapping than security_master_current for a non-UTC reader session; bound both on UTC today.
ALTER TABLE security_instrument_bridge
    ALTER COLUMN valid_from SET DEFAULT ((now() AT TIME ZONE 'utc')::date);
CREATE OR REPLACE VIEW security_instrument_bridge_current AS
SELECT DISTINCT ON (instrument_key) *
FROM security_instrument_bridge
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
ORDER BY instrument_key, valid_from DESC, processing_version DESC;
COMMENT ON VIEW security_instrument_bridge_current IS '[Dimension] Latest effective mapping per instrument_key (valid_from <= UTC today). What VEC-420 resolves security against.';

-- position_classification (VEC-401) and position_entity_link (VEC-415): no current view (the PK is one
-- row per position / role), so only the valid_from default carries the TimeZone sensitivity; normalize
-- it to match the masters and bridge.
ALTER TABLE position_classification
    ALTER COLUMN valid_from SET DEFAULT ((now() AT TIME ZONE 'utc')::date);
ALTER TABLE position_entity_link
    ALTER COLUMN valid_from SET DEFAULT ((now() AT TIME ZONE 'utc')::date);

INSERT INTO migrations (filename) VALUES ('20260721_120000_utc_normalize_enrichment_valid_from.sql') ON CONFLICT (filename) DO NOTHING;
