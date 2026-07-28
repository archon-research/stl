-- VEC-409: position_current — the current state of every position.
--
-- position_state (VEC-402..408 spine) records one row per (position_id, observation);
-- a position_id maps to many observations over time. This view collapses that to the
-- single latest observation per position_id (DISTINCT ON), which is the current state
-- for exposure/aggregation queries. It is the Position-ID stream's output that feeds
-- position_enriched (VEC-523).
--
-- "Current" = the latest observation. A position that has closed carries a latest
-- observation with quantity 0 (the per-protocol materializers emit a closing zero-row
-- on a real transition-to-zero, VEC-624 follow-up), so exposure queries filter
-- quantity <> 0; this view does not itself decide open/closed, it just surfaces the
-- latest state.
--
-- Under the shared-table design, this is a straight DISTINCT ON over position_state
-- (not a union of per-protocol views), so a new materializer needs no change here.

CREATE OR REPLACE VIEW position_current AS
SELECT DISTINCT ON (position_id) *
FROM position_state
ORDER BY position_id, block_number DESC, block_version DESC, processing_version DESC;

COMMENT ON VIEW position_current IS '[Operational] Current state per position (VEC-409): the latest observation per position_id from position_state (DISTINCT ON, ordered by block_number/version/processing_version DESC). Feeds position_enriched (VEC-523). A closed position surfaces with quantity 0; exposure queries filter quantity <> 0.';

GRANT SELECT ON position_current TO stl_readonly;
GRANT SELECT ON position_current TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260724_150000_create_position_current_view.sql') ON CONFLICT (filename) DO NOTHING;
