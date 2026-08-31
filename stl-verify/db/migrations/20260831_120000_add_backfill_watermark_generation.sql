-- ARCT-379: the gap filler's compare-and-set matched on the watermark alone,
-- which a reorg does not necessarily change. The ordinary steady state has the
-- watermark one below head, and that is exactly where a reorg's common ancestor
-- lands, so the rewind is a no-op: the pass that scanned before the reorg then
-- found the value it expected, advanced over the height the reorg had just
-- orphaned, and no later pass scanned it again (FindGaps only looks above the
-- watermark). generation gives the compare-and-set a term every reorg commit
-- moves, whether or not the watermark itself did.
--
-- Existing rows start at 0, which is also what a chain with no row reads as, so
-- the first pass after this migration compares against the same pair either way.

ALTER TABLE backfill_watermark
    ADD COLUMN generation BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN backfill_watermark.generation IS
  'Count of reorg commits that have touched this chain''s cursor, unitless and monotonic. Roles: none (operational). Bumped by every reorg commit (HandleReorgAtomic), including one whose rewind changed no watermark; the gap filler''s compare-and-set advance matches on (watermark, generation), so a scan that straddled a reorg is refused and re-run against the new cursor instead of retiring the hole the reorg opened.';

INSERT INTO migrations (filename)
VALUES ('20260831_120000_add_backfill_watermark_generation.sql')
ON CONFLICT (filename) DO NOTHING;
