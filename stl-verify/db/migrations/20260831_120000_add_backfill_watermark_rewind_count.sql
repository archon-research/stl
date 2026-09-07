-- ARCT-379: the gap filler's compare-and-set matched on the watermark alone,
-- which a reorg does not necessarily change. The ordinary steady state has the
-- watermark one below head, and that is exactly where a reorg's common ancestor
-- lands, so the rewind is a no-op: the pass that scanned before the reorg then
-- found the value it expected, advanced over the height the reorg had just
-- orphaned, and no later pass scanned it again (FindGaps only looks above the
-- watermark). rewind_count gives the compare-and-set a term every rewind moves,
-- whether or not the watermark itself did.
--
-- Existing rows start at 0, which is also what a chain with no row reads as, so
-- the first pass after this migration compares against the same pair either way.

ALTER TABLE backfill_watermark
    ADD COLUMN rewind_count BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN backfill_watermark.rewind_count IS
  'Count of rewinds applied to this chain''s cursor, unitless and monotonic. Roles: none (operational). Bumped by every writer that lowers the watermark, or could have, whether or not the stored value actually moved: HandleReorgAtomic''s reorg commit; RewindBackfillWatermark, the backfill service''s rewind before it empties a height (stale-chain recovery, retry reconcile, post-save linkage orphan); and an operator''s manual rewind from docs/runbooks/vector-cronjobs.md, which must bump it in the same UPDATE. The gap filler''s compare-and-set advance (AdvanceBackfillWatermark) is the one writer that never bumps it — it matches on the (watermark, rewind_count) pair instead, so a scan that straddled a rewind is refused and re-run against the new cursor rather than retiring the hole the rewind just opened.';

INSERT INTO migrations (filename)
VALUES ('20260831_120000_add_backfill_watermark_rewind_count.sql')
ON CONFLICT (filename) DO NOTHING;
