-- ARCT-379: correct the backfill_watermark.generation comment. The one set by
-- 20260831_120000 names only HandleReorgAtomic, which was already incomplete
-- when it landed and is more so now: RewindBackfillWatermark bumps the counter
-- from the backfill service's own paths, and the runbook's manual repair has to
-- bump it by hand. COMMENT ON is the catalogue's source of truth, so it has to
-- name every writer. New migration because 20260831_120000 is immutable once
-- applied.
--
-- "Count of reorg commits" was wrong too: a rewind is not always a reorg.

COMMENT ON COLUMN backfill_watermark.generation IS
  'Count of rewinds applied to this chain''s cursor, unitless and monotonic. Roles: none (operational). Bumped by every writer that lowers the watermark, or could have, whether or not the stored value actually moved: HandleReorgAtomic''s reorg commit; RewindBackfillWatermark, the backfill service''s rewind before it empties a height (stale-chain recovery, retry reconcile, post-save linkage orphan); and an operator''s manual rewind from docs/runbooks/vector-cronjobs.md, which must bump it in the same UPDATE. The gap filler''s compare-and-set advance (AdvanceBackfillWatermark) is the one writer that never bumps it — it matches on the (watermark, generation) pair instead, so a scan that straddled a rewind is refused and re-run against the new cursor rather than retiring the hole the rewind just opened.';

INSERT INTO migrations (filename)
VALUES ('20260901_100000_backfill_watermark_generation_comment.sql')
ON CONFLICT (filename) DO NOTHING;
