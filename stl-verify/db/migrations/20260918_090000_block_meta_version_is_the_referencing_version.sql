-- VEC-835: block_version is the version that REFERENCED the block, not necessarily the
-- S3 object version. A December raw-block backfill hardcoded key version 1, so the two
-- diverge; the loader reads whichever version the archive holds and files it under this one.

COMMENT ON COLUMN block_meta.block_version IS 'PK. Reorg version: a reorged block at the same height is a distinct block with its own header time. Carries the block_version an observation table references, which is the join key and the key block_meta_worklist''s anti-join clears on. Usually the S3 object version too, but not always: a December raw-block backfill wrote deep history under key version 1 regardless of the referencing table''s bookkeeping (VEC-835), so where the requested version holds no block object the loader reads the highest archived version at that height that does, descending, and files that header time here. The resolved version may be above or below the requested one, so the time can come from a later or an earlier block; it is the same block''s time only where the two versions agree. The error is one slot wherever the difference is a single reorg, measured at exactly 12s across 521 chain-1 heights holding both v0 and v1, but nothing in the schema or the loader bounds it. Rows read this way are counted by the block_meta.headers.resolved metric and carry no marker of their own.';

INSERT INTO migrations (filename)
VALUES ('20260918_090000_block_meta_version_is_the_referencing_version.sql')
ON CONFLICT (filename) DO NOTHING;
