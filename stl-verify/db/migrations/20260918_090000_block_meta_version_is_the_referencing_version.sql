-- VEC-835: block_version is the version that REFERENCED the block, not the S3 object
-- version. A December raw-block backfill hardcoded key version 1, so the two diverge;
-- the loader reads the archive's real version but files the row under the requested one.

COMMENT ON COLUMN block_meta.block_version IS 'PK. Reorg version: a reorged block at the same height is a distinct block with its own header time. Carries the block_version an observation table references, which is the join key and the key block_meta_worklist''s anti-join clears on. Usually the S3 object version too, but not always: a December raw-block backfill wrote deep history under key version 1 regardless of the referencing table''s bookkeeping (VEC-835), so where the requested version has no object the loader reads the archive''s highest version and files the header time under the requested version. Where both versions are archived each is read directly and keeps its own time; only an absent one inherits, bounded by one slot for a reorg replacement.';

INSERT INTO migrations (filename)
VALUES ('20260918_090000_block_meta_version_is_the_referencing_version.sql')
ON CONFLICT (filename) DO NOTHING;
