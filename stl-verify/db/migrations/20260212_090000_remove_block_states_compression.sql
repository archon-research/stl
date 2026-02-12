-- Remove compression from block_states.
--
-- block_states is actively updated (MarkPublishComplete, MarkBlockOrphaned, reorg handling).
-- These UPDATEs filter by (chain_id, hash) — not by the partition column (created_at) —
-- so TimescaleDB cannot do chunk exclusion and must scan all chunks, including compressed
-- ones. This triggers "tuple decompression limit exceeded" (SQLSTATE 53400).
--
-- Compression is unnecessary here: with the 30-day retention policy the table holds ~216K
-- rows per chain (~7,200 blocks/day × 30 days), which is trivially small.

-- 1. Remove the compression policy (stops future compression jobs)
SELECT remove_compression_policy('block_states');

-- 2. Decompress all currently compressed chunks
SELECT decompress_chunk(c, if_compressed => true)
FROM show_chunks('block_states') c;

-- 3. Disable compression on the hypertable
ALTER TABLE block_states SET (timescaledb.compress = false);
