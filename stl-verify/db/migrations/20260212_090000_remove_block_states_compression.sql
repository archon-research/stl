-- Remove compression from block_states.
--
-- block_states is actively updated (MarkPublishComplete, MarkBlockOrphaned, reorg handling).
-- These UPDATEs filter by (chain_id, hash) — not by the partition column (created_at) —
-- so TimescaleDB cannot do chunk exclusion and must scan all chunks, including compressed
-- ones. This triggers "tuple decompression limit exceeded" (SQLSTATE 53400).
--
-- Compression is unnecessary here: with the 30-day retention policy the table holds ~216K
-- rows per chain (~7,200 blocks/day × 30 days), which is trivially small.
--
-- Wrapped in DO block so it's a no-op in dev environments where the local TimescaleDB
-- edition doesn't support columnstore compression.

DO $$
DECLARE
    _chunk REGCLASS;
BEGIN
    -- 1. Remove the compression policy (stops future compression jobs)
    PERFORM remove_compression_policy('block_states', if_exists => true);

    -- 2. Decompress all currently compressed chunks
    FOR _chunk IN SELECT c FROM show_chunks('block_states') c
    LOOP
        PERFORM decompress_chunk(_chunk, if_compressed => true);
    END LOOP;

    -- 3. Disable compression on the hypertable
    ALTER TABLE block_states SET (timescaledb.compress = false);
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping block_states compression removal: %', SQLERRM;
END;
$$;

INSERT INTO migrations (filename)
VALUES ('20260212_090000_remove_block_states_compression.sql')
ON CONFLICT (filename) DO NOTHING;
