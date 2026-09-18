-- migrate: no-transaction
-- The instant-to-block lookup on block_meta: a time index, and the surviving version of each height.

DROP INDEX CONCURRENTLY IF EXISTS public.block_meta_chain_time_idx;

CREATE INDEX CONCURRENTLY block_meta_chain_time_idx
    ON public.block_meta (chain_id, block_timestamp DESC, block_number DESC, block_version DESC, processing_version DESC);

COMMENT ON INDEX public.block_meta_chain_time_idx IS '[Dimension] Serves instant-to-block lookups through block_meta_surviving, such as position_maple_loan (VEC-405) placing a sync cycle. Column order matches a pick ordered by block_timestamp then block_number descending, so the pick and its surviving-version check are index-only scans.';

CREATE OR REPLACE VIEW public.block_meta_surviving AS
SELECT m.chain_id, m.block_number, m.block_version, m.processing_version, m.block_timestamp
FROM public.block_meta m
WHERE NOT EXISTS (SELECT 1 FROM public.block_meta o
                   WHERE o.chain_id = m.chain_id AND o.block_number = m.block_number
                     AND (o.block_version, o.processing_version) > (m.block_version, m.processing_version));

COMMENT ON VIEW public.block_meta_surviving IS '[Dimension] One row per (chain_id, block_number): the block_meta row with the highest (block_version, processing_version), so header time rises with height across reorgs and reprocesses. A plain view, so a predicate on chain_id and block_timestamp reaches block_meta_chain_time_idx when the view is inlined into a lateral pick.';

INSERT INTO migrations (filename) VALUES ('20260909_135000_block_meta_surviving.sql') ON CONFLICT (filename) DO NOTHING;
