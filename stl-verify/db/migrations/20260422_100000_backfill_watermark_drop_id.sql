-- VEC-149: drop vestigial `id` column, make `chain_id` the primary key.
-- Fixes UPSERT ON CONFLICT (chain_id) which was failing on L2 chains because
-- the column default `id = 1` collided with the seeded Ethereum row before the
-- chain_id arbiter was evaluated.

ALTER TABLE backfill_watermark DROP CONSTRAINT backfill_watermark_pkey;
ALTER TABLE backfill_watermark DROP COLUMN id;
ALTER TABLE backfill_watermark DROP CONSTRAINT unique_backfill_watermark_chain;
ALTER TABLE backfill_watermark ADD PRIMARY KEY (chain_id);
ALTER TABLE backfill_watermark ALTER COLUMN chain_id DROP DEFAULT;

-- Seed a zero-watermark row for every chain currently in the `chain` table
-- that doesn't already have one. Covers 10/130/8453/42161, which the bug
-- prevented from self-creating a row via SetBackfillWatermark's UPSERT.
-- Chains added to `chain` after this migration runs will get their row on
-- their first successful backfill pass, which is now unblocked.
INSERT INTO backfill_watermark (chain_id, watermark)
SELECT chain_id, 0 FROM chain
ON CONFLICT (chain_id) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260422_100000_backfill_watermark_drop_id.sql')
ON CONFLICT (filename) DO NOTHING;
