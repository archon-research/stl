-- Correct curve_pool.deploy_block values that were seeded ABOVE the true
-- on-chain deployment block (PR#519 review finding #4).
--
-- deploy_block is load-bearing (see the column COMMENT set in
-- 20260703_120000_curve_deploy_block_loadbearing_comment.sql): dexconsumer.DueSet
-- gates snapshot scheduling on it and HARD-ERRORS ("registry bug" -> the SQS
-- message is never acked -> the whole chain poison-stalls) when a touched pool
-- reports deploy_block greater than the block being processed. A value set
-- ABOVE the true deploy height is therefore a latent stall, reachable on any
-- touched block in [true_deploy, seeded-1] (gap-backfill / historical reindex,
-- not just the live tip). The value MUST be a lower bound (<= true deploy).
--
-- True deploy heights below are cast-verified against Ethereum mainnet:
-- lowest block whose `cast code <addr>` is non-empty (code absent at B-1,
-- present at B). The original seed lives in
-- 20260521_110000_create_curve_dex_tables.sql, which is immutable once applied,
-- hence this new migration rather than an edit.
--
-- Per-pool verification (chain_id = 1):
--   stETH classic  0xDC24316b9AE028F1497c275EB9192a3Ea0f67022
--       seeded 11592551 == true 11592551 (absent @ 11592550, present @ 11592551) -- exact, unchanged
--   stETH-ng       0x21E27a5E5513D6e65C4f830167390997aA84843a
--       seeded 17500000 >  true 17272519 (absent @ 17272518, present @ 17272519) -- ~227k too high, CORRECTED below
--   3pool          0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7
--       seeded 10809473 == true 10809473 (absent @ 10809472, present @ 10809473) -- exact, unchanged
--   TricryptoUSDC  0x7F86Bf177Dd4F3494b841a37e810A34dD56c829B
--       seeded 17072859 <  true 17371455 (absent @ 17371454, present @ 17371455) -- already a safe lower bound, unchanged
--
-- Only stETH-ng was above its true deploy height, so it is the only correction.
-- Runs as stl_migrator: an object-level UPDATE on curve_pool, no role-level ops.

UPDATE curve_pool
SET    deploy_block = 17272519
WHERE  chain_id = 1
  AND  pool_address = '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea;

-- Self-verify the final state rather than the UPDATE's row count: if the WHERE
-- ever fails to match (address stored differently than this bytea literal, wrong
-- chain_id), the UPDATE silently no-ops and the latent poison-stall this
-- migration exists to remove would survive a green migration run. Asserting the
-- end state (not affected-row count) keeps this idempotent on re-run.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM curve_pool
        WHERE chain_id = 1
          AND pool_address = '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea
          AND deploy_block = 17272519
    ) THEN
        RAISE EXCEPTION 'stETH-ng deploy_block correction did not apply (pool row missing or deploy_block != 17272519)';
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260707_120000_fix_curve_deploy_blocks.sql')
ON CONFLICT (filename) DO NOTHING;
