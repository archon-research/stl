-- Correct the deploy_block column comment on curve_pool. It was written when
-- deploy_block was merely advisory. It is now load-bearing: dexconsumer.DueSet
-- gates snapshot scheduling on it and hard-errors ("registry bug") when a
-- touched pool reports a deploy_block greater than the block being processed.
-- A value set ABOVE the true deploy block therefore stalls the indexer, so the
-- value must be a lower bound. New migration because 20260521_120000 (which set
-- the original curve_pool comment) is immutable once applied.
--
-- The uniswap_v3_pool.deploy_block comment lives in
-- 20260701_100000_create_uniswap_v3_tables.sql (the migration that creates the
-- table); it is not repeated here.

COMMENT ON COLUMN curve_pool.deploy_block IS
  'Configuration (load-bearing). On-chain deployment block, used to gate snapshot reads. If non-NULL it MUST be a lower bound of the true deploy block (<= actual deploy height): DueSet hard-errors ("registry bug") when a touched pool reports deploy_block greater than the processed block, and skips sweep scheduling for not-yet-deployed pools. NULL is permitted and disables the gate (treated as 0 / genesis). Never set it above the true deploy block.';
