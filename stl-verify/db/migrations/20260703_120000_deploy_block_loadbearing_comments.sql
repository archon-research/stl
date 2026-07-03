-- Correct the deploy_block column comments on curve_pool and uniswap_v3_pool.
-- Both were written when deploy_block was merely advisory. It is now
-- load-bearing: dexconsumer.DueSet gates snapshot scheduling on it and
-- hard-errors ("registry bug") when a touched pool reports a deploy_block
-- greater than the block being processed. A value set ABOVE the true deploy
-- block therefore stalls the indexer, so the value must be a lower bound.
-- The two DEXes differ on NULL: Curve maps NULL->0 (gate disabled); Uniswap V3
-- LoadPools rejects NULL. The previous uniswap_v3_pool comment wrongly claimed
-- NULL was allowed. New migration because migrations are immutable once applied.

COMMENT ON COLUMN curve_pool.deploy_block IS
  'Configuration (load-bearing). On-chain deployment block, used to gate snapshot reads. If non-NULL it MUST be a lower bound of the true deploy block (<= actual deploy height): DueSet hard-errors ("registry bug") when a touched pool reports deploy_block greater than the processed block, and skips sweep scheduling for not-yet-deployed pools. NULL is permitted and disables the gate (treated as 0 / genesis). Never set it above the true deploy block.';

COMMENT ON COLUMN uniswap_v3_pool.deploy_block IS
  'Configuration (load-bearing). Block at which the pool contract was deployed (PoolCreated), used to gate snapshot reads. REQUIRED: LoadPools rejects a NULL deploy_block (unlike Curve), since a NULL defeats the reorg deploy-gate. Must be a lower bound of the true deploy block (<= actual deploy height): DueSet hard-errors ("registry bug") when a touched pool reports deploy_block greater than the processed block, and skips sweep scheduling before this height.';
