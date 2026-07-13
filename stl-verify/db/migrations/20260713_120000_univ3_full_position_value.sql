-- Uniswap V3 positions are now valued fully (both sides) by the allocation
-- tracker: the source converts amount0+amount1 at the pool's own spot price
-- into the entry's hint asset and writes the result to both balance and
-- underlying_value/underlying_token_id (previously NULL for uni_v3 rows,
-- with balance carrying only the hint-side amount). Live-forward only: rows
-- written before this deploy are not reprocessed.
--
-- 1) Refresh the underlying_value catalogue comment (base text from
--    20260702_120000_add_allocation_position_underlying_value.sql) and the
--    sibling balance/scaled_balance comments (base text from
--    20260609_120000_add_schema_comments.sql), whose stated semantics no
--    longer hold for uni_v3 rows.
-- 2) Rename the misleading token row for the AUSD/USDC V3 pool contract.

COMMENT ON COLUMN allocation_position.underlying_value IS
  'Current position value denominated in underlying_token_id''s asset (decimals-normalised by that asset), read on-chain at (block_number, block_version) at the same pinned block as balance. erc4626: convertToAssets(shares). atoken: balanceOf (1:1 underlying by construction). erc20: balanceOf, deliberately duplicating balance so non-NULL uniformly means "valued"; do not deduplicate. uni_v3_pool/uni_v3_lp: full position value (both sides, converted at the pool''s own spot price) in the hint asset''s units, principal only (uncollected fees excluded); uni_v3 rows written before 2026-07-13 are NULL. NULL: not computable (curve/NAV-token rows this phase, reverting or undecodable convertToAssets, missing asset_address) and every row written before this column (or its type''s valuation) existed. NULL is never zero exposure; consumers fall back to balance-based pricing.';

COMMENT ON COLUMN allocation_position.balance IS
  'On-chain balanceOf reading for the proxy at this block, decimals-normalized to a human-readable value. Populated with the real balance (not 0). uni_v3_pool/uni_v3_lp: no balanceOf exists (the row is keyed by the V3 pool contract); balance is the tracker-computed full position value in the hint asset''s units, equal to underlying_value; rows written before 2026-07-13 held only the hint-side token amount.';

COMMENT ON COLUMN allocation_position.scaled_balance IS
  'Nullable, decimals-normalized. On-chain scaledBalanceOf reading (interest-free balance), not computed from liquidity_index. Populated only for aTokens. uni_v3_pool/uni_v3_lp: NULL since 2026-07-13 (a V3 position has no share count; earlier rows held a meaningless mixed-unit amount0+amount1 sum).';

-- The synome export prices the grove AUSD/USDC Uniswap V3 position through an
-- asset_address hint (USDC), and the tracker used to stamp that hint's symbol
-- onto the POOL address's token row: an impostor "USDC" row whose address is
-- actually the V3 pool contract 0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d.
-- The tracker now composes a truthful pair symbol (token0 || token1 ||
-- '-UNIV3') when it creates the row, so fresh DBs never see the impostor;
-- this UPDATE fixes DBs that already carry it. The token upsert never
-- refreshes symbol on conflict, so the rename sticks. The symbol filter makes
-- re-runs a no-op instead of rewriting updated_at.
-- Runs as stl_migrator: an object-level UPDATE on token, no role-level ops.
UPDATE token
SET    symbol = 'AUSDUSDC-UNIV3',
       updated_at = NOW()
WHERE  chain_id = 1
  AND  address = decode('bafead7c60ea473758ed6c6021505e8bbd7e8e5d', 'hex')
  AND  symbol IS DISTINCT FROM 'AUSDUSDC-UNIV3';

-- Self-verify the end state rather than the UPDATE's row count (mirrors
-- 20260707_120000): any row at the pool address must now carry the composed
-- symbol. Zero rows is acceptable and expected on fresh DBs: the token row
-- does not exist until ingestion observes the position, and the tracker now
-- creates it with the truthful composed symbol itself. (chain_id, address) is
-- unique, so at most one row can ever match; the count formulation simply
-- fails on ANY mismatched row.
DO $$
DECLARE
    mismatched INT;
BEGIN
    SELECT count(*) INTO mismatched
    FROM token
    WHERE chain_id = 1
      AND address = decode('bafead7c60ea473758ed6c6021505e8bbd7e8e5d', 'hex')
      AND symbol IS DISTINCT FROM 'AUSDUSDC-UNIV3';
    IF mismatched > 0 THEN
        RAISE EXCEPTION 'AUSD/USDC UniV3 pool token row still carries a misleading symbol (% row(s))', mismatched;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260713_120000_univ3_full_position_value.sql')
ON CONFLICT (filename) DO NOTHING;
