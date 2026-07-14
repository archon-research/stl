-- Uniswap V3 positions are now valued fully (both sides) by the allocation
-- tracker: the source converts amount0+amount1 at the pool's own spot price
-- into the entry's hint asset and writes the result to both balance and
-- underlying_value/underlying_token_id (previously NULL for uni_v3 rows,
-- with balance carrying only the hint-side amount). Positions are matched by
-- the pool's full identity (token0, token1, fee), and a wallet with no live
-- matching position writes an explicit zero row (the API's latest-row read
-- has no freshness cutoff, so a skipped entry would freeze the last positive
-- row as current exposure). Live-forward only: rows written before this
-- deploy are not reprocessed.
--
-- 1) Rewrite the underlying_value catalogue comment (base text from
--    20260702_120000_add_allocation_position_underlying_value.sql) and the
--    sibling balance/scaled_balance comments (base text from
--    20260609_120000_add_schema_comments.sql): the uni_v3 semantics changed,
--    and the old texts also asserted claims the writers never honoured
--    (balance "not 0" vs curve/erc4626 zero rows; scaled_balance "only for
--    aTokens" vs curve/erc4626 share counts; an unconditional balance-based
--    pricing fallback vs the API's underlying_value-only allowlist).
-- 2) Rename the misleading token row for the AUSD/USDC V3 pool contract.

COMMENT ON COLUMN allocation_position.underlying_value IS
  'Current position value denominated in underlying_token_id''s asset (decimals-normalised by that asset), read on-chain at (block_number, block_version) at the same pinned block as balance. erc4626: convertToAssets(shares), an explicit 0 when the wallet holds no shares. atoken: balanceOf (1:1 underlying by construction). erc20: balanceOf, deliberately duplicating balance so non-NULL uniformly means "valued"; do not deduplicate. uni_v3_pool/uni_v3_lp: full position value (both sides, converted at the pool''s own spot price) in the hint asset''s units, principal only (uncollected fees excluded); an explicit 0 once the wallet no longer holds a live matching position (exited/burned/transferred); rows written before the 20260713_140000 deploy are NULL. NULL: not computable (curve/NAV-token rows with no valuation implemented, reverting or undecodable convertToAssets, missing asset_address) and every row written before this column (or its type''s valuation) existed. NULL is never zero exposure: consumers priced by balance fall back to balance-based pricing, while tokens the API prices solely by underlying_value (its allowlisted set, incl. uni_v3 pool rows) surface NULL amount_usd instead of a wrong-basis balance valuation.';

COMMENT ON COLUMN allocation_position.balance IS
  'On-chain balanceOf reading for the proxy at this block, decimals-normalized to a human-readable value. An emptied position is an explicit 0 row (every source persists the as-read zero; curve/erc4626/uni_v3 have explicit zero branches), so the latest row never freezes stale exposure. uni_v3_pool/uni_v3_lp: no balanceOf exists (the row is keyed by the V3 pool contract); balance is the tracker-computed full position value in the hint asset''s units, equal to underlying_value; rows written before the 20260713_140000 deploy held only the hint-side token amount.';

COMMENT ON COLUMN allocation_position.scaled_balance IS
  'Nullable, decimals-normalized. atoken: on-chain scaledBalanceOf reading (interest-free balance), not computed from liquidity_index; NULL when that read failed. curve/erc4626: the raw share count, duplicating balance by design (balance is the share-denominated on-chain reading for these types). uni_v3_pool/uni_v3_lp: NULL since the 20260713_140000 deploy (a V3 position has no share count; earlier rows held a meaningless mixed-unit amount0+amount1 sum). NULL for all other types.';

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
VALUES ('20260713_140000_univ3_full_position_value.sql')
ON CONFLICT (filename) DO NOTHING;
