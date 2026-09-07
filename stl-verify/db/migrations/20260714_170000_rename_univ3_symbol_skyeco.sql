-- Rename the AUSD/USDC Uniswap V3 pool token row to skyeco's line naming.
--
-- 20260713_140000 renamed this row to 'AUSDUSDC-UNIV3' (the pair symbol the
-- tracker used to compose). Simon wants the symbol to match skyeco's backend,
-- which calls this position 'UNIV3-LP-AUSD-USDC' (prefix UNIV3-LP-, then the
-- pool's on-chain token0/token1 symbols hyphen-separated: token0 AUSD, token1
-- USDC). The tracker's composition helper now emits that format for the rows it
-- creates; this UPDATE brings DBs that already carry the old 'AUSDUSDC-UNIV3'
-- name into line. A second rename migration is required rather than editing
-- 20260713_140000: applied migrations are immutable (the migrator tracks
-- checksums and rejects modified files), so a fix is always a new file.
--
-- Renamed to match skyeco's line naming so cross-referencing the two systems
-- (info-sky risk-capital allocations vs. our allocation_position rows) needs no
-- mental mapping.
--
-- The token upsert never refreshes symbol on conflict, so the rename sticks.
-- The symbol filter makes re-runs a no-op instead of rewriting updated_at.
-- Runs as stl_migrator: an object-level UPDATE on token, no role-level ops.
UPDATE token
SET    symbol = 'UNIV3-LP-AUSD-USDC',
       updated_at = NOW()
WHERE  chain_id = 1
  AND  address = decode('bafead7c60ea473758ed6c6021505e8bbd7e8e5d', 'hex')
  AND  symbol IS DISTINCT FROM 'UNIV3-LP-AUSD-USDC';

-- Self-verify the end state rather than the UPDATE's row count (mirrors
-- 20260713_140000): any row at the pool address must now carry the skyeco
-- symbol. Zero rows is acceptable and expected on fresh DBs: the token row does
-- not exist until ingestion observes the position, and the tracker now creates
-- it with the skyeco-format symbol itself. (chain_id, address) is unique, so at
-- most one row can ever match; the count formulation simply fails on ANY
-- mismatched row.
DO $$
DECLARE
    mismatched INT;
BEGIN
    SELECT count(*) INTO mismatched
    FROM token
    WHERE chain_id = 1
      AND address = decode('bafead7c60ea473758ed6c6021505e8bbd7e8e5d', 'hex')
      AND symbol IS DISTINCT FROM 'UNIV3-LP-AUSD-USDC';
    IF mismatched > 0 THEN
        RAISE EXCEPTION 'AUSD/USDC UniV3 pool token row does not carry the skyeco symbol (% row(s))', mismatched;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260714_170000_rename_univ3_symbol_skyeco.sql')
ON CONFLICT (filename) DO NOTHING;
