-- VEC-215: re-seed aave_v3_lido and aave_v3_rwa oracle_asset bindings by token
-- ADDRESS instead of symbol. The original VEC-210 migration
-- (20260505_135100_add_aave_lido_horizon_oracles.sql) joined oracle_asset to
-- the token table by symbol, but token has UNIQUE (chain_id, address) and no
-- uniqueness on symbol. Long-running runtime indexers (morpho_indexer,
-- allocation tracker, etc.) auto-insert tokens via GetOrCreateToken keyed by
-- address, so any non-canonical contract whose ERC20 symbol() collides with
-- a seeded symbol survives untouched and the symbol JOIN matches it
-- alongside the canonical row. AaveOracle has no source for the impostor
-- address and its fallback is address(0), so getAssetsPrices reverts on
-- every block via Multicall3: "Multicall3: call failed".
--
-- Confirmed in staging on 2026-05-07: the token table holds three chain_id=1
-- rows with symbol='USDC':
--   * 0xa0b86991...0eb48 — canonical Circle USDC (decimals=6)
--   * 0xb6a9c337...8a61e — asset of Morpho vault 0x633cec92... "ACRDX USDC
--                          Vault V1" (decimals=18); inserted by morpho_indexer
--   * 0xbafead7c...8e5d — referenced by 4,554 allocation_position rows
--                          (decimals=6); inserted by the allocation tracker
-- All three landed in oracle_asset for both new oracles. Aave V3 Lido /
-- Aave V3 RWA only register a source for the canonical USDC, so the
-- batched getAssetsPrices reverts on every block. Sparklend (which also
-- seeds USDC by symbol) was unaffected because its seed migration ran in
-- February 2026 — before the impostor rows appeared in March. The new
-- oracles seeded on 2026-05-05, after both impostors were in place.
--
-- This migration is idempotent: it removes whatever aave-style asset rows
-- exist for the two oracles, then re-inserts the canonical reserve set keyed
-- by address. Re-running converges on the correct state.

-- 1. Drop existing aave-style asset bindings for both oracles. Feed-style
--    rows (feed_address IS NOT NULL) are left alone — neither oracle uses
--    them today, but the predicate keeps this safe if any are added later.
DELETE FROM oracle_asset
WHERE oracle_id IN (SELECT id FROM oracle WHERE name IN ('aave_v3_lido', 'aave_v3_rwa'))
  AND feed_address IS NULL;

-- 2. Aave V3 Lido — re-seed by address.
--    AaveV3EthereumLidoAssets per aave-address-book: wstETH, WETH, USDS,
--    USDC, ezETH, sUSDe, GHO, rsETH, tETH. tETH is omitted (no token row;
--    same scope as VEC-210).
INSERT INTO oracle_asset (oracle_id, token_id, enabled)
SELECT o.id, t.id, true
FROM oracle o
JOIN token t ON t.chain_id = 1 AND t.address = ANY (ARRAY[
    '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, -- wstETH
    '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea, -- WETH
    '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, -- USDS
    '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, -- USDC
    '\xbf5495Efe5DB9ce00f80364C8B423567e58d2110'::bytea, -- ezETH
    '\x9D39A5DE30e57443BfF2A8307A4256c8797A3497'::bytea, -- sUSDe
    '\x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f'::bytea, -- GHO
    '\xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7'::bytea  -- rsETH
])
WHERE o.name = 'aave_v3_lido'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

-- 3. Aave V3 RWA (Horizon) — re-seed by address.
--    AaveV3EthereumHorizonAssets: GHO, USDC, RLUSD, USTB, USCC, USYC,
--    JTRSY, JAAA, VBILL, ACRED. Only GHO and USDC are token-table-resident
--    today; the RWA tokens need their own seed work (same scope as VEC-210).
INSERT INTO oracle_asset (oracle_id, token_id, enabled)
SELECT o.id, t.id, true
FROM oracle o
JOIN token t ON t.chain_id = 1 AND t.address = ANY (ARRAY[
    '\x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f'::bytea, -- GHO
    '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea  -- USDC
])
WHERE o.name = 'aave_v3_rwa'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260507_120000_fix_aave_lido_horizon_oracle_assets.sql')
ON CONFLICT (filename) DO NOTHING;
