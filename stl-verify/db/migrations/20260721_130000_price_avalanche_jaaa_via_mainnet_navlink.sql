-- Price the Avalanche JAAA position (grove, ~$260M, chain_id = 43114) whose
-- allocation amount_usd is NULL today (ER calculation).
--
-- CROSS-CHAIN NAV ATTACH — DEVIATES FROM docs/prices_oracles_reference.md, FLAG FOR
-- MAINTAINER REVIEW.
-- prices_oracles_reference.md ("Reasoning for Oracle Selection") states we currently
-- only use an oracle when the asset is on the SAME chain as the oracle, a conservative
-- default rooted in the worry that a chain-local oracle may aggregate market prices
-- from other chains/CEXes that do not reflect the asset's own chain. This migration
-- deviates: it links the Avalanche JAAA token to the Ethereum-mainnet Chainlink NAVLink
-- "JAAA NAV" feed. The deviation is safe for THIS asset specifically because a NAVLink
-- NAV is a fund-level, chain-independent figure (the Janus Henderson Anemoy AAA CLO
-- Fund's net asset value per token), not a chain-local market price — the same fund NAV
-- backs every chain's JAAA representation, so "which chain's market" is not a meaningful
-- question here. This is not a general lifting of the same-chain rule; it is one curated
-- fund-NAV exception. A maintainer should confirm the exception before we generalise it.
--
-- WHY CROSS-CHAIN AT ALL — NO JAAA ORACLE EXISTS ON AVALANCHE (checked 2026-07-21):
--   * Chronicle's VAO registry (chronicleprotocol/documentation adapters.md/routers.md)
--     lists JAAA adapters/routers only on Ethereum / Base / Monad, not Avalanche; the
--     deterministic Chronicle adapter/router addresses have no deployed code on avax
--     (cast eth_getCode = 0x).
--   * Chainlink's avalanche-mainnet feed registry (92 feeds) contains no JAAA feed.
--   * The VEC-499 ticket's "JAAA uses the same oracle address as mainnet" claim is FALSE:
--     cast eth_getCode(0x02cf8C9fBa24d79886dAc40cb620f0930C6E8eC0) on Avalanche = 0x (no
--     code). That address is the mainnet Chronicle JAAA aggregator only.
-- The mainnet NAVLink "JAAA NAV" feed is therefore the only on-chain source of a JAAA
-- price, and it already exists and is polled (mainnet JAAA row, 20260709 below).
--
-- DESIGN — ONE oracle_asset ROW, NO NEW ORACLE / NO PROTOCOL BINDING / NO NEW WORKER:
-- Avalanche JAAA is a DIRECT holding of grove's avalanche ALM (not an aave/morpho reserve
-- receipt token), so it is valued through the direct-holdings path
-- (_DIRECT_ASSET_HOLDINGS_SQL), which looks up onchain_token_price purely by token_id with
-- no chain or position-block constraint. We add a single oracle_asset row linking the avax
-- JAAA token to the SAME existing mainnet 'chainlink' oracle and the SAME NAVLink feed as
-- the mainnet JAAA row. The mainnet oracle-price-worker already polls that feed every block;
-- with this row it also writes an onchain_token_price row for the avax token_id (the worker's
-- asset loading filters only by oracle_id+enabled and joins token by id with no chain filter,
-- and onchain_token_price carries no chain_id column, so the cross-chain link needs no code
-- change). Prices begin at the worker's next restart/redeploy (units load once at startup).
--
-- WHY NOT HORIZON / an aave-style protocol oracle: same reasoning as the mainnet JAAA row
-- in 20260709_120000_add_er_missing_price_feeds.sql (step 2) — aave-style oracles fetch all
-- reserves in one getAssetsPrices multicall, so a single unpriceable/delisted asset reverts
-- the whole batch and stalls the oracle every block; an independent per-feed row cannot
-- couple failures. It does not apply here regardless (avax JAAA is directly held, not an
-- aave reserve), so the independent NAVLink feed row is the correct, decoupled home.
--
-- Cast-verified 2026-07-21: Avalanche JAAA token 0x58F93d6b1EF2F44eC379Cb975657C132CBeD3B6b
-- symbol "JAAA", decimals 6. Mainnet NAVLink "JAAA NAV" feed
-- 0x3BbccB2301759D2e4A5692bA72DAb4b75dC43B1a, decimals() = 6, quote USD (re-used from the
-- mainnet JAAA row).

-- ============================================================================
-- 1. Seed the Avalanche JAAA token (fresh-DB determinism; if the avalanche
--    indexers already inserted it the ON CONFLICT makes this a no-op). Chain
--    43114 is seeded in 20260207_120000_add_chain_id_and_hypertable.sql.
-- ============================================================================
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (43114, '\x58F93d6b1EF2F44eC379Cb975657C132CBeD3B6b'::bytea, 'JAAA', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- 2. Link avax JAAA to the mainnet 'chainlink' oracle via the mainnet NAVLink
--    "JAAA NAV" feed (6 decimals, USD). The feed-unique partial index
--    (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL lets the
--    SAME feed serve a second token_id (the mainnet JAAA row keeps its own).
-- ============================================================================
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\x3BbccB2301759D2e4A5692bA72DAb4b75dC43B1a'::bytea, 6, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink' AND t.chain_id = 43114
  AND t.address = '\x58F93d6b1EF2F44eC379Cb975657C132CBeD3B6b'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- ============================================================================
-- Resolution assertion (precedent: 20260709_120000_add_er_missing_price_feeds.sql).
-- The INSERT above resolves FKs by natural key with ON CONFLICT DO NOTHING, so a
-- typoed address, a missing 'chainlink' oracle, or a pre-existing row with the same
-- key but enabled=false / wrong feed_decimals would silently insert nothing. Fail the
-- migration instead of shipping a silent hole. Strictness mirrors the template: the
-- row must be enabled, USD-quoted, feed_decimals=6, and the token decimals must be 6.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'chainlink'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 43114
     AND t.address = '\x58F93d6b1EF2F44eC379Cb975657C132CBeD3B6b'::bytea
     AND t.decimals = 6
    WHERE oa.enabled
      AND oa.feed_address = '\x3BbccB2301759D2e4A5692bA72DAb4b75dC43B1a'::bytea
      AND oa.feed_decimals = 6
      AND oa.quote_currency = 'USD';
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 enabled chainlink avax-JAAA NAVLink oracle_asset row (feed_decimals 6, USD, token decimals 6), found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260721_130000_price_avalanche_jaaa_via_mainnet_navlink.sql')
ON CONFLICT (filename) DO NOTHING;
