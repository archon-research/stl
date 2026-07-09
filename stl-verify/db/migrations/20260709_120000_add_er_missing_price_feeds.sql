-- Price feeds for the grove/spark RWA and vault tokens whose allocations amount_usd is
-- NULL today (ER calculation), Ethereum mainnet (chain_id = 1).
-- LIVE-FORWARD ONLY, no backfill (design decision 2026-07-08): rows start producing
-- onchain_token_price after the next oracle-price-worker restart/redeploy (units load
-- once at startup); history stays empty.
-- All token contracts (symbol/decimals), feed contracts (decimals/description/answer) and
-- the Curve pool (coins, get_virtual_price) re-verified live via cast on 2026-07-09.
-- Four steps, ordered by dependency:
--   1. Seed the priced tokens (fresh-DB determinism; prod rows already exist, no-ops).
--   2. oracle_asset feed rows under the existing chainlink/redstone oracles.
--   3. protocol_oracle binding Morpho Blue -> chainlink.
--   4. curve_lp_ng oracle registry rows for the AUSD/USDC pool (shipped disabled).

-- ============================================================================
-- 1. Token seeds. Same precedent as 20260702_120000_maple_syrup_allocation_exposure.sql:
--    seed the fixed, verified set so every JOIN below resolves on a fresh DB; in
--    environments where runtime indexers already inserted these rows the ON CONFLICT
--    makes this a no-op. syrupUSDC is not re-seeded here: 20260702 (applied strictly
--    before this file) already guarantees it. AUSD exists only on Avalanche so far
--    (20260318); this is its chain-1 row.
-- ============================================================================
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\x8c213ee79581Ff4984583C6a801e5263418C4b86'::bytea, 'JTRSY', 6),
    (1, '\x5a0F93D040De44e78F251b03c43be9CF317Dcf64'::bytea, 'JAAA', 6),
    (1, '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea, 'AUSD', 6),
    (1, '\x51C2d74017390CbBd30550179A16A1c28F7210fc'::bytea, 'STAC', 6),
    (1, '\x6a9DA2D710BB9B700acde7Cb81F10F1fF8C89041'::bytea, 'BUIDL-I', 6),
    -- Curve StableSwap-NG pools are their own LP token; 18 decimals.
    (1, '\xE79C1C7E24755574438A26D5e062Ad2626C04662'::bytea, 'AUSDUSDC', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- 2. Per-token USD feed rows under the existing chainlink/redstone oracles.
--    Cast-verified answers 2026-07-09: JTRSY 1.108804, JAAA 1.040239, AUSD 0.99975,
--    STAC 1023.041098, BUIDL-I 1.00000000.
--
--    JTRSY and JAAA are DELIBERATELY NOT priced via the Horizon aave_v3_rwa oracle,
--    even though both are Horizon reserves: aave-style oracles fetch all assets in one
--    getAssetsPrices multicall, so any one unpriceable or delisted asset reverts the
--    batch and stalls the whole oracle every block (the syrupUSDC lesson, see
--    20260702_120000_maple_syrup_allocation_exposure.sql). Independent per-feed rows
--    cannot couple failures, and NAVLink is raw fund NAV rather than Aave's
--    risk-adjusted view.
-- ============================================================================

-- JTRSY / Chainlink NAVLink "JTRSY NAV" (6 decimals).
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\x0C2e4Df738e99e8db80012f5bb2a303f3f48Ca74'::bytea, 6, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink' AND t.chain_id = 1
  AND t.address = '\x8c213ee79581Ff4984583C6a801e5263418C4b86'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- JAAA / Chainlink NAVLink "JAAA NAV" (6 decimals).
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\x3BbccB2301759D2e4A5692bA72DAb4b75dC43B1a'::bytea, 6, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink' AND t.chain_id = 1
  AND t.address = '\x5a0F93D040De44e78F251b03c43be9CF317Dcf64'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- AUSD / Chainlink "AUSD / USD". UNUSUAL: 18 feed decimals, not the customary 8 of
-- Chainlink USD pairs; verified via decimals() on the aggregator. Copying 8 here would
-- inflate the price by 1e10.
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD'::bytea, 18, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink' AND t.chain_id = 1
  AND t.address = '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- STAC / RedStone "STAC_FUNDAMENTAL" (8 decimals).
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\xEdC6287D3D41b322AF600317628D7E226DD3add4'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'redstone' AND t.chain_id = 1
  AND t.address = '\x51C2d74017390CbBd30550179A16A1c28F7210fc'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- BUIDL-I / RedStone "BUIDL_I_ETHEREUM_FUNDAMENTAL" (8 decimals). The NAV is pinned at
-- 1.00 by fund design: yield distributes as newly minted tokens, so 1.00 is the correct
-- per-token exposure price and yield shows up as balance growth. RedStone's companion
-- DAILY_ACCRUAL feed (accrued dividends, saw-tooths back to par at each distribution)
-- is deliberately not used.
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\xf2db7b3455077Fb177215d45D62d441DF3C17bf3'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'redstone' AND t.chain_id = 1
  AND t.address = '\x6a9DA2D710BB9B700acde7Cb81F10F1fF8C89041'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- ============================================================================
-- 3. Bind Morpho Blue -> chainlink. This binding was missing entirely: the allocations
--    receipt-token USD join walks protocol_oracle to reach onchain_token_price, so every
--    Morpho receipt token (grove-bbqUSDC, grove-bbqAUSD, ...) was unpriceable regardless
--    of feed coverage. USDC already has a chainlink oracle_asset row (feed
--    0x8fFfFfd4...818f6, 20260212) and AUSD gets one above; nothing else to add.
-- ============================================================================

-- Protocol seed mirrors the values morpho_indexer's GetOrCreateProtocol writes at
-- runtime (singleton 0xBBBB..FFCb, deployed at block 18883124 on mainnet), so a fresh
-- DB binds deterministically and live DBs no-op on conflict.
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (1, '\xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb'::bytea,
        'Morpho Blue', 'lending', 18883124, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

-- from_block = Morpho Blue deploy block: the binding is valid from the later of the
-- protocol and oracle deploy blocks, and chainlink (10606501) long predates Morpho.
-- Same convention as the maple -> aave_v3 binding in 20260702 (16291127).
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 18883124
FROM protocol p, oracle o
WHERE p.chain_id = 1 AND p.name = 'Morpho Blue' AND o.name = 'chainlink'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- ============================================================================
-- 4. AUSD/USDC Curve LP: new curve_lp_ng oracle type. StableSwap-NG pools are their own
--    LP token, so oracle.address = the pool = the priced token. deployment_block 20457618
--    found by bisecting eth_getCode over an archive node (empty at 20457617, code at
--    20457618). Starts disabled, same sequencing as fsUSDS
--    (20260626_120000_add_fsusds_erc4626_oracle.sql): an old binary that encounters an
--    unknown oracle_type would error every block and DLQ-loop the whole pipeline. Flip
--    enabled=true in a follow-up migration after the image supporting curve_lp_ng is
--    rolled out.
-- ============================================================================
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
VALUES (
    'curve_ausdusdc_lp',
    'Curve AUSD/USDC LP (StableSwap-NG)',
    1,
    '\xE79C1C7E24755574438A26D5e062Ad2626C04662'::bytea,
    'curve_lp_ng',
    20457618,
    false,
    8
)
ON CONFLICT (name) DO NOTHING;

-- The LP token appears TWICE under this oracle, once per pool coin feed (the
-- (oracle_id, token_id, feed_address) uniqueness supports this). The worker prices the
-- LP as get_virtual_price() x min(coin USD feeds), the standard manipulation-resistant
-- stable-LP valuation: min() assumes the worst-case coin backs the whole pool, so a
-- depeg of either coin can only underprice, never overprice, the LP.
-- Coin feeds (coins() verified on the pool): coin0 USDC via the chainlink USDC/USD
-- feed (8 decimals), coin1 AUSD via the chainlink AUSD/USD feed (18 decimals, see the
-- decimals note in step 2).
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, f.feed::bytea, f.feed_decimals, 'USD'
FROM (VALUES
    ('\x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6', 8),
    ('\xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD', 18)
) AS f(feed, feed_decimals)
CROSS JOIN oracle o
JOIN token t ON t.chain_id = 1 AND t.address = '\xE79C1C7E24755574438A26D5e062Ad2626C04662'::bytea
WHERE o.name = 'curve_ausdusdc_lp'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- oracle_type is comment-documented, not CHECK-constrained; extend the enum list and
-- feed_address's per-type role (last stated in 20260626).
COMMENT ON COLUMN public.oracle.oracle_type IS
  'Determines ABI method called when reading prices: aave_oracle | chainlink_feed | chronicle | redstone | erc4626_share | curve_lp_ng.';
COMMENT ON COLUMN public.oracle_asset.feed_address IS
  'Specific feed contract for this token (20 bytes). Null for Aave-style oracles. '
  'For chainlink_feed / chronicle / redstone: the priced token''s own feed, denominated in '
  'quote_currency (non-USD quotes are converted to USD via USD reference feeds). '
  'For erc4626_share: the UNDERLYING token''s USD feed (not the share token''s own feed); '
  'the binary reads this address via DirectCaller to obtain the underlying spot price. '
  'For curve_lp_ng: one row per pool coin, each carrying that coin''s USD feed; the '
  'worker prices the LP token as get_virtual_price() x min(coin feed prices).';

-- ============================================================================
-- Resolution assertions (precedent: 20260610_120000_create_maple_graphql_tables.sql).
-- Every INSERT above resolves FKs by natural key with ON CONFLICT DO NOTHING, so a
-- typoed address or missing registry row would silently insert nothing. Fail the
-- migration instead of shipping a silent hole.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    -- Rows must also be enabled with the exact expected feed_decimals: a
    -- pre-existing row with the same key but enabled=false or wrong decimals
    -- would make the ON CONFLICT DO NOTHING above a silent no-op; fail the
    -- migration instead.
    SELECT COUNT(*) INTO cnt
    FROM (VALUES
        ('chainlink', '\x8c213ee79581Ff4984583C6a801e5263418C4b86'::bytea, '\x0C2e4Df738e99e8db80012f5bb2a303f3f48Ca74'::bytea, 6),
        ('chainlink', '\x5a0F93D040De44e78F251b03c43be9CF317Dcf64'::bytea, '\x3BbccB2301759D2e4A5692bA72DAb4b75dC43B1a'::bytea, 6),
        ('chainlink', '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea, '\xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD'::bytea, 18),
        ('redstone',  '\x51C2d74017390CbBd30550179A16A1c28F7210fc'::bytea, '\xEdC6287D3D41b322AF600317628D7E226DD3add4'::bytea, 8),
        ('redstone',  '\x6a9DA2D710BB9B700acde7Cb81F10F1fF8C89041'::bytea, '\xf2db7b3455077Fb177215d45D62d441DF3C17bf3'::bytea, 8)
    ) AS x(oracle_name, token_address, feed_address, feed_decimals)
    JOIN oracle o ON o.name = x.oracle_name
    JOIN token t ON t.chain_id = 1 AND t.address = x.token_address
    JOIN oracle_asset oa
      ON oa.oracle_id = o.id AND oa.token_id = t.id AND oa.feed_address = x.feed_address
     AND oa.enabled AND oa.feed_decimals = x.feed_decimals;
    IF cnt <> 5 THEN
        RAISE EXCEPTION 'expected 5 enabled chainlink/redstone feed oracle_asset rows with expected feed_decimals, found %', cnt;
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Morpho Blue -> chainlink protocol_oracle binding missing';
    END IF;

    -- Same enabled + exact-feed_decimals strictness as assertion 1.
    SELECT COUNT(*) INTO cnt
    FROM (VALUES
        ('\x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6'::bytea, 8),
        ('\xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD'::bytea, 18)
    ) AS f(feed_address, feed_decimals)
    JOIN oracle o ON o.name = 'curve_ausdusdc_lp' AND o.oracle_type = 'curve_lp_ng'
    JOIN oracle_asset oa
      ON oa.oracle_id = o.id AND oa.feed_address = f.feed_address
     AND oa.enabled AND oa.feed_decimals = f.feed_decimals;
    IF cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 enabled curve_ausdusdc_lp oracle_asset rows with expected feed_decimals, found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260709_120000_add_er_missing_price_feeds.sql')
ON CONFLICT (filename) DO NOTHING;
