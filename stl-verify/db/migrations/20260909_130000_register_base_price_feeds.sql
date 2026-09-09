-- Base (chain_id = 8453) price feeds for cbBTC, WETH and USDS under the existing
-- 'chainlink_base' oracle, plus the aBasUSDC (Aave V3 Base) receipt_token row
-- (ARCT-463; folds in ARCT-464).
--
-- Why: chainlink_base carries exactly one oracle_asset row (Base USDC,
-- 20260721_140000). The sparkUSDC Morpho vault's cbBTC collateral therefore has
-- no price and the $276M backed breakdown returns zero rows; aBasUSDC has no
-- receipt_token row so it falls to the direct path and prices to NULL.
--
-- All constants verified live on 2026-09-09 (block 51150810) against
-- base-rpc.publicnode.com (eth_call) and base.drpc.org (archive eth_getCode
-- bisect for deploy blocks). Feed addresses cross-checked against the Chainlink
-- reference data directory (reference-data-directory.vercel.app/
-- feeds-ethereum-mainnet-base-1.json, the source behind docs.chain.link
-- /data-feeds/price-feeds/addresses?network=base):
--   * cbBTC 0xcbb7..33bf  symbol() 'cbBTC', decimals() 8
--     feed 0x07DA..f9D  description() 'cbBTC / USD', decimals() 8, path cbbtc-usd
--     (the 18-decimal 0x1050..b79 'CBBTC / USD' is the shared-SVR variant; not used)
--   * WETH  0x4200..0006  symbol() 'WETH', decimals() 18
--     feed 0x7104..Bb70  description() 'ETH / USD', decimals() 8 -- the standard
--     proxy (directory secondaryProxyAddress; the primary 0x5001..3a8b is the
--     shared-SVR proxy and returned identical latestRoundData)
--   * USDS  0x820c..21dc  symbol() 'USDS', decimals() 18
--     feed 0x2330..2930  description() 'USDS / USD', decimals() 8, path usds-usd
--   * Aave V3 Base Pool 0xA238..d1c5: code present, deploy block 2357134;
--     aBasUSDC 0x4e65..c0ab: symbol() 'aBasUSDC', decimals() 6,
--     UNDERLYING_ASSET_ADDRESS() = Base USDC 0x8335..2913, POOL() = 0xA238..d1c5,
--     deploy block 8192239.
--
-- Deferred (ARCT-464 second half): Base sUSDS and fsUSDS. Base sUSDS
-- (0x5875..467a) is a bridged token -- asset() reverts, so erc4626_share cannot
-- price it -- and Chainlink Base publishes only 'sUSDS / USDS Exchange Rate'
-- (USDS-quoted; the feed unit chains ETH/BTC quotes only). The mainnet Chronicle
-- sUSDS/USD feed address has no code on Base. fsUSDS' receipt row is held back
-- with it so the position stays an honest quantity instead of a receipt priced
-- to NULL.
--
-- Live-forward only, no backfill: the Base oracle-price-worker loads units at
-- startup and the deploy that applies this migration rolls the pods.

-- ============================================================================
-- 1. Token seeds (fresh-DB determinism; a live Base-indexed DB already has these
--    rows, so ON CONFLICT no-ops).
-- ============================================================================
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (8453, '\xcbb7c0000ab88b473b1f5afd9ef808440eed33bf'::bytea, 'cbBTC', 8),
    (8453, '\x4200000000000000000000000000000000000006'::bytea, 'WETH', 18),
    (8453, '\x820c137fa70c8691f0e44dc420a5e53c168921dc'::bytea, 'USDS', 18),
    (8453, '\x4e65fe4dba92790696d040ac24aa414708f5c0ab'::bytea, 'aBasUSDC', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- 2. chainlink_base oracle_asset rows, same shape as the Base USDC row
--    (20260721_140000): feed_decimals 8, USD quote, enabled. Written against the
--    append-on-change key (20260901_120000): processing_version 0, change_reason
--    mandatory.
-- ============================================================================
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency, processing_version, change_reason)
SELECT o.id, t.id, true, v.feed_address, 8, 'USD', 0, 'ARCT-463: initial Base feed registration'
FROM (VALUES
    ('\xcbb7c0000ab88b473b1f5afd9ef808440eed33bf'::bytea, '\x07DA0E54543a844a80ABE69c8A12F22B3aA59f9D'::bytea),
    ('\x4200000000000000000000000000000000000006'::bytea, '\x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70'::bytea),
    ('\x820c137fa70c8691f0e44dc420a5e53c168921dc'::bytea, '\x2330aaE3bca5F05169d5f4597964D44522F62930'::bytea)
) AS v(token_address, feed_address)
JOIN token t ON t.chain_id = 8453 AND t.address = v.token_address
JOIN oracle o ON o.name = 'chainlink_base'
ON CONFLICT (oracle_id, token_id, feed_key, processing_version) DO NOTHING;

-- ============================================================================
-- 3. Aave V3 on Base, mirroring the mainnet 'Aave V3' row (20260205_120000) but
--    keyed to chain 8453 and the Base Pool proxy; created_at_block = deploy block.
-- ============================================================================
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (8453, '\xA238Dd80C259a72e81d7e4664a9801593F98d1c5'::bytea,
        'Aave V3', 'lending', 2357134, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- 4. Bind Aave V3 Base -> chainlink_base so the receipt path can price the
--    aBasUSDC underlying (Base USDC). from_block = later-of-deploys: the USDC/USD
--    feed (2093500) predates the Pool (2357134).
-- ============================================================================
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 2357134
FROM protocol p, oracle o
WHERE p.chain_id = 8453 AND p.name = 'Aave V3' AND o.name = 'chainlink_base'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- ============================================================================
-- 5. aBasUSDC receipt_token row (same shape as the SparkLend seeds,
--    20260814_120000). Moves the position onto the receipt path:
--    underlying_value (USDC) x Base USDC price.
-- ============================================================================
INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block)
SELECT 8453, p.id, t.id, '\x4e65fe4dba92790696d040ac24aa414708f5c0ab'::bytea, 'aBasUSDC', 8192239
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.address = '\xA238Dd80C259a72e81d7e4664a9801593F98d1c5'::bytea
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- ============================================================================
-- Resolution assertions (precedent: 20260721_140000). Every INSERT above resolves
-- FKs by natural key with ON CONFLICT DO NOTHING; fail loud instead of shipping
-- a silent hole.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt
    FROM (VALUES
        ('\xcbb7c0000ab88b473b1f5afd9ef808440eed33bf'::bytea, '\x07DA0E54543a844a80ABE69c8A12F22B3aA59f9D'::bytea),
        ('\x4200000000000000000000000000000000000006'::bytea, '\x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70'::bytea),
        ('\x820c137fa70c8691f0e44dc420a5e53c168921dc'::bytea, '\x2330aaE3bca5F05169d5f4597964D44522F62930'::bytea)
    ) AS v(token_address, feed_address)
    JOIN token t ON t.chain_id = 8453 AND t.address = v.token_address
    JOIN oracle o ON o.name = 'chainlink_base' AND o.enabled AND o.chain_id = 8453
    JOIN oracle_asset oa ON oa.oracle_id = o.id AND oa.token_id = t.id
                        AND oa.feed_address = v.feed_address
                        AND oa.enabled AND oa.feed_decimals = 8 AND oa.quote_currency = 'USD';
    IF cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 enabled chainlink_base oracle_asset rows (cbBTC, WETH, USDS), found %', cnt;
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 8453 AND p.name = 'Aave V3'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink_base';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Aave V3 Base -> chainlink_base protocol_oracle binding missing';
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM receipt_token rt
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 8453 AND p.name = 'Aave V3'
    JOIN token t ON t.id = rt.underlying_token_id AND t.chain_id = 8453
                AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
    WHERE rt.chain_id = 8453
      AND rt.receipt_token_address = '\x4e65fe4dba92790696d040ac24aa414708f5c0ab'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 aBasUSDC receipt_token row under Aave V3 Base with Base USDC underlying, found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260909_130000_register_base_price_feeds.sql')
ON CONFLICT (filename) DO NOTHING;
