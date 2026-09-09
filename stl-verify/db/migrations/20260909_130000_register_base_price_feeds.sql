-- ARCT-463 (folds in ARCT-464): Base price feeds for cbBTC, WETH, USDS under the
-- existing chainlink_base oracle, plus Aave V3 Base and its aBasUSDC receipt_token row.
-- Feed/token addresses from the Chainlink Base directory, verified on-chain at block
-- 51150810 (description/decimals/symbol/UNDERLYING_ASSET_ADDRESS). WETH uses the
-- standard ETH/USD proxy, not the shared-SVR one.
-- Deferred: Base sUSDS/fsUSDS — bridged sUSDS has no asset() and Chainlink Base only
-- quotes it in USDS; needs a USDS-quoted feed unit first (tracked on ARCT-463).

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (8453, '\xcbb7c0000ab88b473b1f5afd9ef808440eed33bf'::bytea, 'cbBTC', 8),
    (8453, '\x4200000000000000000000000000000000000006'::bytea, 'WETH', 18),
    (8453, '\x820c137fa70c8691f0e44dc420a5e53c168921dc'::bytea, 'USDS', 18),
    (8453, '\x4e65fe4dba92790696d040ac24aa414708f5c0ab'::bytea, 'aBasUSDC', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

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

INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (8453, '\xA238Dd80C259a72e81d7e4664a9801593F98d1c5'::bytea,
        'Aave V3', 'lending', 2357134, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 2357134
FROM protocol p, oracle o
WHERE p.chain_id = 8453 AND p.name = 'Aave V3' AND o.name = 'chainlink_base'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block)
SELECT 8453, p.id, t.id, '\x4e65fe4dba92790696d040ac24aa414708f5c0ab'::bytea, 'aBasUSDC', 8192239
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.address = '\xA238Dd80C259a72e81d7e4664a9801593F98d1c5'::bytea
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

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
