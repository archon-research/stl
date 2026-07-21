-- Register the two Base (chain_id = 8453) Morpho vault receipts held by grove --
-- steakUSDC (~$177M) and grove-bbqUSDC (~$6M) -- with the FULL pricing chain, so
-- that once the Base allocation tracker writes their positions they resolve to a
-- USD value instead of NULL. This front-loads the whole registry (token, protocol,
-- oracle, oracle_asset, protocol_oracle, morpho_vault, receipt_token) in ONE file:
-- the sparkUSDCbc lesson (20260709_120000, step 7) is that registering a receipt
-- token WITHOUT its pricing chain flips the position onto the receipt path and then
-- prices it to NULL. Deploys must not be able to race a half-built registry, so all
-- five parts ship together and ahead of the two runtime workers that consume them.
--
-- The receipt path (allocation_position_repository.py, _RECEIPT_TOKEN_POSITIONS_SQL)
-- prices a receipt position as COALESCE(underlying_value, balance) x underlying
-- price, where underlying_value is the tracker's ERC4626 convertToAssets reading and
-- the underlying price is resolved through protocol_oracle -> onchain_token_price for
-- receipt_token.underlying_token_id. So Base Morpho Blue must bind to a Base chainlink
-- oracle that prices Base USDC. Both vaults expose convertToAssets (V2 and V1.1 are
-- both ERC4626; cast-verified below), so the tracker can populate underlying_value.
--
-- ALL constants cast-verified live on 2026-07-21 against base-rpc.publicnode.com
-- (contract reads) and base.drpc.org (archive eth_getCode; publicnode rejects
-- archive requests without a token). Deploy blocks found by bisecting eth_getCode
-- (empty at N-1, code at N):
--   * Morpho Blue singleton 0xBBBB..FFCb  -> block 13977148 (Base deploy; SAME
--     singleton address as mainnet 0xBBBB..FFCb, code present, 15.6 KB).
--   * steakUSDC 0xbeef0e08..73C9          -> block 37404640 (on-chain creation).
--   * grove-bbqUSDC 0xbeef2d50..461a       -> block 36780134 (on-chain creation).
--   * Base USDC/USD feed 0x7e86..bc6B      -> block 2093500 (feed proxy creation).
--
-- Token / feed reads (base-rpc.publicnode.com, 2026-07-21):
--   * Base USDC 0x8335..2913: symbol() = 'USDC', decimals() = 6.
--   * Feed 0x7e86..bc6B: description() = 'USDC / USD', decimals() = 8,
--     latestRoundData().answer = 99979348 (0.9998), updatedAt = 1784557595 (fresh).
--
-- V2-vs-V1.1 ASYMMETRY (an earlier plan wrongly assumed both were VaultV2; they are
-- not -- verified by whether MORPHO() reverts, the VaultV2 discriminator in
-- entity.MorphoVault, internal/domain/entity/morpho_vault.go):
--   * steakUSDC     symbol() = 'steakUSDC', name() = 'Steakhouse Prime USDC',
--                   asset() = Base USDC, MORPHO() REVERTS, convertToAssets(1e18) =
--                   1033761 -> Morpho VaultV2 -> vault_version 3 (MorphoVaultV2).
--   * grove-bbqUSDC symbol() = 'grove-bbqUSDC', name() = 'Grove x Steakhouse USDC
--                   High Yield', asset() = Base USDC, MORPHO() = the singleton
--                   (0xBBBB..FFCb, does NOT revert), convertToAssets(1e18) = 1036687
--                   -> MetaMorpho V1.1 -> vault_version 2 (MorphoVaultV1_1).
-- Every JOIN below resolves the vaults STRICTLY by address, never by symbol (labels
-- are not authoritative; the registries key on (chain_id, address)).
--
-- Live-forward only, no backfill. USD only starts flowing once (a) the Base
-- allocation-tracker (separate stacked VEC-499 PR) writes these positions and (b) a
-- Base oracle-price-worker polls chainlink_base into onchain_token_price. Both are
-- tracked in VEC-499; this migration is deliberately ahead of both.
--
-- Five parts, ordered by dependency:
--   1. token:            Base USDC.
--   2. protocol:         Morpho Blue on Base (8453), same singleton address.
--   3. oracle:           new 'chainlink_base' + its Base USDC/USD oracle_asset feed.
--   4. protocol_oracle:  Base Morpho Blue -> chainlink_base (from later-of-deploys).
--   5. morpho_vault + receipt_token rows for BOTH vaults.

-- ============================================================================
-- 1. Base USDC token seed (fresh-DB determinism; a live Base-indexed DB already
--    has this row, so ON CONFLICT no-ops). Base isn't indexed yet, so seed the
--    verified address by natural key. symbol/decimals cast-verified above.
-- ============================================================================
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES (8453, '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea, 'USDC', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- 2. Morpho Blue protocol on Base. Mirrors the mainnet 'Morpho Blue' row shape
--    (20260709_120000, step 3: name 'Morpho Blue', protocol_type 'lending',
--    metadata '{}') that morpho_indexer's GetOrCreateProtocol writes at runtime,
--    but keyed to Base: chain_id 8453, created_at_block 13977148 = the Base
--    singleton deploy block. Natural key is (chain_id, address); the singleton
--    lives at the SAME address on Base as on mainnet, and the (chain_id, address)
--    uniqueness keeps the two chains' rows distinct.
-- ============================================================================
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (8453, '\xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb'::bytea,
        'Morpho Blue', 'lending', 13977148, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- 3. New 'chainlink_base' oracle + its Base USDC/USD feed row. oracle.name is
--    globally unique, and the existing 'chainlink' oracle is chain_id 1; the
--    future Base oracle-price-worker selects oracles WHERE chain_id = 8453, so
--    Base needs its own oracle row (a chain-1 'chainlink' row would be invisible
--    to it and would point the mainnet worker at a Base feed). oracle_type
--    'chainlink_feed' already exists (20260212); no schema/comment change needed.
--    address NULL: feed-based oracles carry the contract per-asset in
--    oracle_asset.feed_address, not on the oracle row (20260212). deployment_block
--    2093500 = the Base USDC/USD feed proxy creation block, matching the mainnet
--    'chainlink' precedent (deployment_block = earliest feed).
-- ============================================================================
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('chainlink_base', 'Chainlink (Base)', 8453, NULL, 'chainlink_feed', 2093500, 8, true)
ON CONFLICT (name) DO NOTHING;

-- Base USDC / Chainlink 'USDC / USD' feed (0x7e86..bc6B, 8 decimals, USD quote),
-- cast-verified above. This is the enabled oracle_asset the receipt path's
-- EXISTS(oracle_asset ... enabled) gate requires to price Base USDC.
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\x7e860098F58bBFC8648a4311b374B1D669a2bc6B'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink_base' AND t.chain_id = 8453
  AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- ============================================================================
-- 4. Bind Base Morpho Blue -> chainlink_base. The receipt-token USD join walks
--    protocol_oracle to reach onchain_token_price, so without this binding every
--    Base Morpho receipt is unpriceable regardless of feed coverage (the same gap
--    step 3 of 20260709_120000 fixed for mainnet). from_block = later-of-deploys
--    of the protocol (Morpho Blue Base, 13977148) and the oracle (Base USDC/USD
--    feed, 2093500): the feed long predates the protocol, so from_block =
--    13977148. Same convention as the mainnet Morpho Blue -> chainlink binding.
-- ============================================================================
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 13977148
FROM protocol p, oracle o
WHERE p.chain_id = 8453 AND p.name = 'Morpho Blue' AND o.name = 'chainlink_base'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- ============================================================================
-- 5. morpho_vault + receipt_token rows for BOTH vaults, mirroring exactly what
--    morpho_indexer's discoverAndRegisterVault upserts (the pair together). All
--    FKs resolve by natural key: protocol by (chain_id 8453, name 'Morpho Blue'),
--    underlying/asset token by (chain_id 8453, Base USDC address). Resolved
--    STRICTLY by address; symbol is never a join key. created_at_block = each
--    vault's on-chain creation block (bisected above), which is what the indexer's
--    LEAST(created_at_block) receipt upsert and DO UPDATE vault upsert converge to.
--    receipt_token uniqueness is (chain_id, receipt_token_address) since
--    20260319_100000, so two USDC-underlying vaults coexist under one protocol.
-- ============================================================================

-- steakUSDC: Morpho VaultV2 (vault_version 3), MORPHO() reverts.
INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
SELECT 8453, p.id, '\xbeef0e0834849aCC03f0089F01f4F1Eeb06873C9'::bytea,
       'Steakhouse Prime USDC', 'steakUSDC', t.id, 3, 37404640
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.name = 'Morpho Blue'
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
SELECT 8453, p.id, t.id, '\xbeef0e0834849aCC03f0089F01f4F1Eeb06873C9'::bytea,
       'steakUSDC', 37404640, '{}'::jsonb, NOW()
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.name = 'Morpho Blue'
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- grove-bbqUSDC: MetaMorpho V1.1 (vault_version 2), MORPHO() = the singleton.
INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
SELECT 8453, p.id, '\xbeef2d50b428675a1921bc6bbf4bfb9d8cf1461a'::bytea,
       'Grove x Steakhouse USDC High Yield', 'grove-bbqUSDC', t.id, 2, 36780134
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.name = 'Morpho Blue'
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
SELECT 8453, p.id, t.id, '\xbeef2d50b428675a1921bc6bbf4bfb9d8cf1461a'::bytea,
       'grove-bbqUSDC', 36780134, '{}'::jsonb, NOW()
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.name = 'Morpho Blue'
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- ============================================================================
-- Resolution assertions (precedent: 20260709_120000, 20260714_120000). Every
-- INSERT above resolves FKs by natural key with ON CONFLICT DO NOTHING, so a
-- typoed address or a missing registry row would silently insert nothing. Fail
-- the migration loud instead of shipping a silent hole. The last two assertions
-- guard the pricing prerequisites this file itself creates -- without them the
-- receipt path would resolve the positions but price them to NULL.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    -- Both vaults must resolve as morpho_vault rows with the Base USDC underlying
    -- under Base Morpho Blue, each with its correct vault_version (guards a V2/V1.1
    -- swap: steakUSDC=3, grove-bbqUSDC=2).
    SELECT COUNT(*) INTO cnt
    FROM (VALUES
        ('\xbeef0e0834849aCC03f0089F01f4F1Eeb06873C9'::bytea, 3::smallint),
        ('\xbeef2d50b428675a1921bc6bbf4bfb9d8cf1461a'::bytea, 2::smallint)
    ) AS v(address, vault_version)
    JOIN morpho_vault mv ON mv.chain_id = 8453 AND mv.address = v.address
                        AND mv.vault_version = v.vault_version
    JOIN protocol p ON p.id = mv.protocol_id AND p.chain_id = 8453 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = mv.asset_token_id AND t.chain_id = 8453
                AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea;
    IF cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 Base Morpho morpho_vault rows (steakUSDC v3, grove-bbqUSDC v2) with Base USDC underlying, found %', cnt;
    END IF;

    -- Both vaults must resolve as receipt_token rows with the Base USDC underlying
    -- under Base Morpho Blue (the rows that move the positions onto the receipt path).
    SELECT COUNT(*) INTO cnt
    FROM (VALUES
        ('\xbeef0e0834849aCC03f0089F01f4F1Eeb06873C9'::bytea),
        ('\xbeef2d50b428675a1921bc6bbf4bfb9d8cf1461a'::bytea)
    ) AS v(address)
    JOIN receipt_token rt ON rt.chain_id = 8453 AND rt.receipt_token_address = v.address
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 8453 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = rt.underlying_token_id AND t.chain_id = 8453
                AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea;
    IF cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 Base Morpho receipt_token rows (steakUSDC, grove-bbqUSDC) with Base USDC underlying, found %', cnt;
    END IF;

    -- Pricing prerequisite 1: Base Morpho Blue must be bound to chainlink_base,
    -- the protocol_oracle join the receipt path resolves the USDC price through.
    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 8453 AND p.name = 'Morpho Blue'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink_base';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Base Morpho Blue -> chainlink_base protocol_oracle binding missing';
    END IF;

    -- Pricing prerequisite 2: chainlink_base must price Base USDC with an enabled
    -- oracle_asset carrying the exact feed_decimals, resolved by token ADDRESS. A
    -- pre-existing row with enabled=false or wrong decimals would make the ON
    -- CONFLICT DO NOTHING a silent no-op; fail instead. o.chain_id = 8453 and
    -- oracle_type 'chainlink_feed' are asserted so the future Base worker (selects
    -- WHERE chain_id = 8453) actually sees this oracle.
    SELECT COUNT(*) INTO cnt
    FROM oracle o
    JOIN oracle_asset oa ON oa.oracle_id = o.id
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 8453
                AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
    WHERE o.name = 'chainlink_base' AND o.chain_id = 8453 AND o.oracle_type = 'chainlink_feed'
      AND oa.enabled AND oa.feed_decimals = 8
      AND oa.feed_address = '\x7e860098F58bBFC8648a4311b374B1D669a2bc6B'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 enabled chainlink_base Base USDC feed oracle_asset (feed_decimals 8); receipt path would price Base Morpho receipts to NULL, found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260721_140000_register_base_morpho_receipts.sql')
ON CONFLICT (filename) DO NOTHING;
