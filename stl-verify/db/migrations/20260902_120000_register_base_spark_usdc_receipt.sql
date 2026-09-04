-- Register sparkUSDC (Spark USDC Vault on Base, MetaMorpho V1.1) as a
-- morpho_vault + receipt_token pair, so the allocation tracker's ~$253M Base
-- position (already in allocation_position_current, token id 4461318) resolves
-- through the receipt path (underlying_value x Base USDC price) instead of
-- sitting unpriced on the legacy branch, and the /v1/risk breakdown endpoint
-- stops 404ing on it. The morpho-indexer only runs on mainnet, and the one Base
-- seed (20260721_140000) registered steakUSDC + grove-bbqUSDC but not sparkUSDC
-- (it held no tracked capital when that migration was scoped). The whole Base
-- pricing chain from that file — Base USDC token, Base 'Morpho Blue' protocol,
-- chainlink_base oracle + enabled Base USDC/USD feed, protocol_oracle binding —
-- already exists, so per the sparkUSDCbc lesson (20260709_120000: a receipt
-- without its pricing chain prices to NULL) it is asserted below, not re-created.
-- This file only adds the vault pair; both rows ship together (a receipt row
-- without its morpho_vault row turns the breakdown 404 into a 422).
--
-- Identity verified live 2026-09-02 by ADDRESS
-- (0x7bfa7c4f149e7415b73bdedfe609237e29cbf34a) via eth_call against
-- base-mainnet Alchemy:
-- name() = 'Spark USDC Vault', symbol() = 'sparkUSDC', decimals() = 18,
-- asset() = Base USDC (0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913),
-- MORPHO() = the singleton (0xBBBB..FFCb, does NOT revert) and
-- convertToAssets(1e18) = 1077552 (~1.08 USDC)
-- -> MetaMorpho V1.1 -> vault_version 2 (entity.MorphoVaultV1_1, the same
-- discriminator as grove-bbqUSDC in 20260721_140000). created_at_block
-- 24392934 = the on-chain creation block, bisected via eth_getCode (empty at
-- 24392933, code at 24392934); cross-checked against Morpho's public API
-- (vaultByAddress chainId 8453: creationBlockNumber 24392934, symbol
-- 'sparkUSDC', asset Base USDC). The indexer's LEAST(created_at_block) upserts
-- only converge downward — no Base indexer runs today, but if one ever
-- discovers the vault first with a later first-seen block, this
-- INSERT ... DO NOTHING no-ops and the later block stays.
--
-- Everything below resolves the vault STRICTLY by address, never by symbol
-- (labels are not authoritative; the registries key on (chain_id, address)).
-- Live-forward only, no backfill: breakdown items stay empty until Base Morpho
-- market/state ingestion exists (follow-up epic; no Base vault has items today).

-- Mirror discoverAndRegisterVault's morpho_vault upsert (fresh-DB determinism;
-- on a DB where the vault is already registered this no-ops).
INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
SELECT 8453, p.id, '\x7bfa7c4f149e7415b73bdedfe609237e29cbf34a'::bytea,
       'Spark USDC Vault', 'sparkUSDC', t.id, 2, 24392934
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.name = 'Morpho Blue'
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, address) DO NOTHING;

-- Mirror discoverAndRegisterVault's receipt_token upsert: the row that moves
-- the position onto the receipt path (underlying_value x Base USDC price).
INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
SELECT 8453, p.id, t.id, '\x7bfa7c4f149e7415b73bdedfe609237e29cbf34a'::bytea,
       'sparkUSDC', 24392934, '{}'::jsonb, NOW()
FROM protocol p, token t
WHERE p.chain_id = 8453 AND p.name = 'Morpho Blue'
  AND t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- ============================================================================
-- Resolution assertions (precedent: 20260721_140000, 20260825_120000). Both
-- INSERTs resolve FKs by natural key with ON CONFLICT DO NOTHING, so a typoed
-- address or a missing 'Morpho Blue' / Base USDC registry row would silently
-- insert nothing. The last two assertions guard the pricing prerequisites this
-- registration depends on (created by 20260721_140000): without them the
-- receipt path would resolve the row but price it to NULL. Fail the migration
-- loud instead of shipping a silent hole.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    -- morpho_vault: the vault row must resolve with the Base USDC underlying
    -- under Base Morpho Blue, with vault_version 2 (guards a V1.1/V2 swap).
    SELECT COUNT(*) INTO cnt
    FROM morpho_vault mv
    JOIN protocol p ON p.id = mv.protocol_id AND p.chain_id = 8453 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = mv.asset_token_id AND t.chain_id = 8453
     AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
    WHERE mv.chain_id = 8453 AND mv.address = '\x7bfa7c4f149e7415b73bdedfe609237e29cbf34a'::bytea
      AND mv.vault_version = 2;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'sparkUSDC morpho_vault row missing or mis-linked, found %', cnt;
    END IF;

    -- receipt_token: the new row must resolve with the Base USDC underlying
    -- under Base Morpho Blue.
    SELECT COUNT(*) INTO cnt
    FROM receipt_token rt
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 8453 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = rt.underlying_token_id AND t.chain_id = 8453
     AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
    WHERE rt.chain_id = 8453 AND rt.receipt_token_address = '\x7bfa7c4f149e7415b73bdedfe609237e29cbf34a'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'sparkUSDC receipt_token row missing or mis-linked, found %', cnt;
    END IF;

    -- Pricing prerequisite 1: Base Morpho Blue must be bound to chainlink_base
    -- (20260721_140000), the protocol_oracle join the receipt path resolves the
    -- Base USDC price through.
    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 8453 AND p.name = 'Morpho Blue'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink_base';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Base Morpho Blue -> chainlink_base protocol_oracle binding missing';
    END IF;

    -- Pricing prerequisite 2: chainlink_base must price Base USDC (enabled
    -- oracle_asset on an enabled oracle, created by 20260721_140000), resolved
    -- by Base USDC ADDRESS (labels are not authoritative). oracle_asset is
    -- append-on-change since 20260901_120000, so only the LATEST version per
    -- (oracle_id, token_id, feed_key) counts — an unqualified read would match
    -- a retired version as readily as the live one.
    SELECT COUNT(*) INTO cnt
    FROM (
        SELECT DISTINCT ON (oracle_id, token_id, feed_key) *
        FROM oracle_asset
        ORDER BY oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC
    ) oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'chainlink_base' AND o.enabled
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 8453
     AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
    WHERE oa.enabled;
    IF cnt < 1 THEN
        RAISE EXCEPTION 'no enabled chainlink_base oracle_asset version for Base USDC; receipt path would price sparkUSDC to NULL';
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260902_120000_register_base_spark_usdc_receipt.sql')
ON CONFLICT (filename) DO NOTHING;
