-- Register sparkUSDTbc (Spark Blue Chip USDT Vault, Morpho VaultV2) as a
-- morpho_vault + receipt_token pair, ahead of the allocation tracker observing
-- the position (the vault is added to the axis-synome contract in the same
-- change). The 20260709_120000 sparkUSDCbc lesson: registering a receipt token
-- WITHOUT its pricing chain flips the position onto the receipt path and then
-- prices it to NULL — so the pricing prerequisites are asserted below. For
-- mainnet USDT the whole chain already exists: USDT token (20260204_110000),
-- enabled chainlink USDT/USD oracle_asset (asserted by 20260709_120000), and
-- the Morpho Blue -> chainlink protocol_oracle binding (20260709_120000).
-- Nothing is missing, so this file only adds the vault pair.
--
-- Identity verified live 2026-08-25 by ADDRESS
-- (0xb0c424116172B55CbB6dD3136F5989F7959e5B91) via eth.drpc.org:
-- name() = 'Spark Blue Chip USDT Vault', symbol() = 'sparkUSDTbc',
-- decimals() = 18, asset() = USDT (0xdAC17F958D2ee523a2206206994597C13D831ec7),
-- MORPHO() REVERTS and convertToAssets(1e18) = 1012040 (~1.01 USDT)
-- -> Morpho VaultV2 -> vault_version 3 (entity.MorphoVaultV2, the same
-- discriminator as steakUSDC in 20260721_140000). created_at_block 24794487 =
-- the on-chain creation block, bisected via archive eth_getCode (empty at
-- 24794486, code at 24794487); the indexer's LEAST(created_at_block) upserts
-- only converge downward — where the vault was already discovered organically
-- with a later first-seen block (prod), this INSERT ... DO NOTHING no-ops and
-- the later block stays; the allocation tracker takes 24794487 from
-- created_at_blocks.go regardless.
--
-- Everything below resolves the vault STRICTLY by address, never by symbol
-- (labels are not authoritative; the registries key on (chain_id, address)).
-- Live-forward only, no backfill.

-- Mirror discoverAndRegisterVault's morpho_vault upsert (fresh-DB determinism;
-- on a DB where auto-discovery already registered the vault this no-ops).
INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
SELECT 1, p.id, '\xb0c424116172B55CbB6dD3136F5989F7959e5B91'::bytea,
       'Spark Blue Chip USDT Vault', 'sparkUSDTbc', t.id, 3, 24794487
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'Morpho Blue'
  AND t.chain_id = 1 AND t.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
ON CONFLICT (chain_id, address) DO NOTHING;

-- Mirror discoverAndRegisterVault's receipt_token upsert: the row that moves
-- the position onto the receipt path (underlying_value x USDT price).
INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
SELECT 1, p.id, t.id, '\xb0c424116172B55CbB6dD3136F5989F7959e5B91'::bytea,
       'sparkUSDTbc', 24794487, '{}'::jsonb, NOW()
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'Morpho Blue'
  AND t.chain_id = 1 AND t.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- ============================================================================
-- Resolution assertions (precedent: 20260714_120000, 20260721_140000). Both
-- INSERTs resolve FKs by natural key with ON CONFLICT DO NOTHING, so a typoed
-- address or a missing 'Morpho Blue' / USDT registry row would silently insert
-- nothing. The last two assertions guard the pricing prerequisites this
-- registration depends on (created by earlier migrations): without them the
-- receipt path would resolve the row but price it to NULL. Fail the migration
-- loud instead of shipping a silent hole.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    -- morpho_vault: the vault row must resolve with the USDT underlying under
    -- Morpho Blue, with vault_version 3 (guards a V1.1/V2 swap).
    SELECT COUNT(*) INTO cnt
    FROM morpho_vault mv
    JOIN protocol p ON p.id = mv.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = mv.asset_token_id AND t.chain_id = 1
     AND t.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
    WHERE mv.chain_id = 1 AND mv.address = '\xb0c424116172B55CbB6dD3136F5989F7959e5B91'::bytea
      AND mv.vault_version = 3;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'sparkUSDTbc morpho_vault row missing or mis-linked, found %', cnt;
    END IF;

    -- receipt_token: the new row must resolve with the USDT underlying under
    -- Morpho Blue.
    SELECT COUNT(*) INTO cnt
    FROM receipt_token rt
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = rt.underlying_token_id AND t.chain_id = 1
     AND t.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
    WHERE rt.chain_id = 1 AND rt.receipt_token_address = '\xb0c424116172B55CbB6dD3136F5989F7959e5B91'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'sparkUSDTbc receipt_token row missing or mis-linked, found %', cnt;
    END IF;

    -- Pricing prerequisite 1: Morpho Blue must be bound to chainlink (20260709),
    -- the protocol_oracle join the receipt path resolves the USDT price through.
    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Morpho Blue -> chainlink protocol_oracle binding missing';
    END IF;

    -- Pricing prerequisite 2: chainlink must price USDT (enabled oracle_asset,
    -- asserted by 20260709), resolved by USDT ADDRESS (labels are not
    -- authoritative).
    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'chainlink'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 1
     AND t.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
    WHERE oa.enabled;
    IF cnt < 1 THEN
        RAISE EXCEPTION 'no enabled chainlink oracle_asset row for USDT; receipt path would price sparkUSDTbc to NULL';
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260825_120000_register_spark_usdt_bc_receipt.sql')
ON CONFLICT (filename) DO NOTHING;
