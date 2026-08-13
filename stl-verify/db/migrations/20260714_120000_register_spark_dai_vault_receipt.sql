-- Register the Spark DAI Vault (MetaMorpho v1.1) as a receipt token so its
-- ~$678 position (proxy 0x1601843c…, 380.33 shares) stops resolving to NULL
-- amount_usd in the allocations API (ER calculation). This is the last
-- remaining NULL amount_usd row in the ER allocation set on Ethereum mainnet
-- (chain_id = 1) after 20260713_150000 priced the dust rows.
--
-- Root cause: the vault is ALREADY in morpho_vault (id 244, vault_version 2)
-- via the morpho_indexer's auto-discovery, but it has NO receipt_token row, so
-- the position falls to the direct-holdings path where a vault share has no
-- own oracle price -> NULL. The allocation tracker already writes
-- underlying_value (~678 DAI) and underlying_token_id (DAI) for it, so
-- adding the receipt_token row moves it onto the receipt path, which prices it
-- as underlying_value x DAI via the Morpho Blue -> chainlink protocol_oracle
-- binding (20260709) and the enabled chainlink DAI oracle_asset (20260212).
--
-- Identity re-verified live via cast 2026-07-14 by ADDRESS
-- (0x73e65DBD630f90604062f6E02fAb9138e713edD9): symbol() = 'spDAI',
-- name() = 'Spark DAI Vault', decimals() = 18, asset() = DAI
-- (0x6B175474E89094C44Da98b954EedeAC495271d0F), MORPHO() = the Morpho Blue
-- singleton (0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb),
-- convertToAssets(380.33e18) = 678.18 DAI (matches the tracker's
-- underlying_value), and skimRecipient() resolves (the V1.1 discriminator).
-- Vault bytecode is present on chain from block 19441174 (archive eth_getCode:
-- empty at 19441173, code at 19441174), well before the discovery block below.
--
-- SYMBOL-TWIN WARNING (the 20260507 impostor lesson): a SECOND mainnet token is
-- also named 'spDAI' — the SparkLend aToken 0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B
-- (already a receipt_token, id 723). token has no uniqueness on symbol, only on
-- (chain_id, address), so a symbol JOIN would match both. Everything below
-- resolves the vault STRICTLY by its 0x73e65DBD… address; the twin is a
-- different address and is untouched.
--
-- Mirrors the sparkUSDS registration in 20260713_150000 and the sparkUSDCbc
-- registration in 20260709_120000 exactly: the same morpho_vault +
-- receipt_token pair that morpho_indexer's discoverAndRegisterVault upserts. On
-- a live DB the morpho_vault statement no-ops (id 244 already exists); the
-- receipt_token statement is the missing insert this migration exists to make,
-- and only re-no-ops if auto-discovery ever re-fires that upsert later.
-- vault_version 2 = MetaMorpho V1.1
-- (entity.MorphoVaultV1_1). created_at_block 19540209 = the discovery block on
-- the live morpho_vault id 244 row (NOT the on-chain creation block 19441174):
-- using it verbatim makes the indexer's LEAST(created_at_block) receipt upsert
-- and its DO UPDATE vault upsert both no-op.
--
-- Live-forward only: no backfill. The receipt path values the position at the
-- latest chainlink DAI price the moment this row exists; no worker restart or
-- reprocess is required (the pricing rows already exist and refresh live).

-- Mirror discoverAndRegisterVault's morpho_vault upsert (fresh-DB determinism;
-- the live row already exists, so this no-ops there).
INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
SELECT 1, p.id, '\x73e65DBD630f90604062f6E02fAb9138e713edD9'::bytea,
       'Spark DAI Vault', 'spDAI', t.id, 2, 19540209
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'Morpho Blue'
  AND t.chain_id = 1 AND t.address = '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
ON CONFLICT (chain_id, address) DO NOTHING;

-- The missing piece: mirror discoverAndRegisterVault's receipt_token upsert.
INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
SELECT 1, p.id, t.id, '\x73e65DBD630f90604062f6E02fAb9138e713edD9'::bytea,
       'spDAI', 19540209, '{}'::jsonb, NOW()
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'Morpho Blue'
  AND t.chain_id = 1 AND t.address = '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- ============================================================================
-- Resolution assertions (precedent: 20260713_150000, 20260709_120000). Both
-- INSERTs resolve FKs by natural key with ON CONFLICT DO NOTHING, so a typoed
-- address or a missing 'Morpho Blue' / DAI registry row would silently insert
-- nothing. The last two assertions guard the pricing prerequisites this
-- registration depends on (created by earlier migrations): without them the
-- receipt path would resolve the row but price it to NULL. Fail the migration
-- loud instead of shipping a silent hole.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    -- morpho_vault: the vault row must resolve with the DAI underlying under
    -- Morpho Blue (resolved by the vault ADDRESS, not the twin's symbol).
    SELECT COUNT(*) INTO cnt
    FROM morpho_vault mv
    JOIN protocol p ON p.id = mv.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = mv.asset_token_id
     AND t.address = '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
    WHERE mv.chain_id = 1 AND mv.address = '\x73e65DBD630f90604062f6E02fAb9138e713edD9'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'Spark DAI Vault morpho_vault row missing or mis-linked, found %', cnt;
    END IF;

    -- receipt_token: the new row must resolve with the DAI underlying under
    -- Morpho Blue. This is the row that moves the position onto the receipt path.
    SELECT COUNT(*) INTO cnt
    FROM receipt_token rt
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = rt.underlying_token_id
     AND t.address = '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
    WHERE rt.chain_id = 1 AND rt.receipt_token_address = '\x73e65DBD630f90604062f6E02fAb9138e713edD9'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'Spark DAI Vault receipt_token row missing or mis-linked, found %', cnt;
    END IF;

    -- Pricing prerequisite 1: Morpho Blue must be bound to chainlink (20260709),
    -- the protocol_oracle join the receipt path resolves the DAI price through.
    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Morpho Blue -> chainlink protocol_oracle binding missing';
    END IF;

    -- Pricing prerequisite 2: chainlink must price DAI (enabled oracle_asset,
    -- 20260212), resolved by DAI ADDRESS (labels are not authoritative).
    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'chainlink'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 1
     AND t.address = '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
    WHERE oa.enabled;
    IF cnt < 1 THEN
        RAISE EXCEPTION 'no enabled chainlink oracle_asset row for DAI; receipt path would price Spark DAI Vault to NULL';
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260714_120000_register_spark_dai_vault_receipt.sql')
ON CONFLICT (filename) DO NOTHING;
