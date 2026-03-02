-- Add protocol_asset table as the universal asset indirection layer.
--
-- Every borrower/borrower_collateral row references protocol_asset instead of token
-- directly. For EVM-based protocols (SparkLend, Aave) each protocol_asset row also
-- carries a token_id link. For non-EVM or symbol-only assets (Maple collateral like
-- BTC, XRP) token_id is left NULL until it can be resolved.
--
-- asset_key is the stable identifier per protocol:
--   - EVM assets: token contract address as lowercase hex (e.g. "0x6b175474e...")
--   - Non-EVM / symbol-only assets: symbol string (e.g. "BTC", "USDC")

-- ─── Step 1: Create protocol_asset ──────────────────────────────────────────

CREATE TABLE protocol_asset (
    id          BIGSERIAL PRIMARY KEY,
    protocol_id BIGINT NOT NULL REFERENCES protocol(id),
    asset_key   TEXT   NOT NULL,   -- stable per-protocol identifier (see above)
    symbol      TEXT   NOT NULL,
    decimals    INT    NOT NULL,
    chain_id    BIGINT,            -- NULL for non-EVM assets
    address     BYTEA,             -- NULL for non-EVM assets
    token_id    BIGINT REFERENCES token(id), -- NULL until linked to a canonical token
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(protocol_id, asset_key)
);

CREATE INDEX idx_protocol_asset_protocol_id ON protocol_asset(protocol_id);
CREATE INDEX idx_protocol_asset_token_id    ON protocol_asset(token_id) WHERE token_id IS NOT NULL;

-- ─── Step 2: Seed SparkLend protocol_asset rows ──────────────────────────────
-- One row per token that SparkLend actually uses, keyed by lowercase hex address.

INSERT INTO protocol_asset (protocol_id, asset_key, symbol, decimals, chain_id, address, token_id)
SELECT
    p.id                                                   AS protocol_id,
    encode(t.address, 'hex')                               AS asset_key,
    t.symbol                                               AS symbol,
    t.decimals                                             AS decimals,
    t.chain_id                                             AS chain_id,
    t.address                                              AS address,
    t.id                                                   AS token_id
FROM token t
JOIN protocol p ON p.name = 'SparkLend' AND p.chain_id = t.chain_id
WHERE t.chain_id = 1
ON CONFLICT (protocol_id, asset_key) DO NOTHING;

-- ─── Step 3: Seed Aave V2 protocol_asset rows ────────────────────────────────

INSERT INTO protocol_asset (protocol_id, asset_key, symbol, decimals, chain_id, address, token_id)
SELECT
    p.id                                                   AS protocol_id,
    encode(t.address, 'hex')                               AS asset_key,
    t.symbol                                               AS symbol,
    t.decimals                                             AS decimals,
    t.chain_id                                             AS chain_id,
    t.address                                              AS address,
    t.id                                                   AS token_id
FROM token t
JOIN protocol p ON p.name = 'Aave V2' AND p.chain_id = t.chain_id
WHERE t.chain_id = 1
ON CONFLICT (protocol_id, asset_key) DO NOTHING;

-- ─── Step 4: Seed Aave V3 protocol_asset rows ────────────────────────────────

INSERT INTO protocol_asset (protocol_id, asset_key, symbol, decimals, chain_id, address, token_id)
SELECT
    p.id                                                   AS protocol_id,
    encode(t.address, 'hex')                               AS asset_key,
    t.symbol                                               AS symbol,
    t.decimals                                             AS decimals,
    t.chain_id                                             AS chain_id,
    t.address                                              AS address,
    t.id                                                   AS token_id
FROM token t
JOIN protocol p ON p.name = 'Aave V3' AND p.chain_id = t.chain_id
WHERE t.chain_id = 1
ON CONFLICT (protocol_id, asset_key) DO NOTHING;

-- ─── Step 5: Seed Aave V3 Lido protocol_asset rows ──────────────────────────

INSERT INTO protocol_asset (protocol_id, asset_key, symbol, decimals, chain_id, address, token_id)
SELECT
    p.id                                                   AS protocol_id,
    encode(t.address, 'hex')                               AS asset_key,
    t.symbol                                               AS symbol,
    t.decimals                                             AS decimals,
    t.chain_id                                             AS chain_id,
    t.address                                              AS address,
    t.id                                                   AS token_id
FROM token t
JOIN protocol p ON p.name = 'Aave V3 Lido' AND p.chain_id = t.chain_id
WHERE t.chain_id = 1
ON CONFLICT (protocol_id, asset_key) DO NOTHING;

-- ─── Step 6: Seed Aave V3 RWA protocol_asset rows ───────────────────────────

INSERT INTO protocol_asset (protocol_id, asset_key, symbol, decimals, chain_id, address, token_id)
SELECT
    p.id                                                   AS protocol_id,
    encode(t.address, 'hex')                               AS asset_key,
    t.symbol                                               AS symbol,
    t.decimals                                             AS decimals,
    t.chain_id                                             AS chain_id,
    t.address                                              AS address,
    t.id                                                   AS token_id
FROM token t
JOIN protocol p ON p.name = 'Aave V3 RWA' AND p.chain_id = t.chain_id
WHERE t.chain_id = 1
ON CONFLICT (protocol_id, asset_key) DO NOTHING;

-- ─── Step 7: Seed Maple Finance protocol_asset rows ─────────────────────────
-- Maple assets are symbol-only (no on-chain address). Pool assets that happen to
-- be EVM tokens (USDC, WBTC, etc.) share the same symbol as the token table entry.
-- Collateral assets include native non-EVM assets (BTC, XRP, SOL, etc.).
--
-- asset_key = symbol (upper-case), token_id linked where a matching EVM token exists.

INSERT INTO protocol_asset (protocol_id, asset_key, symbol, decimals, chain_id, address, token_id)
SELECT
    p.id        AS protocol_id,
    assets.symbol AS asset_key,
    assets.symbol AS symbol,
    assets.decimals,
    NULL::BIGINT  AS chain_id,
    NULL::BYTEA   AS address,
    t.id          AS token_id   -- NULL when no EVM token matches
FROM protocol p
CROSS JOIN (VALUES
    -- Pool lending assets (EVM, match existing token rows by symbol)
    ('USDC',  6),
    ('WBTC',  8),
    ('WETH',  18),
    ('USDT',  6),
    ('DAI',   18),
    -- Collateral assets seen in Maple loans (non-EVM unless noted)
    ('BTC',   8),
    ('XRP',   6),
    ('SOL',   9),
    ('weETH', 18),
    ('PYUSD', 6),
    ('cbBTC', 8),
    ('tBTC',  18),
    ('rsETH', 18),
    ('ezETH', 18)
) AS assets(symbol, decimals)
LEFT JOIN token t ON t.symbol = assets.symbol AND t.chain_id = 1
WHERE p.name = 'Maple Finance'
ON CONFLICT (protocol_id, asset_key) DO NOTHING;

-- ─── Step 8: Add protocol_asset_id to borrower (nullable initially for backfill) ──

ALTER TABLE borrower
    ADD COLUMN protocol_asset_id BIGINT REFERENCES protocol_asset(id);

-- ─── Step 9: Backfill borrower.protocol_asset_id ────────────────────────────
-- Join via token_id since all existing rows are EVM (SparkLend/Aave).

UPDATE borrower b
SET protocol_asset_id = pa.id
FROM protocol_asset pa
WHERE pa.token_id = b.token_id
  AND pa.protocol_id = b.protocol_id;

-- Safety check: abort if any row was not backfilled.
DO $$
DECLARE
    missing BIGINT;
BEGIN
    SELECT COUNT(*) INTO missing
    FROM borrower
    WHERE protocol_asset_id IS NULL;

    IF missing > 0 THEN
        RAISE EXCEPTION 'protocol_asset backfill incomplete: % borrower rows still have NULL protocol_asset_id', missing;
    END IF;
END $$;

-- ─── Step 10: Promote borrower.protocol_asset_id to NOT NULL, drop old FK ───

ALTER TABLE borrower
    ALTER COLUMN protocol_asset_id SET NOT NULL,
    DROP COLUMN token_id;

ALTER TABLE borrower
    ADD CONSTRAINT borrower_unique
        UNIQUE (user_id, protocol_id, protocol_asset_id, block_number, block_version);

DROP INDEX IF EXISTS idx_borrower_token;
CREATE INDEX idx_borrower_protocol_asset ON borrower(protocol_asset_id);

-- ─── Step 11: Add protocol_asset_id to borrower_collateral ──────────────────

ALTER TABLE borrower_collateral
    ADD COLUMN protocol_asset_id BIGINT REFERENCES protocol_asset(id);

-- ─── Step 12: Backfill borrower_collateral.protocol_asset_id ────────────────

UPDATE borrower_collateral bc
SET protocol_asset_id = pa.id
FROM protocol_asset pa
WHERE pa.token_id = bc.token_id
  AND pa.protocol_id = bc.protocol_id;

-- Safety check.
DO $$
DECLARE
    missing BIGINT;
BEGIN
    SELECT COUNT(*) INTO missing
    FROM borrower_collateral
    WHERE protocol_asset_id IS NULL;

    IF missing > 0 THEN
        RAISE EXCEPTION 'protocol_asset backfill incomplete: % borrower_collateral rows still have NULL protocol_asset_id', missing;
    END IF;
END $$;

-- ─── Step 13: Promote, drop old FK, rebuild constraint/indexes ───────────────

ALTER TABLE borrower_collateral
    ALTER COLUMN protocol_asset_id SET NOT NULL,
    DROP COLUMN token_id;

ALTER TABLE borrower_collateral
    ADD CONSTRAINT borrower_collateral_unique
        UNIQUE (user_id, protocol_id, protocol_asset_id, block_number, block_version);

DROP INDEX IF EXISTS idx_borrower_collateral_token;
CREATE INDEX idx_borrower_collateral_protocol_asset ON borrower_collateral(protocol_asset_id);

-- ─── Step 14: Make tx_hash nullable on both tables ───────────────────────────
-- Maple snapshots have no transaction hash; the application stores nil/NULL.

ALTER TABLE borrower           ALTER COLUMN tx_hash DROP NOT NULL;
ALTER TABLE borrower_collateral ALTER COLUMN tx_hash DROP NOT NULL;

-- ─── Grant permissions ───────────────────────────────────────────────────────

GRANT SELECT ON protocol_asset TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON protocol_asset TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE protocol_asset_id_seq TO stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260222_100000_add_protocol_asset.sql')
ON CONFLICT (filename) DO NOTHING;
