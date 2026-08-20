-- Current-position tables: the newest row per key for the four append-only
-- histories the /risk-capital reads walk (borrower, borrower_collateral,
-- sparklend_reserve_data, onchain_token_price). Picking the latest row per key
-- out of those hypertables scans mostly-compressed chunks on every request, which
-- no index can fix; these hold the same answer in a few thousand rows. Plain
-- tables (point lookups by PK), not hypertables: one row per key, no history.
--
-- The history tables stay append-only and unchanged. These tables are a derived
-- cache: an AFTER INSERT trigger keeps each one fresh. To rebuild one from
-- history (the only recovery ever needed — e.g. after truncating its history
-- table, which the triggers cannot observe), truncate the cache table and re-run
-- its backfill statement below.
--
-- Trigger semantics, identical for all four:
--   * AFTER INSERT, not BEFORE — trigger_assign_processing_version assigns
--     processing_version in a BEFORE trigger, and the upsert must see the final
--     value.
--   * The upsert only wins when the new row is newer by (block_number,
--     block_version, processing_version), so an out-of-order insert (backfill,
--     retry) can never regress the current row.
--
-- Each trigger is created before its backfill runs: CREATE TRIGGER takes a lock
-- that blocks inserts, so no row can commit in the window between the backfill's
-- snapshot and the trigger going live.

-- ============================================================================
-- borrower_current
-- ============================================================================

CREATE TABLE IF NOT EXISTS borrower_current (
    protocol_id        BIGINT   NOT NULL,
    user_id            BIGINT   NOT NULL,
    token_id           BIGINT   NOT NULL,
    amount             NUMERIC  NOT NULL,
    block_number       BIGINT   NOT NULL,
    block_version      INT      NOT NULL,
    processing_version INT      NOT NULL,
    PRIMARY KEY (protocol_id, user_id, token_id)
);

COMMENT ON TABLE borrower_current IS '[Operational] Newest borrower row per (protocol, user, token). Derived cache of the borrower history; rebuildable from it at any time.';
COMMENT ON COLUMN borrower_current.protocol_id IS 'PK. FK→protocol.id.';
COMMENT ON COLUMN borrower_current.user_id IS 'PK. FK→user.id.';
COMMENT ON COLUMN borrower_current.token_id IS 'PK. FK→token.id. The borrowed (debt) token.';
COMMENT ON COLUMN borrower_current.amount IS 'Derived (copy of borrower.amount). Outstanding debt as a raw on-chain integer in the token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN borrower_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN borrower_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN borrower_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON borrower_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON borrower_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_borrower_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO borrower_current AS cur
        (protocol_id, user_id, token_id, amount, block_number, block_version, processing_version)
    VALUES
        (NEW.protocol_id, NEW.user_id, NEW.token_id, NEW.amount,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (protocol_id, user_id, token_id) DO UPDATE SET
        amount = EXCLUDED.amount,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_borrower_current
    AFTER INSERT ON borrower
    FOR EACH ROW
EXECUTE FUNCTION upsert_borrower_current();

INSERT INTO borrower_current
    (protocol_id, user_id, token_id, amount, block_number, block_version, processing_version)
SELECT DISTINCT ON (b.protocol_id, b.user_id, b.token_id)
    b.protocol_id, b.user_id, b.token_id, b.amount,
    b.block_number, b.block_version, b.processing_version
FROM borrower b
ORDER BY b.protocol_id, b.user_id, b.token_id,
         b.block_number DESC, b.block_version DESC, b.processing_version DESC;

-- ============================================================================
-- borrower_collateral_current
-- ============================================================================

CREATE TABLE IF NOT EXISTS borrower_collateral_current (
    protocol_id        BIGINT   NOT NULL,
    user_id            BIGINT   NOT NULL,
    token_id           BIGINT   NOT NULL,
    amount             NUMERIC  NOT NULL,
    collateral_enabled BOOLEAN  NOT NULL,
    block_number       BIGINT   NOT NULL,
    block_version      INT      NOT NULL,
    processing_version INT      NOT NULL,
    PRIMARY KEY (protocol_id, user_id, token_id)
);

COMMENT ON TABLE borrower_collateral_current IS '[Operational] Newest borrower_collateral row per (protocol, user, token). Derived cache of the borrower_collateral history; rebuildable from it at any time.';
COMMENT ON COLUMN borrower_collateral_current.protocol_id IS 'PK. FK→protocol.id.';
COMMENT ON COLUMN borrower_collateral_current.user_id IS 'PK. FK→user.id.';
COMMENT ON COLUMN borrower_collateral_current.token_id IS 'PK. FK→token.id. The deposited (collateral) token.';
COMMENT ON COLUMN borrower_collateral_current.amount IS 'Derived (copy of borrower_collateral.amount). Deposited balance as a raw on-chain integer in the token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN borrower_collateral_current.collateral_enabled IS 'Derived. Whether the user still has this deposit enabled as collateral, as of the winning row.';
COMMENT ON COLUMN borrower_collateral_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN borrower_collateral_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN borrower_collateral_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON borrower_collateral_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON borrower_collateral_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_borrower_collateral_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO borrower_collateral_current AS cur
        (protocol_id, user_id, token_id, amount, collateral_enabled,
         block_number, block_version, processing_version)
    VALUES
        (NEW.protocol_id, NEW.user_id, NEW.token_id, NEW.amount, NEW.collateral_enabled,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (protocol_id, user_id, token_id) DO UPDATE SET
        amount = EXCLUDED.amount,
        collateral_enabled = EXCLUDED.collateral_enabled,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_borrower_collateral_current
    AFTER INSERT ON borrower_collateral
    FOR EACH ROW
EXECUTE FUNCTION upsert_borrower_collateral_current();

INSERT INTO borrower_collateral_current
    (protocol_id, user_id, token_id, amount, collateral_enabled,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (bc.protocol_id, bc.user_id, bc.token_id)
    bc.protocol_id, bc.user_id, bc.token_id, bc.amount, bc.collateral_enabled,
    bc.block_number, bc.block_version, bc.processing_version
FROM borrower_collateral bc
ORDER BY bc.protocol_id, bc.user_id, bc.token_id,
         bc.block_number DESC, bc.block_version DESC, bc.processing_version DESC;

-- ============================================================================
-- sparklend_reserve_data_current
-- ============================================================================

-- usage_as_collateral_enabled stays nullable, mirroring the history column: a
-- NOT NULL here would abort the insert that fires the trigger.
CREATE TABLE IF NOT EXISTS sparklend_reserve_data_current (
    protocol_id                 BIGINT  NOT NULL,
    token_id                    BIGINT  NOT NULL,
    usage_as_collateral_enabled BOOLEAN,
    block_number                BIGINT  NOT NULL,
    block_version               INT     NOT NULL,
    processing_version          INT     NOT NULL,
    PRIMARY KEY (protocol_id, token_id)
);

COMMENT ON TABLE sparklend_reserve_data_current IS '[Operational] Newest sparklend_reserve_data row per (protocol, token), reduced to the reserve flag the backing reads need. Derived cache of the sparklend_reserve_data history; rebuildable from it at any time.';
COMMENT ON COLUMN sparklend_reserve_data_current.protocol_id IS 'PK. FK→protocol.id.';
COMMENT ON COLUMN sparklend_reserve_data_current.token_id IS 'PK. FK→token.id. The reserve''s token.';
COMMENT ON COLUMN sparklend_reserve_data_current.usage_as_collateral_enabled IS 'Derived. Whether the protocol still accepts this reserve as collateral. NULL when the history row left it unset.';
COMMENT ON COLUMN sparklend_reserve_data_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN sparklend_reserve_data_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN sparklend_reserve_data_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON sparklend_reserve_data_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON sparklend_reserve_data_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_sparklend_reserve_data_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO sparklend_reserve_data_current AS cur
        (protocol_id, token_id, usage_as_collateral_enabled,
         block_number, block_version, processing_version)
    VALUES
        (NEW.protocol_id, NEW.token_id, NEW.usage_as_collateral_enabled,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (protocol_id, token_id) DO UPDATE SET
        usage_as_collateral_enabled = EXCLUDED.usage_as_collateral_enabled,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_sparklend_reserve_data_current
    AFTER INSERT ON sparklend_reserve_data
    FOR EACH ROW
EXECUTE FUNCTION upsert_sparklend_reserve_data_current();

INSERT INTO sparklend_reserve_data_current
    (protocol_id, token_id, usage_as_collateral_enabled,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (srd.protocol_id, srd.token_id)
    srd.protocol_id, srd.token_id, srd.usage_as_collateral_enabled,
    srd.block_number, srd.block_version, srd.processing_version
FROM sparklend_reserve_data srd
ORDER BY srd.protocol_id, srd.token_id,
         srd.block_number DESC, srd.block_version DESC, srd.processing_version DESC;

-- ============================================================================
-- token_price_current
-- ============================================================================

-- oracle_id and block_version are the canonical widths (int8 / int4) here, not the
-- narrower ones onchain_token_price kept from before the convention: the widths this
-- table joins against (oracle.id, protocol_oracle.oracle_id) are BIGINT, and the
-- schema_master register requires canonical widths on a table created after it.
CREATE TABLE IF NOT EXISTS token_price_current (
    protocol_id        BIGINT         NOT NULL,
    token_id           BIGINT         NOT NULL,
    price_usd          NUMERIC(30,18) NOT NULL,
    oracle_id          BIGINT         NOT NULL,
    block_number       BIGINT         NOT NULL,
    block_version      INT            NOT NULL,
    processing_version INT            NOT NULL,
    PRIMARY KEY (protocol_id, token_id)
);

COMMENT ON TABLE token_price_current IS '[Operational] Newest on-chain USD price per (protocol, token), fanned out from onchain_token_price across every protocol bound to the writing oracle. Derived cache of that history; rebuildable from it at any time.';
COMMENT ON COLUMN token_price_current.protocol_id IS 'PK. FK→protocol.id. A protocol bound to oracle_id through protocol_oracle.';
COMMENT ON COLUMN token_price_current.token_id IS 'PK. FK→token.id. The priced token.';
COMMENT ON COLUMN token_price_current.price_usd IS 'Derived (copy of onchain_token_price.price_usd). USD per whole token, already decimals-normalized — not a raw integer and not fixed-point.';
COMMENT ON COLUMN token_price_current.oracle_id IS 'Derived. FK→oracle.id. Oracle that reported the winning price; final tie-break (higher id = later-registered oracle) and the key the read side re-checks against oracle_asset.';
COMMENT ON COLUMN token_price_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN token_price_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN token_price_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON token_price_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON token_price_current TO stl_readwrite;

-- A price row feeds every protocol bound to its oracle, and only while an ENABLED
-- oracle_asset (oracle_id, token_id) mapping exists — mirroring the read queries.
-- Known limitation: the cache reacts to PRICE inserts only, not to configuration
-- changes (disabling/enabling an oracle_asset mapping, adding a protocol_oracle
-- binding). After such a change the read side's enabled re-check keeps stale
-- prices from surfacing, but the affected token can go unpriced — or miss a
-- fallback oracle the old history read would have found — until the next price
-- insert from an enabled oracle. A migration that changes these mappings must
-- refresh the affected token_price_current rows in the same file.
CREATE OR REPLACE FUNCTION upsert_token_price_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO token_price_current AS cur
        (protocol_id, token_id, price_usd, oracle_id,
         block_number, block_version, processing_version)
    -- DISTINCT: protocol_oracle holds one row per binding period, so a protocol
    -- that re-bound the same oracle would otherwise hit the same conflict twice,
    -- which ON CONFLICT DO UPDATE rejects.
    SELECT DISTINCT po.protocol_id, NEW.token_id, NEW.price_usd, NEW.oracle_id,
           NEW.block_number, NEW.block_version, NEW.processing_version
    FROM protocol_oracle po
    WHERE po.oracle_id = NEW.oracle_id
      AND EXISTS (
          SELECT 1 FROM oracle_asset oa
          WHERE oa.oracle_id = NEW.oracle_id
            AND oa.token_id = NEW.token_id
            AND oa.enabled
      )
    ON CONFLICT (protocol_id, token_id) DO UPDATE SET
        price_usd = EXCLUDED.price_usd,
        oracle_id = EXCLUDED.oracle_id,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.oracle_id)
        > (cur.block_number, cur.block_version, cur.processing_version, cur.oracle_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_token_price_current
    AFTER INSERT ON onchain_token_price
    FOR EACH ROW
EXECUTE FUNCTION upsert_token_price_current();

INSERT INTO token_price_current
    (protocol_id, token_id, price_usd, oracle_id,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (po.protocol_id, otp.token_id)
    po.protocol_id, otp.token_id, otp.price_usd, otp.oracle_id,
    otp.block_number, otp.block_version, otp.processing_version
FROM onchain_token_price otp
JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
WHERE EXISTS (
    SELECT 1 FROM oracle_asset oa
    WHERE oa.oracle_id = otp.oracle_id
      AND oa.token_id = otp.token_id
      AND oa.enabled
)
ORDER BY po.protocol_id, otp.token_id,
         otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC, otp.oracle_id DESC;

-- Fresh tables, so the first reads after deploy would otherwise plan on no stats.
ANALYZE borrower_current;
ANALYZE borrower_collateral_current;
ANALYZE sparklend_reserve_data_current;
ANALYZE token_price_current;

INSERT INTO migrations (filename)
VALUES ('20260820_120000_create_current_position_tables.sql')
ON CONFLICT (filename) DO NOTHING;
