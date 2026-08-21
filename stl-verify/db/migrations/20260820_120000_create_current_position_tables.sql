-- Current-position tables: the newest row per key for the four append-only
-- histories the /risk-capital reads walk (borrower, borrower_collateral,
-- sparklend_reserve_data, onchain_token_price). Picking the latest row per key
-- out of those hypertables scans mostly-compressed chunks on every request, which
-- no index can fix. Plain tables (point lookups by PK), not hypertables: one row
-- per key, no history.
--
-- Size: one row per key ever touched, not per live position. On staging today
-- that is ~95k rows for borrower_current and ~326k for borrower_collateral_current
-- (Aave V3 alone ~164k), and a few hundred for the two config-shaped caches. Fully
-- repaid / withdrawn positions are deliberately retained at amount = 0 (~21% of
-- borrower keys) rather than pruned: the newest row for a key IS the zero, and
-- deleting it would make the cache disagree with history. The debt read drops them
-- with `amount > 0`; a zeroed collateral row contributes nothing in USD. Row count
-- therefore grows monotonically with keys touched, not with live positions.
--
-- Relationship to the strict append-only rule (see AGENTS.md in this directory):
-- these four tables are OUTSIDE it, in the same way the transformed layer is
-- (20260706_140000_create_transformed_bucket1.sql). The rule governs history —
-- ingest tables whose rows are observations, where "current" must stay a query so
-- reorgs roll back and replays are order-independent. These tables are a derived
-- cache of exactly that query: the histories they read stay append-only and
-- untouched, nothing here is a source of truth, and every row is reproducible by
-- re-running the backfill below. Consequently they DO take
-- `ON CONFLICT DO UPDATE` (the trigger) and hold UPDATE grant, and they must
-- never be read as a history — there is no "as of block N" answer in them.
--
-- Rebuild: re-run the backfill statement for the table. The backfills carry the
-- same newer-wins ON CONFLICT guard as the triggers, so they are idempotent and
-- safe to run against a live cache with ingest running — no truncate, no window
-- where the cache is half-populated. A truncate is needed only if rows must be
-- REMOVED (history itself was truncated, which the triggers cannot observe); that
-- must run as the table owner, since stl_readwrite has no TRUNCATE grant.
--
-- Trigger semantics, identical for all four:
--   * AFTER INSERT, not BEFORE — trigger_assign_processing_version assigns
--     processing_version in a BEFORE trigger, and the upsert must see the final
--     value.
--   * The upsert only wins when the new row is newer by (block_number,
--     block_version, processing_version), so an out-of-order insert (backfill,
--     retry) can never regress the current row.
--
-- Because both the triggers and the backfills are newer-wins upserts on the same
-- key, their order does not matter and neither does interleaving with live
-- inserts: whichever runs second is a guarded no-op. That much does not depend on
-- the migrator wrapping this file in one transaction. The two SET LOCAL statements
-- below DO: outside a transaction block SET LOCAL only warns and changes nothing
-- (verified — lock_timeout stays 0), which would silently restore both hazards
-- they exist to prevent. This file must therefore never be marked
-- `-- migrate: no-transaction`.

-- Fail fast rather than convoy ingestion. Each CREATE TRIGGER below takes
-- SHARE ROW EXCLUSIVE on its (busy) history table, held to commit and propagated
-- to that hypertable's chunks, and the migrator runs this whole file in one
-- transaction — so without a bound this waits out every in-flight writer while
-- queueing all new INSERTs behind it. Same rationale and value as
-- 20260706_140000_create_transformed_bucket1.sql; re-run in a quieter window.
SET LOCAL lock_timeout = '10s';

-- The backfills below must see S3-tiered history. The GUC defaults to off, so
-- without this a backfill silently computes "newest per key" over local chunks
-- only, and a key whose newest row has been tiered gets a stale row or none at
-- all — onchain_token_price is already tiering. Same reasoning as
-- cmd/backfillers/transform-bootstrap (which treats failure to set it as fatal).
SET LOCAL timescaledb.enable_tiered_reads = 'on';

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
GRANT SELECT, INSERT, UPDATE ON borrower_current TO stl_readwrite;

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
         b.block_number DESC, b.block_version DESC, b.processing_version DESC
ON CONFLICT (protocol_id, user_id, token_id) DO UPDATE SET
    amount = EXCLUDED.amount,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (borrower_current.block_number, borrower_current.block_version, borrower_current.processing_version);

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
GRANT SELECT, INSERT, UPDATE ON borrower_collateral_current TO stl_readwrite;

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
         bc.block_number DESC, bc.block_version DESC, bc.processing_version DESC
ON CONFLICT (protocol_id, user_id, token_id) DO UPDATE SET
    amount = EXCLUDED.amount,
    collateral_enabled = EXCLUDED.collateral_enabled,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (borrower_collateral_current.block_number, borrower_collateral_current.block_version, borrower_collateral_current.processing_version);

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
GRANT SELECT, INSERT, UPDATE ON sparklend_reserve_data_current TO stl_readwrite;

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
         srd.block_number DESC, srd.block_version DESC, srd.processing_version DESC
ON CONFLICT (protocol_id, token_id) DO UPDATE SET
    usage_as_collateral_enabled = EXCLUDED.usage_as_collateral_enabled,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (sparklend_reserve_data_current.block_number, sparklend_reserve_data_current.block_version, sparklend_reserve_data_current.processing_version);

-- ============================================================================
-- token_price_current
-- ============================================================================

-- Keyed by (oracle_id, token_id) — onchain_token_price's own natural key — and NOT
-- by protocol. Which protocols a price serves, and whether its oracle_asset mapping
-- is still enabled, are configuration questions; they are answered at READ time,
-- against protocol_oracle + oracle_asset (a few hundred rows), exactly as the reads
-- did before this cache existed. Deriving them at write time instead would bake
-- config into the cache and cost three things:
--   * the trigger would fan one price row out across every bound protocol, taking
--     several row locks per insert in plan-dependent order — two concurrent price
--     writers (the live worker beside the oracle-pricing backfill, or a rolling
--     deploy) then deadlock;
--   * only one row per (protocol, token) would survive, so disabling one oracle's
--     mapping could no longer fall back to the protocol's next enabled oracle. The
--     collateral would silently leave both sides of the backing ratio instead;
--   * a config change (disable/enable a mapping, add a protocol_oracle binding)
--     would leave the cache wrong until that feed's next price insert, which
--     suppressAsUnchanged can defer indefinitely — staging has feeds untouched for
--     months.
-- Keying by the history table's own key makes all three impossible rather than
-- something to detect and reconcile.
--
-- Note this table caches prices for DISABLED mappings too: the write path has no
-- config predicate at all. That is deliberate — it is what lets a re-enable, a new
-- binding, or a fallback take effect on the next read instead of the next price.
--
-- oracle_id and block_version are the canonical widths (int8 / int4) here, not the
-- narrower ones onchain_token_price kept from before the convention: the widths this
-- table joins against (oracle.id, protocol_oracle.oracle_id) are BIGINT, and the
-- schema_master register requires canonical widths on a table created after it.
CREATE TABLE IF NOT EXISTS token_price_current (
    oracle_id          BIGINT         NOT NULL,
    token_id           BIGINT         NOT NULL,
    price_usd          NUMERIC(30,18) NOT NULL,
    block_number       BIGINT         NOT NULL,
    block_version      INT            NOT NULL,
    processing_version INT            NOT NULL,
    PRIMARY KEY (oracle_id, token_id)
);

COMMENT ON TABLE token_price_current IS '[Operational] Newest on-chain USD price per (oracle, token). Derived cache of onchain_token_price; rebuildable from it at any time. Says nothing about which protocol uses a price or whether the mapping is enabled — the reads resolve that against protocol_oracle/oracle_asset, so rows for disabled mappings are expected.';
COMMENT ON COLUMN token_price_current.oracle_id IS 'PK. FK→oracle.id. Oracle that reported the price.';
COMMENT ON COLUMN token_price_current.token_id IS 'PK. FK→token.id. The priced token.';
COMMENT ON COLUMN token_price_current.price_usd IS 'Derived (copy of onchain_token_price.price_usd). USD per whole token, already decimals-normalized — not a raw integer and not fixed-point.';
COMMENT ON COLUMN token_price_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN token_price_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN token_price_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON token_price_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON token_price_current TO stl_readwrite;

-- One row in, at most one row touched: the same trivial newer-wins upsert as the
-- three position triggers. Unlike them there is no cross-oracle tie-break here,
-- because oracle_id is part of the key — competing oracles hold separate rows and
-- the read picks between them (ORDER BY … oracle_id DESC), which is where that
-- tie-break lived before this cache existed.
CREATE OR REPLACE FUNCTION upsert_token_price_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO token_price_current AS cur
        (oracle_id, token_id, price_usd,
         block_number, block_version, processing_version)
    VALUES
        (NEW.oracle_id, NEW.token_id, NEW.price_usd,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (oracle_id, token_id) DO UPDATE SET
        price_usd = EXCLUDED.price_usd,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_token_price_current
    AFTER INSERT ON onchain_token_price
    FOR EACH ROW
EXECUTE FUNCTION upsert_token_price_current();

-- DISTINCT ON over the history table's own key: one ordered pass matching the
-- columnstore segmentby/orderby, no join to config and no fan-out.
INSERT INTO token_price_current
    (oracle_id, token_id, price_usd,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (otp.oracle_id, otp.token_id)
    otp.oracle_id, otp.token_id, otp.price_usd,
    otp.block_number, otp.block_version, otp.processing_version
FROM onchain_token_price otp
ORDER BY otp.oracle_id, otp.token_id,
         otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
ON CONFLICT (oracle_id, token_id) DO UPDATE SET
    price_usd = EXCLUDED.price_usd,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (token_price_current.block_number, token_price_current.block_version, token_price_current.processing_version);

-- Fresh tables, so the first reads after deploy would otherwise plan on no stats.
ANALYZE borrower_current;
ANALYZE borrower_collateral_current;
ANALYZE sparklend_reserve_data_current;
ANALYZE token_price_current;

INSERT INTO migrations (filename)
VALUES ('20260820_120000_create_current_position_tables.sql')
ON CONFLICT (filename) DO NOTHING;
