-- Current-state tables for the three Morpho histories the backed-breakdown read
-- walks: morpho_vault_state, morpho_market_state and morpho_market_position.
-- Same recipe, rationale and guarantees as the four tables in
-- 20260820_120000_create_current_position_tables.sql — read that file's header for
-- the design (plain tables, why these sit outside the strict append-only rule, how
-- to rebuild, and why the triggers are AFTER INSERT). Only what differs is
-- restated here.
--
-- Why these three: the Morpho breakdown reconstructed "newest row per key" over
-- 180-chunk hypertables three times per execution, twice inside a LATERAL that
-- repeats per market, and ran about three times per /risk-capital request. The
-- cost is the planner building ~1,060 chunk-scan nodes, not the rows — the answer
-- is one row per key each time.
--
-- What each carries: only the columns the read needs, as
-- sparklend_reserve_data_current does. Utilization stays a read-time expression
-- over the two market totals rather than a stored column, so the cache holds
-- copies of history columns and nothing derived.
--
-- processing_version is NOT NULL on all three histories (it is part of every one
-- of their primary keys, see 20260410_130000_alter_constraints.sql), so unlike
-- sparklend_reserve_data_current these need no COALESCE sentinel and the
-- newer-wins comparison is total as written.
--
-- Lock ordering: the Morpho indexer writes these histories one row per event
-- rather than in sorted batches, so there is no repository-side key sort to lean
-- on the way borrower_current does. A concurrent writer on the same keys in the
-- opposite order (live worker beside morpho-vault-backfill) can therefore lose a
-- deadlock. That aborts the transaction and the SQS message is retried; it cannot
-- leave the cache disagreeing with history, because every path to a cache row is
-- the same guarded upsert.

-- Fail fast rather than convoy ingestion: each CREATE TRIGGER takes SHARE ROW
-- EXCLUSIVE on its busy history table for the rest of this transaction. Same
-- rationale and value as the migration above; re-run in a quieter window.
SET LOCAL lock_timeout = '10s';

-- The backfills must see S3-tiered history, or a key whose newest row has already
-- been tiered silently caches a stale row or none. All three tables carry a
-- 1-year tiering policy (20260224_100000_create_morpho_tables.sql). Set
-- explicitly rather than inherited, in either direction.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

-- ============================================================================
-- morpho_vault_state_current
-- ============================================================================

CREATE TABLE IF NOT EXISTS morpho_vault_state_current (
    morpho_vault_id    BIGINT  NOT NULL,
    total_assets       NUMERIC NOT NULL,
    block_number       BIGINT  NOT NULL,
    block_version      INT     NOT NULL,
    processing_version INT     NOT NULL,
    PRIMARY KEY (morpho_vault_id)
);

COMMENT ON TABLE morpho_vault_state_current IS '[Operational] Newest morpho_vault_state row per vault, reduced to the total the backing reads need. Derived cache of the morpho_vault_state history; rebuildable from it at any time.';
COMMENT ON COLUMN morpho_vault_state_current.morpho_vault_id IS 'PK. FK→morpho_vault.id.';
COMMENT ON COLUMN morpho_vault_state_current.total_assets IS 'Derived (copy of morpho_vault_state.total_assets). Assets under management as a raw on-chain integer in the vault asset token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN morpho_vault_state_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_vault_state_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_vault_state_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON morpho_vault_state_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON morpho_vault_state_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_morpho_vault_state_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO morpho_vault_state_current AS cur
        (morpho_vault_id, total_assets, block_number, block_version, processing_version)
    VALUES
        (NEW.morpho_vault_id, NEW.total_assets,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (morpho_vault_id) DO UPDATE SET
        total_assets = EXCLUDED.total_assets,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_morpho_vault_state_current
    AFTER INSERT ON morpho_vault_state
    FOR EACH ROW
EXECUTE FUNCTION upsert_morpho_vault_state_current();

INSERT INTO morpho_vault_state_current
    (morpho_vault_id, total_assets, block_number, block_version, processing_version)
SELECT DISTINCT ON (vs.morpho_vault_id)
    vs.morpho_vault_id, vs.total_assets,
    vs.block_number, vs.block_version, vs.processing_version
FROM morpho_vault_state vs
ORDER BY vs.morpho_vault_id,
         vs.block_number DESC, vs.block_version DESC, vs.processing_version DESC
ON CONFLICT (morpho_vault_id) DO UPDATE SET
    total_assets = EXCLUDED.total_assets,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (morpho_vault_state_current.block_number, morpho_vault_state_current.block_version,
       morpho_vault_state_current.processing_version);

-- ============================================================================
-- morpho_market_state_current
-- ============================================================================

CREATE TABLE IF NOT EXISTS morpho_market_state_current (
    morpho_market_id    BIGINT  NOT NULL,
    total_supply_assets NUMERIC NOT NULL,
    total_borrow_assets NUMERIC NOT NULL,
    block_number        BIGINT  NOT NULL,
    block_version       INT     NOT NULL,
    processing_version  INT     NOT NULL,
    PRIMARY KEY (morpho_market_id)
);

COMMENT ON TABLE morpho_market_state_current IS '[Operational] Newest morpho_market_state row per market, reduced to the two totals the backing reads need. Derived cache of the morpho_market_state history; rebuildable from it at any time.';
COMMENT ON COLUMN morpho_market_state_current.morpho_market_id IS 'PK. FK→morpho_market.id.';
COMMENT ON COLUMN morpho_market_state_current.total_supply_assets IS 'Derived (copy of morpho_market_state.total_supply_assets). Market-wide supply as a raw on-chain integer in the loan token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN morpho_market_state_current.total_borrow_assets IS 'Derived (copy of morpho_market_state.total_borrow_assets). Market-wide borrows as a raw on-chain integer in the loan token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN morpho_market_state_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_state_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_state_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON morpho_market_state_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON morpho_market_state_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_morpho_market_state_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO morpho_market_state_current AS cur
        (morpho_market_id, total_supply_assets, total_borrow_assets,
         block_number, block_version, processing_version)
    VALUES
        (NEW.morpho_market_id, NEW.total_supply_assets, NEW.total_borrow_assets,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (morpho_market_id) DO UPDATE SET
        total_supply_assets = EXCLUDED.total_supply_assets,
        total_borrow_assets = EXCLUDED.total_borrow_assets,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_morpho_market_state_current
    AFTER INSERT ON morpho_market_state
    FOR EACH ROW
EXECUTE FUNCTION upsert_morpho_market_state_current();

INSERT INTO morpho_market_state_current
    (morpho_market_id, total_supply_assets, total_borrow_assets,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (ms.morpho_market_id)
    ms.morpho_market_id, ms.total_supply_assets, ms.total_borrow_assets,
    ms.block_number, ms.block_version, ms.processing_version
FROM morpho_market_state ms
ORDER BY ms.morpho_market_id,
         ms.block_number DESC, ms.block_version DESC, ms.processing_version DESC
ON CONFLICT (morpho_market_id) DO UPDATE SET
    total_supply_assets = EXCLUDED.total_supply_assets,
    total_borrow_assets = EXCLUDED.total_borrow_assets,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (morpho_market_state_current.block_number, morpho_market_state_current.block_version,
       morpho_market_state_current.processing_version);

-- ============================================================================
-- morpho_market_position_current
-- ============================================================================

-- The PK doubles as the market-set index the read needs: "which markets does this
-- vault hold" is a prefix scan on user_id, which is what replaced the unbounded
-- SELECT DISTINCT morpho_market_id over the whole position history.
CREATE TABLE IF NOT EXISTS morpho_market_position_current (
    user_id            BIGINT  NOT NULL,
    morpho_market_id   BIGINT  NOT NULL,
    supply_assets      NUMERIC NOT NULL,
    block_number       BIGINT  NOT NULL,
    block_version      INT     NOT NULL,
    processing_version INT     NOT NULL,
    PRIMARY KEY (user_id, morpho_market_id)
);

COMMENT ON TABLE morpho_market_position_current IS '[Operational] Newest morpho_market_position row per (user, market), reduced to the supply leg the backing reads need. Derived cache of the morpho_market_position history; rebuildable from it at any time.';
COMMENT ON COLUMN morpho_market_position_current.user_id IS 'PK. FK→user.id. For a vault position this is the vault''s own address.';
COMMENT ON COLUMN morpho_market_position_current.morpho_market_id IS 'PK. FK→morpho_market.id.';
COMMENT ON COLUMN morpho_market_position_current.supply_assets IS 'Derived (copy of morpho_market_position.supply_assets). Supplied balance as a raw on-chain integer in the market loan token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN morpho_market_position_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_position_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_position_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';

GRANT SELECT ON morpho_market_position_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON morpho_market_position_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_morpho_market_position_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO morpho_market_position_current AS cur
        (user_id, morpho_market_id, supply_assets,
         block_number, block_version, processing_version)
    VALUES
        (NEW.user_id, NEW.morpho_market_id, NEW.supply_assets,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (user_id, morpho_market_id) DO UPDATE SET
        supply_assets = EXCLUDED.supply_assets,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_morpho_market_position_current
    AFTER INSERT ON morpho_market_position
    FOR EACH ROW
EXECUTE FUNCTION upsert_morpho_market_position_current();

INSERT INTO morpho_market_position_current
    (user_id, morpho_market_id, supply_assets,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (mp.user_id, mp.morpho_market_id)
    mp.user_id, mp.morpho_market_id, mp.supply_assets,
    mp.block_number, mp.block_version, mp.processing_version
FROM morpho_market_position mp
ORDER BY mp.user_id, mp.morpho_market_id,
         mp.block_number DESC, mp.block_version DESC, mp.processing_version DESC
ON CONFLICT (user_id, morpho_market_id) DO UPDATE SET
    supply_assets = EXCLUDED.supply_assets,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (morpho_market_position_current.block_number, morpho_market_position_current.block_version,
       morpho_market_position_current.processing_version);

-- Fresh tables, so the first reads after deploy would otherwise plan on no stats.
ANALYZE morpho_vault_state_current;
ANALYZE morpho_market_state_current;
ANALYZE morpho_market_position_current;

INSERT INTO migrations (filename)
VALUES ('20260825_140000_create_morpho_current_tables.sql')
ON CONFLICT (filename) DO NOTHING;
