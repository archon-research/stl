-- morpho_market_position_current: the newest morpho_market_position row per
-- (user_id, morpho_market_id). Sixth table of the trigger-maintained current-cache
-- set — the design, the append-only relationship and the closed write path are the
-- ones 20260825_120000_create_allocation_position_current.sql documents in full;
-- this header states only what is specific to this table.
--
-- Why this key needs one (VEC-753): the CORE model's Morpho positions reader finds
-- each borrower's newest row with DISTINCT ON (user_id, morpho_market_id) over the
-- WHOLE morpho_market_position history — 1.23M rows and 1.7 s on the staging
-- replica (9 Sep 2026), growing with every touched block. Neither key column is
-- the partition column, so TimescaleDB cannot prune a single chunk, and past the
-- 1-year tiering horizon the scan needs timescaledb.enable_tiered_reads or a
-- long-idle borrower silently drops out. The cache answers the same question in
-- one plain-table read, with no GUC.
--
-- Size: one row per (user, market) ever seen — bounded by borrowers, not history.
--
-- NEWER-WINS. The new row wins iff
--   (block_number, block_version, timestamp, processing_version)
-- is greater than the cached row's, compared left to right. Identity terms first,
-- processing_version LAST: it versions ONE row — a reprocess of the same block —
-- so it must never rank rows of differing identity against each other
-- (db/migrations/AGENTS.md). timestamp is in the comparison only because the
-- history PK admits it — the indexer never varies it within one
-- (block_number, block_version). With all four terms the comparison is total over
-- morpho_market_position's PK for a single cache key.
--
-- Deadlock-freedom: the live writers are the per-chain morpho-indexer workers
-- (one SQS consumer each), whose keys never overlap across chains (a market
-- belongs to one chain), so concurrent multi-row transactions cannot visit the
-- same cache rows in opposite orders. A replay/backfill overlapping the live
-- consumer of the SAME chain writes one block per transaction in block order; the
-- newer-wins guard makes either arrival order converge.
--
-- No FK columns, matching the sibling caches: test fixtures that TRUNCATE the
-- history must TRUNCATE this cache alongside it (CASCADE will not reach it).
--
-- This file creates the table and its maintainer only. The initial backfill is the
-- SEPARATE next migration, 20260909_150100 — the split is load-bearing for the
-- lock-holding reason 20260825_120000 documents.

-- Fail fast rather than convoy ingestion: CREATE TRIGGER takes SHARE ROW EXCLUSIVE
-- on morpho_market_position. Same rationale and value as the sibling migrations;
-- re-run in a quieter window. This file must never be `-- migrate: no-transaction`.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS morpho_market_position_current (
    user_id            BIGINT      NOT NULL,
    morpho_market_id   BIGINT      NOT NULL,
    supply_shares      NUMERIC     NOT NULL,
    borrow_shares      NUMERIC     NOT NULL,
    collateral         NUMERIC     NOT NULL,
    supply_assets      NUMERIC     NOT NULL,
    borrow_assets      NUMERIC     NOT NULL,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL,
    processing_version INT         NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, morpho_market_id)
);

COMMENT ON TABLE morpho_market_position_current IS '[Operational] Newest morpho_market_position row per (user, Blue market). Derived cache of that history; rebuildable from it at any time by re-running 20260909_150100. Never read it as a history — it holds no "as of block N" answer. Written only by its SECURITY DEFINER trigger and the migrator''s backfill (VEC-753; design per allocation_position_current, 20260825_120000).';
COMMENT ON COLUMN morpho_market_position_current.user_id IS 'PK. FK→user.id (app-only, matching the sibling caches). The position holder.';
COMMENT ON COLUMN morpho_market_position_current.morpho_market_id IS 'PK. FK→morpho_market.id (app-only). The Blue market (one collateral/loan pair at one LLTV).';
COMMENT ON COLUMN morpho_market_position_current.supply_shares IS 'Derived (copy of morpho_market_position.supply_shares). Raw on-chain share units, NOT decimals-normalized; semantics identical to the history column.';
COMMENT ON COLUMN morpho_market_position_current.borrow_shares IS 'Derived (copy of morpho_market_position.borrow_shares). Raw on-chain share units, NOT decimals-normalized.';
COMMENT ON COLUMN morpho_market_position_current.collateral IS 'Derived (copy of morpho_market_position.collateral). Raw integer in the market''s collateral-token native decimals (scale by token.decimals).';
COMMENT ON COLUMN morpho_market_position_current.supply_assets IS 'Derived (copy of morpho_market_position.supply_assets). Raw integer in the market''s loan-token native decimals.';
COMMENT ON COLUMN morpho_market_position_current.borrow_assets IS 'Derived (copy of morpho_market_position.borrow_assets). Raw integer in the market''s loan-token native decimals.';
COMMENT ON COLUMN morpho_market_position_current.block_timestamp IS 'Derived (copy of morpho_market_position.timestamp). On-chain block time of the winning row; part of the newer-wins comparison, ranked below block_version and above processing_version because the history PK admits it.';
COMMENT ON COLUMN morpho_market_position_current.block_number IS 'Derived. Block the winning history row was observed at; the highest-ranked term of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_position_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_position_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); the LOWEST-ranked term of the newer-wins comparison, because it versions one identity and must not rank rows of differing identity against each other.';
COMMENT ON COLUMN morpho_market_position_current.created_at IS 'Audit. When the content of this row was written — the first insert or the latest overwrite by a newer history row. Not block time (see block_timestamp). max(created_at) is the cache''s staleness signal.';

GRANT SELECT ON morpho_market_position_current TO stl_readonly;

-- SELECT only for the application role; the REVOKE is the operative statement
-- against 20260122_140100's ALTER DEFAULT PRIVILEGES, exactly as on
-- allocation_position_current (see that file's header for the full rationale).
GRANT SELECT ON morpho_market_position_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON morpho_market_position_current FROM stl_readwrite;

-- AFTER INSERT, not BEFORE: assign_processing_version_morpho_market_position runs
-- BEFORE and this upsert must see the final processing_version.
--
-- SECURITY DEFINER with a pinned search_path, for the reasons the sibling
-- trigger's header states: the appending role holds no write grant on the cache.
CREATE OR REPLACE FUNCTION upsert_morpho_market_position_current()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    INSERT INTO morpho_market_position_current AS cur
        (user_id, morpho_market_id, supply_shares, borrow_shares, collateral,
         supply_assets, borrow_assets, block_timestamp,
         block_number, block_version, processing_version)
    VALUES
        (NEW.user_id, NEW.morpho_market_id, NEW.supply_shares, NEW.borrow_shares,
         NEW.collateral, NEW.supply_assets, NEW.borrow_assets, NEW."timestamp",
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (user_id, morpho_market_id) DO UPDATE SET
        supply_shares = EXCLUDED.supply_shares,
        borrow_shares = EXCLUDED.borrow_shares,
        collateral = EXCLUDED.collateral,
        supply_assets = EXCLUDED.supply_assets,
        borrow_assets = EXCLUDED.borrow_assets,
        block_timestamp = EXCLUDED.block_timestamp,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        created_at = now()
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.block_timestamp,
           EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.block_timestamp,
           cur.processing_version);
    RETURN NULL;
END;
$$;

CREATE TRIGGER trigger_upsert_morpho_market_position_current
    AFTER INSERT ON morpho_market_position
    FOR EACH ROW
EXECUTE FUNCTION upsert_morpho_market_position_current();

INSERT INTO migrations (filename)
VALUES ('20260909_150000_create_morpho_market_position_current.sql')
ON CONFLICT (filename) DO NOTHING;
