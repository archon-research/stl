-- morpho_vault_state_current and morpho_market_state_current: the newest
-- morpho_vault_state row per vault and the newest morpho_market_state row per
-- market (VEC-659). Seventh and eighth tables of the trigger-maintained
-- current-cache set — the design, the append-only relationship and the closed
-- write path are the ones 20260825_120000_create_allocation_position_current.sql
-- documents in full, and 20260909_150000 applies to the third Morpho history
-- (morpho_market_position); this header states only what is specific to these two.
--
-- Why these keys need one: the Morpho backed-breakdown read
-- (backed_breakdown_repository_morpho.py) reconstructed "newest row per key" over
-- the vault-state and market-state histories on every /risk-capital request, the
-- market-state one inside a LATERAL repeated per market. Neither key column is the
-- partition column, so TimescaleDB cannot prune a chunk and the planner builds a
-- scan node per chunk per execution; the cost is that planning, not the rows, and
-- it grows with every chunk. With these two caches beside
-- morpho_market_position_current the read touches no hypertable at all.
--
-- Payload: the state columns of the source row, so the next reader needs no
-- migration. The AccrueInterest raw fields (morpho_vault_state.fee_shares,
-- new_total_assets, previous_total_assets, management_fee_shares;
-- morpho_market_state.prev_borrow_rate, interest_accrued, fee_shares) are NOT
-- carried: they are set only on the row an AccrueInterest event wrote and NULL on
-- every other row, so the newest row's copy is a fact about that event, not a
-- current state, and a cache would mislead any reader asking "the last accrual".
-- id-like and audit columns (build_id, run_id, created_at) are not carried for the
-- reasons 20260825_120000 gives. morpho_market_state.last_update is carried under
-- its CANONICAL name/type, last_update_at (timestamptz), because schema_master
-- governs this table and keys on column name: the register sanctions the history's
-- Unix-epoch bigint only through a declared cast transform with plausibility
-- bounds 1500000000..4100000000, and the guard is applied here with the cast so an
-- implausible epoch caches as NULL instead of raising inside the trigger and
-- aborting the history insert (the shape 20260910_130000 uses for
-- sparklend_reserve_data.last_update_timestamp). It is the one NULL-able payload
-- column for that reason.
--
-- NEWER-WINS. The new row wins iff
--   (block_number, block_version, block_timestamp, processing_version)
-- is greater than the cached row's, compared left to right. Identity terms first,
-- processing_version LAST: it versions ONE row — a reprocess of the same block —
-- so it must never rank rows of differing identity against each other
-- (db/migrations/AGENTS.md). timestamp is in the comparison only because both
-- history PKs admit it (20260410_130000) — the indexer never varies it within one
-- (block_number, block_version). With all four terms the comparison is total over
-- each history's PK for a single cache key. processing_version is a PK column on
-- both histories, so it is never NULL and no sentinel is needed.
--
-- Deadlock-freedom: as 20260909_150000 states for the position cache. The live
-- writers are the per-chain morpho-indexer workers (one SQS consumer each), whose
-- keys never overlap across chains (a vault and a market belong to one chain), so
-- concurrent multi-row transactions cannot visit the same cache rows in opposite
-- orders. A replay/backfill overlapping the live consumer of the SAME chain writes
-- one block per transaction in block order; the newer-wins guard makes either
-- arrival order converge.
--
-- No FK columns, matching the sibling caches: test fixtures that TRUNCATE a history
-- must TRUNCATE its cache alongside it (CASCADE will not reach it).
--
-- This file creates the tables and their maintainers only. The initial backfill is
-- the SEPARATE next migration, 20260910_140050 — the split is load-bearing for the
-- lock-holding reason 20260825_120000 documents: CREATE TRIGGER takes SHARE ROW
-- EXCLUSIVE on the history and every one of its chunks, and that must not be held
-- for the length of a full-history scan.

-- Fail fast rather than convoy ingestion: each CREATE TRIGGER below takes SHARE ROW
-- EXCLUSIVE on its history for the rest of this transaction. Same rationale and
-- value as the sibling migrations; re-run in a quieter window. This file must never
-- be `-- migrate: no-transaction`.
SET LOCAL lock_timeout = '10s';

-- ============================================================================
-- morpho_vault_state_current
-- ============================================================================

CREATE TABLE IF NOT EXISTS morpho_vault_state_current (
    morpho_vault_id    BIGINT      NOT NULL,
    total_assets       NUMERIC     NOT NULL,
    total_shares       NUMERIC     NOT NULL,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL,
    processing_version INT         NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (morpho_vault_id)
);

COMMENT ON TABLE morpho_vault_state_current IS '[Operational] Newest morpho_vault_state row per vault. Derived cache of that history; rebuildable from it at any time by re-running 20260910_140050. Never read it as a history — it holds no "as of block N" answer. Written only by its SECURITY DEFINER trigger and the migrator''s backfill (VEC-659; design per allocation_position_current, 20260825_120000).';
COMMENT ON COLUMN morpho_vault_state_current.morpho_vault_id IS 'PK. FK→morpho_vault.id (app-only, matching the sibling caches).';
COMMENT ON COLUMN morpho_vault_state_current.total_assets IS 'Derived (copy of morpho_vault_state.total_assets). Assets under management as a raw on-chain integer in the vault asset token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN morpho_vault_state_current.total_shares IS 'Derived (copy of morpho_vault_state.total_shares). Vault ERC-4626 share supply, raw on-chain integer (see the history column''s COMMENT for its relation to morpho_vault_position).';
COMMENT ON COLUMN morpho_vault_state_current.block_timestamp IS 'Derived (copy of morpho_vault_state.timestamp). On-chain block time of the winning row; part of the newer-wins comparison, ranked below block_version and above processing_version because the history PK admits it.';
COMMENT ON COLUMN morpho_vault_state_current.block_number IS 'Derived. Block the winning history row was observed at; the highest-ranked term of the newer-wins comparison.';
COMMENT ON COLUMN morpho_vault_state_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_vault_state_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); the LOWEST-ranked term of the newer-wins comparison, because it versions one identity and must not rank rows of differing identity against each other.';
COMMENT ON COLUMN morpho_vault_state_current.created_at IS 'Audit. When the content of this row was written — the first insert or the latest overwrite by a newer history row. Not block time (see block_timestamp). max(created_at) is the cache''s staleness signal.';

GRANT SELECT ON morpho_vault_state_current TO stl_readonly;

-- SELECT only for the application role; the REVOKE is the operative statement
-- against 20260122_140100's ALTER DEFAULT PRIVILEGES, exactly as on
-- allocation_position_current (see that file's header for the full rationale).
GRANT SELECT ON morpho_vault_state_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON morpho_vault_state_current FROM stl_readwrite;

-- AFTER INSERT, not BEFORE: assign_processing_version_morpho_vault_state runs BEFORE
-- and this upsert must see the final processing_version.
--
-- SECURITY DEFINER with a pinned search_path, for the reasons the sibling
-- trigger's header states: the appending role holds no write grant on the cache.
CREATE OR REPLACE FUNCTION upsert_morpho_vault_state_current()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    INSERT INTO morpho_vault_state_current AS cur
        (morpho_vault_id, total_assets, total_shares, block_timestamp,
         block_number, block_version, processing_version)
    VALUES
        (NEW.morpho_vault_id, NEW.total_assets, NEW.total_shares, NEW."timestamp",
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (morpho_vault_id) DO UPDATE SET
        total_assets = EXCLUDED.total_assets,
        total_shares = EXCLUDED.total_shares,
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

CREATE TRIGGER trigger_upsert_morpho_vault_state_current
    AFTER INSERT ON morpho_vault_state
    FOR EACH ROW
EXECUTE FUNCTION upsert_morpho_vault_state_current();

-- ============================================================================
-- morpho_market_state_current
-- ============================================================================

CREATE TABLE IF NOT EXISTS morpho_market_state_current (
    morpho_market_id    BIGINT      NOT NULL,
    total_supply_assets NUMERIC     NOT NULL,
    total_supply_shares NUMERIC     NOT NULL,
    total_borrow_assets NUMERIC     NOT NULL,
    total_borrow_shares NUMERIC     NOT NULL,
    last_update_at      TIMESTAMPTZ,
    fee                 NUMERIC     NOT NULL,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL,
    processing_version  INT         NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (morpho_market_id)
);

COMMENT ON TABLE morpho_market_state_current IS '[Operational] Newest morpho_market_state row per Blue market. Derived cache of that history; rebuildable from it at any time by re-running 20260910_140050. Never read it as a history — it holds no "as of block N" answer. Written only by its SECURITY DEFINER trigger and the migrator''s backfill (VEC-659; design per allocation_position_current, 20260825_120000).';
COMMENT ON COLUMN morpho_market_state_current.morpho_market_id IS 'PK. FK→morpho_market.id (app-only, matching the sibling caches). The Blue market (one collateral/loan pair at one LLTV).';
COMMENT ON COLUMN morpho_market_state_current.total_supply_assets IS 'Derived (copy of morpho_market_state.total_supply_assets). Market-wide supply as a raw on-chain integer in the loan token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN morpho_market_state_current.total_supply_shares IS 'Derived (copy of morpho_market_state.total_supply_shares). Raw on-chain share units, NOT decimals-normalized; semantics identical to the history column.';
COMMENT ON COLUMN morpho_market_state_current.total_borrow_assets IS 'Derived (copy of morpho_market_state.total_borrow_assets). Market-wide borrows as a raw on-chain integer in the loan token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN morpho_market_state_current.total_borrow_shares IS 'Derived (copy of morpho_market_state.total_borrow_shares). Raw on-chain share units, NOT decimals-normalized.';
COMMENT ON COLUMN morpho_market_state_current.last_update_at IS 'Derived (canonical cast of morpho_market_state.last_update, a Unix epoch). Time of the market''s last on-chain interest accrual as the contract reports it — NOT the time this cache row was written. NULL when the epoch falls outside the schema_master plausibility bounds (1500000000..4100000000).';
COMMENT ON COLUMN morpho_market_state_current.fee IS 'Derived (copy of morpho_market_state.fee). Market fee as a WAD fixed-point fraction (÷1e18): 100000000000000000 = 10%.';
COMMENT ON COLUMN morpho_market_state_current.block_timestamp IS 'Derived (copy of morpho_market_state.timestamp). On-chain block time of the winning row; part of the newer-wins comparison, ranked below block_version and above processing_version because the history PK admits it.';
COMMENT ON COLUMN morpho_market_state_current.block_number IS 'Derived. Block the winning history row was observed at; the highest-ranked term of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_state_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN morpho_market_state_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); the LOWEST-ranked term of the newer-wins comparison, because it versions one identity and must not rank rows of differing identity against each other.';
COMMENT ON COLUMN morpho_market_state_current.created_at IS 'Audit. When the content of this row was written — the first insert or the latest overwrite by a newer history row. Not block time (see block_timestamp). max(created_at) is the cache''s staleness signal.';

GRANT SELECT ON morpho_market_state_current TO stl_readonly;

GRANT SELECT ON morpho_market_state_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON morpho_market_state_current FROM stl_readwrite;

-- AFTER INSERT, not BEFORE: assign_processing_version_morpho_market_state runs
-- BEFORE and this upsert must see the final processing_version.
CREATE OR REPLACE FUNCTION upsert_morpho_market_state_current()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    INSERT INTO morpho_market_state_current AS cur
        (morpho_market_id, total_supply_assets, total_supply_shares,
         total_borrow_assets, total_borrow_shares, last_update_at, fee, block_timestamp,
         block_number, block_version, processing_version)
    VALUES
        (NEW.morpho_market_id, NEW.total_supply_assets, NEW.total_supply_shares,
         NEW.total_borrow_assets, NEW.total_borrow_shares,
         CASE WHEN NEW.last_update BETWEEN 1500000000 AND 4100000000
              THEN to_timestamp(NEW.last_update) END,
         NEW.fee, NEW."timestamp", NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (morpho_market_id) DO UPDATE SET
        total_supply_assets = EXCLUDED.total_supply_assets,
        total_supply_shares = EXCLUDED.total_supply_shares,
        total_borrow_assets = EXCLUDED.total_borrow_assets,
        total_borrow_shares = EXCLUDED.total_borrow_shares,
        last_update_at = EXCLUDED.last_update_at,
        fee = EXCLUDED.fee,
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

CREATE TRIGGER trigger_upsert_morpho_market_state_current
    AFTER INSERT ON morpho_market_state
    FOR EACH ROW
EXECUTE FUNCTION upsert_morpho_market_state_current();

INSERT INTO migrations (filename)
VALUES ('20260910_140000_create_morpho_state_current_tables.sql')
ON CONFLICT (filename) DO NOTHING;
