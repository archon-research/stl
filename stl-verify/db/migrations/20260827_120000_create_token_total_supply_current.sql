-- token_total_supply_current: the newest token_total_supply row per
-- (chain_id, token_id). One of the set created by
-- 20260820_120000_create_current_position_tables.sql, same recipe and same
-- rationale — read that file's header for the design, the relationship to the
-- append-only rule, the rebuild procedure, and why the SET LOCAL below needs the
-- migrator's per-file transaction; all of it applies verbatim here.
--
-- Why this key needs one: the share lookup (allocation_share_repository.py)
-- behind the risk-capital and RRC endpoints resolves balance / totalSupply per
-- receipt token, and picks the newest supply row per (chain, token) on every
-- request. token_total_supply is partitioned on block_timestamp and segmented by
-- chain_id alone, so nothing in that predicate excludes a chunk: the plan is a
-- Merge Append over every chunk of the history (22 on staging) to return one row
-- per pair. The as-of read of allocation_position in the same query (newest
-- balance at or before the supply's block) is a history question and stays on
-- history.
--
-- Size: one row per (chain, token) ever observed — 39 on staging today.
--
-- Two timestamps, deliberately: block_timestamp is copied from the winning row
-- because the read's staleness check is on chain time; updated_at is the cache
-- row's own write time, set on every winning upsert (backfill included) — a
-- pipeline-liveness signal, which lags block_timestamp by the ingest delay.
--
-- Split from its backfill (20260827_120100_backfill_token_total_supply_current.sql):
-- CREATE TRIGGER takes SHARE ROW EXCLUSIVE on token_total_supply, held to commit,
-- and the migrator runs a file as one transaction — a backfill in this file would
-- hold that lock, and every allocation-tracker insert behind it, for the whole
-- history scan. Newer-wins on both sides makes the split safe: rows the trigger
-- caches between the two files are guarded no-ops for the backfill, and vice versa.
--
-- Deadlock-freedom, as for the sibling caches, rests on the writer sorting its
-- batch: TokenTotalSupplyRepository.SaveSupplies orders rows by (chain_id,
-- token_address, …) before any version column. That is not the cache key's order,
-- and it does not need to be — every writer applies the same sort, so every writer
-- takes the token-row and cache-row locks in the same order, which is all lock
-- ordering needs.
--
-- Fail fast rather than convoy ingestion behind CREATE TRIGGER's lock.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS token_total_supply_current (
    chain_id            INT         NOT NULL,
    token_id            BIGINT      NOT NULL,
    total_supply        NUMERIC     NOT NULL,
    scaled_total_supply NUMERIC,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL,
    processing_version  INT         NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, token_id)
);

COMMENT ON TABLE token_total_supply_current IS '[Operational] Newest token_total_supply row per (chain, token). Derived cache of the token_total_supply history; rebuildable from it at any time. Never read it as a history — it holds no "as of block N" answer.';
COMMENT ON COLUMN token_total_supply_current.chain_id IS 'PK. FK→chain.chain_id.';
COMMENT ON COLUMN token_total_supply_current.token_id IS 'PK. FK→token.id. The token whose totalSupply() this is.';
COMMENT ON COLUMN token_total_supply_current.total_supply IS 'Derived (copy of token_total_supply.total_supply). Total circulating supply, decimals-normalized to a human-readable value (not a raw integer).';
COMMENT ON COLUMN token_total_supply_current.scaled_total_supply IS 'Derived (copy of token_total_supply.scaled_total_supply). Nullable, decimals-normalized. On-chain scaledTotalSupply reading (interest-free). Populated only for aTokens.';
COMMENT ON COLUMN token_total_supply_current.block_timestamp IS 'Derived (copy of token_total_supply.block_timestamp). On-chain time of the winning row''s block; what the share lookup''s staleness check measures.';
COMMENT ON COLUMN token_total_supply_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN token_total_supply_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN token_total_supply_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';
COMMENT ON COLUMN token_total_supply_current.updated_at IS 'Audit. When this cache row was last written (insert or winning upsert, backfill included) — wall-clock, not block time. An old value means no newer observation has landed since: the token left the tracked set, or ingest has stalled.';

GRANT SELECT ON token_total_supply_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON token_total_supply_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_token_total_supply_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO token_total_supply_current AS cur
        (chain_id, token_id, total_supply, scaled_total_supply, block_timestamp,
         block_number, block_version, processing_version)
    VALUES
        (NEW.chain_id, NEW.token_id, NEW.total_supply, NEW.scaled_total_supply, NEW.block_timestamp,
         NEW.block_number, NEW.block_version, NEW.processing_version)
    ON CONFLICT (chain_id, token_id) DO UPDATE SET
        total_supply = EXCLUDED.total_supply,
        scaled_total_supply = EXCLUDED.scaled_total_supply,
        block_timestamp = EXCLUDED.block_timestamp,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        updated_at = now()
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.processing_version);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_token_total_supply_current
    AFTER INSERT ON token_total_supply
    FOR EACH ROW
EXECUTE FUNCTION upsert_token_total_supply_current();

INSERT INTO migrations (filename)
VALUES ('20260827_120000_create_token_total_supply_current.sql')
ON CONFLICT (filename) DO NOTHING;
