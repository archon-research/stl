-- offchain_token_price_current: the newest offchain_token_price row per
-- (token_id, source_id). Seventh table of the trigger-maintained current-cache
-- set — the design, the append-only relationship and the closed write path are
-- the ones 20260825_120000_create_allocation_position_current.sql documents in
-- full; this header states only what is specific to this table.
--
-- Why this key needs one (VEC-672): /v1/tokens/{id}/price serves the newer of a
-- token's on-chain and off-chain quotes. Its off-chain half was
-- `ORDER BY "timestamp" DESC LIMIT 1` over the offchain_token_price hypertable:
-- 344 one-day chunks holding ~12k rows in total (34 rows per chunk on staging),
-- and no time predicate that could exclude any of them, so every call planned
-- over all 344 — half of the 63 MB / 5.6 s measured for the whole statement on
-- 2026-09-10. token_price_current (20260820_120000, widened with a timestamp by
-- 20260910_120000) answers the on-chain half; this table answers the off-chain
-- half the same way.
--
-- Size: one row per (token, source) ever priced — bounded by the catalogue (one
-- source today), not by history.
--
-- NEWER-WINS. The new row wins iff (snapshot_time, processing_version) is greater
-- than the cached row's, compared left to right. The observation time is the
-- history's snapshot key — there is no block tuple, the prices come from an API —
-- and processing_version LAST because it versions ONE row and must never rank
-- rows of differing identity against each other (db/migrations/AGENTS.md). With
-- both terms the comparison is total over offchain_token_price's PK for one
-- cache key.
--
-- Deadlock-freedom: the live writer is offchain-price-indexer, one poller per
-- source writing each snapshot in one transaction in a fixed token order; a
-- backfill (offchain-price-backfill) overlapping it converges under the
-- newer-wins guard whichever order the two arrive in.
--
-- source_id is the canonical int8 here (offchain_price_source.id is BIGSERIAL),
-- not the int2 the history kept from before the convention — the same widening
-- token_price_current applies to oracle_id.
--
-- No FK columns, matching the sibling caches: test fixtures that TRUNCATE the
-- history must TRUNCATE this cache alongside it (CASCADE will not reach it).
--
-- This file creates the table and its maintainer only. The initial backfill is
-- the SEPARATE next migration, 20260910_120200 — the split is load-bearing for
-- the lock-holding reason 20260825_120000 documents.

-- Fail fast rather than convoy ingestion: CREATE TRIGGER takes SHARE ROW EXCLUSIVE
-- on offchain_token_price. Same rationale and value as the sibling migrations;
-- re-run in a quieter window. This file must never be `-- migrate: no-transaction`.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS offchain_token_price_current (
    token_id           BIGINT         NOT NULL,
    source_id          BIGINT         NOT NULL,
    price_usd          NUMERIC(30,18) NOT NULL,
    snapshot_time      TIMESTAMPTZ    NOT NULL,
    processing_version INT            NOT NULL,
    created_at         TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (token_id, source_id)
);

COMMENT ON TABLE offchain_token_price_current IS '[Operational] Newest offchain_token_price row per (token, off-chain source). Derived cache of that history; rebuildable from it at any time by re-running 20260910_120200. Never read it as a history — it holds no "as of" answer. Written only by its SECURITY DEFINER trigger and the migrator''s backfill (VEC-672; design per allocation_position_current, 20260825_120000).';
COMMENT ON COLUMN offchain_token_price_current.token_id IS 'PK. FK→token.id (app-only, matching the sibling caches). The priced token.';
COMMENT ON COLUMN offchain_token_price_current.source_id IS 'PK. FK→offchain_price_source.id (app-only). The provider that reported the price; canonical int8 where the history keeps int2.';
COMMENT ON COLUMN offchain_token_price_current.price_usd IS 'Derived (copy of offchain_token_price.price_usd). USD per whole token, already decimals-normalized — not a raw integer and not fixed-point.';
COMMENT ON COLUMN offchain_token_price_current.snapshot_time IS 'Derived (copy of offchain_token_price.timestamp, under the register''s canonical name for API observation time, as the transformed layer renames it). Provider observation time of the winning row; the highest-ranked term of the newer-wins comparison, and what /v1/tokens/…/price orders by and reports as staleness.';
COMMENT ON COLUMN offchain_token_price_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); the LOWEST-ranked term of the newer-wins comparison, because it versions one identity and must not rank rows of differing identity against each other.';
COMMENT ON COLUMN offchain_token_price_current.created_at IS 'Audit. When the content of this row was written — the first insert or the latest overwrite by a newer history row. Not observation time (see snapshot_time). max(created_at) is the cache''s staleness signal.';

GRANT SELECT ON offchain_token_price_current TO stl_readonly;

-- SELECT only for the application role; the REVOKE is the operative statement
-- against 20260122_140100's ALTER DEFAULT PRIVILEGES, exactly as on
-- allocation_position_current (see that file's header for the full rationale).
GRANT SELECT ON offchain_token_price_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON offchain_token_price_current FROM stl_readwrite;

-- AFTER INSERT, not BEFORE: assign_processing_version_offchain_token_price runs
-- BEFORE and this upsert must see the final processing_version.
--
-- SECURITY DEFINER with a pinned search_path, for the reasons the sibling
-- trigger's header states: the appending role holds no write grant on the cache.
CREATE OR REPLACE FUNCTION upsert_offchain_token_price_current()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    INSERT INTO offchain_token_price_current AS cur
        (token_id, source_id, price_usd, snapshot_time, processing_version)
    VALUES
        (NEW.token_id, NEW.source_id, NEW.price_usd, NEW."timestamp", NEW.processing_version)
    ON CONFLICT (token_id, source_id) DO UPDATE SET
        price_usd = EXCLUDED.price_usd,
        snapshot_time = EXCLUDED.snapshot_time,
        processing_version = EXCLUDED.processing_version,
        created_at = now()
    WHERE (EXCLUDED.snapshot_time, EXCLUDED.processing_version)
        > (cur.snapshot_time, cur.processing_version);
    RETURN NULL;
END;
$$;

CREATE TRIGGER trigger_upsert_offchain_token_price_current
    AFTER INSERT ON offchain_token_price
    FOR EACH ROW
EXECUTE FUNCTION upsert_offchain_token_price_current();

INSERT INTO migrations (filename)
VALUES ('20260910_120100_create_offchain_token_price_current.sql')
ON CONFLICT (filename) DO NOTHING;
