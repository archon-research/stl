-- offchain_token_price_current: the newest offchain_token_price row per
-- (token_id, source_id). Seventh trigger-maintained current cache; the design,
-- the append-only relationship and the closed write path are the ones
-- 20260825_120000_create_allocation_position_current.sql documents.
--
-- Why (VEC-672): /v1/tokens/{id}/price serves the newer of a token's on-chain
-- and off-chain quotes. Its off-chain half was a latest-row read over the
-- offchain_token_price hypertable with no time predicate, so it planned over
-- every chunk on every call. token_price_current answers the on-chain half;
-- this table answers the off-chain half the same way.
--
-- NEWER-WINS: (snapshot_time, processing_version), left to right. The history
-- has no block tuple (prices come from an API), so observation time is the
-- identity term; processing_version LAST because it versions one row and must
-- never rank rows of differing identity (db/migrations/AGENTS.md). Total over
-- offchain_token_price's PK for one cache key.
--
-- Deadlock-freedom: one writer per source (offchain-price-indexer) in a fixed
-- token order; a backfill overlapping it converges under the guard either way.
-- source_id is the canonical int8 (offchain_price_source.id is BIGSERIAL), as
-- token_price_current widens oracle_id. No FK columns, like the sibling caches:
-- fixtures that clear the history must clear this cache too (CASCADE will not).
--
-- Table and maintainer only; the backfill is the SEPARATE next migration,
-- 20260910_120200, for the lock-holding reason 20260825_120000 documents.
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

COMMENT ON TABLE offchain_token_price_current IS '[Operational] Newest offchain_token_price row per (token, off-chain source). Derived cache of that history; rebuildable at any time by re-running 20260910_120200. Never read it as a history. Written only by its SECURITY DEFINER trigger and the migrator''s backfill (VEC-672; design per allocation_position_current, 20260825_120000).';
COMMENT ON COLUMN offchain_token_price_current.token_id IS 'PK. FK→token.id (app-only, matching the sibling caches). The priced token.';
COMMENT ON COLUMN offchain_token_price_current.source_id IS 'PK. FK→offchain_price_source.id (app-only). Canonical int8 where the history keeps int2.';
COMMENT ON COLUMN offchain_token_price_current.price_usd IS 'Derived (copy of offchain_token_price.price_usd). USD per whole token, decimals-normalized.';
COMMENT ON COLUMN offchain_token_price_current.snapshot_time IS 'Derived (copy of offchain_token_price.timestamp, under the register''s canonical name). Provider observation time of the winning row; first term of the newer-wins comparison.';
COMMENT ON COLUMN offchain_token_price_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); last term of the newer-wins comparison.';
COMMENT ON COLUMN offchain_token_price_current.created_at IS 'Audit. When this cache row was written or last overwritten; not observation time. max(created_at) is the cache''s staleness signal.';

GRANT SELECT ON offchain_token_price_current TO stl_readonly;
-- SELECT only for the application role; the REVOKE is the operative statement
-- against 20260122_140100's ALTER DEFAULT PRIVILEGES (see 20260825_120000).
GRANT SELECT ON offchain_token_price_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON offchain_token_price_current FROM stl_readwrite;

-- AFTER INSERT, not BEFORE: assign_processing_version_offchain_token_price runs
-- BEFORE and this upsert must see the final processing_version. SECURITY DEFINER
-- with a pinned search_path: the appending role holds no write grant on the cache.
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
