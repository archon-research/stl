-- allocation_position_current: the newest allocation_position row per
-- (chain_id, proxy_address, token_id). Fifth table of the set created by
-- 20260820_120000_create_current_position_tables.sql, same recipe and same
-- rationale — read that file's header for the design, the relationship to the
-- append-only rule, and the rebuild procedure; all of it applies verbatim here.
--
-- Why this key needs one: the receipt-position reads select the newest row per
-- receipt token for one ALM proxy. proxy_address is not the partition column, so
-- TimescaleDB cannot exclude a single chunk and the plan is an Append over every
-- allocation_position chunk (107 locally, ~174 on staging) to return a few dozen
-- rows. receipt_token is unique on (chain_id, receipt_token_address) and token on
-- (chain_id, address), so "newest per receipt token for a proxy" and "newest per
-- (chain_id, proxy_address, token_id)" are the same set of rows.
--
-- Size: one row per (chain, proxy, token) ever touched — 75 on the staging clone.
--
-- Newer-wins tuple is (block_number, block_version, processing_version,
-- log_index), which is the reads' existing ORDER BY, NOT the four-column tuple the
-- sibling caches use. allocation_position is the only one of these histories with
-- several rows per key inside one block (one per transfer log), so log_index has
-- to be part of the comparison or a within-block winner is picked arbitrarily.
-- processing_version outranking log_index is likewise the reads' order, kept so
-- the cache answers exactly what the query it replaces answered. Every one of the
-- four columns is NOT NULL on the history, so the comparison is total.
--
-- Deadlock-freedom, as for the sibling caches, rests on AllocationRepository
-- SavePositions sorting its batch by allocation_position's natural key: that sort
-- orders rows by (chain_id, token_id, prime_id, proxy_address, ...) before any
-- version column, and a cache key is a projection of that prefix, so every writer
-- visits the cache rows in the same order. That sort is load-bearing (VEC-643).
SET LOCAL lock_timeout = '10s';

-- allocation_position has a 1-year tiering policy
-- (20260409_130000_convert_event_tables_to_hypertables.sql), so a key whose newest
-- row has already been tiered is invisible to the backfill without this. Set
-- explicitly in either direction rather than inherited; see the sibling migration.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

CREATE TABLE IF NOT EXISTS allocation_position_current (
    chain_id            INT         NOT NULL,
    proxy_address       BYTEA       NOT NULL,
    token_id            BIGINT      NOT NULL,
    balance             NUMERIC     NOT NULL,
    underlying_value    NUMERIC,
    underlying_token_id BIGINT,
    tx_amount           NUMERIC     NOT NULL,
    direction           TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL,
    processing_version  INT         NOT NULL,
    log_index           INT         NOT NULL,
    PRIMARY KEY (chain_id, proxy_address, token_id)
);

COMMENT ON TABLE allocation_position_current IS '[Operational] Newest allocation_position row per (chain, ALM proxy, token). Derived cache of the allocation_position history; rebuildable from it at any time. Never read it as a history — it holds no "as of block N" answer.';
COMMENT ON COLUMN allocation_position_current.chain_id IS 'PK. FK→chain.chain_id.';
COMMENT ON COLUMN allocation_position_current.proxy_address IS 'PK. The ALM proxy holding the position, raw 20-byte address.';
COMMENT ON COLUMN allocation_position_current.token_id IS 'PK. FK→token.id. The held token (a receipt token for a wrapped position, the asset itself for a direct holding).';
COMMENT ON COLUMN allocation_position_current.balance IS 'Derived (copy of allocation_position.balance). Post-transaction balance as a raw on-chain integer in the token''s native decimals; divide by 10^token.decimals.';
COMMENT ON COLUMN allocation_position_current.underlying_value IS 'Derived (copy of allocation_position.underlying_value). Redeemable value of the balance in the UNDERLYING token''s native decimals (convertToAssets), raw integer. NULL on rows written before the column existed.';
COMMENT ON COLUMN allocation_position_current.underlying_token_id IS 'Derived (copy of allocation_position.underlying_token_id). FK→token.id. The underlying the row''s own underlying_value is denominated in; NULL when the row carries no redeemable value.';
COMMENT ON COLUMN allocation_position_current.tx_amount IS 'Derived (copy of allocation_position.tx_amount). Magnitude of the transfer that produced this row, raw on-chain integer in the token''s native decimals.';
COMMENT ON COLUMN allocation_position_current.direction IS 'Derived (copy of allocation_position.direction). in | out | sweep.';
COMMENT ON COLUMN allocation_position_current.created_at IS 'Derived (copy of allocation_position.created_at). When the winning history row was written, not when the block was mined.';
COMMENT ON COLUMN allocation_position_current.block_number IS 'Derived. Block the winning history row was observed at; part of the newer-wins comparison.';
COMMENT ON COLUMN allocation_position_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN allocation_position_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison, ranked ABOVE log_index to match the reads it replaces.';
COMMENT ON COLUMN allocation_position_current.log_index IS 'Derived. Position of the winning row''s event within its block; the within-block tiebreak, and the lowest-ranked term of the newer-wins comparison.';

GRANT SELECT ON allocation_position_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON allocation_position_current TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_allocation_position_current()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO allocation_position_current AS cur
        (chain_id, proxy_address, token_id, balance, underlying_value, underlying_token_id,
         tx_amount, direction, created_at,
         block_number, block_version, processing_version, log_index)
    VALUES
        (NEW.chain_id, NEW.proxy_address, NEW.token_id, NEW.balance, NEW.underlying_value,
         NEW.underlying_token_id, NEW.tx_amount, NEW.direction, NEW.created_at,
         NEW.block_number, NEW.block_version, NEW.processing_version, NEW.log_index)
    ON CONFLICT (chain_id, proxy_address, token_id) DO UPDATE SET
        balance = EXCLUDED.balance,
        underlying_value = EXCLUDED.underlying_value,
        underlying_token_id = EXCLUDED.underlying_token_id,
        tx_amount = EXCLUDED.tx_amount,
        direction = EXCLUDED.direction,
        created_at = EXCLUDED.created_at,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        log_index = EXCLUDED.log_index
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.log_index)
        > (cur.block_number, cur.block_version, cur.processing_version, cur.log_index);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_upsert_allocation_position_current
    AFTER INSERT ON allocation_position
    FOR EACH ROW
EXECUTE FUNCTION upsert_allocation_position_current();

INSERT INTO allocation_position_current
    (chain_id, proxy_address, token_id, balance, underlying_value, underlying_token_id,
     tx_amount, direction, created_at,
     block_number, block_version, processing_version, log_index)
SELECT DISTINCT ON (ap.chain_id, ap.proxy_address, ap.token_id)
    ap.chain_id, ap.proxy_address, ap.token_id, ap.balance, ap.underlying_value,
    ap.underlying_token_id, ap.tx_amount, ap.direction, ap.created_at,
    ap.block_number, ap.block_version, ap.processing_version, ap.log_index
FROM allocation_position ap
ORDER BY ap.chain_id, ap.proxy_address, ap.token_id,
         ap.block_number DESC, ap.block_version DESC,
         ap.processing_version DESC, ap.log_index DESC
ON CONFLICT (chain_id, proxy_address, token_id) DO UPDATE SET
    balance = EXCLUDED.balance,
    underlying_value = EXCLUDED.underlying_value,
    underlying_token_id = EXCLUDED.underlying_token_id,
    tx_amount = EXCLUDED.tx_amount,
    direction = EXCLUDED.direction,
    created_at = EXCLUDED.created_at,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version,
    log_index = EXCLUDED.log_index
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.log_index)
    > (allocation_position_current.block_number, allocation_position_current.block_version,
       allocation_position_current.processing_version, allocation_position_current.log_index);

-- Fresh table, so the first reads after deploy would otherwise plan on no stats.
ANALYZE allocation_position_current;

INSERT INTO migrations (filename)
VALUES ('20260825_120000_create_allocation_position_current.sql')
ON CONFLICT (filename) DO NOTHING;
