-- allocation_position_current: the newest allocation_position row per
-- (proxy_address, chain_id, token_id). Fifth table of the set created by
-- 20260820_120000_create_current_position_tables.sql — read that file's header for
-- the design and the sizing rationale. It differs from those four in one respect,
-- and that respect is the two sections below: its write path is closed.
--
-- RELATIONSHIP TO THE APPEND-ONLY RULE (db/migrations/AGENTS.md). This table is a
-- derived cache of the newest-row query over append-only history: the history stays
-- untouched, nothing here is a source of truth, every row is reproducible from it,
-- and it is never read as a history — there is no "as of block N" answer in it. So
-- it does take `ON CONFLICT … DO UPDATE`. What it does not take is an update
-- CHANNEL for callers. Exactly two paths write it:
--   1. upsert_allocation_position_current(), the AFTER INSERT trigger below, which
--      is SECURITY DEFINER and so writes under the table owner's privileges; and
--   2. the migrator's backfill, 20260825_120100, which runs as that same owner.
-- No login role holds a write grant. stl_readwrite gets SELECT and nothing else,
-- and the REVOKE below is what makes that true: 20260122_140100's ALTER DEFAULT
-- PRIVILEGES hands stl_readwrite SELECT, INSERT, UPDATE and DELETE on every
-- migrator-owned table at creation, so a grant this file omits arrives anyway.
--
-- The sanctioned update channel is therefore a FUNCTION, not a role privilege. That
-- is what makes "the cache is a function of history" structural rather than a
-- convention: a stray UPDATE in an adapter fails at runtime with SQLSTATE 42501
-- instead of forking the cache from history, and no scheduled reconciliation job is
-- needed to catch a write nobody can make.
-- TestTriggerOnlyCachesGrantTheAppRoleNoWrite and
-- TestAllocationPositionCurrentIsWrittenOnlyByItsTrigger (db/migrator) hold the two
-- halves — the grants, and the trigger path still working without them.
--
-- RECOVERY. The trigger is the only live writer, so any period in which it did not
-- fire leaves the cache behind history: a restore from a dump, a load under
-- `session_replication_role = replica` (which suppresses ordinary triggers), or a
-- window with the trigger explicitly disabled. After any of those, re-run
-- 20260825_120100 as the owner. It is an idempotent FORWARD-ONLY merge — it raises
-- a cached row to a newer history row and never lowers or removes one — so it
-- repairs a cache that is BEHIND, with ingest running, at any time. A cache holding
-- rows AHEAD of history, or keys history no longer has, needs TRUNCATE first, which
-- is owner-only (ALTER DEFAULT PRIVILEGES never grants TRUNCATE).
--
-- Why this key needs one: the receipt-position reads select the newest row per
-- receipt token for one ALM proxy. proxy_address is not the partition column, so
-- TimescaleDB cannot exclude a single chunk and the plan is an Append over every
-- allocation_position chunk (107 locally, ~174 on staging) to return a few dozen
-- rows. receipt_token is unique on (chain_id, receipt_token_address) and token on
-- (chain_id, address), so "newest per receipt token for a proxy" and "newest per
-- (proxy_address, chain_id, token_id)" are the same set of rows.
--
-- Size: one row per (proxy, chain, token) ever touched — 75 on the staging clone.
--
-- NEWER-WINS. The new row wins iff
--   (block_number, block_version, block_timestamp, log_index, direction, tx_hash,
--    processing_version)
-- is greater than the cached row's, compared left to right. Identity terms first,
-- row version last, and that order is the point rather than an accident:
-- processing_version versions ONE row — a reprocess of the same block, log,
-- direction and tx — so it may only rank rows of the same identity. Ranked any
-- higher it would let a reprocessed EARLIER event outrank a later ORIGINAL event
-- in the same block, which is a correction to one log winning over a different
-- log. It is also the order db/migrations/AGENTS.md prescribes for selecting a
-- current row (block_number DESC, block_version DESC[, log_index DESC],
-- processing_version DESC), and the reads are being aligned to it.
--
-- direction DESC, tx_hash DESC settle the one tie the tracker can actually write:
-- an event row at log_index 0 beside the same block's sweep row, a sweep carrying
-- log_index 0 and the zero tx hash (the sweep path in
-- internal/services/allocation_tracker/service.go reads a balance, not a log).
-- Both paths read balanceOf at the same block hash (service.go passes
-- event.ParsedBlockHash() to FetchAll for the event path and for the sweep), so
-- the two tied rows carry the SAME balance by construction: the tiebreak settles
-- only which activity metadata surfaces (latest_activity_action/_amount/tx_hash).
-- Its job is to make the comparison total and deterministic in either arrival
-- order — that is what makes the cache a function of history instead of of ingest
-- timing — and it happens to surface the sweep row. It does NOT outrank an event
-- at a higher log_index: log_index ranks above both.
--
-- block_timestamp is in the comparison only because the PK admits it — the tracker
-- never varies it within one (block_number, block_version). With all seven terms
-- the comparison is total over allocation_position's PK for a single cache key:
-- the only PK column it leaves free is prime_id, which is a function of
-- proxy_address. prime_id is deliberately NOT carried into this table. Keyed by it
-- as well, one proxy would hold two rows the reads have no way to choose between;
-- carried as a payload column, a proxy that changed prime would collapse two
-- identities into one arbitrary payload.
--
-- Deadlock-freedom, as for the sibling caches, rests on AllocationRepository
-- SavePositions sorting its batch by allocation_position's natural key: that sort
-- orders rows by (chain_id, token_id, prime_id, proxy_address, ...) before any
-- version column, and a cache key is a projection of that prefix, so every writer
-- visits the cache rows in the same order. That sort is load-bearing (VEC-643).
--
-- This file creates the table and its maintainer only. The initial backfill is the
-- SEPARATE next migration, 20260825_120100_backfill_allocation_position_current,
-- and the split is load-bearing: the migrator runs a whole file in one transaction,
-- so keeping the two together holds CREATE TRIGGER's SHARE ROW EXCLUSIVE on
-- allocation_position — propagated to that hypertable's chunks — for the length of
-- a full-history scan, and that lock conflicts with the ROW EXCLUSIVE every ingest
-- INSERT takes. SET LOCAL lock_timeout bounds only the acquisition, never the hold.
-- Split, this file commits first, so the trigger is live for the whole of that
-- scan and no row can land in a gap; where the two overlap, the newer-wins guard
-- each carries makes the second one a no-op.

-- Fail fast rather than convoy ingestion. CREATE TRIGGER below takes SHARE ROW
-- EXCLUSIVE on allocation_position, so without a bound it waits out every
-- in-flight writer while queueing all new INSERTs behind it. Same rationale and
-- value as the sibling migration; re-run in a quieter window. Outside a
-- transaction block SET LOCAL only warns and changes nothing, so this file must
-- never be marked `-- migrate: no-transaction`.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS allocation_position_current (
    proxy_address       BYTEA       NOT NULL,
    chain_id            INT         NOT NULL,
    token_id            BIGINT      NOT NULL,
    balance             NUMERIC     NOT NULL,
    underlying_value    NUMERIC,
    underlying_token_id BIGINT,
    tx_amount           NUMERIC     NOT NULL,
    direction           TEXT        NOT NULL CHECK (direction IN ('in', 'out', 'sweep')),
    tx_hash             BYTEA       NOT NULL,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL,
    log_index           INT         NOT NULL,
    processing_version  INT         NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (proxy_address, chain_id, token_id)
);

COMMENT ON TABLE allocation_position_current IS '[Operational] Newest allocation_position row per (ALM proxy, chain, token). Derived cache of the allocation_position history; rebuildable from it at any time by re-running 20260825_120100. Never read it as a history — it holds no "as of block N" answer. Keyed without prime_id, which is a function of proxy_address, so a proxy that changed prime holds one row: its newest.';
COMMENT ON COLUMN allocation_position_current.proxy_address IS 'PK. The ALM proxy holding the position, raw 20-byte address.';
COMMENT ON COLUMN allocation_position_current.chain_id IS 'PK. FK→chain.chain_id.';
COMMENT ON COLUMN allocation_position_current.token_id IS 'PK. FK→token.id. The held token (a receipt token for a wrapped position, the asset itself for a direct holding).';
COMMENT ON COLUMN allocation_position_current.balance IS 'Derived (copy of allocation_position.balance). Post-transaction balanceOf reading, ALREADY decimals-normalized to a human-readable value at write time — never divide by 10^token.decimals. uni_v3_pool/uni_v3_lp rows carry no balanceOf: the tracker-computed full position value in the hint asset''s units. Semantics identical to allocation_position.balance; that comment is the full per-type statement.';
COMMENT ON COLUMN allocation_position_current.underlying_value IS 'Derived (copy of allocation_position.underlying_value). Redeemable value of the balance denominated in the UNDERLYING asset (underlying_token_id), decimals-normalised by that asset — never divide by 10^decimals. NULL where not computable: no valuation implemented for the type, a reverting or undecodable convertToAssets, a missing asset_address, and every row written before the column or its type''s valuation existed. NULL is never zero exposure. Semantics identical to allocation_position.underlying_value; that comment is the full per-type statement.';
COMMENT ON COLUMN allocation_position_current.underlying_token_id IS 'Derived (copy of allocation_position.underlying_token_id). FK→token.id. The underlying the row''s own underlying_value is denominated in; NULL when the row carries no redeemable value.';
COMMENT ON COLUMN allocation_position_current.tx_amount IS 'Derived (copy of allocation_position.tx_amount). Magnitude of the transfer that produced this row, ALREADY decimals-normalized to a human-readable value at write time — never divide by 10^token.decimals. 0 on a sweep, which reads a balance rather than a transfer. Semantics identical to allocation_position.tx_amount.';
COMMENT ON COLUMN allocation_position_current.direction IS 'Derived (copy of allocation_position.direction). in | out | sweep. Part of the newer-wins comparison, ranked below log_index and above tx_hash. A tied sweep/event pair carries the same balance (both paths read balanceOf at one block hash), so this term settles only which activity metadata surfaces; it is there to make the comparison total and deterministic, and it surfaces the sweep row.';
COMMENT ON COLUMN allocation_position_current.tx_hash IS 'Derived (copy of allocation_position.tx_hash). Transaction that produced the winning row, raw 32 bytes, or the 32 zero bytes on a sweep, which has no transaction (VEC-340). Part of the newer-wins comparison, its lowest-ranked identity term.';
COMMENT ON COLUMN allocation_position_current.block_timestamp IS 'Derived (copy of allocation_position.created_at). On-chain block time of the winning row; surfaced by the reads as latest_activity_at; part of the newer-wins comparison.';
COMMENT ON COLUMN allocation_position_current.block_number IS 'Derived. Block the winning history row was observed at; the highest-ranked term of the newer-wins comparison.';
COMMENT ON COLUMN allocation_position_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison, ranked directly below block_number so a replacement wins wherever in the block its log lands.';
COMMENT ON COLUMN allocation_position_current.log_index IS 'Derived. Position of the winning row''s event within its block; part of the newer-wins comparison, and the term that orders the several rows one block can hold for one key.';
COMMENT ON COLUMN allocation_position_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); the LOWEST-ranked term of the newer-wins comparison, because it versions one identity and must not rank rows of differing identity against each other.';
COMMENT ON COLUMN allocation_position_current.created_at IS 'Audit. When the content of this row was written — the first insert or the latest overwrite by a newer history row (there is only ever one row per key, so an overwrite is the creation of the current row). Not block time (see block_timestamp). max(created_at) per proxy is the cache''s staleness signal.';

GRANT SELECT ON allocation_position_current TO stl_readonly;

-- SELECT only for the application role: the trigger below and 20260825_120100 both
-- write as the owner, so no caller needs a write grant, and holding none is what
-- closes the write path (see this file's header). The REVOKE is the operative
-- statement, not a formality — 20260122_140100 sets `ALTER DEFAULT PRIVILEGES IN
-- SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stl_readwrite`,
-- so this table arrives with full DML whatever this file grants, and only an
-- explicit REVOKE takes it back. Unguarded, like the GRANTs above and for the same
-- reason: 20260122_140100 creates stl_readwrite unconditionally. TRUNCATE needs no
-- revoke — ALTER DEFAULT PRIVILEGES never grants it. Nothing is revoked from the
-- OWNER: the owner is the writer.
GRANT SELECT ON allocation_position_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON allocation_position_current FROM stl_readwrite;

-- AFTER INSERT, not BEFORE: trigger_assign_processing_version assigns
-- processing_version in a BEFORE trigger, and this upsert must see the final value.
--
-- SECURITY DEFINER, so the upsert runs under the owner's privileges: the role that
-- appends to allocation_position holds no write grant on the cache, and without
-- this every ingest INSERT would fail at the AFTER trigger. search_path is pinned
-- to the same fixed value as the transformed-layer enqueue triggers
-- (20260706_140000) — mandatory on a SECURITY DEFINER function, so a caller's
-- search_path cannot bind these unqualified names to objects of its own.
CREATE OR REPLACE FUNCTION upsert_allocation_position_current()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    INSERT INTO allocation_position_current AS cur
        (proxy_address, chain_id, token_id, balance, underlying_value, underlying_token_id,
         tx_amount, direction, tx_hash, block_timestamp,
         block_number, block_version, log_index, processing_version)
    VALUES
        (NEW.proxy_address, NEW.chain_id, NEW.token_id, NEW.balance, NEW.underlying_value,
         NEW.underlying_token_id, NEW.tx_amount, NEW.direction, NEW.tx_hash, NEW.created_at,
         NEW.block_number, NEW.block_version, NEW.log_index, NEW.processing_version)
    ON CONFLICT (proxy_address, chain_id, token_id) DO UPDATE SET
        balance = EXCLUDED.balance,
        underlying_value = EXCLUDED.underlying_value,
        underlying_token_id = EXCLUDED.underlying_token_id,
        tx_amount = EXCLUDED.tx_amount,
        direction = EXCLUDED.direction,
        tx_hash = EXCLUDED.tx_hash,
        block_timestamp = EXCLUDED.block_timestamp,
        block_number = EXCLUDED.block_number,
        block_version = EXCLUDED.block_version,
        log_index = EXCLUDED.log_index,
        processing_version = EXCLUDED.processing_version,
        created_at = now()
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.block_timestamp,
           EXCLUDED.log_index, EXCLUDED.direction, EXCLUDED.tx_hash, EXCLUDED.processing_version)
        > (cur.block_number, cur.block_version, cur.block_timestamp,
           cur.log_index, cur.direction, cur.tx_hash, cur.processing_version);
    RETURN NULL;
END;
$$;

CREATE TRIGGER trigger_upsert_allocation_position_current
    AFTER INSERT ON allocation_position
    FOR EACH ROW
EXECUTE FUNCTION upsert_allocation_position_current();

INSERT INTO migrations (filename)
VALUES ('20260825_120000_create_allocation_position_current.sql')
ON CONFLICT (filename) DO NOTHING;
