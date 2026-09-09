-- position_current (VEC-409): one row per position, its newest observation from position_state. A
-- derived cache -- it answers "now", never "as of block N". Its limits are in the table COMMENT.

-- Bounds the wait for CREATE TRIGGER's SHARE ROW EXCLUSIVE on position_state, which every ingest
-- INSERT conflicts with. Never mark this file `migrate: no-transaction`: SET LOCAL would be inert.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS position_current (
    position_id        bytea       NOT NULL,
    chain_id           integer,
    protocol_id        bigint,
    instrument_key     text        NOT NULL,
    holder_id          text        NOT NULL,
    quantity           numeric     NOT NULL,
    block_number       bigint      NOT NULL,
    block_version      integer     NOT NULL,
    processing_version integer     NOT NULL,
    block_timestamp    timestamptz NOT NULL,
    projection         text        NOT NULL,
    build_id           integer     NOT NULL,
    deal_type          text,
    CONSTRAINT position_current_pkey PRIMARY KEY (position_id)
);

COMMENT ON TABLE position_current IS '[Operational] One row per position: its newest observation from position_state, by (block_number, block_version, processing_version, block_timestamp). Derived cache maintained by trigger_upsert_position_current; rebuildable with CALL rebuild_position_current(). Two classes it cannot repair: a cache row ahead of history, and a row whose position has no history left. Never read it as history - point-in-time questions are answered from position_state.';
COMMENT ON COLUMN position_current.position_id IS 'Roles: PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_current.chain_id IS 'Roles: Derived (copy of position_state.chain_id). NULL is a materializer convention for an off-chain source, not missing data.';
COMMENT ON COLUMN position_current.protocol_id IS 'Roles: Derived (copy of position_state.protocol_id). NULL only for an off-chain source, which has no protocol row.';
COMMENT ON COLUMN position_current.instrument_key IS 'Roles: Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_current.holder_id IS 'Roles: Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_current.quantity IS 'Roles: Derived (copy of position_state.quantity). Native units; scale is source-defined and NOT normalized across projections.';
COMMENT ON COLUMN position_current.block_number IS 'Roles: Derived. Block of the latest observation; the leading leg of the newer-wins comparison.';
COMMENT ON COLUMN position_current.block_version IS 'Roles: Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.processing_version IS 'Roles: Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.block_timestamp IS 'Roles: Derived. On-chain time of the latest observation; the last leg of the newer-wins comparison, so the pick is total.';
COMMENT ON COLUMN position_current.projection IS 'Roles: Audit. Which projection view wrote the latest observation.';
COMMENT ON COLUMN position_current.deal_type IS 'Roles: Derived (copy of position_state.deal_type). The deal type of the latest observation; a position that flips direction changes this value.';
COMMENT ON COLUMN position_current.build_id IS 'Roles: Audit. Which build wrote the latest observation (build_registry.id; 0 = pre-tracking).';

-- Trigger-only cache, like allocation_position_current (20260825_120000): the app role reads and the
-- SECURITY DEFINER maintainer writes, so no caller needs a write grant and the cache cannot fork from
-- history. ALTER DEFAULT PRIVILEGES (20260122_140100) grants full DML, so the REVOKE is what closes it.
GRANT SELECT ON position_current TO stl_readonly;
GRANT SELECT ON position_current TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON position_current FROM stl_readwrite;

-- SECURITY DEFINER so the upsert runs as the owner: the role appending to position_state holds no write
-- grant here. search_path is then mandatory, so a caller's path cannot bind these names to its own objects.
CREATE OR REPLACE FUNCTION upsert_position_current() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    INSERT INTO public.position_current AS cur
        (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         deal_type)
    -- One upsert per STATEMENT over the transition table, ordered by position_id: the only total order
    -- a block_timestamp cannot permute, so this cannot cross the rebuild's lock order.
    SELECT DISTINCT ON (n.position_id)
           n.position_id, n.chain_id, n.protocol_id,
           n.instrument_key, n.holder_id, n.quantity, n.block_number, n.block_version,
           n.processing_version, n.block_timestamp, n.projection, n.build_id, n.deal_type
    FROM newrows n
    ORDER BY n.position_id,
             n.block_number DESC, n.block_version DESC, n.processing_version DESC, n.block_timestamp DESC
    ON CONFLICT (position_id) DO UPDATE SET
        chain_id           = EXCLUDED.chain_id,
        protocol_id        = EXCLUDED.protocol_id,
        instrument_key     = EXCLUDED.instrument_key,
        holder_id          = EXCLUDED.holder_id,
        quantity           = EXCLUDED.quantity,
        block_number       = EXCLUDED.block_number,
        block_version      = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        block_timestamp    = EXCLUDED.block_timestamp,
        projection         = EXCLUDED.projection,
        build_id           = EXCLUDED.build_id,
        deal_type          = EXCLUDED.deal_type
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
        > (cur.block_number, cur.block_version, cur.processing_version, cur.block_timestamp);
    RETURN NULL;
END;
$fn$;

COMMENT ON FUNCTION upsert_position_current() IS '[Operational] Keeps position_current at the latest observation per position (VEC-409). AFTER INSERT FOR EACH STATEMENT on position_state, one upsert over the transition table ordered by position_id, guarded by (block_number, block_version, processing_version, block_timestamp). SECURITY DEFINER: the appending role holds no write grant on the cache.';

-- The rebuild an operator re-runs, as a procedure rather than a copied region: the settings cannot be
-- forgotten or stepped over, and there is no second call site to keep in step with this one.
-- enable_tiered_reads because newest-per-key over local chunks only computes it over a PARTIAL table.
CREATE OR REPLACE PROCEDURE rebuild_position_current()
    LANGUAGE sql
    SET search_path = pg_catalog, public
    SET timescaledb.enable_tiered_reads = 'on'
    SET lock_timeout = '10s'
AS $proc$
    INSERT INTO public.position_current
        (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         deal_type)
    -- position_id first, which is both the DISTINCT ON key and the lock order, so the two writers
    -- sweep the PK in one key-derived total order.
    SELECT DISTINCT ON (p.position_id)
           p.position_id, p.chain_id, p.protocol_id,
           p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
           p.processing_version, p.block_timestamp, p.projection, p.build_id, p.deal_type
    FROM public.position_state p
    ORDER BY p.position_id,
             p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
    ON CONFLICT (position_id) DO UPDATE SET
        chain_id           = EXCLUDED.chain_id,
        protocol_id        = EXCLUDED.protocol_id,
        instrument_key     = EXCLUDED.instrument_key,
        holder_id          = EXCLUDED.holder_id,
        quantity           = EXCLUDED.quantity,
        block_number       = EXCLUDED.block_number,
        block_version      = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        block_timestamp    = EXCLUDED.block_timestamp,
        projection         = EXCLUDED.projection,
        build_id           = EXCLUDED.build_id,
        deal_type          = EXCLUDED.deal_type
    -- Forward-only: raise a stale row, never lower one. No equal-coordinate arm is needed now that the
    -- cache has no write channel outside these two writers, which cannot disagree on one coordinate.
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
        > (position_current.block_number, position_current.block_version, position_current.processing_version, position_current.block_timestamp);
$proc$;

COMMENT ON PROCEDURE rebuild_position_current() IS '[Operational] Rebuilds position_current from position_state (VEC-409): CALL rebuild_position_current(), as the owner -- this procedure is invoker-rights, and the app role holds SELECT only. Forward-only, so it raises a stale row and never lowers or removes one; it cannot repair a cache row ahead of history or a row whose position has no history left. Pins enable_tiered_reads so newest-per-key is computed over the whole table, tiered chunks included.';

-- Guarded like every other DDL statement here, so a re-run does not fail with "trigger already exists".
DROP TRIGGER IF EXISTS trigger_upsert_position_current ON position_state;
CREATE TRIGGER trigger_upsert_position_current
    AFTER INSERT ON position_state
    REFERENCING NEW TABLE AS newrows
    FOR EACH STATEMENT
EXECUTE FUNCTION upsert_position_current();

-- KNOWN GAP: TimescaleDB refuses ENABLE ALWAYS on a hypertable trigger, so this one stays at ORIGIN and
-- does not fire under session_replication_role = 'replica' (pg_restore --disable-triggers). Recovery is
-- CALL rebuild_position_current(), as the owner.

INSERT INTO migrations (filename) VALUES ('20260819_150000_create_position_current.sql') ON CONFLICT (filename) DO NOTHING;
