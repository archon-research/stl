-- Morpho VaultV2 structured tracking (VEC-218): let the INSERT supply
-- morpho_adapter_state.processing_version instead of only the trigger.
--
-- A new migration rather than an edit: 20260721_130000 has been applied in staging since
-- 2026-08-20, and the migrator rejects a modified file on checksum.
--
-- What it fixes, found in the VEC-218 E2E on 2026-08-21 (TimescaleDB 2.25.1-pg17): once a
-- chunk is columnstored, every correction row for a position that chunk already holds is
-- silently lost. TimescaleDB resolves the INSERT's ON CONFLICT against compressed data
-- BEFORE row triggers fire, so a processing_version left to the trigger still carries the
-- column's DEFAULT 0 at that moment, matches the pv=0 row already there, and is discarded
-- with no error and no rows affected. Measured: 150 rows replayed into a compressed chunk
-- under a new build_id wrote 0 rows, where the identical replay wrote all 150 while the
-- chunk was still rowstore. Compression lands at 2 days, so that is every chunk a
-- backfill replay touches, and ADR-0002's corrections-as-new-rows model was inoperative
-- for this table.
--
-- Only this table is fixed here because it is the one strictly-append-only table in this
-- change that both carries a compression policy and receives correction replays; the
-- siblings the same work adds (morpho_vault_cap, morpho_vault_fee,
-- morpho_adapter_membership) have no compression policy, so their trigger-assigned
-- versions reach the arbiter intact on rowstore chunks. The general case — every
-- assign_processing_version_* table sitting on a compressed hypertable shares this defect
-- — is tracked as VEC-615.
--
-- The rule therefore moves into next_processing_version_morpho_adapter_state, so the
-- INSERT and the trigger share ONE definition of it — the advisory-lock key included,
-- which has to match or the serialization ADR-0002 §3 requires is lost. A repository
-- calls it in the INSERT's VALUES list, evaluated before the arbiter, so the arbiter sees
-- the version the row will really carry, and the position's lock is held before any
-- arbiter or decompression work begins.
--
-- The trigger is the floor on ROWSTORE chunks only. A writer that does not call the
-- function — psql, an ad-hoc script — still loses its correction on a columnstored chunk,
-- because the arbiter has already discarded the row by the time the trigger could assign
-- anything; it also reaches the position's lock only after that arbiter work. What the
-- trigger still guarantees is that such a writer never lands a DEFAULT-0 row on top of an
-- existing series.
--
-- Cost: the rule now runs twice per inserted row (the VALUES call, then the trigger's
-- recompute under the lock it already holds) — two advisory-lock calls and four PK-prefix
-- probes where there were one and two. Deliberate: ~200 µs/row buys a DB-level floor that
-- no writer can bypass, against a table sized for governance events.
--
-- compress_orderby is deliberately left alone. Adding processing_version to it changes
-- none of this — the drop reproduces identically with and without it, because the
-- arbiter's blind spot is the trigger's timing, not the column's absence from the
-- ordering.

-- ============================================================================
-- The version rule, callable from both the INSERT and the trigger.
-- ============================================================================
-- Pinned to force_custom_plan for the same reason the trigger functions are: its per-row
-- lookups must keep pruning chunks instead of fanning out over every chunk once plpgsql
-- caches a generic plan (VEC-541, db/migrations/AGENTS.md).
--
-- VOLATILE is spelled out because correctness rests on it: a STABLE function would read
-- the CALLING statement's snapshot, so a writer released from the advisory lock would
-- recompute the same version as the writer it waited for and insert a duplicate the
-- unique index cannot catch on compressed data.
CREATE OR REPLACE FUNCTION next_processing_version_morpho_adapter_state(
    p_adapter_id    BIGINT,
    p_block_number  BIGINT,
    p_block_version INT,
    p_timestamp     TIMESTAMPTZ,
    p_build_id      INT)
RETURNS INT
VOLATILE
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    -- Timestamp wrapped in EXTRACT(epoch FROM …) so the key is TimeZone/DateStyle-stable.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mas|%s|%s|%s|%s', p_adapter_id, p_block_number, p_block_version,
               EXTRACT(epoch FROM p_timestamp)), 0));

    SELECT processing_version INTO existing_ver
    FROM morpho_adapter_state
    WHERE morpho_adapter_id = p_adapter_id
      AND block_number      = p_block_number
      AND block_version     = p_block_version
      AND timestamp         = p_timestamp
      AND build_id          = p_build_id
    LIMIT 1;

    IF FOUND THEN
        RETURN existing_ver;
    END IF;

    SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
    FROM morpho_adapter_state
    WHERE morpho_adapter_id = p_adapter_id
      AND block_number      = p_block_number
      AND block_version     = p_block_version
      AND timestamp         = p_timestamp;
    RETURN max_ver + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION next_processing_version_morpho_adapter_state(BIGINT, BIGINT, INT, TIMESTAMPTZ, INT) IS
  'Returns the processing_version a morpho_adapter_state row at (adapter, block, block_version, timestamp) must carry for build_id: the version that build already wrote there, else MAX+1. Takes the position''s advisory lock (ADR-0002 §3) and holds it for the transaction, so the value cannot go stale before the INSERT lands. Call it in the INSERT''s VALUES list: on a columnstored chunk TimescaleDB resolves ON CONFLICT before row triggers fire, so a version left to the trigger reaches the arbiter as DEFAULT 0 and the correction row is silently discarded.';

-- ============================================================================
-- The trigger now delegates to it (CREATE OR REPLACE resets a function's settings,
-- so force_custom_plan is re-declared here — db/migrations/AGENTS.md).
-- ============================================================================
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_adapter_state()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
BEGIN
    NEW.processing_version := next_processing_version_morpho_adapter_state(
        NEW.morpho_adapter_id, NEW.block_number, NEW.block_version, NEW.timestamp, NEW.build_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO migrations (filename)
VALUES ('20260821_120000_morpho_adapter_state_version_function.sql')
ON CONFLICT (filename) DO NOTHING;
