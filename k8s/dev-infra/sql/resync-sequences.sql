-- Resync every column-owned sequence to max(column) — LOCAL DEV ONLY (ARCT-399).
--
-- Why this exists
-- ---------------
-- The kind dev database is periodically filled by bulk-importing rows carrying
-- their own explicit ids (a staging clone). An explicit id does not advance the
-- table's sequence, so afterwards `token_id_seq.last_value` sat at 1454 while
-- `max(token.id)` was 6,156,714: every INSERT of a genuinely new row collided
-- with an already-imported one and failed with
--   duplicate key value violates unique constraint "token_pkey" (SQLSTATE 23505)
-- until nextval() eventually stumbled onto a free id. The upsert code is not at
-- fault — it conflicts on (chain_id, address), a different constraint from the
-- one the lagging sequence violates, so ON CONFLICT cannot absorb it.
--
-- Staging and prod cannot reach this state: ids there only ever come from the
-- sequence. This script is therefore an operational repair for the dev clone
-- path and MUST NOT be turned into a migration in stl-verify/db/migrations/ —
-- it would then run against staging/prod, where it is at best a no-op and at
-- worst papers over a real anomaly.
--
-- What it does
-- ------------
-- Walks every sequence that is OWNED BY a table column (`serial`/`bigserial`
-- via a pg_depend 'a' dependency, and GENERATED ... AS IDENTITY via 'i') and
-- fast-forwards it past max(column). No table name is hard-coded, so a new
-- table is covered the day it is created.
--
-- Properties:
--   * Idempotent — a second run finds nothing to do and reports 0 resynced.
--   * Monotonic — it only ever advances a sequence. A sequence already ahead of
--     max(column) (the normal, healthy case) is left alone, so a run can never
--     manufacture the very collision it exists to prevent.
--   * Empty tables are skipped: max(column) IS NULL means there is no row to
--     collide with, and rewinding the sequence to its start would be a
--     regression, not a fix.
--   * TimescaleDB internals are excluded — `_timescaledb_catalog` sequences are
--     Timescale's to manage, and hypertable chunks are not resynced
--     individually (max() on the parent hypertable already spans every chunk).
--
-- Out of scope: a sequence that is merely a column DEFAULT without an OWNED BY
-- link, which PostgreSQL records as a normal ('n') dependency rather than the
-- 'a'/'i' this walks. No such sequence exists — every sequence in the schema
-- comes from SERIAL/BIGSERIAL (`grep -i 'create sequence' db/migrations/*.sql`
-- is empty), and they all carry OWNED BY. Write a migration with a bare
-- CREATE SEQUENCE and you are on your own for the dev clone.
--
-- Safe to re-run at any time. Runs as the database owner/superuser, because
-- setval() needs UPDATE on the sequence.

DO $resync$
DECLARE
    r          record;
    max_id     bigint;
    last_val   bigint;
    is_called  boolean;
    next_val   bigint;  -- the value nextval() would hand out right now
    scanned    integer := 0;
    resynced   integer := 0;
BEGIN
    FOR r IN
        SELECT seq.oid::regclass        AS seq_ident,
               tbl.oid::regclass        AS tbl_ident,
               quote_ident(att.attname) AS col_ident,
               att.attname              AS col_name,
               s.increment_by           AS increment_by
        FROM pg_class seq
        JOIN pg_namespace seq_ns
          ON seq_ns.oid = seq.relnamespace
        JOIN pg_depend dep
          ON dep.classid    = 'pg_class'::regclass
         AND dep.objid      = seq.oid
         AND dep.refclassid = 'pg_class'::regclass
         -- 'a' = serial/bigserial (sequence owned by the column),
         -- 'i' = GENERATED ... AS IDENTITY.
         AND dep.deptype IN ('a', 'i')
        JOIN pg_class tbl
          ON tbl.oid = dep.refobjid
        JOIN pg_namespace tbl_ns
          ON tbl_ns.oid = tbl.relnamespace
        JOIN pg_attribute att
          ON att.attrelid = tbl.oid
         AND att.attnum   = dep.refobjsubid
        JOIN pg_sequences s
          ON s.schemaname   = seq_ns.nspname
         AND s.sequencename = seq.relname
        WHERE seq.relkind = 'S'
          AND tbl.relkind IN ('r', 'p')       -- ordinary + partitioned tables
          AND NOT att.attisdropped
          AND dep.refobjsubid > 0             -- column-owned, not table-owned
          -- Descending sequences are never used for surrogate keys, and
          -- "fast-forward past max()" is meaningless for them.
          AND s.increment_by > 0
          AND seq_ns.nspname NOT IN ('pg_catalog', 'information_schema')
          AND tbl_ns.nspname NOT IN ('pg_catalog', 'information_schema')
          AND seq_ns.nspname NOT LIKE '\_timescaledb%'
          AND tbl_ns.nspname NOT LIKE '\_timescaledb%'
        ORDER BY seq.oid::regclass::text
    LOOP
        scanned := scanned + 1;

        -- No ONLY: on a hypertable (or a partitioned table) the rows live in
        -- child chunks, and max() must span all of them.
        EXECUTE format('SELECT max(%s) FROM %s', r.col_ident, r.tbl_ident)
           INTO max_id;

        EXECUTE format('SELECT last_value, is_called FROM %s', r.seq_ident)
           INTO last_val, is_called;

        next_val := CASE WHEN is_called THEN last_val + r.increment_by
                         ELSE last_val
                    END;

        -- Nothing to collide with, or the sequence is already past the data.
        CONTINUE WHEN max_id IS NULL OR max_id < next_val;

        PERFORM setval(r.seq_ident, max_id, true);
        resynced := resynced + 1;
        RAISE NOTICE 'resynced % : next value was %, max(%.%) is % -> next value is now %',
            r.seq_ident, next_val, r.tbl_ident, r.col_name, max_id, max_id + r.increment_by;
    END LOOP;

    RAISE NOTICE 'sequence resync complete: % column-owned sequence(s) scanned, % resynced, % already correct',
        scanned, resynced, scanned - resynced;
END
$resync$;
