-- Resync every column-owned sequence to max(column) — LOCAL DEV ONLY (ARCT-399).
--
-- Bulk-importing rows into the kind dev database carries their explicit ids along,
-- and an explicit id does not advance the table's sequence; the next genuinely new
-- INSERT then collides on the pkey. Why this is dev-only, how to run it, and what
-- to do when it fails: k8s/README.md § "Bulk-importing rows into the dev database".
--
-- Monotonic by construction: it only ever advances a sequence, and skips empty
-- tables, so it can never manufacture the collision it exists to prevent. Safe to
-- re-run, and safe to run while the app is writing.
--
-- It is one transaction, so it holds an AccessShareLock on every table it probes
-- until it commits — fine in dev, where nothing takes conflicting DDL locks.
--
-- Scope: sequences with an OWNED BY link (pg_depend 'a' = serial/bigserial,
-- 'i' = GENERATED ... AS IDENTITY). A bare CREATE SEQUENCE used as a column
-- DEFAULT records its dependency in the opposite direction and is invisible here;
-- the tail of this script warns if one ever appears rather than leaving that as a
-- comment nobody reads. TimescaleDB's own `_timescaledb_catalog` sequences are
-- also out of scope: nothing here repairs them, and a restore that includes them
-- needs timescaledb_pre_restore()/timescaledb_post_restore() instead.

-- Bound the whole run. Most max() probes below are an index scan on the sequence
-- column and return instantly, but sparklend_reserve_data is a columnstore
-- hypertable whose id is neither compress_segmentby nor compress_orderby
-- (20260410_140000), so once its chunks compress, max(id) has to decompress them.
-- This runs inside `make dev-up`, so cap it: a clear "canceling statement due to
-- statement timeout" beats a bring-up that hangs until kubectl gives up. If you
-- hit it, re-run with a larger timeout — the script is idempotent.
--
-- This has to be its own top-level statement: statement_timeout is armed when the
-- outer statement begins, so a SET LOCAL inside the DO block below would be
-- silently ignored (verified — the block ran to completion past its own timeout).
SET statement_timeout = '120s';

DO $resync$
DECLARE
    r          record;
    max_id     bigint;
    last_val   bigint;
    is_called  boolean;
    next_val   bigint;  -- the value nextval() would hand out right now
    unmatched  text[];
    scanned    integer := 0;
    resynced   integer := 0;
    empty      integer := 0;
    failed     integer := 0;
BEGIN
    FOR r IN
        SELECT seq.oid::regclass AS seq_ident,
               tbl.oid::regclass AS tbl_ident,
               att.attname       AS col_name,
               s.seqincrement    AS increment_by
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
        -- pg_sequence, the catalog, not the pg_sequences view: the view silently
        -- drops any sequence the caller lacks privileges on, which would shrink
        -- the walk to nothing instead of erroring if this ran as a lesser role.
        JOIN pg_sequence s
          ON s.seqrelid = seq.oid
        WHERE seq.relkind = 'S'
          AND tbl.relkind IN ('r', 'p')       -- ordinary + partitioned tables
          AND NOT att.attisdropped
          AND dep.refobjsubid > 0             -- column-owned, not table-owned
          -- Descending sequences are never used for surrogate keys, and
          -- "fast-forward past max()" is meaningless for them.
          AND s.seqincrement > 0
          AND seq_ns.nspname NOT IN ('pg_catalog', 'information_schema')
          AND tbl_ns.nspname NOT IN ('pg_catalog', 'information_schema')
          AND seq_ns.nspname NOT LIKE '\_timescaledb%'
          AND tbl_ns.nspname NOT LIKE '\_timescaledb%'
        ORDER BY seq.oid::regclass::text
    LOOP
        scanned := scanned + 1;

        -- No ONLY: on a hypertable or a partitioned table the rows live in
        -- child chunks, and max() must span all of them.
        EXECUTE format('SELECT max(%I) FROM %s', r.col_name, r.tbl_ident)
           INTO max_id;

        EXECUTE format('SELECT last_value, is_called FROM %s', r.seq_ident)
           INTO last_val, is_called;

        next_val := CASE WHEN is_called THEN last_val + r.increment_by
                         ELSE last_val
                    END;

        IF max_id IS NULL THEN
            empty := empty + 1;
            CONTINUE;
        END IF;

        CONTINUE WHEN max_id < next_val;

        -- Degrade one unrepairable sequence to a warning instead of aborting the
        -- loop: an uncaught error would abandon every sequence later in the scan
        -- order and suppress the summary below. The classic case is a column
        -- widened to bigint whose serial sequence kept maxvalue 2147483647.
        BEGIN
            -- GREATEST against a fresh read of last_value, in the same statement,
            -- so a worker that called nextval() past max_id since the guard above
            -- is not rewound into the collision this exists to prevent. Narrow,
            -- not airtight — scale the writers down first if you want certainty.
            EXECUTE format(
                'SELECT setval(%L::regclass, GREATEST($1, (SELECT last_value FROM %s)), true)',
                r.seq_ident::text, r.seq_ident)
              USING max_id;
            resynced := resynced + 1;
            RAISE NOTICE 'resynced % : next value was %, max(%.%) is % -> next value is now %',
                r.seq_ident, next_val, r.tbl_ident, r.col_name, max_id, max_id + r.increment_by;
        EXCEPTION WHEN OTHERS THEN
            failed := failed + 1;
            RAISE WARNING 'could not resync % to max(%.%) = % : %',
                r.seq_ident, r.tbl_ident, r.col_name, max_id, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE 'sequence resync: % scanned, % resynced, % already ahead, % empty, % failed',
        scanned, resynced, scanned - resynced - empty - failed, empty, failed;

    -- Zero sequences found is not success — it is what running against a database
    -- whose migrations have not been applied looks like. Without this, a genuine
    -- no-op and a wrong-database run print the same green summary.
    IF scanned = 0 THEN
        RAISE WARNING 'no column-owned sequences found in % — are the migrations applied?',
            current_database();
    END IF;

    -- A sequence with no OWNED BY link is outside the walk above and stays
    -- stale after an import. None exists today (every id column is SERIAL or
    -- BIGSERIAL); this says so at the moment that stops being true, which a
    -- comment could not.
    SELECT array_agg(seq.oid::regclass::text ORDER BY seq.oid::regclass::text)
      INTO unmatched
      FROM pg_class seq
      JOIN pg_namespace ns ON ns.oid = seq.relnamespace
     WHERE seq.relkind = 'S'
       AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
       AND ns.nspname NOT LIKE '\_timescaledb%'
       AND NOT EXISTS (
           SELECT 1 FROM pg_depend dep
            WHERE dep.classid    = 'pg_class'::regclass
              AND dep.objid      = seq.oid
              AND dep.refclassid = 'pg_class'::regclass
              AND dep.deptype IN ('a', 'i')
       );

    IF unmatched IS NOT NULL THEN
        RAISE WARNING 'not resynced (no OWNED BY column): %. Give the sequence an OWNED BY, or resync it by hand after an import.',
            array_to_string(unmatched, ', ');
    END IF;

    IF failed > 0 THEN
        RAISE EXCEPTION '% sequence(s) could not be resynced (see the warnings above)', failed;
    END IF;
END
$resync$;

RESET statement_timeout;
