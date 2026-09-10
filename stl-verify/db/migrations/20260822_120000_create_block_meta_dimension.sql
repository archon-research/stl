-- VEC-491: block_meta, the canonical (chain_id, block_number, block_version) -> block header time
-- lookup for observation tables that carry no event-time column, filled out of band from the block
-- headers in the S3 raw-block archive. Supersedes block_time (block_states is a rolling window).

-- SCOPE: only the blocks the observation tables reference, per chain, not every archived block.
-- Order 10^6 rows at the time of writing, of which ~0.1% carry block_version > 0. Re-measure the
-- referenced set before any decision that assumes its size.

-- Plain table, not a hypertable: reads are equality point lookups on the PK prefix with no time
-- predicate, so chunking and compression buy nothing, and a columnstore policy would need an
-- integer_now_func that does not exist across six chains. DDL only; the load runs out of band.

-- Superseded by block_meta. Empty and unconsumed (bucket 2 was never built); its population
-- procedure is reproducible from block_states if it is ever needed again.
DROP TABLE IF EXISTS block_time;

CREATE TABLE IF NOT EXISTS block_meta (
    chain_id        integer     NOT NULL,
    block_number    bigint      NOT NULL,
    block_version      integer     NOT NULL DEFAULT 0,
    processing_version integer     NOT NULL DEFAULT 0,
    block_timestamp    timestamptz NOT NULL,
    build_id           integer     NOT NULL DEFAULT 0,
    -- ADR-0006 §2: a dimension's rows name the writer run that wrote them. Nullable, no default, no
    -- FK, matching every governed table; NULL means written before run tracking.
    run_id             bigint,
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- The version tuple is the PK, as on every append-only table: UPDATE and DELETE are revoked below,
    -- so a mis-parsed header time can only be corrected by appending the same block at a higher
    -- processing_version. Readers take the highest one. The loader inserts ON CONFLICT DO NOTHING.
    CONSTRAINT block_meta_pkey PRIMARY KEY (chain_id, block_number, block_version, processing_version),
    -- Corruption guards at the loader's chokepoint: a hex-parse bug or a bad S3 key must fail here
    -- rather than be served as event-time to every fill consumer. The upper bound is a fixed constant
    -- so the CHECK stays immutable; it catches gross overshoots only, a cross-check is loader-side work.
    CONSTRAINT block_meta_coord_nonneg_chk CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0 AND build_id >= 0),
    CONSTRAINT block_meta_chain_pos_chk CHECK (chain_id > 0),
    CONSTRAINT block_meta_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz
                                         AND block_timestamp <  '2100-01-01 00:00:00+00'::timestamptz)
);

COMMENT ON TABLE block_meta IS '[Dimension] Canonical (chain_id, block_number, block_version) -> on-chain block metadata, versioned by processing_version so a mis-parsed header is corrected by a new row and readers take the highest. Source of block_timestamp for observation tables that carry no event-time column, via the schema_master block_time fill (VEC-491). Populated out of band from the block header in the S3 raw-block archive. Supersedes block_time. Plain table rather than a hypertable: reads are equality point lookups on the PK prefix, so chunk exclusion has nothing to act on.';
COMMENT ON COLUMN block_meta.chain_id IS 'PK. Chain the block belongs to (chain.chain_id).';
COMMENT ON COLUMN block_meta.block_number IS 'PK. Block height on that chain.';
COMMENT ON COLUMN block_meta.block_version IS 'PK. Reorg version: a reorged block at the same height is a distinct block with its own header time. Matches the S3 object version.';
COMMENT ON COLUMN block_meta.processing_version IS 'PK. Correction axis: the loader writes 0, and a mis-parsed header time is corrected by appending the same (chain_id, block_number, block_version) at a higher value, since UPDATE and DELETE are revoked. Readers take the highest per block.';
COMMENT ON COLUMN block_meta.block_timestamp IS 'On-chain block-header timestamp (the header''s Unix epoch seconds) as a UTC instant. Not node receipt time; that is block_states.received_at.';
COMMENT ON COLUMN block_meta.build_id IS 'Audit. build_registry.id of the loader build that wrote the row; metadata, not identity.';
COMMENT ON COLUMN block_meta.run_id IS 'Audit. writer_run.id of the loader run that wrote the row (ADR-0006 §2); NULL means written before run tracking. Metadata, not identity.';
COMMENT ON COLUMN block_meta.created_at IS 'Audit. Row insert time.';

GRANT SELECT ON block_meta TO stl_readonly;
-- Append-only, per the db/migrations/AGENTS.md default: a reorg appends a block_version, a correction
-- appends a processing_version, and nothing rewrites a stored row. ALTER DEFAULT PRIVILEGES hands
-- stl_readwrite full DML, so the REVOKE below is the enforcement, not the narrowed GRANT.
GRANT SELECT, INSERT ON block_meta TO stl_readwrite;
-- stl_migrator is created by the infra bootstrap, not by a migration, so it is absent under the test harness.
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON block_meta FROM %I', role);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260822_120000_create_block_meta_dimension.sql') ON CONFLICT (filename) DO NOTHING;
