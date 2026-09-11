-- VEC-491: the block-meta loader's run scratch list, as a committed table rather than a temp table.
-- A temp table is ON COMMIT DROP, so the run had to hold one transaction open for its whole duration.
-- That transaction's backend_xid pins VACUUM's removable cutoff database-wide even with no snapshot
-- held, measured on pg18: an idle-in-transaction session with a filled temp table left 5,000 dead
-- tuples unremovable on an unrelated table. Chain 1 has ~982k pending blocks against a 6h deadline,
-- so that is hours of held-off vacuum, and on a deadline hit the temp table dropped with nothing reusable.

-- UNLOGGED: this is a work list, not history. Every row is reproducible by re-running the enumeration,
-- it is never read as "as of block N", and losing it on a crash costs one re-enumeration.
CREATE UNLOGGED TABLE IF NOT EXISTS block_meta_worklist (
    chain_id      integer NOT NULL REFERENCES chain (chain_id),
    block_number  bigint  NOT NULL,
    block_version integer NOT NULL,
    CONSTRAINT block_meta_worklist_pkey PRIMARY KEY (chain_id, block_number, block_version)
);

COMMENT ON TABLE block_meta_worklist IS '[Operational] VEC-491 run scratch: the blocks a chain references that block_meta lacks, enumerated once per run and paged with a keyset cursor. NOT history and never read as of a block: rows are fully reproducible by re-running the enumeration, which is why it is UNLOGGED and why the loader DELETEs its own chain rows at the start of a run. Surviving a run makes the run resumable after a restart or a deadline hit.';
COMMENT ON COLUMN block_meta_worklist.chain_id IS 'Roles: PK, FK→chain.chain_id. The chain this work list slice belongs to; one loader run owns exactly one chain.';
COMMENT ON COLUMN block_meta_worklist.block_number IS 'Roles: PK. Block height referenced by an observation table and absent from block_meta.';
COMMENT ON COLUMN block_meta_worklist.block_version IS 'Roles: PK. Reorg version of that block, matching the referencing observation row.';

-- The loader owns this table's lifecycle: it clears its own chain and repopulates per run. DELETE is
-- the run boundary, not a correction to history, so the append-only default does not engage here.
GRANT SELECT, INSERT, DELETE ON block_meta_worklist TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260911_130000_create_block_meta_worklist.sql') ON CONFLICT (filename) DO NOTHING;
