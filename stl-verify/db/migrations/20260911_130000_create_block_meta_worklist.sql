-- VEC-491: the block-meta loader's run scratch list. A committed table, not a temp one, so a run
-- holds no transaction open: an open backend_xid pins VACUUM's removable cutoff database-wide, and
-- chain 1's ~982k pending blocks are hours of it.

-- UNLOGGED: this is a work list, not history. Every row is reproducible by re-running the enumeration,
-- it is never read as "as of block N", and losing it on a crash costs one re-enumeration.
CREATE UNLOGGED TABLE IF NOT EXISTS block_meta_worklist (
    chain_id      integer NOT NULL REFERENCES chain (chain_id),
    block_number  bigint  NOT NULL,
    block_version integer NOT NULL,
    CONSTRAINT block_meta_worklist_pkey PRIMARY KEY (chain_id, block_number, block_version)
);

COMMENT ON TABLE block_meta_worklist IS '[Operational] VEC-491 run scratch: the blocks a chain references that block_meta lacks, enumerated once per run and paged with a keyset cursor. NOT history and never read as of a block: rows are fully reproducible by re-running the enumeration, which is why it is UNLOGGED. The loader clears its chain at the start of every run and enumerates again: rows surviving a run say nothing about whether its enumeration finished, since windows commit one at a time. Resuming the expensive half -- the archive reads -- is the anti-join against block_meta, which does not depend on this table.';
COMMENT ON COLUMN block_meta_worklist.chain_id IS 'Roles: PK, FK→chain.chain_id. The chain this work list slice belongs to; one loader run owns exactly one chain.';
COMMENT ON COLUMN block_meta_worklist.block_number IS 'Roles: PK. Block height referenced by an observation table and absent from block_meta.';
COMMENT ON COLUMN block_meta_worklist.block_version IS 'Roles: PK. Reorg version of that block, matching the referencing observation row.';

-- The loader owns this table's lifecycle: it clears its own chain at the start of every run and
-- repopulates it. DELETE is the run boundary, not a correction to history, so append-only does not
-- engage (db/migrations/AGENTS.md, "Sanctioned delete channel: block_meta_worklist").
-- UPDATE is revoked: default privileges grant it, and no writer here amends a row in place.
GRANT SELECT, INSERT, DELETE ON block_meta_worklist TO stl_readwrite;
REVOKE UPDATE ON block_meta_worklist FROM stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260911_130000_create_block_meta_worklist.sql') ON CONFLICT (filename) DO NOTHING;
