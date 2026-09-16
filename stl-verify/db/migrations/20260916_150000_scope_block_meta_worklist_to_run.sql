-- VEC-815: the work list gains an owner. A second writer -- the scheduled top-up -- now shares the
-- table with the on-demand loader, and "clear my chain, then enumerate" let either wipe the list the
-- other was paging, ending that run early with a success.

ALTER TABLE block_meta_worklist ADD COLUMN IF NOT EXISTS run_id bigint REFERENCES writer_run (id);

-- Scratch, reproducible by re-enumerating: rows that predate the column belong to no run and are
-- discarded rather than attributed to one.
DELETE FROM block_meta_worklist WHERE run_id IS NULL;
ALTER TABLE block_meta_worklist ALTER COLUMN run_id SET NOT NULL;

-- run_id joins the key rather than replacing chain_id: two runs may hold the same block, and the key
-- order (chain, run, block) is the keyset cursor's order, so paging still reads one index range.
ALTER TABLE block_meta_worklist DROP CONSTRAINT block_meta_worklist_pkey;
ALTER TABLE block_meta_worklist ADD CONSTRAINT block_meta_worklist_pkey
    PRIMARY KEY (chain_id, run_id, block_number, block_version);

COMMENT ON TABLE block_meta_worklist IS '[Operational] VEC-491 run scratch: the blocks a chain references that block_meta lacks, enumerated once per run and paged with a keyset cursor. NOT history and never read as of a block: rows are fully reproducible by re-running the enumeration, which is why it is UNLOGGED. Each run owns its own slice, keyed by run_id (VEC-815): two writers share this table -- the on-demand loader and the scheduled top-up -- and a run only ever deletes its own rows, so an overlap costs duplicated archive reads and never truncates the list another run is paging. Rows surviving a run say nothing about whether its enumeration finished, since windows commit one at a time; resuming the expensive half -- the archive reads -- is the anti-join against block_meta, which does not depend on this table.';
COMMENT ON COLUMN block_meta_worklist.chain_id IS 'Roles: PK, FK→chain.chain_id. The chain this work list slice belongs to; one run covers exactly one chain.';
COMMENT ON COLUMN block_meta_worklist.run_id IS 'Roles: PK, FK→writer_run.id. The run that enumerated this row and the only writer that deletes it. Rows of a run that died are swept by the next run on that chain once the owning run is older than a day, which is longer than any run takes.';

INSERT INTO migrations (filename) VALUES ('20260916_150000_scope_block_meta_worklist_to_run.sql') ON CONFLICT (filename) DO NOTHING;
