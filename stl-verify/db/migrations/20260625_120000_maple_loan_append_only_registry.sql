-- maple_loan becomes an APPEND-ONLY registry.
--
-- loanMeta (loan_meta_*) is off-chain editorial metadata Maple enriches after a
-- loan originates: a stored NULL loan_meta_type later resolves to a value such
-- as 'intercompany'. Mutating the stored row in place would erase the metadata
-- that was live when earlier maple_loan_state / maple_loan_collateral snapshots
-- were taken, breaking the reproducibility of downstream loan-risk calculations
-- that join back to the registry. Instead the indexer appends a NEW row whenever
-- loanMeta differs, leaving prior versions intact; each state snapshot keeps its
-- FK to the maple_loan row that was current at its sync cycle.
--
-- Drop the one-row-per-loan unique constraint so versions can coexist. Rename
-- first_seen_at -> synced_at: the column now records the sync-cycle timestamp of
-- each version (the same key the maple_*_state tables use), so the current
-- version of a loan is the row with the greatest (synced_at, id) for its
-- (chain_id, loan_address) — id breaks ties when two versions share a synced_at.
-- Keying the timeline on snapshot time (not insert wall-clock) means even a
-- backfill that replays an older cycle orders versions correctly. The repository
-- supplies synced_at explicitly, so drop the NOW() default. Existing rows keep
-- their former first_seen_at value as their synced_at (best available). maple_pool_id
-- and borrower_user_id remain immutable per loan — enforced in the repository (a
-- change fails the run), not by a schema constraint.

ALTER TABLE maple_loan DROP CONSTRAINT IF EXISTS maple_loan_chain_id_loan_address_key;

ALTER TABLE maple_loan RENAME COLUMN first_seen_at TO synced_at;
ALTER TABLE maple_loan ALTER COLUMN synced_at DROP DEFAULT;

CREATE INDEX IF NOT EXISTS idx_maple_loan_latest
    ON maple_loan (chain_id, loan_address, synced_at DESC);

COMMENT ON COLUMN maple_loan.synced_at IS
    'Sync-cycle timestamp of THIS metadata version row (maple_loan is append-only). A loan was first observed at MIN(synced_at) over its (chain_id, loan_address); its current version is the row with the greatest (synced_at, id).';

INSERT INTO migrations (filename)
VALUES ('20260625_120000_maple_loan_append_only_registry.sql')
ON CONFLICT (filename) DO NOTHING;
