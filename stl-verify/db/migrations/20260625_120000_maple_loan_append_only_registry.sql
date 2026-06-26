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
-- Drop the one-row-per-loan unique constraint so versions can coexist. The
-- current version of a loan is the row with the greatest (first_seen_at, id) for
-- its (chain_id, loan_address) — id breaks ties when two versions share a
-- first_seen_at (same NOW()). This insert-time ordering is correct only for
-- forward-only ingestion (the live cron advances synced_at monotonically); a
-- backfill that replays an older cycle after a newer one would mis-order
-- versions and must instead key the timeline on snapshot time. maple_pool_id and
-- borrower_user_id remain immutable per loan — enforced in the repository (a
-- change fails the run), not by a schema constraint.

ALTER TABLE maple_loan DROP CONSTRAINT IF EXISTS maple_loan_chain_id_loan_address_key;

CREATE INDEX IF NOT EXISTS idx_maple_loan_latest
    ON maple_loan (chain_id, loan_address, first_seen_at DESC, id DESC);

COMMENT ON COLUMN maple_loan.first_seen_at IS
    'Insert time of THIS metadata version row (maple_loan is append-only). A loan was first observed at MIN(first_seen_at) over its (chain_id, loan_address); its current version is the row with the greatest (first_seen_at, id).';
