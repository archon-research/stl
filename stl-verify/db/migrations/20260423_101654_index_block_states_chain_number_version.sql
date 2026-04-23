-- migrate: no-transaction
-- VEC-144: Add non-partial index on (chain_id, number, version DESC) to accelerate
-- the assign_block_version() BEFORE INSERT trigger.
--
-- The trigger runs SELECT COALESCE(MAX(version), -1) + 1 WHERE chain_id=$1 AND
-- number=$2 on every INSERT. No existing index matches that predicate:
--   - block_states_pkey and unique_chain_block_version start with created_at
--   - idx_block_states_chain_hash is (chain_id, hash) — wrong column
--   - idx_block_states_chain_canonical and idx_block_states_chain_incomplete_publish
--     are partial (WHERE NOT is_orphaned) so cannot serve an unfiltered query
-- On arbitrum (7.4M rows, 124 chunks) this forces parallel seq scans, ~160k
-- buffer hits per INSERT and ~1500 ms per SaveBlock. This index drops the same
-- query to an Index Only Scan per chunk (verified via EXPLAIN on 7.4M real rows:
-- 2051 ms seq-scan -> 0.059 ms indexed).
--
-- Locking: TimescaleDB does not support CREATE INDEX CONCURRENTLY on hypertables
-- (SQLSTATE 0A000). Plain CREATE INDEX is used here. It holds a hypertable-wide
-- ShareLock for the duration of the build (~1-5 min on arbitrum), which briefly
-- blocks watcher INSERTs during the ArgoCD PreSync window. The backfill service
-- recovers any dropped blocks afterward. One-time cost for a one-time migration.
--
-- We deliberately avoid WITH (timescaledb.transaction_per_chunk): it has better
-- lock behaviour, but a partial failure leaves the parent index marked invalid.
-- A retry with CREATE INDEX IF NOT EXISTS would see the invalid parent and skip,
-- leaving the DB permanently broken without manual intervention. Plain CREATE
-- INDEX is atomic — a failed run rolls back cleanly and a retry is guaranteed
-- to either succeed or leave the table unchanged. Correctness > lock impact here.

CREATE INDEX IF NOT EXISTS idx_block_states_chain_number_version
    ON block_states (chain_id, number, version DESC);

INSERT INTO migrations (filename)
VALUES ('20260423_101654_index_block_states_chain_number_version.sql')
ON CONFLICT (filename) DO NOTHING;
