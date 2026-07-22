-- VEC-491: block_time dimension -- canonical (chain_id, block_number) -> on-chain block_timestamp.
--
-- Several raw pipeline tables (borrower, borrower_collateral, protocol_event, allocation_position,
-- sparklend_reserve_data) record a block_number but no observation-time column. The bucket-2
-- transform recovers block_timestamp for them by joining this dimension on (chain_id, block_number).
--
-- Why a dedicated table and not block_states: block_states already carries the true on-chain time
-- (in created_at -- verified 0s vs the natively-timestamped indexers), but it is a rolling ~1-month
-- reorg-tracking window, so it cannot answer historical blocks. block_time is a durable dimension,
-- populated out of band (see docs/runbooks/block-time-backfill.md) from block_states plus the
-- native-timestamp pipeline tables. Plain table (point lookups by PK), not a hypertable.
--
-- This migration is DDL only. Population and the coverage measurement run out of band on staging;
-- a 19M+ row INSERT...SELECT does not belong in the migrator's single transaction.

CREATE TABLE IF NOT EXISTS block_time (
    chain_id        integer     NOT NULL,
    block_number    bigint      NOT NULL,
    block_timestamp timestamptz NOT NULL,
    PRIMARY KEY (chain_id, block_number)
);

COMMENT ON TABLE block_time IS 'Canonical (chain_id, block_number) -> on-chain block_timestamp lookup. Source of block_timestamp for bucket-2 transform tables that carry no event-time column (VEC-491). Populated out of band from block_states (canonical rows; created_at is on-chain time) and the native-timestamp pipeline tables.';
COMMENT ON COLUMN block_time.block_timestamp IS 'On-chain block-header timestamp (UTC). NOT node receipt time (that is block_states.received_at).';

INSERT INTO migrations (filename) VALUES ('20260722_140000_create_block_time_dimension.sql') ON CONFLICT (filename) DO NOTHING;
