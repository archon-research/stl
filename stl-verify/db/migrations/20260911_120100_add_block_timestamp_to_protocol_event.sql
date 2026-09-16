-- protocol_event.block_timestamp (VEC-711): on-chain event time under the
-- register's canonical name, so ADR-0005 can declare the series event-time.
--
-- created_at cannot carry that claim even though every writer has set it to the
-- block timestamp since 2026-04-14. It is DEFAULT NOW(), so a writer that omits
-- it silently records ingest time -- which is what the rows below that date hold
-- -- and it is the partition key AND part of the primary key, so those rows
-- cannot be corrected in place. A separate column is the only way to date them.
--
-- DDL only. The backfill is out of band, per VEC-491's precedent for this table:
-- docs/one-off-runbooks/protocol-event-block-timestamp-backfill.md. A whole-table UPDATE over
-- compressed chunks does not belong in the migrator's single transaction.
--
-- Nullable until that backfill lands, and permanently for any row it cannot date;
-- readers filter IS NOT NULL.
SET LOCAL lock_timeout = '10s';

ALTER TABLE protocol_event ADD COLUMN IF NOT EXISTS block_timestamp TIMESTAMPTZ;

COMMENT ON COLUMN protocol_event.block_timestamp IS
  'Derived (copy of the block-header timestamp the indexer decoded this event from). On-chain time the event happened, UTC. Equal to created_at for every row a writer set explicitly; NULL for a row the backfill could not date, which reads treat as undated.';

COMMENT ON COLUMN protocol_event.created_at IS
  'Partition key. Set to the block-header timestamp by every writer (deterministic, so a replay dedups against the PK). DEFAULT NOW() is a safety net that, before 2026-04-14, let some rows record ingest time instead; read block_timestamp, not this column, for event time.';

INSERT INTO migrations (filename)
VALUES ('20260911_120100_add_block_timestamp_to_protocol_event.sql')
ON CONFLICT (filename) DO NOTHING;
