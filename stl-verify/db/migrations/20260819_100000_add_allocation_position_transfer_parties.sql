-- ARCT-223: store both sides of the Transfer log that produced an allocation position,
-- so inflow and outflow can be attributed to the address a proxy transacted with.
--
-- Both sides rather than a single derived "counterparty": the extractor decodes from
-- and to from the log topics, and collapsing them to whichever one is not the proxy is
-- a derivation made at ingest that a reader cannot undo. Storing the pair keeps the
-- fact; the counterparty is then a trivial read-side expression, and direction becomes
-- checkable against the addresses instead of being their sole interpreter.
--
-- One of the two always equals proxy_address on the same row. That redundancy is
-- deliberate: it costs 20 bytes on a compressed hypertable and removes a whole class of
-- silent corruption, where a row records the wrong side of the transfer and nothing can
-- detect it without re-reading the transaction off-chain.
--
-- allocation_position is a columnstore-enabled hypertable with a tiering policy: the
-- columns are added bare (nullable, no DEFAULT) per the TigerData restriction
-- documented in 20260410_110000. Rows written before this migration keep NULL; they are
-- not backfilled (the historical block payloads would have to be re-decoded, which is a
-- separate job).
--
-- Raw addresses, not FKs to "user": either side can be any address that transacted with
-- a proxy, so an FK would mean minting a user row for every unknown EOA that ever
-- touched one. proxy_address is stored the same way in this table. Entity resolution
-- joins the address to entity_ref_codes_current the way holder_entity
-- (20260722_180000) already does for user.address.
--
-- No octet_length CHECK: ALTER TABLE ... ADD CHECK on a columnstore hypertable is broken
-- on TSDB 2.26.x (see 20260702_120000). The 20-byte width is guaranteed by the writer's
-- domain type (common.Address).

ALTER TABLE allocation_position ADD COLUMN IF NOT EXISTS from_address BYTEA;
ALTER TABLE allocation_position ADD COLUMN IF NOT EXISTS to_address BYTEA;

COMMENT ON COLUMN allocation_position.from_address IS
  'Sender of the Transfer log that triggered this row (20 bytes), exactly as decoded. Equals proxy_address when direction = out. The zero address is a genuine value (mint), never "unknown". NULL means there was no transfer to read it from: every direction = sweep row, and every row written before this column existed (not backfilled). Not an FK: an arbitrary chain address gets no registry row; resolve to a legal entity through entity_ref_codes_current.';

COMMENT ON COLUMN allocation_position.to_address IS
  'Recipient of the Transfer log that triggered this row (20 bytes), exactly as decoded. Equals proxy_address when direction = in. The zero address is a genuine value (burn), never "unknown". NULL means there was no transfer to read it from: every direction = sweep row, and every row written before this column existed (not backfilled). Not an FK: an arbitrary chain address gets no registry row; resolve to a legal entity through entity_ref_codes_current. The counterparty is the side that is not proxy_address.';

INSERT INTO migrations (filename)
VALUES ('20260819_100000_add_allocation_position_transfer_parties.sql')
ON CONFLICT (filename) DO NOTHING;
