-- ARCT-223: store the transfer counterparty on allocation positions, so inflow and
-- outflow can be attributed to the address on the other side of the Transfer log.
--
-- allocation_position is a columnstore-enabled hypertable with a tiering policy:
-- the column is added bare (nullable, no DEFAULT) per the TigerData restriction
-- documented in 20260410_110000. Rows written before this migration keep NULL;
-- they are not backfilled (the historical block payloads would have to be
-- re-decoded, which is a separate job).
--
-- Raw address, not an FK to "user": a counterparty is any address that transacted
-- with a proxy, so an FK would mean minting a user row for every unknown EOA that
-- ever touched one. proxy_address is stored the same way in this table. Entity
-- resolution joins the address to entity_ref_codes_current the way holder_entity
-- (20260722_180000) already does for user.address.
--
-- No octet_length CHECK: ALTER TABLE ... ADD CHECK on a columnstore hypertable is
-- broken on TSDB 2.26.x (see 20260702_120000). The 20-byte width is guaranteed by
-- the writer's domain type (common.Address).

ALTER TABLE allocation_position ADD COLUMN IF NOT EXISTS counterparty_address BYTEA;

COMMENT ON COLUMN allocation_position.counterparty_address IS
  'The other side of the Transfer log that triggered this row (20 bytes): the sender when direction = in, the recipient when direction = out. The zero address is a genuine value (mint/burn), never "unknown". NULL means there was no transfer to read a counterparty from: every direction = sweep row, and every row written before this column existed (not backfilled). Not an FK: an arbitrary chain address gets no registry row; resolve to a legal entity through entity_ref_codes_current.';

INSERT INTO migrations (filename)
VALUES ('20260819_100000_add_allocation_position_counterparty.sql')
ON CONFLICT (filename) DO NOTHING;
