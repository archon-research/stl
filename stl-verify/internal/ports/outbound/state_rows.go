package outbound

// StateRowCounts splits one block's pool-state INSERTs into queued and appended:
// an idempotent replay conflicts every row away, so Persisted 0 is still healthy.
// TicksPersisted and PositionsPersisted are the append-on-change writers' rows
// that landed, not the rows offered to them; a DEX without such a table leaves
// them zero.
type StateRowCounts struct {
	Attempted          int64
	Persisted          int64
	TicksPersisted     int64
	PositionsPersisted int64
}
