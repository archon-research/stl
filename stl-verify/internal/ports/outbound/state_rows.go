package outbound

// StateRowCounts is what one block's pool-state INSERTs did, split into the two
// questions the DEX silent-empty alerts need answered separately. Every DEX
// repository returns it from SaveBlock; the indexer services feed it to
// `<prefix>.state.rows.attempted` and `<prefix>.state.rows.written`.
//
// Attempted and Persisted differ precisely on an idempotent replay. Replaying an
// already-committed range under the same build_id makes the
// assign_processing_version trigger reuse the existing processing_version, so
// every INSERT lands on the identical primary key, hits ON CONFLICT DO NOTHING
// and appends nothing — a healthy block with zero Persisted. Only Attempted can
// tell that apart from a block whose state rows were silently dropped before the
// INSERT, so it is Attempted that the NotWritingState/NoStateWritten rules key
// on. Persisted stays as volume observability: it is the count of rows this
// worker actually added to the hypertable.
type StateRowCounts struct {
	// Attempted is the number of pool-state INSERTs the block queued. For curve
	// this spans both the stableswap and cryptoswap drains.
	Attempted int64
	// Persisted is how many of them appended a row (RowsAffected), which is zero
	// for every statement that conflicted away.
	Persisted int64
}
