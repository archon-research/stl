package outbound

// StateRowCounts splits one block's pool-state INSERTs into queued and appended:
// an idempotent replay conflicts every row away, so Persisted 0 is still healthy.
type StateRowCounts struct {
	Attempted int64
	Persisted int64
}
