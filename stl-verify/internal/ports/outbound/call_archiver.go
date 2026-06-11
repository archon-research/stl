package outbound

import (
	"context"
	"time"
)

// CallEntry is one raw smart contract call within a batch. CallData and
// Response are the raw ABI-encoded bytes exactly as exchanged with the node;
// no decoding is performed.
type CallEntry struct {
	ContractAddress string
	Selector        string
	CallData        []byte
	Success         bool
	Response        []byte
}

// CallBatchRecord is one multicall batch captured for archival. A batch
// corresponds to a single call to Multicaller.Execute and carries every
// (call, result) pair produced by that execution.
//
// Ownership: the record owns its Calls slice and every CallData/Response
// inside it. The archiving decorator copies the underlying bytes at capture,
// so implementations need not defensively copy and may safely read the
// slices asynchronously.
type CallBatchRecord struct {
	ChainID      int64
	BlockNumber  int64
	BlockVersion int
	BuildID      int64
	Source       string
	Multicaller  string
	Timestamp    time.Time
	Calls        []CallEntry
}

// CallArchiver persists one multicall batch as a single durable object.
type CallArchiver interface {
	// Archive writes one batch to durable storage. Implementations must be
	// idempotent: archiving the same
	// (chainID, blockNumber, blockVersion, source, batch composition) twice
	// is a no-op.
	Archive(ctx context.Context, record CallBatchRecord) error
}
