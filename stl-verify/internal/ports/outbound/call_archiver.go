package outbound

import (
	"context"
	"time"
)

// CallRecord is a single raw smart contract call captured for archival.
// CallData and Response are the raw ABI-encoded bytes exactly as exchanged
// with the node; no decoding is performed.
//
// Ownership: the record owns its CallData and Response slices. The caller must
// pass slices the record can retain unshared and must not mutate them after
// constructing the record (the archiving decorator copies the underlying call
// and result bytes at capture, so they are safe to use asynchronously).
// Implementations therefore need not defensively copy these fields.
type CallRecord struct {
	ChainID         int64
	BlockNumber     int64
	BlockVersion    int
	BuildID         int64
	Source          string
	Multicaller     string
	Timestamp       time.Time
	ContractAddress string
	Selector        string
	CallData        []byte
	Success         bool
	Response        []byte
}

// CallArchiver persists a single raw smart contract call.
type CallArchiver interface {
	// Archive writes one call record to durable storage. Implementations must
	// be idempotent: archiving the same
	// (chainID, blockNumber, blockVersion, source, callData) twice is a no-op.
	Archive(ctx context.Context, record CallRecord) error
}
