package outbound

import (
	"context"
	"time"
)

// CallRecord is a single raw smart contract call captured for archival.
// CallData and Response are the raw ABI-encoded bytes exactly as exchanged
// with the node; no decoding is performed.
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
	// be idempotent: archiving the same (block, blockVersion, callData) twice
	// is a no-op.
	Archive(ctx context.Context, record CallRecord) error
}
