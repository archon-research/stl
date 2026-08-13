package outbound

import (
	"context"
	"errors"
)

// ErrCanonicalSourceUnavailable marks a transient failure to reach the canonical
// chain source (rate-limit, timeout, 5xx) that survived the adapter's own retries.
// It means "we could not check this block right now", NOT "our stored data is
// wrong", so the validator records the check as skipped instead of failing the run.
var ErrCanonicalSourceUnavailable = errors.New("canonical source temporarily unavailable")

// CanonicalBlock represents a block from a canonical chain data source.
type CanonicalBlock struct {
	// Number is the block number.
	Number int64

	// Hash is the block hash.
	Hash string

	// Timestamp is the block timestamp (Unix seconds).
	Timestamp int64
}

// BlockVerifier fetches canonical block data from an authoritative chain source.
type BlockVerifier interface {
	// Name returns the verifier name (e.g., "etherscan").
	Name() string

	// GetBlockByNumber fetches a block by its number from the canonical chain.
	GetBlockByNumber(ctx context.Context, number int64) (*CanonicalBlock, error)

	// GetBlockByHash fetches a block by its hash from the canonical chain.
	GetBlockByHash(ctx context.Context, hash string) (*CanonicalBlock, error)

	// GetLatestBlockNumber returns the latest block number on the canonical chain.
	GetLatestBlockNumber(ctx context.Context) (int64, error)
}
