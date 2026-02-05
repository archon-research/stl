package outbound

import "context"

// CanonicalBlock represents a block from a canonical chain data source.
type CanonicalBlock struct {
	// Number is the block number.
	Number int64

	// Hash is the block hash.
	Hash string

	// Timestamp is the block timestamp (Unix seconds).
	Timestamp int64
}

// BlockVerifier verifies block data against authoritative sources like Etherscan.
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
