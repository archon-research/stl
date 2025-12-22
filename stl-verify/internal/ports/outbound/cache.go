package outbound

import (
	"context"
	"encoding/json"
)

// BlockCache defines the interface for caching block data.
// Each data type is stored separately and keyed by chain ID and block number.
type BlockCache interface {
	// SetBlock stores the full block with transactions.
	SetBlock(ctx context.Context, chainID int64, blockNumber int64, data json.RawMessage) error

	// SetReceipts stores transaction receipts for a block.
	SetReceipts(ctx context.Context, chainID int64, blockNumber int64, data json.RawMessage) error

	// SetTraces stores execution traces for a block.
	SetTraces(ctx context.Context, chainID int64, blockNumber int64, data json.RawMessage) error

	// SetBlobs stores blob sidecars for a block.
	SetBlobs(ctx context.Context, chainID int64, blockNumber int64, data json.RawMessage) error

	// DeleteBlock removes all cached data for a block (used on reorg).
	DeleteBlock(ctx context.Context, chainID int64, blockNumber int64) error

	// Close closes the cache connection.
	Close() error
}
