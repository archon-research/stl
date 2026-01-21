package outbound

import (
	"context"
	"encoding/json"
)

// BlockCache defines the interface for caching block data.
// Each data type is stored separately and keyed by chain ID, block number, and version.
// The version is incremented each time a block at the same height is reorged.
type BlockCache interface {
	// SetBlock stores the full block with transactions.
	SetBlock(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error

	// SetReceipts stores transaction receipts for a block.
	SetReceipts(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error

	// SetTraces stores execution traces for a block.
	SetTraces(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error

	// SetBlobs stores blob sidecars for a block.
	SetBlobs(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error

	// GetBlock retrieves the full block with transactions.
	// Returns nil, nil if the block is not in cache.
	GetBlock(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error)

	// GetReceipts retrieves transaction receipts for a block.
	// Returns nil, nil if the receipts are not in cache.
	GetReceipts(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error)

	// GetTraces retrieves execution traces for a block.
	// Returns nil, nil if the traces are not in cache.
	GetTraces(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error)

	// GetBlobs retrieves blob sidecars for a block.
	// Returns nil, nil if the blobs are not in cache.
	GetBlobs(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error)

	// DeleteBlock removes all cached data for a block (used on reorg).
	DeleteBlock(ctx context.Context, chainID int64, blockNumber int64, version int) error

	// Close closes the cache connection.
	Close() error
}
