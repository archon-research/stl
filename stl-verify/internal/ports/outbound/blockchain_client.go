package outbound

import (
	"context"
	"encoding/json"
)

// BlockData holds all fetched data for a single block.
type BlockData struct {
	BlockNumber int64
	Block       json.RawMessage
	Receipts    json.RawMessage
	Traces      json.RawMessage
	Blobs       json.RawMessage

	// Errors for each data type (nil if successful)
	BlockErr    error
	ReceiptsErr error
	TracesErr   error
	BlobsErr    error
}

// HasErrors returns true if any of the data fetches failed.
func (b *BlockData) HasErrors() bool {
	return b.BlockErr != nil || b.ReceiptsErr != nil || b.TracesErr != nil || b.BlobsErr != nil
}

// BlockchainClient defines the interface for fetching blockchain data via RPC.
// This is separate from BlockSubscriber which handles real-time subscriptions.
type BlockchainClient interface {
	// GetBlockByNumber fetches a block by its number.
	// If fullTx is true, includes full transaction objects; otherwise just hashes.
	GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error)

	// GetBlockByHash fetches a block by its hash.
	// If fullTx is true, includes full transaction objects; otherwise just hashes.
	GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*BlockHeader, error)

	// GetBlockReceipts fetches all transaction receipts for a block.
	GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error)

	// GetBlockTraces fetches execution traces for a block.
	GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error)

	// GetBlobSidecars fetches blob sidecars for a block.
	GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error)

	// GetCurrentBlockNumber fetches the latest block number.
	GetCurrentBlockNumber(ctx context.Context) (int64, error)

	// GetBlocksBatch fetches all data for multiple blocks in a single batched RPC call.
	// Returns a slice of BlockData in the same order as the input block numbers.
	// Per-block errors are reported in the BlockData error fields (BlockErr, ReceiptsErr, etc.).
	// Use BlockData.HasErrors() to check if any data fetch failed for a block.
	// Only returns an error if the entire batch request fails (network error, etc.).
	GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]BlockData, error)
}
