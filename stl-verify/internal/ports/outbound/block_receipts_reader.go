package outbound

import (
	"context"
	"encoding/json"
)

// BlockReceiptsReader fetches raw transaction receipts for a single block by number.
// Satisfied by *alchemy.Client.
type BlockReceiptsReader interface {
	// GetBlockReceipts returns the raw JSON array of receipts for the given block number.
	GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error)
}
