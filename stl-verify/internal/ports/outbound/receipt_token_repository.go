package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
)

// TrackedReceiptToken is a receipt token row joined with protocol identity.
type TrackedReceiptToken struct {
	ID                  int64
	ProtocolID          int64
	ProtocolAddress     common.Address
	UnderlyingTokenID   int64
	ReceiptTokenAddress common.Address
	Symbol              string
	ChainID             int64
}

// ReceiptTokenRepository defines receipt-token lookup operations.
type ReceiptTokenRepository interface {
	// ListTrackedReceiptTokens returns all tracked receipt tokens for a chain.
	ListTrackedReceiptTokens(ctx context.Context, chainID int64) ([]TrackedReceiptToken, error)
}
