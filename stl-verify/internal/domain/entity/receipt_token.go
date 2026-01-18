package entity

import (
	"fmt"
)

// ReceiptToken represents a receipt token (aToken, spToken, cToken, etc.).
type ReceiptToken struct {
	ID                  int64
	ProtocolID          int64
	UnderlyingTokenID   int64
	ReceiptTokenAddress []byte // 20 bytes
	Symbol              string
	CreatedAtBlock      int64
	Metadata            map[string]any
}

// NewReceiptToken creates a new ReceiptToken entity with validation.
func NewReceiptToken(id, protocolID, underlyingTokenID int64, receiptTokenAddress []byte, symbol string) (*ReceiptToken, error) {
	if len(receiptTokenAddress) != 20 {
		return nil, fmt.Errorf("invalid receipt token address length: expected 20, got %d", len(receiptTokenAddress))
	}
	return &ReceiptToken{
		ID:                  id,
		ProtocolID:          protocolID,
		UnderlyingTokenID:   underlyingTokenID,
		ReceiptTokenAddress: receiptTokenAddress,
		Symbol:              symbol,
		Metadata:            make(map[string]any),
	}, nil
}

// AddressHex returns the receipt token address as a hex string with 0x prefix.
func (r *ReceiptToken) AddressHex() string {
	return fmt.Sprintf("0x%x", r.ReceiptTokenAddress)
}
