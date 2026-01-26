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
func NewReceiptToken(id, protocolID, underlyingTokenID, createdAtBlock int64, receiptTokenAddress []byte, symbol string) (*ReceiptToken, error) {
	rt := &ReceiptToken{
		ID:                  id,
		ProtocolID:          protocolID,
		UnderlyingTokenID:   underlyingTokenID,
		ReceiptTokenAddress: receiptTokenAddress,
		Symbol:              symbol,
		CreatedAtBlock:      createdAtBlock,
		Metadata:            make(map[string]any),
	}
	if err := rt.validate(); err != nil {
		return nil, err
	}
	return rt, nil
}

// validate checks that all fields have valid values.
func (rt *ReceiptToken) validate() error {
	if rt.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", rt.ID)
	}
	if rt.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", rt.ProtocolID)
	}
	if rt.UnderlyingTokenID <= 0 {
		return fmt.Errorf("underlyingTokenID must be positive, got %d", rt.UnderlyingTokenID)
	}
	if rt.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", rt.CreatedAtBlock)
	}
	if len(rt.ReceiptTokenAddress) != 20 {
		return fmt.Errorf("invalid receipt token address length: expected 20, got %d", len(rt.ReceiptTokenAddress))
	}
	if rt.Symbol == "" {
		return fmt.Errorf("symbol must not be empty")
	}
	return nil
}

// AddressHex returns the receipt token address as a hex string with 0x prefix.
func (r *ReceiptToken) AddressHex() string {
	return fmt.Sprintf("0x%x", r.ReceiptTokenAddress)
}
