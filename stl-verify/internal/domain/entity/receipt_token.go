package entity

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// ReceiptToken represents a receipt token (aToken, spToken, cToken, etc.).
type ReceiptToken struct {
	ID                  int64
	ChainID             int64
	ProtocolID          int64
	UnderlyingTokenID   int64
	ReceiptTokenAddress common.Address
	Symbol              string
	CreatedAtBlock      int64
	Metadata            map[string]any
}

// NewReceiptToken creates a new ReceiptToken entity with validation.
func NewReceiptToken(chainID, protocolID, underlyingTokenID, createdAtBlock int64, receiptTokenAddress common.Address, symbol string) (*ReceiptToken, error) {
	rt := &ReceiptToken{
		ChainID:             chainID,
		ProtocolID:          protocolID,
		UnderlyingTokenID:   underlyingTokenID,
		ReceiptTokenAddress: receiptTokenAddress,
		Symbol:              symbol,
		CreatedAtBlock:      createdAtBlock,
		Metadata:            make(map[string]any),
	}
	if err := rt.Validate(); err != nil {
		return nil, fmt.Errorf("NewReceiptToken: %w", err)
	}
	return rt, nil
}

// validate checks that all fields have valid values.
func (rt *ReceiptToken) Validate() error {
	if rt.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", rt.ChainID)
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
	if rt.ReceiptTokenAddress == (common.Address{}) {
		return fmt.Errorf("receipt token address must not be zero")
	}
	return nil
}

// AddressHex returns the receipt token address as a hex string with 0x prefix.
func (r *ReceiptToken) AddressHex() string {
	return r.ReceiptTokenAddress.Hex()
}
