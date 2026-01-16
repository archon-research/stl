package entity

import (
	"fmt"
)

// Token represents an ERC20 token.
type Token struct {
	ID             int64
	ChainID        int
	Address        []byte // 20 bytes
	Symbol         string
	Decimals       int16
	CreatedAtBlock int64
	Metadata       map[string]any
}

// NewToken creates a new Token entity with validation.
func NewToken(id int64, chainID int, address []byte, symbol string, decimals int16) (*Token, error) {
	if len(address) != 20 {
		return nil, fmt.Errorf("invalid address length: expected 20, got %d", len(address))
	}
	return &Token{
		ID:       id,
		ChainID:  chainID,
		Address:  address,
		Symbol:   symbol,
		Decimals: decimals,
		Metadata: make(map[string]any),
	}, nil
}

// AddressHex returns the address as a hex string with 0x prefix.
func (t *Token) AddressHex() string {
	return fmt.Sprintf("0x%x", t.Address)
}
