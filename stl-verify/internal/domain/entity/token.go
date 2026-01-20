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
func NewToken(id int64, chainID int, address []byte, symbol string, decimals int16, createdAtBlock int64) (*Token, error) {
	t := &Token{
		ID:             id,
		ChainID:        chainID,
		Address:        address,
		Symbol:         symbol,
		Decimals:       decimals,
		CreatedAtBlock: createdAtBlock,
		Metadata:       make(map[string]any),
	}
	if err := t.validate(); err != nil {
		return nil, err
	}
	return t, nil
}

// validate checks that all fields have valid values.
func (t *Token) validate() error {
	if t.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", t.ID)
	}
	if t.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", t.ChainID)
	}
	if len(t.Address) != 20 {
		return fmt.Errorf("invalid address length: expected 20, got %d", len(t.Address))
	}
	if t.Symbol == "" {
		return fmt.Errorf("symbol must not be empty")
	}
	if t.Decimals < 0 {
		return fmt.Errorf("decimals must be non-negative, got %d", t.Decimals)
	}
	if t.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", t.CreatedAtBlock)
	}
	return nil
}

// AddressHex returns the address as a hex string with 0x prefix.
func (t *Token) AddressHex() string {
	return fmt.Sprintf("0x%x", t.Address)
}
