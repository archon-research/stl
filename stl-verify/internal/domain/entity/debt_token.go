package entity

import (
	"fmt"
)

// DebtToken represents debt tokens (variable and stable).
type DebtToken struct {
	ID                  int64
	ProtocolID          int64
	UnderlyingTokenID   int64
	VariableDebtAddress []byte // 20 bytes, may be nil
	StableDebtAddress   []byte // 20 bytes, may be nil (not all protocols have stable debt)
	VariableSymbol      string
	StableSymbol        string
	CreatedAtBlock      int64
	Metadata            map[string]any
}

// NewDebtToken creates a new DebtToken entity with validation.
func NewDebtToken(id, protocolID, underlyingTokenID int64, variableDebtAddress, stableDebtAddress []byte, variableSymbol, stableSymbol string) (*DebtToken, error) {
	if variableDebtAddress != nil && len(variableDebtAddress) != 20 {
		return nil, fmt.Errorf("invalid variable debt address length: expected 20, got %d", len(variableDebtAddress))
	}
	if stableDebtAddress != nil && len(stableDebtAddress) != 20 {
		return nil, fmt.Errorf("invalid stable debt address length: expected 20, got %d", len(stableDebtAddress))
	}
	return &DebtToken{
		ID:                  id,
		ProtocolID:          protocolID,
		UnderlyingTokenID:   underlyingTokenID,
		VariableDebtAddress: variableDebtAddress,
		StableDebtAddress:   stableDebtAddress,
		VariableSymbol:      variableSymbol,
		StableSymbol:        stableSymbol,
		Metadata:            make(map[string]any),
	}, nil
}

// VariableAddressHex returns the variable debt address as a hex string with 0x prefix.
func (d *DebtToken) VariableAddressHex() string {
	if d.VariableDebtAddress == nil {
		return ""
	}
	return fmt.Sprintf("0x%x", d.VariableDebtAddress)
}

// StableAddressHex returns the stable debt address as a hex string with 0x prefix.
func (d *DebtToken) StableAddressHex() string {
	if d.StableDebtAddress == nil {
		return ""
	}
	return fmt.Sprintf("0x%x", d.StableDebtAddress)
}
