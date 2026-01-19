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
func NewDebtToken(id, protocolID, underlyingTokenID, createdAtBlock int64, variableDebtAddress, stableDebtAddress []byte, variableSymbol, stableSymbol string) (*DebtToken, error) {
	dt := &DebtToken{
		ID:                  id,
		ProtocolID:          protocolID,
		UnderlyingTokenID:   underlyingTokenID,
		VariableDebtAddress: variableDebtAddress,
		StableDebtAddress:   stableDebtAddress,
		VariableSymbol:      variableSymbol,
		StableSymbol:        stableSymbol,
		CreatedAtBlock:      createdAtBlock,
		Metadata:            make(map[string]any),
	}
	if err := dt.validate(); err != nil {
		return nil, err
	}
	return dt, nil
}

// validate checks that all fields have valid values.
func (dt *DebtToken) validate() error {
	if dt.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", dt.ID)
	}
	if dt.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", dt.ProtocolID)
	}
	if dt.UnderlyingTokenID <= 0 {
		return fmt.Errorf("underlyingTokenID must be positive, got %d", dt.UnderlyingTokenID)
	}
	if dt.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", dt.CreatedAtBlock)
	}
	if dt.VariableDebtAddress != nil && len(dt.VariableDebtAddress) != 20 {
		return fmt.Errorf("invalid variable debt address length: expected 20, got %d", len(dt.VariableDebtAddress))
	}
	if dt.StableDebtAddress != nil && len(dt.StableDebtAddress) != 20 {
		return fmt.Errorf("invalid stable debt address length: expected 20, got %d", len(dt.StableDebtAddress))
	}
	if dt.VariableDebtAddress == nil && dt.StableDebtAddress == nil {
		return fmt.Errorf("at least one debt address must be provided")
	}
	if dt.VariableSymbol == "" && dt.StableSymbol == "" {
		return fmt.Errorf("at least one symbol must be provided")
	}
	return nil
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
