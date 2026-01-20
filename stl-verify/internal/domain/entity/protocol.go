package entity

import (
	"fmt"
)

// Protocol represents a DeFi protocol (e.g., SparkLend, Aave).
type Protocol struct {
	ID             int64
	ChainID        int
	Address        []byte // 20 bytes
	Name           string
	ProtocolType   string // "lending", "rwa", etc.
	CreatedAtBlock int64
	Metadata       map[string]any
}

// NewProtocol creates a new Protocol entity with validation.
func NewProtocol(id int64, chainID int, address []byte, name, protocolType string, createdAtBlock int64) (*Protocol, error) {
	p := &Protocol{
		ID:             id,
		ChainID:        chainID,
		Address:        address,
		Name:           name,
		ProtocolType:   protocolType,
		CreatedAtBlock: createdAtBlock,
		Metadata:       make(map[string]any),
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// validate checks that all fields have valid values.
func (p *Protocol) validate() error {
	if p.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", p.ID)
	}
	if p.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", p.ChainID)
	}
	if len(p.Address) != 20 {
		return fmt.Errorf("invalid address length: expected 20, got %d", len(p.Address))
	}
	if p.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	if p.ProtocolType == "" {
		return fmt.Errorf("protocolType must not be empty")
	}
	if p.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", p.CreatedAtBlock)
	}
	return nil
}

// AddressHex returns the address as a hex string with 0x prefix.
func (p *Protocol) AddressHex() string {
	return fmt.Sprintf("0x%x", p.Address)
}
