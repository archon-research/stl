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
func NewProtocol(id int64, chainID int, address []byte, name, protocolType string) (*Protocol, error) {
	if len(address) != 20 {
		return nil, fmt.Errorf("invalid address length: expected 20, got %d", len(address))
	}
	return &Protocol{
		ID:           id,
		ChainID:      chainID,
		Address:      address,
		Name:         name,
		ProtocolType: protocolType,
		Metadata:     make(map[string]any),
	}, nil
}

// AddressHex returns the address as a hex string with 0x prefix.
func (p *Protocol) AddressHex() string {
	return fmt.Sprintf("0x%x", p.Address)
}
