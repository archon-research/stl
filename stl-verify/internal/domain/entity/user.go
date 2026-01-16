package entity

import (
	"fmt"
)

// User represents a wallet address that interacts with protocols.
type User struct {
	ID             int64
	ChainID        int
	Address        []byte // 20 bytes
	FirstSeenBlock int64
	Metadata       map[string]any
}

// NewUser creates a new User entity with validation.
func NewUser(id int64, chainID int, address []byte) (*User, error) {
	if len(address) != 20 {
		return nil, fmt.Errorf("invalid address length: expected 20, got %d", len(address))
	}
	return &User{
		ID:       id,
		ChainID:  chainID,
		Address:  address,
		Metadata: make(map[string]any),
	}, nil
}

// AddressHex returns the address as a hex string with 0x prefix.
func (u *User) AddressHex() string {
	return fmt.Sprintf("0x%x", u.Address)
}
