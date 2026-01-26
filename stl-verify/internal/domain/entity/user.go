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
func NewUser(id int64, chainID int, address []byte, firstSeenBlock int64) (*User, error) {
	u := &User{
		ID:             id,
		ChainID:        chainID,
		Address:        address,
		FirstSeenBlock: firstSeenBlock,
		Metadata:       make(map[string]any),
	}
	if err := u.validate(); err != nil {
		return nil, err
	}
	return u, nil
}

// validate checks that all fields have valid values.
func (u *User) validate() error {
	if u.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", u.ID)
	}
	if u.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", u.ChainID)
	}
	if len(u.Address) != 20 {
		return fmt.Errorf("invalid address length: expected 20, got %d", len(u.Address))
	}
	if u.FirstSeenBlock <= 0 {
		return fmt.Errorf("firstSeenBlock must be positive, got %d", u.FirstSeenBlock)
	}
	return nil
}

// AddressHex returns the address as a hex string with 0x prefix.
func (u *User) AddressHex() string {
	return fmt.Sprintf("0x%x", u.Address)
}
