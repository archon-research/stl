package entity

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// User represents a wallet address that interacts with protocols.
//
// FirstSeenBlock is a pointer so "no block context" (nil) is distinct from
// "genesis block" (0). Off-block-context sources (e.g. GraphQL indexers) set
// nil, which inserts NULL and lets the LEAST() merge preserve any existing
// block; a 0 would clobber it (VEC-353).
type User struct {
	ID             int64
	ChainID        int64
	Address        common.Address
	FirstSeenBlock *int64
	Metadata       map[string]any
}

// NewUser creates a new User entity with validation. firstSeenBlock is nil when
// the caller has no block context.
func NewUser(id, chainID int64, address common.Address, firstSeenBlock *int64) (*User, error) {
	u := &User{
		ID:             id,
		ChainID:        chainID,
		Address:        address,
		FirstSeenBlock: firstSeenBlock,
		Metadata:       make(map[string]any),
	}
	if err := u.Validate(); err != nil {
		return nil, fmt.Errorf("NewUser: %w", err)
	}
	return u, nil
}

// validate checks that all fields have valid values.
func (u *User) Validate() error {
	if u.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", u.ID)
	}
	if u.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", u.ChainID)
	}
	if u.Address == (common.Address{}) {
		return fmt.Errorf("address cannot be empty")
	}
	// nil means "no block context" (a valid state for off-chain sources); a
	// supplied block must be a real, positive block; 0 would clobber the
	// stored value via the LEAST() merge (VEC-353).
	if u.FirstSeenBlock != nil && *u.FirstSeenBlock <= 0 {
		return fmt.Errorf("firstSeenBlock must be positive when set, got %d", *u.FirstSeenBlock)
	}
	return nil
}

// AddressHex returns the address as a hex string with 0x prefix.
func (u *User) AddressHex() string {
	return u.Address.Hex()
}
