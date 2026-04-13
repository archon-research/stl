package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MorphoVaultPosition represents a user's position snapshot in a MetaMorpho vault at a specific block.
type MorphoVaultPosition struct {
	ID            int64
	UserID        int64
	MorphoVaultID int64
	BlockNumber   int64
	BlockVersion  int
	Timestamp     time.Time // block timestamp
	Shares        *big.Int
	Assets        *big.Int
}

// NewMorphoVaultPosition creates a new MorphoVaultPosition entity with validation.
func NewMorphoVaultPosition(userID, morphoVaultID, blockNumber int64, blockVersion int, timestamp time.Time, shares, assets *big.Int) (*MorphoVaultPosition, error) {
	p := &MorphoVaultPosition{
		UserID:        userID,
		MorphoVaultID: morphoVaultID,
		BlockNumber:   blockNumber,
		BlockVersion:  blockVersion,
		Timestamp:     timestamp,
		Shares:        shares,
		Assets:        assets,
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *MorphoVaultPosition) Validate() error {
	if p.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", p.UserID)
	}
	if p.MorphoVaultID <= 0 {
		return fmt.Errorf("morphoVaultID must be positive, got %d", p.MorphoVaultID)
	}
	if p.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", p.BlockNumber)
	}
	if p.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", p.BlockVersion)
	}
	if p.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	if p.Shares == nil {
		return fmt.Errorf("shares must not be nil")
	}
	if p.Assets == nil {
		return fmt.Errorf("assets must not be nil")
	}
	return nil
}
