package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MapleVaultPosition is a per-block snapshot of one user's position in a Syrup vault.
//
// build_id and processing_version are assigned at the persistence layer.
type MapleVaultPosition struct {
	UserID         int64
	MapleVaultID   int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	Shares         *big.Int
	Assets         *big.Int
}

// NewMapleVaultPosition creates a MapleVaultPosition entity with validation.
func NewMapleVaultPosition(
	userID, mapleVaultID, blockNumber int64,
	blockVersion int,
	timestamp time.Time,
	shares, assets *big.Int,
) (*MapleVaultPosition, error) {
	p := &MapleVaultPosition{
		UserID:         userID,
		MapleVaultID:   mapleVaultID,
		BlockNumber:    blockNumber,
		BlockVersion:   blockVersion,
		BlockTimestamp: timestamp,
		Shares:         shares,
		Assets:         assets,
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("NewMapleVaultPosition: %w", err)
	}
	return p, nil
}

// Validate ensures the entity satisfies all invariants.
func (p *MapleVaultPosition) Validate() error {
	if p.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", p.UserID)
	}
	if p.MapleVaultID <= 0 {
		return fmt.Errorf("mapleVaultID must be positive, got %d", p.MapleVaultID)
	}
	if p.BlockNumber < 0 {
		return fmt.Errorf("blockNumber must be non-negative, got %d", p.BlockNumber)
	}
	if p.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", p.BlockVersion)
	}
	if p.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if p.Shares == nil {
		return fmt.Errorf("shares must not be nil")
	}
	if p.Assets == nil {
		return fmt.Errorf("assets must not be nil")
	}
	if p.Shares.Sign() < 0 {
		return fmt.Errorf("shares must be non-negative, got %s", p.Shares)
	}
	if p.Assets.Sign() < 0 {
		return fmt.Errorf("assets must be non-negative, got %s", p.Assets)
	}
	return nil
}
