package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MorphoMarketPosition represents a user's position snapshot in a Morpho Blue market at a specific block.
type MorphoMarketPosition struct {
	ID             int64
	UserID         int64
	MorphoMarketID int64
	BlockNumber    int64
	BlockVersion   int
	Timestamp      time.Time // block timestamp
	SupplyShares   *big.Int
	BorrowShares   *big.Int
	Collateral     *big.Int
	SupplyAssets   *big.Int // computed: supplyShares * totalSupplyAssets / totalSupplyShares
	BorrowAssets   *big.Int // computed: round-up division
}

// NewMorphoMarketPosition creates a new MorphoMarketPosition entity with validation.
func NewMorphoMarketPosition(userID, morphoMarketID, blockNumber int64, blockVersion int, timestamp time.Time, supplyShares, borrowShares, collateral, supplyAssets, borrowAssets *big.Int) (*MorphoMarketPosition, error) {
	p := &MorphoMarketPosition{
		UserID:         userID,
		MorphoMarketID: morphoMarketID,
		BlockNumber:    blockNumber,
		BlockVersion:   blockVersion,
		Timestamp:      timestamp,
		SupplyShares:   supplyShares,
		BorrowShares:   borrowShares,
		Collateral:     collateral,
		SupplyAssets:   supplyAssets,
		BorrowAssets:   borrowAssets,
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("NewMorphoMarketPosition: %w", err)
	}
	return p, nil
}

func (p *MorphoMarketPosition) Validate() error {
	if p.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", p.UserID)
	}
	if p.MorphoMarketID <= 0 {
		return fmt.Errorf("morphoMarketID must be positive, got %d", p.MorphoMarketID)
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
	if p.SupplyShares == nil {
		return fmt.Errorf("supplyShares must not be nil")
	}
	if p.BorrowShares == nil {
		return fmt.Errorf("borrowShares must not be nil")
	}
	if p.Collateral == nil {
		return fmt.Errorf("collateral must not be nil")
	}
	if p.SupplyAssets == nil {
		return fmt.Errorf("supplyAssets must not be nil")
	}
	if p.BorrowAssets == nil {
		return fmt.Errorf("borrowAssets must not be nil")
	}
	return nil
}

// ComputeSupplyAssets calculates supply assets from shares using: supplyShares * totalSupplyAssets / totalSupplyShares (round down).
// Returns 0 if totalSupplyShares is zero.
func ComputeSupplyAssets(supplyShares, totalSupplyAssets, totalSupplyShares *big.Int) *big.Int {
	if supplyShares == nil || totalSupplyAssets == nil || totalSupplyShares == nil || totalSupplyShares.Sign() == 0 {
		return new(big.Int)
	}
	num := new(big.Int).Mul(supplyShares, totalSupplyAssets)
	return new(big.Int).Div(num, totalSupplyShares)
}

// ComputeBorrowAssets calculates borrow assets from shares using: (borrowShares * totalBorrowAssets + totalBorrowShares - 1) / totalBorrowShares (round up).
// Returns 0 if totalBorrowShares is zero.
func ComputeBorrowAssets(borrowShares, totalBorrowAssets, totalBorrowShares *big.Int) *big.Int {
	if borrowShares == nil || totalBorrowAssets == nil || totalBorrowShares == nil || totalBorrowShares.Sign() == 0 {
		return new(big.Int)
	}
	if borrowShares.Sign() == 0 {
		return new(big.Int)
	}
	// (borrowShares * totalBorrowAssets + totalBorrowShares - 1) / totalBorrowShares
	num := new(big.Int).Mul(borrowShares, totalBorrowAssets)
	num.Add(num, new(big.Int).Sub(totalBorrowShares, big.NewInt(1)))
	return new(big.Int).Div(num, totalBorrowShares)
}
