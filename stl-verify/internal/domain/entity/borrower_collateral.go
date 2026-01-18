package entity

import (
	"fmt"
	"math/big"
)

// BorrowerCollateral represents a user's collateral position at a specific block.
type BorrowerCollateral struct {
	ID           int64
	UserID       int64
	ProtocolID   int64
	TokenID      int64
	BlockNumber  int64
	BlockVersion int
	Amount       *big.Int // current total collateral amount
	Change       *big.Int // change from previous snapshot
}

// NewBorrowerCollateral creates a new BorrowerCollateral entity.
func NewBorrowerCollateral(id, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change *big.Int) (*BorrowerCollateral, error) {
	if amount == nil {
		return nil, fmt.Errorf("amount must not be nil")
	}
	if change == nil {
		return nil, fmt.Errorf("change must not be nil")
	}
	return &BorrowerCollateral{
		ID:           id,
		UserID:       userID,
		ProtocolID:   protocolID,
		TokenID:      tokenID,
		BlockNumber:  blockNumber,
		BlockVersion: blockVersion,
		Amount:       amount,
		Change:       change,
	}, nil
}
