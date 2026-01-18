package entity

import (
	"fmt"
	"math/big"
)

// Borrower represents a user's debt position at a specific block.
type Borrower struct {
	ID           int64
	UserID       int64
	ProtocolID   int64
	TokenID      int64
	BlockNumber  int64
	BlockVersion int
	Amount       *big.Int // current total debt amount
	Change       *big.Int // change from previous snapshot
}

// NewBorrower creates a new Borrower entity.
func NewBorrower(id, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change *big.Int) (*Borrower, error) {
	if amount == nil {
		return nil, fmt.Errorf("amount must not be nil")
	}
	if change == nil {
		return nil, fmt.Errorf("change must not be nil")
	}
	return &Borrower{
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
