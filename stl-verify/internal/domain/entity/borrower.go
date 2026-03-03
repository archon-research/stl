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
	Amount       *big.Int  // current total debt amount
	Change       *big.Int  // change from previous snapshot
	EventType    EventType // The type of event that triggered this position snapshot (e.g., "Borrow", "Repay", "LiquidationCall")
	TxHash       []byte    // The transaction hash
}

// NewBorrower creates a new Borrower entity.
func NewBorrower(id, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change *big.Int, eventType EventType, txHash []byte) (*Borrower, error) {
	b := &Borrower{
		ID:           id,
		UserID:       userID,
		ProtocolID:   protocolID,
		TokenID:      tokenID,
		BlockNumber:  blockNumber,
		BlockVersion: blockVersion,
		Amount:       amount,
		Change:       change,
		EventType:    eventType,
		TxHash:       txHash,
	}
	if err := b.validate(); err != nil {
		return nil, err
	}
	return b, nil
}

// validate checks that all fields have valid values.
func (b *Borrower) validate() error {
	if b.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", b.ID)
	}
	if b.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", b.UserID)
	}
	if b.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", b.ProtocolID)
	}
	if b.TokenID <= 0 {
		return fmt.Errorf("tokenID must be positive, got %d", b.TokenID)
	}
	if b.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", b.BlockNumber)
	}
	if b.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", b.BlockVersion)
	}
	if b.Amount == nil {
		return fmt.Errorf("amount must not be nil")
	}
	if b.Amount.Sign() < 0 {
		return fmt.Errorf("amount must be non-negative")
	}
	if b.Change == nil {
		return fmt.Errorf("change must not be nil")
	}
	if !b.EventType.IsValid() {
		return fmt.Errorf("invalid eventType: %s", b.EventType)
	}
	if len(b.TxHash) == 0 {
		return fmt.Errorf("txHash must not be empty")
	}
	return nil
}
