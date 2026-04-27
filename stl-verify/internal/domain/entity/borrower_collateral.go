package entity

import (
	"fmt"
	"math/big"
	"time"
)

// BorrowerCollateral represents a user's collateral position at a specific block.
type BorrowerCollateral struct {
	UserID            int64
	ProtocolID        int64
	TokenID           int64
	BlockNumber       int64
	BlockVersion      int
	Amount            *big.Int  // current total collateral amount
	Change            *big.Int  // change from previous snapshot
	EventType         EventType // type of event that triggered this position snapshot
	TxHash            []byte    // transaction hash
	CollateralEnabled bool      // whether this asset is enabled as collateral
	CreatedAt         time.Time // block timestamp — deterministic for hypertable dedup
}

// NewBorrowerCollateral creates a new BorrowerCollateral entity.
func NewBorrowerCollateral(userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change *big.Int, eventType EventType, txHash []byte, collateralEnabled bool, createdAt time.Time) (*BorrowerCollateral, error) {
	bc := &BorrowerCollateral{
		UserID:            userID,
		ProtocolID:        protocolID,
		TokenID:           tokenID,
		BlockNumber:       blockNumber,
		BlockVersion:      blockVersion,
		Amount:            amount,
		Change:            change,
		EventType:         eventType,
		TxHash:            txHash,
		CollateralEnabled: collateralEnabled,
		CreatedAt:         createdAt,
	}
	if err := bc.Validate(); err != nil {
		return nil, fmt.Errorf("NewBorrowerCollateral: %w", err)
	}
	return bc, nil
}

// validate checks that all fields have valid values.
func (bc *BorrowerCollateral) Validate() error {
	if bc.CreatedAt.IsZero() {
		return fmt.Errorf("createdAt must be set explicitly (block timestamp)")
	}
	if bc.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", bc.UserID)
	}
	if bc.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", bc.ProtocolID)
	}
	if bc.TokenID <= 0 {
		return fmt.Errorf("tokenID must be positive, got %d", bc.TokenID)
	}
	if bc.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", bc.BlockNumber)
	}
	if bc.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", bc.BlockVersion)
	}
	if bc.Amount == nil {
		return fmt.Errorf("amount must not be nil")
	}
	if bc.Amount.Sign() < 0 {
		return fmt.Errorf("amount must be non-negative")
	}
	if bc.Change == nil {
		return fmt.Errorf("change must not be nil")
	}
	if !bc.EventType.IsValid() {
		return fmt.Errorf("invalid eventType: %s", bc.EventType)
	}
	if len(bc.TxHash) == 0 {
		return fmt.Errorf("txHash must not be empty")
	}
	return nil
}
