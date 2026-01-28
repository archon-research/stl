package entity

import (
	"fmt"
	"math/big"
)

// BorrowerCollateral represents a user's collateral position at a specific block.
type BorrowerCollateral struct {
	ID                int64
	UserID            int64
	ProtocolID        int64
	TokenID           int64
	BlockNumber       int64
	BlockVersion      int
	Amount            *big.Int // current total collateral amount
	Change            *big.Int // change from previous snapshot
	EventType         string   // type of event that triggered this position snapshot
	TxHash            string   // transaction hash
	CollateralEnabled bool     // whether this asset is enabled as collateral
}

// NewBorrowerCollateral creates a new BorrowerCollateral entity.
func NewBorrowerCollateral(id, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change *big.Int, eventType, txHash string, collateralEnabled bool) (*BorrowerCollateral, error) {
	bc := &BorrowerCollateral{
		ID:                id,
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
	}
	if err := bc.validate(); err != nil {
		return nil, err
	}
	return bc, nil
}

// validate checks that all fields have valid values.
func (bc *BorrowerCollateral) validate() error {
	if bc.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", bc.ID)
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
	if bc.EventType == "" {
		return fmt.Errorf("eventType must not be empty")
	}
	if bc.TxHash == "" {
		return fmt.Errorf("txHash must not be empty")
	}
	return nil
}
