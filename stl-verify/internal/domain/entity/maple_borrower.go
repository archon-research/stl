package entity

import (
	"fmt"
	"math/big"
)

// MapleBorrower represents a Maple Finance borrower's debt position at a specific block.
// Unlike the generic Borrower entity, MapleBorrower uses a pool asset symbol (e.g., "USDC")
// instead of a token ID, because Maple data comes from off-chain snapshots without
// on-chain contract addresses.
//
// Each MapleBorrower references a MapleLoan via LoanID, which contains the loan metadata.
type MapleBorrower struct {
	ID           int64
	LoanID       int64 // FK to maple_loan.id
	UserID       int64
	ProtocolID   int64
	PoolAsset    string   // e.g., "USDC", "WBTC"
	PoolDecimals int      // decimal precision for the pool asset
	Amount       *big.Int // current debt amount in native decimals
	BlockNumber  int64
	BlockVersion int
}

// NewMapleBorrower creates a new MapleBorrower entity with validation.
func NewMapleBorrower(loanID, userID, protocolID int64, poolAsset string, poolDecimals int, amount *big.Int, blockNumber int64, blockVersion int) (*MapleBorrower, error) {
	b := &MapleBorrower{
		LoanID:       loanID,
		UserID:       userID,
		ProtocolID:   protocolID,
		PoolAsset:    poolAsset,
		PoolDecimals: poolDecimals,
		Amount:       amount,
		BlockNumber:  blockNumber,
		BlockVersion: blockVersion,
	}
	if err := b.validate(); err != nil {
		return nil, err
	}
	return b, nil
}

// validate checks that all fields have valid values.
func (b *MapleBorrower) validate() error {
	if b.LoanID <= 0 {
		return fmt.Errorf("loanID must be positive, got %d", b.LoanID)
	}
	if b.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", b.UserID)
	}
	if b.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", b.ProtocolID)
	}
	if b.PoolAsset == "" {
		return fmt.Errorf("poolAsset must not be empty")
	}
	if b.PoolDecimals < 0 {
		return fmt.Errorf("poolDecimals must be non-negative, got %d", b.PoolDecimals)
	}
	if b.Amount == nil {
		return fmt.Errorf("amount must not be nil")
	}
	if b.Amount.Sign() < 0 {
		return fmt.Errorf("amount must be non-negative")
	}
	if b.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", b.BlockNumber)
	}
	if b.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", b.BlockVersion)
	}
	return nil
}
