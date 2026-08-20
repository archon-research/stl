package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MorphoVaultFee is a snapshot of a Morpho VaultV2's full fee configuration at a
// single block. A VaultV2 carries one fee config — a performance fee, a
// management fee, and a recipient address for each — set on-chain by four
// separate Set* events.
//
// PerformanceFee and ManagementFee are raw on-chain uint96 WAD values, unscaled:
// PerformanceFee is a WAD fraction of accrued interest (1e18 = 100%),
// ManagementFee is a WAD per-second rate. Both stay non-negative. The recipients
// are 20-byte addresses; the zero address is the contract default (no recipient
// set). Each row is an end-of-block snapshot: when any Set* fee event fires, the
// indexer reads all four fields off the vault at the block hash and writes them
// together, so the latest row per vault is the full current fee state. Same-block
// sibling fee events read identical values and dedupe to one row.
type MorphoVaultFee struct {
	MorphoVaultID           int64
	PerformanceFee          *big.Int
	ManagementFee           *big.Int
	PerformanceFeeRecipient []byte // 20 bytes
	ManagementFeeRecipient  []byte // 20 bytes
	BlockNumber             int64
	BlockVersion            int
	Timestamp               time.Time
}

// NewMorphoVaultFee creates a new MorphoVaultFee entity with validation.
func NewMorphoVaultFee(morphoVaultID int64, performanceFee, managementFee *big.Int, performanceFeeRecipient, managementFeeRecipient []byte, blockNumber int64, blockVersion int, timestamp time.Time) (*MorphoVaultFee, error) {
	f := &MorphoVaultFee{
		MorphoVaultID:           morphoVaultID,
		PerformanceFee:          performanceFee,
		ManagementFee:           managementFee,
		PerformanceFeeRecipient: performanceFeeRecipient,
		ManagementFeeRecipient:  managementFeeRecipient,
		BlockNumber:             blockNumber,
		BlockVersion:            blockVersion,
		Timestamp:               timestamp,
	}
	if err := f.Validate(); err != nil {
		return nil, fmt.Errorf("NewMorphoVaultFee: %w", err)
	}
	return f, nil
}

func (f *MorphoVaultFee) Validate() error {
	if f.MorphoVaultID <= 0 {
		return fmt.Errorf("morphoVaultID must be positive, got %d", f.MorphoVaultID)
	}
	if err := requireNonNegativeBigInt("performanceFee", f.PerformanceFee); err != nil {
		return err
	}
	if err := requireNonNegativeBigInt("managementFee", f.ManagementFee); err != nil {
		return err
	}
	if len(f.PerformanceFeeRecipient) != 20 {
		return fmt.Errorf("performanceFeeRecipient must be 20 bytes, got %d", len(f.PerformanceFeeRecipient))
	}
	if len(f.ManagementFeeRecipient) != 20 {
		return fmt.Errorf("managementFeeRecipient must be 20 bytes, got %d", len(f.ManagementFeeRecipient))
	}
	if f.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", f.BlockNumber)
	}
	if f.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", f.BlockVersion)
	}
	if f.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	return nil
}
