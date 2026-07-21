package entity

import (
	"fmt"
	"math/big"
)

// MorphoVaultFeeUpdate is a partial update of a Morpho VaultV2's fee
// configuration on morpho_vault. Each field is optional: a nil field leaves the
// corresponding column untouched, so one struct can carry a single Set* fee
// event (only the field that changed is set) without clobbering the others.
//
// Fees are raw on-chain uint96 WAD values (not basis points): PerformanceFee is
// a WAD fraction of accrued interest, ManagementFee is a WAD per-second rate;
// both stay non-negative. Recipients are 20-byte addresses.
type MorphoVaultFeeUpdate struct {
	PerformanceFee          *big.Int
	ManagementFee           *big.Int
	PerformanceFeeRecipient []byte
	ManagementFeeRecipient  []byte
}

func (u MorphoVaultFeeUpdate) Validate() error {
	if u.PerformanceFee == nil && u.ManagementFee == nil &&
		u.PerformanceFeeRecipient == nil && u.ManagementFeeRecipient == nil {
		return fmt.Errorf("at least one fee field must be set")
	}
	if u.PerformanceFee != nil && u.PerformanceFee.Sign() < 0 {
		return fmt.Errorf("performanceFee must be non-negative, got %s", u.PerformanceFee)
	}
	if u.ManagementFee != nil && u.ManagementFee.Sign() < 0 {
		return fmt.Errorf("managementFee must be non-negative, got %s", u.ManagementFee)
	}
	if u.PerformanceFeeRecipient != nil && len(u.PerformanceFeeRecipient) != 20 {
		return fmt.Errorf("performanceFeeRecipient must be 20 bytes, got %d", len(u.PerformanceFeeRecipient))
	}
	if u.ManagementFeeRecipient != nil && len(u.ManagementFeeRecipient) != 20 {
		return fmt.Errorf("managementFeeRecipient must be 20 bytes, got %d", len(u.ManagementFeeRecipient))
	}
	return nil
}
