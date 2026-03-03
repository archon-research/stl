package entity

// MorphoEventType represents the type of Morpho protocol event.
type MorphoEventType string

// Morpho Blue events
const (
	MorphoEventCreateMarket       MorphoEventType = "CreateMarket"
	MorphoEventSupply             MorphoEventType = "Supply"
	MorphoEventWithdraw           MorphoEventType = "Withdraw"
	MorphoEventBorrow             MorphoEventType = "Borrow"
	MorphoEventRepay              MorphoEventType = "Repay"
	MorphoEventSupplyCollateral   MorphoEventType = "SupplyCollateral"
	MorphoEventWithdrawCollateral MorphoEventType = "WithdrawCollateral"
	MorphoEventLiquidate          MorphoEventType = "Liquidate"
	MorphoEventAccrueInterest     MorphoEventType = "AccrueInterest"
	MorphoEventSetFee             MorphoEventType = "SetFee"
)

// MetaMorpho vault events
const (
	MorphoEventVaultDeposit        MorphoEventType = "VaultDeposit"
	MorphoEventVaultWithdraw       MorphoEventType = "VaultWithdraw"
	MorphoEventVaultTransfer       MorphoEventType = "VaultTransfer"
	MorphoEventVaultAccrueInterest MorphoEventType = "VaultAccrueInterest"
)

var validMorphoEventTypes = map[MorphoEventType]bool{
	MorphoEventCreateMarket:        true,
	MorphoEventSupply:              true,
	MorphoEventWithdraw:            true,
	MorphoEventBorrow:              true,
	MorphoEventRepay:               true,
	MorphoEventSupplyCollateral:    true,
	MorphoEventWithdrawCollateral:  true,
	MorphoEventLiquidate:           true,
	MorphoEventAccrueInterest:      true,
	MorphoEventSetFee:              true,
	MorphoEventVaultDeposit:        true,
	MorphoEventVaultWithdraw:       true,
	MorphoEventVaultTransfer:       true,
	MorphoEventVaultAccrueInterest: true,
}

// IsValid returns true if the MorphoEventType is a known valid type.
func (e MorphoEventType) IsValid() bool {
	return validMorphoEventTypes[e]
}

// String returns the string representation of the MorphoEventType.
func (e MorphoEventType) String() string {
	return string(e)
}
