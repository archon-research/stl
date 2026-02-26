package entity

import "testing"

func TestMorphoEventType_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		event    MorphoEventType
		expected bool
	}{
		{"CreateMarket", MorphoEventCreateMarket, true},
		{"Supply", MorphoEventSupply, true},
		{"Withdraw", MorphoEventWithdraw, true},
		{"Borrow", MorphoEventBorrow, true},
		{"Repay", MorphoEventRepay, true},
		{"SupplyCollateral", MorphoEventSupplyCollateral, true},
		{"WithdrawCollateral", MorphoEventWithdrawCollateral, true},
		{"Liquidate", MorphoEventLiquidate, true},
		{"AccrueInterest", MorphoEventAccrueInterest, true},
		{"SetFee", MorphoEventSetFee, true},
		{"VaultDeposit", MorphoEventVaultDeposit, true},
		{"VaultWithdraw", MorphoEventVaultWithdraw, true},
		{"VaultTransfer", MorphoEventVaultTransfer, true},
		{"VaultAccrueInterest", MorphoEventVaultAccrueInterest, true},
		{"invalid", MorphoEventType("Invalid"), false},
		{"empty", MorphoEventType(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.IsValid(); got != tt.expected {
				t.Errorf("MorphoEventType(%q).IsValid() = %v, want %v", tt.event, got, tt.expected)
			}
		})
	}
}

func TestMorphoEventType_String(t *testing.T) {
	if got := MorphoEventSupply.String(); got != "Supply" {
		t.Errorf("MorphoEventSupply.String() = %q, want %q", got, "Supply")
	}
}
