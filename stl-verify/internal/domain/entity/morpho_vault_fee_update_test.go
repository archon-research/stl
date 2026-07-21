package entity

import (
	"math/big"
	"strings"
	"testing"
)

func TestMorphoVaultFeeUpdate_Validate(t *testing.T) {
	addr20 := make([]byte, 20)

	tests := []struct {
		name        string
		update      MorphoVaultFeeUpdate
		wantErr     bool
		errContains string
	}{
		{
			name:   "performance fee only",
			update: MorphoVaultFeeUpdate{PerformanceFee: big.NewInt(500000000000000000)},
		},
		{
			name:   "management fee only",
			update: MorphoVaultFeeUpdate{ManagementFee: big.NewInt(3170979198)},
		},
		{
			name:   "performance recipient only",
			update: MorphoVaultFeeUpdate{PerformanceFeeRecipient: addr20},
		},
		{
			name:   "management recipient only",
			update: MorphoVaultFeeUpdate{ManagementFeeRecipient: addr20},
		},
		{
			name: "all fields set",
			update: MorphoVaultFeeUpdate{
				PerformanceFee:          big.NewInt(1),
				ManagementFee:           big.NewInt(2),
				PerformanceFeeRecipient: addr20,
				ManagementFeeRecipient:  addr20,
			},
		},
		{
			name:   "zero fee is allowed",
			update: MorphoVaultFeeUpdate{PerformanceFee: big.NewInt(0)},
		},
		{
			name:        "all nil rejected",
			update:      MorphoVaultFeeUpdate{},
			wantErr:     true,
			errContains: "at least one",
		},
		{
			name:        "negative performance fee",
			update:      MorphoVaultFeeUpdate{PerformanceFee: big.NewInt(-1)},
			wantErr:     true,
			errContains: "performanceFee must be non-negative",
		},
		{
			name:        "negative management fee",
			update:      MorphoVaultFeeUpdate{ManagementFee: big.NewInt(-1)},
			wantErr:     true,
			errContains: "managementFee must be non-negative",
		},
		{
			name:        "short performance recipient",
			update:      MorphoVaultFeeUpdate{PerformanceFeeRecipient: make([]byte, 19)},
			wantErr:     true,
			errContains: "performanceFeeRecipient must be 20 bytes",
		},
		{
			name:        "long management recipient",
			update:      MorphoVaultFeeUpdate{ManagementFeeRecipient: make([]byte, 21)},
			wantErr:     true,
			errContains: "managementFeeRecipient must be 20 bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.update.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
