package entity

import (
	"strings"
	"testing"
)

func TestNewFluidVault(t *testing.T) {
	validAddr := make([]byte, 20)

	tests := []struct {
		name        string
		chainID     int64
		protocolID  int64
		address     []byte
		vaultType   string
		collToken   int64
		debtToken   int64
		block       int64
		wantErr     bool
		errContains string
	}{
		{
			name: "valid vault", chainID: 1, protocolID: 1, address: validAddr,
			vaultType: "T1", collToken: 1, debtToken: 2, block: 19000000,
		},
		{
			name: "zero chain ID", chainID: 0, protocolID: 1, address: validAddr,
			vaultType: "T1", collToken: 1, debtToken: 2, block: 1,
			wantErr: true, errContains: "chainID must be positive",
		},
		{
			name: "zero protocol ID", chainID: 1, protocolID: 0, address: validAddr,
			vaultType: "T1", collToken: 1, debtToken: 2, block: 1,
			wantErr: true, errContains: "protocolID must be positive",
		},
		{
			name: "short address", chainID: 1, protocolID: 1, address: make([]byte, 10),
			vaultType: "T1", collToken: 1, debtToken: 2, block: 1,
			wantErr: true, errContains: "address must be 20 bytes",
		},
		{
			name: "empty vault type", chainID: 1, protocolID: 1, address: validAddr,
			vaultType: "", collToken: 1, debtToken: 2, block: 1,
			wantErr: true, errContains: "vaultType must not be empty",
		},
		{
			name: "zero collateral token", chainID: 1, protocolID: 1, address: validAddr,
			vaultType: "T1", collToken: 0, debtToken: 2, block: 1,
			wantErr: true, errContains: "collateralTokenID must be positive",
		},
		{
			name: "zero debt token", chainID: 1, protocolID: 1, address: validAddr,
			vaultType: "T1", collToken: 1, debtToken: 0, block: 1,
			wantErr: true, errContains: "debtTokenID must be positive",
		},
		{
			name: "zero block", chainID: 1, protocolID: 1, address: validAddr,
			vaultType: "T1", collToken: 1, debtToken: 2, block: 0,
			wantErr: true, errContains: "createdAtBlock must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFluidVault(tt.chainID, tt.protocolID, tt.address, tt.vaultType, tt.collToken, tt.debtToken, tt.block)
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
			if got.VaultType != tt.vaultType {
				t.Errorf("VaultType = %q, want %q", got.VaultType, tt.vaultType)
			}
			if got.CollateralTokenID != tt.collToken {
				t.Errorf("CollateralTokenID = %d, want %d", got.CollateralTokenID, tt.collToken)
			}
			if got.DebtTokenID != tt.debtToken {
				t.Errorf("DebtTokenID = %d, want %d", got.DebtTokenID, tt.debtToken)
			}
		})
	}
}
