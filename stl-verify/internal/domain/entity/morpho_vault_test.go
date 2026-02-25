package entity

import (
	"strings"
	"testing"
)

func TestNewMorphoVault(t *testing.T) {
	validAddr := make([]byte, 20)

	tests := []struct {
		name        string
		chainID     int64
		protocolID  int64
		address     []byte
		vaultName   string
		symbol      string
		assetToken  int64
		version     MorphoVaultVersion
		block       int64
		wantErr     bool
		errContains string
	}{
		{
			name: "valid V1 vault", chainID: 1, protocolID: 1, address: validAddr,
			vaultName: "Steakhouse USDC", symbol: "steakUSDC", assetToken: 1,
			version: MorphoVaultV1, block: 19000000,
		},
		{
			name: "valid V2 vault", chainID: 1, protocolID: 1, address: validAddr,
			vaultName: "Gauntlet WETH", symbol: "gtWETH", assetToken: 2,
			version: MorphoVaultV2, block: 20000000,
		},
		{
			name: "zero chain ID", chainID: 0, protocolID: 1, address: validAddr,
			vaultName: "test", symbol: "T", assetToken: 1,
			version: MorphoVaultV1, block: 1,
			wantErr: true, errContains: "chainID must be positive",
		},
		{
			name: "zero protocol ID", chainID: 1, protocolID: 0, address: validAddr,
			vaultName: "test", symbol: "T", assetToken: 1,
			version: MorphoVaultV1, block: 1,
			wantErr: true, errContains: "protocolID must be positive",
		},
		{
			name: "short address", chainID: 1, protocolID: 1, address: make([]byte, 10),
			vaultName: "test", symbol: "T", assetToken: 1,
			version: MorphoVaultV1, block: 1,
			wantErr: true, errContains: "address must be 20 bytes",
		},
		{
			name: "zero asset token", chainID: 1, protocolID: 1, address: validAddr,
			vaultName: "test", symbol: "T", assetToken: 0,
			version: MorphoVaultV1, block: 1,
			wantErr: true, errContains: "assetTokenID must be positive",
		},
		{
			name: "invalid version", chainID: 1, protocolID: 1, address: validAddr,
			vaultName: "test", symbol: "T", assetToken: 1,
			version: MorphoVaultVersion(3), block: 1,
			wantErr: true, errContains: "vaultVersion must be 1 or 2",
		},
		{
			name: "zero block", chainID: 1, protocolID: 1, address: validAddr,
			vaultName: "test", symbol: "T", assetToken: 1,
			version: MorphoVaultV1, block: 0,
			wantErr: true, errContains: "createdAtBlock must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoVault(tt.chainID, tt.protocolID, tt.address, tt.vaultName, tt.symbol, tt.assetToken, tt.version, tt.block)
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
			if got.Name != tt.vaultName {
				t.Errorf("Name = %q, want %q", got.Name, tt.vaultName)
			}
		})
	}
}
