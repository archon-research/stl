package entity

import (
	"math/big"
	"strings"
	"testing"
)

func TestNewMorphoVaultPosition(t *testing.T) {
	zero := big.NewInt(0)
	txHash := []byte{0x01, 0x02}

	tests := []struct {
		name        string
		userID      int64
		vaultID     int64
		block       int64
		version     int
		shares      *big.Int
		assets      *big.Int
		eventType   MorphoEventType
		txHash      []byte
		wantErr     bool
		errContains string
	}{
		{
			name: "valid position", userID: 1, vaultID: 1, block: 100, version: 0,
			shares: big.NewInt(1000), assets: big.NewInt(1050),
			eventType: MorphoEventVaultDeposit, txHash: txHash,
		},
		{
			name: "zero user ID", userID: 0, vaultID: 1, block: 100, version: 0,
			shares: zero, assets: zero,
			eventType: MorphoEventVaultDeposit, txHash: txHash,
			wantErr: true, errContains: "userID must be positive",
		},
		{
			name: "zero vault ID", userID: 1, vaultID: 0, block: 100, version: 0,
			shares: zero, assets: zero,
			eventType: MorphoEventVaultDeposit, txHash: txHash,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "nil shares", userID: 1, vaultID: 1, block: 100, version: 0,
			shares: nil, assets: zero,
			eventType: MorphoEventVaultDeposit, txHash: txHash,
			wantErr: true, errContains: "shares must not be nil",
		},
		{
			name: "nil assets", userID: 1, vaultID: 1, block: 100, version: 0,
			shares: zero, assets: nil,
			eventType: MorphoEventVaultDeposit, txHash: txHash,
			wantErr: true, errContains: "assets must not be nil",
		},
		{
			name: "invalid event type", userID: 1, vaultID: 1, block: 100, version: 0,
			shares: zero, assets: zero,
			eventType: MorphoEventType("BadType"), txHash: txHash,
			wantErr: true, errContains: "invalid eventType",
		},
		{
			name: "empty tx hash", userID: 1, vaultID: 1, block: 100, version: 0,
			shares: zero, assets: zero,
			eventType: MorphoEventVaultDeposit, txHash: nil,
			wantErr: true, errContains: "txHash must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoVaultPosition(tt.userID, tt.vaultID, tt.block, tt.version, tt.shares, tt.assets, tt.eventType, tt.txHash)
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
			if got.UserID != tt.userID {
				t.Errorf("UserID = %d, want %d", got.UserID, tt.userID)
			}
		})
	}
}
