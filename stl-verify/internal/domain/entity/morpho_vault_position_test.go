package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestNewMorphoVaultPosition(t *testing.T) {
	zero := big.NewInt(0)
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		userID      int64
		vaultID     int64
		block       int64
		version     int
		timestamp   time.Time
		shares      *big.Int
		assets      *big.Int
		wantErr     bool
		errContains string
	}{
		{
			name: "valid position", userID: 1, vaultID: 1, block: 100, version: 0, timestamp: ts,
			shares: big.NewInt(1000), assets: big.NewInt(1050),
		},
		{
			name: "zero user ID", userID: 0, vaultID: 1, block: 100, version: 0, timestamp: ts,
			shares: zero, assets: zero,
			wantErr: true, errContains: "userID must be positive",
		},
		{
			name: "zero vault ID", userID: 1, vaultID: 0, block: 100, version: 0, timestamp: ts,
			shares: zero, assets: zero,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "zero timestamp", userID: 1, vaultID: 1, block: 100, version: 0, timestamp: time.Time{},
			shares: zero, assets: zero,
			wantErr: true, errContains: "timestamp must not be zero",
		},
		{
			name: "nil shares", userID: 1, vaultID: 1, block: 100, version: 0, timestamp: ts,
			shares: nil, assets: zero,
			wantErr: true, errContains: "shares must not be nil",
		},
		{
			name: "nil assets", userID: 1, vaultID: 1, block: 100, version: 0, timestamp: ts,
			shares: zero, assets: nil,
			wantErr: true, errContains: "assets must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoVaultPosition(tt.userID, tt.vaultID, tt.block, tt.version, tt.timestamp, tt.shares, tt.assets)
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
