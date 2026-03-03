package entity

import (
	"math/big"
	"strings"
	"testing"
)

func TestNewMapleBorrower(t *testing.T) {
	validAmount := big.NewInt(200_000_000)

	tests := []struct {
		name         string
		loanID       int64
		userID       int64
		protocolID   int64
		poolAsset    string
		poolDecimals int
		amount       *big.Int
		blockNumber  int64
		blockVersion int
		wantErr      bool
		errContains  string
	}{
		{
			name:         "valid maple borrower",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
		},
		{
			name:         "valid with zero amount",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "WBTC",
			poolDecimals: 8,
			amount:       big.NewInt(0),
			blockNumber:  21000000,
			blockVersion: 0,
		},
		{
			name:         "valid with large amount",
			loanID:       1,
			userID:       1,
			protocolID:   1,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil),
			blockNumber:  21000000,
			blockVersion: 2,
		},
		{
			name:         "valid with zero decimals",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "WETH",
			poolDecimals: 0,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
		},
		{
			name:         "zero loanID",
			loanID:       0,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "loanID must be positive",
		},
		{
			name:         "negative loanID",
			loanID:       -1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "loanID must be positive",
		},
		{
			name:         "zero userID",
			loanID:       1,
			userID:       0,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "userID must be positive",
		},
		{
			name:         "negative userID",
			loanID:       1,
			userID:       -1,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "userID must be positive",
		},
		{
			name:         "zero protocolID",
			loanID:       1,
			userID:       10,
			protocolID:   0,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "protocolID must be positive",
		},
		{
			name:         "negative protocolID",
			loanID:       1,
			userID:       10,
			protocolID:   -5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "protocolID must be positive",
		},
		{
			name:         "empty poolAsset",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "poolAsset must not be empty",
		},
		{
			name:         "negative poolDecimals",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: -1,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "poolDecimals must be non-negative",
		},
		{
			name:         "nil amount",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       nil,
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "amount must not be nil",
		},
		{
			name:         "negative amount",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       big.NewInt(-100),
			blockNumber:  21000000,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "amount must be non-negative",
		},
		{
			name:         "zero blockNumber",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  0,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "blockNumber must be positive",
		},
		{
			name:         "negative blockNumber",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  -1,
			blockVersion: 0,
			wantErr:      true,
			errContains:  "blockNumber must be positive",
		},
		{
			name:         "negative blockVersion",
			loanID:       1,
			userID:       10,
			protocolID:   5,
			poolAsset:    "USDC",
			poolDecimals: 6,
			amount:       validAmount,
			blockNumber:  21000000,
			blockVersion: -1,
			wantErr:      true,
			errContains:  "blockVersion must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mb, err := NewMapleBorrower(tt.loanID, tt.userID, tt.protocolID, tt.poolAsset, tt.poolDecimals, tt.amount, tt.blockNumber, tt.blockVersion)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewMapleBorrower() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewMapleBorrower() error = %v, want error containing %q", err, tt.errContains)
				}
				if mb != nil {
					t.Error("NewMapleBorrower() expected nil on error")
				}
				return
			}
			if err != nil {
				t.Errorf("NewMapleBorrower() unexpected error = %v", err)
				return
			}
			if mb == nil {
				t.Errorf("NewMapleBorrower() returned nil")
				return
			}
			if mb.ID != 0 {
				t.Errorf("NewMapleBorrower() ID = %d, want 0 (set by DB)", mb.ID)
			}
			if mb.LoanID != tt.loanID {
				t.Errorf("NewMapleBorrower() LoanID = %d, want %d", mb.LoanID, tt.loanID)
			}
			if mb.UserID != tt.userID {
				t.Errorf("NewMapleBorrower() UserID = %d, want %d", mb.UserID, tt.userID)
			}
			if mb.ProtocolID != tt.protocolID {
				t.Errorf("NewMapleBorrower() ProtocolID = %d, want %d", mb.ProtocolID, tt.protocolID)
			}
			if mb.PoolAsset != tt.poolAsset {
				t.Errorf("NewMapleBorrower() PoolAsset = %q, want %q", mb.PoolAsset, tt.poolAsset)
			}
			if mb.PoolDecimals != tt.poolDecimals {
				t.Errorf("NewMapleBorrower() PoolDecimals = %d, want %d", mb.PoolDecimals, tt.poolDecimals)
			}
			if mb.Amount.Cmp(tt.amount) != 0 {
				t.Errorf("NewMapleBorrower() Amount = %v, want %v", mb.Amount, tt.amount)
			}
			if mb.BlockNumber != tt.blockNumber {
				t.Errorf("NewMapleBorrower() BlockNumber = %d, want %d", mb.BlockNumber, tt.blockNumber)
			}
			if mb.BlockVersion != tt.blockVersion {
				t.Errorf("NewMapleBorrower() BlockVersion = %d, want %d", mb.BlockVersion, tt.blockVersion)
			}
		})
	}
}
