package entity

import (
	"math/big"
	"testing"
)

func TestNewBorrowerCollateral(t *testing.T) {
	validAmount := big.NewInt(1000)
	validChange := big.NewInt(100)

	tests := []struct {
		name         string
		id           int64
		userID       int64
		protocolID   int64
		tokenID      int64
		blockNumber  int64
		blockVersion int
		amount       *big.Int
		change       *big.Int
		wantErr      bool
		errContains  string
	}{
		{
			name:         "valid borrower collateral",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			wantErr:      false,
		},
		{
			name:         "zero id",
			id:           0,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			wantErr:      true,
			errContains:  "id must be positive",
		},
		{
			name:         "negative id",
			id:           -1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			wantErr:      true,
			errContains:  "id must be positive",
		},
		{
			name:         "zero userID",
			id:           1,
			userID:       0,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			wantErr:      true,
			errContains:  "userID must be positive",
		},
		{
			name:         "zero protocolID",
			id:           1,
			userID:       10,
			protocolID:   0,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			wantErr:      true,
			errContains:  "protocolID must be positive",
		},
		{
			name:         "zero tokenID",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      0,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			wantErr:      true,
			errContains:  "tokenID must be positive",
		},
		{
			name:         "zero blockNumber",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  0,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			wantErr:      true,
			errContains:  "blockNumber must be positive",
		},
		{
			name:         "negative blockVersion",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: -1,
			amount:       validAmount,
			change:       validChange,
			wantErr:      true,
			errContains:  "blockVersion must be non-negative",
		},
		{
			name:         "nil amount",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       nil,
			change:       validChange,
			wantErr:      true,
			errContains:  "amount must not be nil",
		},
		{
			name:         "negative amount",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       big.NewInt(-100),
			change:       validChange,
			wantErr:      true,
			errContains:  "amount must be non-negative",
		},
		{
			name:         "nil change",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       nil,
			wantErr:      true,
			errContains:  "change must not be nil",
		},
		{
			name:         "zero amount and change",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       big.NewInt(0),
			change:       big.NewInt(0),
			wantErr:      false,
		},
		{
			name:         "negative change allowed",
			id:           1,
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       big.NewInt(-50),
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bc, err := NewBorrowerCollateral(tt.id, tt.userID, tt.protocolID, tt.tokenID, tt.blockNumber, tt.blockVersion, tt.amount, tt.change)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewBorrowerCollateral() expected error, got nil")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("NewBorrowerCollateral() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewBorrowerCollateral() unexpected error = %v", err)
				return
			}
			if bc == nil {
				t.Errorf("NewBorrowerCollateral() returned nil")
				return
			}
			if bc.ID != tt.id {
				t.Errorf("NewBorrowerCollateral() ID = %v, want %v", bc.ID, tt.id)
			}
			if bc.UserID != tt.userID {
				t.Errorf("NewBorrowerCollateral() UserID = %v, want %v", bc.UserID, tt.userID)
			}
			if bc.ProtocolID != tt.protocolID {
				t.Errorf("NewBorrowerCollateral() ProtocolID = %v, want %v", bc.ProtocolID, tt.protocolID)
			}
			if bc.TokenID != tt.tokenID {
				t.Errorf("NewBorrowerCollateral() TokenID = %v, want %v", bc.TokenID, tt.tokenID)
			}
			if bc.BlockNumber != tt.blockNumber {
				t.Errorf("NewBorrowerCollateral() BlockNumber = %v, want %v", bc.BlockNumber, tt.blockNumber)
			}
			if bc.BlockVersion != tt.blockVersion {
				t.Errorf("NewBorrowerCollateral() BlockVersion = %v, want %v", bc.BlockVersion, tt.blockVersion)
			}
			if bc.Amount.Cmp(tt.amount) != 0 {
				t.Errorf("NewBorrowerCollateral() Amount = %v, want %v", bc.Amount, tt.amount)
			}
			if bc.Change.Cmp(tt.change) != 0 {
				t.Errorf("NewBorrowerCollateral() Change = %v, want %v", bc.Change, tt.change)
			}
		})
	}
}
