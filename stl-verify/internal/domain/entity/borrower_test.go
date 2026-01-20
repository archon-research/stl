package entity

import (
	"math/big"
	"strings"
	"testing"
)

func TestNewBorrower(t *testing.T) {
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
			name:         "valid borrower",
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
			borrower, err := NewBorrower(tt.id, tt.userID, tt.protocolID, tt.tokenID, tt.blockNumber, tt.blockVersion, tt.amount, tt.change)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewBorrower() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewBorrower() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewBorrower() unexpected error = %v", err)
				return
			}
			if borrower == nil {
				t.Errorf("NewBorrower() returned nil borrower")
				return
			}
			if borrower.ID != tt.id {
				t.Errorf("NewBorrower() ID = %v, want %v", borrower.ID, tt.id)
			}
			if borrower.UserID != tt.userID {
				t.Errorf("NewBorrower() UserID = %v, want %v", borrower.UserID, tt.userID)
			}
			if borrower.ProtocolID != tt.protocolID {
				t.Errorf("NewBorrower() ProtocolID = %v, want %v", borrower.ProtocolID, tt.protocolID)
			}
			if borrower.TokenID != tt.tokenID {
				t.Errorf("NewBorrower() TokenID = %v, want %v", borrower.TokenID, tt.tokenID)
			}
			if borrower.BlockNumber != tt.blockNumber {
				t.Errorf("NewBorrower() BlockNumber = %v, want %v", borrower.BlockNumber, tt.blockNumber)
			}
			if borrower.BlockVersion != tt.blockVersion {
				t.Errorf("NewBorrower() BlockVersion = %v, want %v", borrower.BlockVersion, tt.blockVersion)
			}
			if borrower.Amount.Cmp(tt.amount) != 0 {
				t.Errorf("NewBorrower() Amount = %v, want %v", borrower.Amount, tt.amount)
			}
			if borrower.Change.Cmp(tt.change) != 0 {
				t.Errorf("NewBorrower() Change = %v, want %v", borrower.Change, tt.change)
			}
		})
	}
}

func hasSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
