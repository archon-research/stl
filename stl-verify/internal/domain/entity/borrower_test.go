package entity

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestNewBorrower(t *testing.T) {
	validAmount := big.NewInt(1000)
	validChange := big.NewInt(100)
	validEventType := EventBorrow
	validTxHash := common.FromHex("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

	testCreatedAt := time.Unix(1700000000, 0).UTC()

	tests := []struct {
		name         string
		userID       int64
		protocolID   int64
		tokenID      int64
		blockNumber  int64
		blockVersion int
		amount       *big.Int
		change       *big.Int
		eventType    EventType
		txHash       []byte
		wantErr      bool
		errContains  string
	}{
		{
			name:         "valid borrower",
			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      false,
		},
		{
			name:         "zero userID",
			userID:       0,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "userID must be positive",
		},
		{
			name: "zero protocolID",

			userID:       10,
			protocolID:   0,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "protocolID must be positive",
		},
		{
			name: "zero tokenID",

			userID:       10,
			protocolID:   5,
			tokenID:      0,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "tokenID must be positive",
		},
		{
			name: "zero blockNumber",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  0,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "blockNumber must be positive",
		},
		{
			name: "negative blockVersion",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: -1,
			amount:       validAmount,
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "blockVersion must be non-negative",
		},
		{
			name: "nil amount",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       nil,
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "amount must not be nil",
		},
		{
			name: "negative amount",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       big.NewInt(-100),
			change:       validChange,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "amount must be non-negative",
		},
		{
			name: "nil change",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       nil,
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "change must not be nil",
		},
		{
			name: "zero amount and change",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       big.NewInt(0),
			change:       big.NewInt(0),
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      false,
		},
		{
			name: "negative change allowed",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       big.NewInt(-50),
			eventType:    validEventType,
			txHash:       validTxHash,
			wantErr:      false,
		},
		{
			name: "empty eventType",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    "",
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "invalid eventType",
		},
		{
			name: "invalid eventType",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    "InvalidEvent",
			txHash:       validTxHash,
			wantErr:      true,
			errContains:  "invalid eventType",
		},
		{
			name: "empty txHash",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    validEventType,
			txHash:       nil,
			wantErr:      true,
			errContains:  "txHash must not be empty",
		},
		{
			name: "valid with Repay eventType",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    EventRepay,
			txHash:       validTxHash,
			wantErr:      false,
		},
		{
			name: "valid with LiquidationCall eventType",

			userID:       10,
			protocolID:   5,
			tokenID:      3,
			blockNumber:  1000,
			blockVersion: 0,
			amount:       validAmount,
			change:       validChange,
			eventType:    EventLiquidationCall,
			txHash:       validTxHash,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			borrower, err := NewBorrower(tt.userID, tt.protocolID, tt.tokenID, tt.blockNumber, tt.blockVersion, tt.amount, tt.change, tt.eventType, tt.txHash, testCreatedAt)
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
			if borrower.EventType != tt.eventType {
				t.Errorf("NewBorrower() EventType = %v, want %v", borrower.EventType, tt.eventType)
			}
			if !bytes.Equal(borrower.TxHash, tt.txHash) {
				t.Errorf("NewBorrower() TxHash = %v, want %v", borrower.TxHash, tt.txHash)
			}
		})
	}
}
