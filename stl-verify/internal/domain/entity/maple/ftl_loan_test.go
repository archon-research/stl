package maple

import (
	"bytes"
	"strings"
	"testing"
)

func validFTLLoan() *FTLLoan {
	return &FTLLoan{
		ChainID:           1,
		ProtocolID:        7,
		LoanAddress:       bytes.Repeat([]byte{0x01}, 20),
		PoolID:            10,
		BorrowerUserID:    100,
		CollateralTokenID: 500,
		FundsTokenID:      501,
	}
}

func TestFTLLoan_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(l *FTLLoan)
		wantErr string
	}{
		{name: "valid loan"},
		{name: "funds equals collateral token ok", mutate: func(l *FTLLoan) { l.FundsTokenID = l.CollateralTokenID }},
		{
			name:    "zero chain id",
			mutate:  func(l *FTLLoan) { l.ChainID = 0 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "zero protocol id",
			mutate:  func(l *FTLLoan) { l.ProtocolID = 0 },
			wantErr: "protocolID must be positive",
		},
		{
			name:    "short address",
			mutate:  func(l *FTLLoan) { l.LoanAddress = bytes.Repeat([]byte{0x01}, 19) },
			wantErr: "loanAddress must be 20 bytes",
		},
		{
			name:    "nil address",
			mutate:  func(l *FTLLoan) { l.LoanAddress = nil },
			wantErr: "loanAddress must be 20 bytes",
		},
		{
			name:    "zero pool id",
			mutate:  func(l *FTLLoan) { l.PoolID = 0 },
			wantErr: "maplePoolID must be positive",
		},
		{
			name:    "zero borrower id",
			mutate:  func(l *FTLLoan) { l.BorrowerUserID = 0 },
			wantErr: "borrowerUserID must be positive",
		},
		{
			name:    "zero collateral token id",
			mutate:  func(l *FTLLoan) { l.CollateralTokenID = 0 },
			wantErr: "collateralTokenID must be positive",
		},
		{
			name:    "zero funds token id",
			mutate:  func(l *FTLLoan) { l.FundsTokenID = 0 },
			wantErr: "fundsTokenID must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := validFTLLoan()
			if tt.mutate != nil {
				tt.mutate(l)
			}
			err := l.Validate()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewFTLLoan_Constructor(t *testing.T) {
	v := validFTLLoan()

	got, err := NewFTLLoan(v.ChainID, v.ProtocolID, v.LoanAddress, v.PoolID, v.BorrowerUserID, v.CollateralTokenID, v.FundsTokenID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.CollateralTokenID != v.CollateralTokenID || got.FundsTokenID != v.FundsTokenID {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewFTLLoan(0, v.ProtocolID, v.LoanAddress, v.PoolID, v.BorrowerUserID, v.CollateralTokenID, v.FundsTokenID); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewFTLLoan") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
