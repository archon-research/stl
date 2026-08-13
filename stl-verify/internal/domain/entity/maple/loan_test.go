package maple

import (
	"bytes"
	"strings"
	"testing"
)

func validLoan() *Loan {
	return &Loan{
		ChainID:        1,
		ProtocolID:     7,
		LoanAddress:    bytes.Repeat([]byte{0xcc}, 20),
		LoanType:       "OTL",
		PoolID:         3,
		BorrowerUserID: 9,
		LoanMeta:       nil,
	}
}

func TestLoan_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(l *Loan)
		wantErr string
	}{
		{name: "valid external loan"},
		{
			name:   "valid internal loan",
			mutate: func(l *Loan) { l.LoanMeta = &LoanMeta{Type: "amm", DexName: "Uniswap"} },
		},
		{
			name:    "zero chain ID",
			mutate:  func(l *Loan) { l.ChainID = 0 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "zero protocol ID",
			mutate:  func(l *Loan) { l.ProtocolID = 0 },
			wantErr: "protocolID must be positive",
		},
		{
			name:    "short loan address",
			mutate:  func(l *Loan) { l.LoanAddress = []byte{0x01} },
			wantErr: "loanAddress must be 20 bytes",
		},
		{
			name:    "nil loan address",
			mutate:  func(l *Loan) { l.LoanAddress = nil },
			wantErr: "loanAddress must be 20 bytes",
		},
		{
			name:    "empty loan type",
			mutate:  func(l *Loan) { l.LoanType = "" },
			wantErr: `loanType must be "OTL"`,
		},
		{
			name:    "non-OTL loan type",
			mutate:  func(l *Loan) { l.LoanType = "FTL" },
			wantErr: `loanType must be "OTL"`,
		},
		{
			name:    "zero pool ID",
			mutate:  func(l *Loan) { l.PoolID = 0 },
			wantErr: "maplePoolID must be positive",
		},
		{
			name:    "zero borrower user ID",
			mutate:  func(l *Loan) { l.BorrowerUserID = 0 },
			wantErr: "borrowerUserID must be positive",
		},
		{
			// Live API observation: 27 of 61 active loans carry loanMeta
			// with a null type — must be accepted, not rejected.
			name:   "loan meta with empty type ok",
			mutate: func(l *Loan) { l.LoanMeta = &LoanMeta{DexName: "Uniswap"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := validLoan()
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

func TestNewLoan_Constructor(t *testing.T) {
	v := validLoan()

	got, err := NewLoan(v.ChainID, v.ProtocolID, v.LoanAddress, v.PoolID, v.BorrowerUserID, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.LoanType != "OTL" {
		t.Errorf("LoanType = %q, want OTL", got.LoanType)
	}

	if _, err := NewLoan(0, v.ProtocolID, v.LoanAddress, v.PoolID, v.BorrowerUserID, nil); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewLoan") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
