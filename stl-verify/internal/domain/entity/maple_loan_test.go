package entity

import (
	"bytes"
	"strings"
	"testing"
)

func validMapleLoan() *MapleLoan {
	return &MapleLoan{
		ChainID:        1,
		ProtocolID:     7,
		LoanAddress:    bytes.Repeat([]byte{0xcc}, 20),
		LoanType:       "OTL",
		MaplePoolID:    3,
		BorrowerUserID: 9,
		LoanMeta:       nil,
	}
}

func TestMapleLoan_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(l *MapleLoan)
		wantErr string
	}{
		{name: "valid external loan"},
		{
			name:   "valid internal loan",
			mutate: func(l *MapleLoan) { l.LoanMeta = &MapleLoanMeta{Type: "amm", Dex: "Uniswap"} },
		},
		{
			name:    "zero chain ID",
			mutate:  func(l *MapleLoan) { l.ChainID = 0 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "zero protocol ID",
			mutate:  func(l *MapleLoan) { l.ProtocolID = 0 },
			wantErr: "protocolID must be positive",
		},
		{
			name:    "short loan address",
			mutate:  func(l *MapleLoan) { l.LoanAddress = []byte{0x01} },
			wantErr: "loanAddress must be 20 bytes",
		},
		{
			name:    "nil loan address",
			mutate:  func(l *MapleLoan) { l.LoanAddress = nil },
			wantErr: "loanAddress must be 20 bytes",
		},
		{
			name:    "empty loan type",
			mutate:  func(l *MapleLoan) { l.LoanType = "" },
			wantErr: "loanType must not be empty",
		},
		{
			name:    "zero pool ID",
			mutate:  func(l *MapleLoan) { l.MaplePoolID = 0 },
			wantErr: "maplePoolID must be positive",
		},
		{
			name:    "zero borrower user ID",
			mutate:  func(l *MapleLoan) { l.BorrowerUserID = 0 },
			wantErr: "borrowerUserID must be positive",
		},
		{
			name:    "loan meta with empty type",
			mutate:  func(l *MapleLoan) { l.LoanMeta = &MapleLoanMeta{Dex: "Uniswap"} },
			wantErr: "loanMeta.type must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := validMapleLoan()
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

func TestMapleLoan_IsInternal(t *testing.T) {
	tests := []struct {
		name string
		meta *MapleLoanMeta
		want bool
	}{
		{name: "nil meta is external", meta: nil, want: false},
		{name: "amm is internal", meta: &MapleLoanMeta{Type: "amm"}, want: true},
		{name: "strategy is internal", meta: &MapleLoanMeta{Type: "strategy"}, want: true},
		{name: "other type is external", meta: &MapleLoanMeta{Type: "custody"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := validMapleLoan()
			l.LoanMeta = tt.meta
			if got := l.IsInternal(); got != tt.want {
				t.Errorf("IsInternal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMapleLoan_Constructor(t *testing.T) {
	v := validMapleLoan()

	got, err := NewMapleLoan(v.ChainID, v.ProtocolID, v.LoanAddress, v.MaplePoolID, v.BorrowerUserID, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.LoanType != "OTL" {
		t.Errorf("LoanType = %q, want OTL", got.LoanType)
	}

	if _, err := NewMapleLoan(0, v.ProtocolID, v.LoanAddress, v.MaplePoolID, v.BorrowerUserID, nil); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewMapleLoan") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
