package maple

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validLoanState() *LoanState {
	return &LoanState{
		LoanID:        1,
		SyncedAt:      time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		State:         "Active",
		PrincipalOwed: big.NewInt(10000000),
		AcmRatio:      big.NewInt(1445731),
	}
}

func TestLoanState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(s *LoanState)
		wantErr string
	}{
		{name: "valid state"},
		{name: "zero principal ok", mutate: func(s *LoanState) { s.PrincipalOwed = big.NewInt(0) }},
		{name: "nil acm ratio ok", mutate: func(s *LoanState) { s.AcmRatio = nil }},
		{
			name:    "zero loan ID",
			mutate:  func(s *LoanState) { s.LoanID = 0 },
			wantErr: "mapleLoanID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(s *LoanState) { s.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{
			name:    "empty state",
			mutate:  func(s *LoanState) { s.State = "" },
			wantErr: "state must not be empty",
		},
		{
			name:    "nil principal owed",
			mutate:  func(s *LoanState) { s.PrincipalOwed = nil },
			wantErr: "principalOwed must not be nil",
		},
		{
			name:    "negative principal owed",
			mutate:  func(s *LoanState) { s.PrincipalOwed = big.NewInt(-1) },
			wantErr: "principalOwed must be non-negative",
		},
		{
			name:    "negative acm ratio",
			mutate:  func(s *LoanState) { s.AcmRatio = big.NewInt(-1) },
			wantErr: "acmRatio must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validLoanState()
			if tt.mutate != nil {
				tt.mutate(s)
			}
			err := s.Validate()
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

func TestNewLoanState_Constructor(t *testing.T) {
	v := validLoanState()

	got, err := NewLoanState(v.LoanID, v.SyncedAt, v.State, v.PrincipalOwed, v.AcmRatio)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.State != v.State || got.PrincipalOwed.Cmp(v.PrincipalOwed) != 0 {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewLoanState(0, v.SyncedAt, v.State, v.PrincipalOwed, v.AcmRatio); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewLoanState") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
