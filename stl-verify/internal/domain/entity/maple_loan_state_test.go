package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validMapleLoanState() *MapleLoanState {
	return &MapleLoanState{
		MapleLoanID:   1,
		SyncedAt:      time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		State:         "Active",
		PrincipalOwed: big.NewInt(10000000),
		AcmRatio:      big.NewInt(1445731),
	}
}

func TestMapleLoanState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(s *MapleLoanState)
		wantErr string
	}{
		{name: "valid state"},
		{name: "zero principal ok", mutate: func(s *MapleLoanState) { s.PrincipalOwed = big.NewInt(0) }},
		{name: "nil acm ratio ok", mutate: func(s *MapleLoanState) { s.AcmRatio = nil }},
		{
			name:    "zero loan ID",
			mutate:  func(s *MapleLoanState) { s.MapleLoanID = 0 },
			wantErr: "mapleLoanID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(s *MapleLoanState) { s.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{
			name:    "empty state",
			mutate:  func(s *MapleLoanState) { s.State = "" },
			wantErr: "state must not be empty",
		},
		{
			name:    "nil principal owed",
			mutate:  func(s *MapleLoanState) { s.PrincipalOwed = nil },
			wantErr: "principalOwed must not be nil",
		},
		{
			name:    "negative principal owed",
			mutate:  func(s *MapleLoanState) { s.PrincipalOwed = big.NewInt(-1) },
			wantErr: "principalOwed must be non-negative",
		},
		{
			name:    "negative acm ratio",
			mutate:  func(s *MapleLoanState) { s.AcmRatio = big.NewInt(-1) },
			wantErr: "acmRatio must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validMapleLoanState()
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

func TestNewMapleLoanState_Constructor(t *testing.T) {
	v := validMapleLoanState()

	got, err := NewMapleLoanState(v.MapleLoanID, v.SyncedAt, v.State, v.PrincipalOwed, v.AcmRatio)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.State != v.State || got.PrincipalOwed.Cmp(v.PrincipalOwed) != 0 {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewMapleLoanState(0, v.SyncedAt, v.State, v.PrincipalOwed, v.AcmRatio); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewMapleLoanState") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
