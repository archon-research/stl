package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validMapleSkyStrategyState() *MapleSkyStrategyState {
	return &MapleSkyStrategyState{
		MapleSkyStrategyID: 1,
		SyncedAt:           time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		State:              "Active",
		CurrentlyDeployed:  big.NewInt(0),
		DepositedAssets:    big.NewInt(9464548714891221),
		WithdrawnAssets:    big.NewInt(9474661204598509),
		StrategyFeeRate:    big.NewInt(100000),
		TotalFeesCollected: big.NewInt(1121557832133),
	}
}

func TestMapleSkyStrategyState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(s *MapleSkyStrategyState)
		wantErr string
	}{
		{name: "valid state"},
		{
			name: "optional fee fields nil ok",
			mutate: func(s *MapleSkyStrategyState) {
				s.StrategyFeeRate = nil
				s.TotalFeesCollected = nil
			},
		},
		{
			name:    "zero strategy ID",
			mutate:  func(s *MapleSkyStrategyState) { s.MapleSkyStrategyID = 0 },
			wantErr: "mapleSkyStrategyID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(s *MapleSkyStrategyState) { s.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{
			name:    "empty state",
			mutate:  func(s *MapleSkyStrategyState) { s.State = "" },
			wantErr: "state must not be empty",
		},
		{
			name:    "nil currently deployed",
			mutate:  func(s *MapleSkyStrategyState) { s.CurrentlyDeployed = nil },
			wantErr: "currentlyDeployed must not be nil",
		},
		{
			name:    "negative currently deployed",
			mutate:  func(s *MapleSkyStrategyState) { s.CurrentlyDeployed = big.NewInt(-1) },
			wantErr: "currentlyDeployed must be non-negative",
		},
		{
			name:    "nil deposited assets",
			mutate:  func(s *MapleSkyStrategyState) { s.DepositedAssets = nil },
			wantErr: "depositedAssets must not be nil",
		},
		{
			name:    "negative deposited assets",
			mutate:  func(s *MapleSkyStrategyState) { s.DepositedAssets = big.NewInt(-1) },
			wantErr: "depositedAssets must be non-negative",
		},
		{
			name:    "nil withdrawn assets",
			mutate:  func(s *MapleSkyStrategyState) { s.WithdrawnAssets = nil },
			wantErr: "withdrawnAssets must not be nil",
		},
		{
			name:    "negative withdrawn assets",
			mutate:  func(s *MapleSkyStrategyState) { s.WithdrawnAssets = big.NewInt(-1) },
			wantErr: "withdrawnAssets must be non-negative",
		},
		{
			name:    "negative strategy fee rate",
			mutate:  func(s *MapleSkyStrategyState) { s.StrategyFeeRate = big.NewInt(-1) },
			wantErr: "strategyFeeRate must be non-negative",
		},
		{
			name:    "negative total fees collected",
			mutate:  func(s *MapleSkyStrategyState) { s.TotalFeesCollected = big.NewInt(-1) },
			wantErr: "totalFeesCollected must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validMapleSkyStrategyState()
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

func TestNewMapleSkyStrategyState_Constructor(t *testing.T) {
	v := validMapleSkyStrategyState()

	got, err := NewMapleSkyStrategyState(v.MapleSkyStrategyID, v.SyncedAt, v.State, v.CurrentlyDeployed, v.DepositedAssets, v.WithdrawnAssets, v.StrategyFeeRate, v.TotalFeesCollected)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.State != v.State || got.DepositedAssets.Cmp(v.DepositedAssets) != 0 {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewMapleSkyStrategyState(0, v.SyncedAt, v.State, v.CurrentlyDeployed, v.DepositedAssets, v.WithdrawnAssets, v.StrategyFeeRate, v.TotalFeesCollected); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewMapleSkyStrategyState") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
