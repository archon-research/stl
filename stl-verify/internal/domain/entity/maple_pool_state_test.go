package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validMaplePoolState() *MaplePoolState {
	return &MaplePoolState{
		MaplePoolID:        1,
		SyncedAt:           time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		TVL:                big.NewInt(1000),
		LiquidAssets:       big.NewInt(400),
		CollateralValueUSD: big.NewInt(500),
		PrincipalOut:       big.NewInt(600),
		Utilization:        0.6,
		MonthlyAPY:         big.NewInt(123),
		SpotAPY:            big.NewInt(456),
	}
}

func TestMaplePoolState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(s *MaplePoolState)
		wantErr string
	}{
		{name: "valid state"},
		{name: "nil optional APYs ok", mutate: func(s *MaplePoolState) { s.MonthlyAPY = nil; s.SpotAPY = nil }},
		{name: "utilization zero ok", mutate: func(s *MaplePoolState) { s.Utilization = 0 }},
		{name: "utilization one ok", mutate: func(s *MaplePoolState) { s.Utilization = 1 }},
		{
			name:    "zero pool ID",
			mutate:  func(s *MaplePoolState) { s.MaplePoolID = 0 },
			wantErr: "maplePoolID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(s *MaplePoolState) { s.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{name: "nil tvl ok (schema-nullable)", mutate: func(s *MaplePoolState) { s.TVL = nil }},
		{
			name:    "negative tvl",
			mutate:  func(s *MaplePoolState) { s.TVL = big.NewInt(-1) },
			wantErr: "tvl must be non-negative",
		},
		{
			name:    "nil liquid assets",
			mutate:  func(s *MaplePoolState) { s.LiquidAssets = nil },
			wantErr: "liquidAssets must not be nil",
		},
		{
			name:    "negative liquid assets",
			mutate:  func(s *MaplePoolState) { s.LiquidAssets = big.NewInt(-5) },
			wantErr: "liquidAssets must be non-negative",
		},
		{name: "nil collateral value ok (schema-nullable)", mutate: func(s *MaplePoolState) { s.CollateralValueUSD = nil }},
		{
			name:    "negative collateral value",
			mutate:  func(s *MaplePoolState) { s.CollateralValueUSD = big.NewInt(-5) },
			wantErr: "collateralValueUSD must be non-negative",
		},
		{
			name:    "nil principal out",
			mutate:  func(s *MaplePoolState) { s.PrincipalOut = nil },
			wantErr: "principalOut must not be nil",
		},
		{
			name:    "negative principal out",
			mutate:  func(s *MaplePoolState) { s.PrincipalOut = big.NewInt(-5) },
			wantErr: "principalOut must be non-negative",
		},
		{
			name:    "negative utilization",
			mutate:  func(s *MaplePoolState) { s.Utilization = -0.1 },
			wantErr: "utilization must be in [0, 1]",
		},
		{
			name:    "utilization above one",
			mutate:  func(s *MaplePoolState) { s.Utilization = 1.1 },
			wantErr: "utilization must be in [0, 1]",
		},
		{
			name:    "negative monthly apy",
			mutate:  func(s *MaplePoolState) { s.MonthlyAPY = big.NewInt(-1) },
			wantErr: "monthlyAPY must be non-negative",
		},
		{
			name:    "negative spot apy",
			mutate:  func(s *MaplePoolState) { s.SpotAPY = big.NewInt(-1) },
			wantErr: "spotAPY must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validMaplePoolState()
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

func TestNewMaplePoolState_Constructor(t *testing.T) {
	v := validMaplePoolState()

	got, err := NewMaplePoolState(v.MaplePoolID, v.SyncedAt, v.TVL, v.LiquidAssets, v.CollateralValueUSD, v.PrincipalOut, v.MonthlyAPY, v.SpotAPY)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.TVL.Cmp(v.TVL) != 0 {
		t.Errorf("fields not set: %+v", got)
	}
	// Utilization derives from principalOut / (liquidAssets + principalOut):
	// 600 / (400 + 600) = 0.6.
	if got.Utilization != 0.6 {
		t.Errorf("Utilization = %f, want 0.6", got.Utilization)
	}

	if _, err := NewMaplePoolState(0, v.SyncedAt, v.TVL, v.LiquidAssets, v.CollateralValueUSD, v.PrincipalOut, v.MonthlyAPY, v.SpotAPY); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewMaplePoolState") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}

func TestComputeUtilization(t *testing.T) {
	tests := []struct {
		name         string
		principalOut *big.Int
		liquidAssets *big.Int
		want         float64
	}{
		{name: "normal", principalOut: big.NewInt(600), liquidAssets: big.NewInt(400), want: 0.6},
		{name: "empty pool", principalOut: big.NewInt(0), liquidAssets: big.NewInt(0), want: 0},
		{name: "fully utilized", principalOut: big.NewInt(100), liquidAssets: big.NewInt(0), want: 1},
		{name: "fully liquid", principalOut: big.NewInt(0), liquidAssets: big.NewInt(100), want: 0},
		{name: "nil principal", principalOut: nil, liquidAssets: big.NewInt(100), want: 0},
		{name: "nil liquid assets", principalOut: big.NewInt(100), liquidAssets: nil, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeUtilization(tt.principalOut, tt.liquidAssets)
			if got != tt.want {
				t.Errorf("computeUtilization = %f, want %f", got, tt.want)
			}
		})
	}
}
