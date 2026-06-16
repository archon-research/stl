package maple

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validSyrupGlobalState() *SyrupGlobalState {
	apy, _ := new(big.Int).SetString("46314953537216910976747498327", 10)
	return &SyrupGlobalState{
		ChainID:         1,
		SyncedAt:        time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		TVL:             big.NewInt(3563135115920200),
		APY:             apy,
		CollateralAPY:   big.NewInt(1),
		PoolAPY:         big.NewInt(2),
		DripsYieldBoost: big.NewInt(0),
	}
}

func TestSyrupGlobalState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(s *SyrupGlobalState)
		wantErr string
	}{
		{name: "valid state"},
		{name: "nil drips yield boost ok", mutate: func(s *SyrupGlobalState) { s.DripsYieldBoost = nil }},
		{
			name:    "zero chain ID",
			mutate:  func(s *SyrupGlobalState) { s.ChainID = 0 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(s *SyrupGlobalState) { s.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{
			name:    "nil tvl",
			mutate:  func(s *SyrupGlobalState) { s.TVL = nil },
			wantErr: "tvl must not be nil",
		},
		{
			name:    "negative tvl",
			mutate:  func(s *SyrupGlobalState) { s.TVL = big.NewInt(-1) },
			wantErr: "tvl must be non-negative",
		},
		{
			name:    "nil apy",
			mutate:  func(s *SyrupGlobalState) { s.APY = nil },
			wantErr: "apy must not be nil",
		},
		{
			name:    "negative apy",
			mutate:  func(s *SyrupGlobalState) { s.APY = big.NewInt(-1) },
			wantErr: "apy must be non-negative",
		},
		{
			name:    "nil collateral apy",
			mutate:  func(s *SyrupGlobalState) { s.CollateralAPY = nil },
			wantErr: "collateralAPY must not be nil",
		},
		{
			name:    "negative collateral apy",
			mutate:  func(s *SyrupGlobalState) { s.CollateralAPY = big.NewInt(-1) },
			wantErr: "collateralAPY must be non-negative",
		},
		{
			name:    "nil pool apy",
			mutate:  func(s *SyrupGlobalState) { s.PoolAPY = nil },
			wantErr: "poolAPY must not be nil",
		},
		{
			name:    "negative pool apy",
			mutate:  func(s *SyrupGlobalState) { s.PoolAPY = big.NewInt(-1) },
			wantErr: "poolAPY must be non-negative",
		},
		{
			name:    "negative drips yield boost",
			mutate:  func(s *SyrupGlobalState) { s.DripsYieldBoost = big.NewInt(-1) },
			wantErr: "dripsYieldBoost must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validSyrupGlobalState()
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

func TestNewSyrupGlobalState_Constructor(t *testing.T) {
	v := validSyrupGlobalState()

	got, err := NewSyrupGlobalState(v.ChainID, v.SyncedAt, v.TVL, v.APY, v.CollateralAPY, v.PoolAPY, v.DripsYieldBoost)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.TVL.Cmp(v.TVL) != 0 || got.APY.Cmp(v.APY) != 0 {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewSyrupGlobalState(0, v.SyncedAt, v.TVL, v.APY, v.CollateralAPY, v.PoolAPY, v.DripsYieldBoost); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewSyrupGlobalState") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
