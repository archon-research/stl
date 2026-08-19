package entity

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

func TestNewMorphoAdapterIdentity(t *testing.T) {
	validAddr := make([]byte, 20)

	tests := []struct {
		name        string
		vaultID     int64
		address     []byte
		assetToken  int64
		wantErr     bool
		errContains string
	}{
		{name: "valid identity", vaultID: 1, address: validAddr, assetToken: 1},
		{
			name: "zero vault id", vaultID: 0, address: validAddr, assetToken: 1,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "short address", vaultID: 1, address: make([]byte, 10), assetToken: 1,
			wantErr: true, errContains: "address must be 20 bytes",
		},
		{
			name: "zero asset token", vaultID: 1, address: validAddr, assetToken: 0,
			wantErr: true, errContains: "assetTokenID must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoAdapterIdentity(tt.vaultID, tt.address, tt.assetToken)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.MorphoVaultID != tt.vaultID {
				t.Errorf("MorphoVaultID = %d, want %d", got.MorphoVaultID, tt.vaultID)
			}
			if !bytes.Equal(got.Address, tt.address) {
				t.Errorf("Address = %x, want %x", got.Address, tt.address)
			}
			if got.AssetTokenID != tt.assetToken {
				t.Errorf("AssetTokenID = %d, want %d", got.AssetTokenID, tt.assetToken)
			}
		})
	}
}

// TestMorphoAdapterMembership_Validate pins what an observation must carry. Note what
// is deliberately NOT rejected: an assertion of membership with no classification. The
// caller that already knows the adapter is a member probes nothing, and nothing will be
// appended — the rule binds at append time, in the repository (ErrAdapterUnclassified)
// and in the table's CHECK.
func TestMorphoAdapterMembership_Validate(t *testing.T) {
	marketV1 := MorphoAdapterTypeMarketV1
	bogus := MorphoAdapterType(3)
	ts := time.Unix(1_700_000_000, 0).UTC()

	base := func() MorphoAdapterMembership {
		return MorphoAdapterMembership{
			BlockNumber: 1000, BlockVersion: 0, LogIndex: 4, Timestamp: ts,
			IsMember: true, AdapterType: &marketV1, ObservedVia: MembershipFromAddAdapter,
		}
	}

	tests := []struct {
		name        string
		mutate      func(*MorphoAdapterMembership)
		wantErr     bool
		errContains string
	}{
		{name: "valid transition", mutate: func(*MorphoAdapterMembership) {}},
		{
			name: "untyped removal is valid",
			mutate: func(m *MorphoAdapterMembership) {
				m.IsMember, m.AdapterType, m.ObservedVia = false, nil, MembershipFromRemoveAdapter
			},
		},
		{
			name: "untyped assertion of membership is not rejected here",
			mutate: func(m *MorphoAdapterMembership) {
				m.AdapterType, m.ObservedVia = nil, MembershipFromAllocation
			},
		},
		{
			name:   "end-of-block log index is valid",
			mutate: func(m *MorphoAdapterMembership) { m.LogIndex = EndOfBlockLogIndex },
		},
		{
			name:    "zero block",
			mutate:  func(m *MorphoAdapterMembership) { m.BlockNumber = 0 },
			wantErr: true, errContains: "blockNumber must be positive",
		},
		{
			name:    "negative block version",
			mutate:  func(m *MorphoAdapterMembership) { m.BlockVersion = -1 },
			wantErr: true, errContains: "blockVersion must be non-negative",
		},
		{
			name:    "negative log index",
			mutate:  func(m *MorphoAdapterMembership) { m.LogIndex = -1 },
			wantErr: true, errContains: "logIndex must be non-negative",
		},
		{
			name:    "zero timestamp",
			mutate:  func(m *MorphoAdapterMembership) { m.Timestamp = time.Time{} },
			wantErr: true, errContains: "timestamp must not be zero",
		},
		{
			name:    "unknown provenance",
			mutate:  func(m *MorphoAdapterMembership) { m.ObservedVia = MembershipSource("hand_edited") },
			wantErr: true, errContains: "observedVia must be one of",
		},
		{
			name:    "invalid adapter type",
			mutate:  func(m *MorphoAdapterMembership) { m.AdapterType = &bogus },
			wantErr: true, errContains: "adapterType must be 1, 2, or 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := base()
			tt.mutate(&m)
			err := m.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestMembershipSource_IsTransition(t *testing.T) {
	tests := []struct {
		source MembershipSource
		want   bool
	}{
		{MembershipFromAddAdapter, true},
		{MembershipFromRemoveAdapter, true},
		{MembershipFromAllocation, false},
		{MembershipFromDiscovery, false},
		{MembershipFromBootstrapSeed, false},
	}
	for _, tt := range tests {
		t.Run(string(tt.source), func(t *testing.T) {
			if got := tt.source.IsTransition(); got != tt.want {
				t.Errorf("IsTransition() = %v, want %v", got, tt.want)
			}
			if !tt.source.IsValid() {
				t.Errorf("%q should be a valid source", tt.source)
			}
		})
	}
}

// TestEndOfBlockLogIndexOrdersLast pins the sentinel's whole purpose: a hash-pinned
// end-of-block state read is authoritative over every log in its block, so it must sort
// above any log index the chain can produce.
func TestEndOfBlockLogIndexOrdersLast(t *testing.T) {
	if EndOfBlockLogIndex != math.MaxInt32 {
		t.Errorf("EndOfBlockLogIndex = %d, want math.MaxInt32", EndOfBlockLogIndex)
	}
	for _, logIndex := range []int32{0, 1, 4096, math.MaxInt32 - 1} {
		if logIndex >= EndOfBlockLogIndex {
			t.Errorf("log index %d must sort below EndOfBlockLogIndex", logIndex)
		}
	}
}
