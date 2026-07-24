package entity

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewMorphoAdapter(t *testing.T) {
	validAddr := make([]byte, 20)

	tests := []struct {
		name         string
		vaultID      int64
		address      []byte
		assetToken   int64
		adapterType  MorphoAdapterType
		addedBlock   int64
		removedBlock int64 // applied only when hasRemoved is true
		hasRemoved   bool
		wantErr      bool
		errContains  string
	}{
		{
			name: "valid market V1 adapter", vaultID: 1, address: validAddr, assetToken: 1,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 24481834,
		},
		{
			name: "valid nested vault V1 adapter", vaultID: 2, address: validAddr, assetToken: 3,
			adapterType: MorphoAdapterTypeVaultV1, addedBlock: 24481900,
		},
		{
			name: "valid unknown adapter", vaultID: 3, address: validAddr, assetToken: 5,
			adapterType: MorphoAdapterTypeUnknown, addedBlock: 24482000,
		},
		{
			name: "valid removed adapter", vaultID: 1, address: validAddr, assetToken: 1,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 24481834, removedBlock: 24500000, hasRemoved: true,
		},
		{
			name: "removed at same block as added", vaultID: 1, address: validAddr, assetToken: 1,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 24481834, removedBlock: 24481834, hasRemoved: true,
		},
		{
			name: "zero vault id", vaultID: 0, address: validAddr, assetToken: 1,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 1,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "short address", vaultID: 1, address: make([]byte, 10), assetToken: 1,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 1,
			wantErr: true, errContains: "address must be 20 bytes",
		},
		{
			name: "zero asset token", vaultID: 1, address: validAddr, assetToken: 0,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 1,
			wantErr: true, errContains: "assetTokenID must be positive",
		},
		{
			name: "invalid adapter type", vaultID: 1, address: validAddr, assetToken: 1,
			adapterType: MorphoAdapterType(3), addedBlock: 1,
			wantErr: true, errContains: "adapterType must be 1, 2, or 99",
		},
		{
			name: "zero added block", vaultID: 1, address: validAddr, assetToken: 1,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 0,
			wantErr: true, errContains: "addedAtBlock must be positive",
		},
		{
			name: "removed before added", vaultID: 1, address: validAddr, assetToken: 1,
			adapterType: MorphoAdapterTypeMarketV1, addedBlock: 24481834, removedBlock: 24481833, hasRemoved: true,
			wantErr: true, errContains: "removedAtBlock must be >= addedAtBlock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var removedBlock *int64
			if tt.hasRemoved {
				rb := tt.removedBlock
				removedBlock = &rb
			}
			got, err := NewMorphoAdapter(tt.vaultID, tt.address, tt.assetToken, tt.adapterType, tt.addedBlock, removedBlock)
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
			if got.AdapterType != tt.adapterType {
				t.Errorf("AdapterType = %d, want %d", got.AdapterType, tt.adapterType)
			}
			if got.AddedAtBlock != tt.addedBlock {
				t.Errorf("AddedAtBlock = %d, want %d", got.AddedAtBlock, tt.addedBlock)
			}
			if tt.hasRemoved {
				if got.RemovedAtBlock == nil {
					t.Errorf("RemovedAtBlock = nil, want %d", tt.removedBlock)
				} else if *got.RemovedAtBlock != tt.removedBlock {
					t.Errorf("RemovedAtBlock = %d, want %d", *got.RemovedAtBlock, tt.removedBlock)
				}
			} else if got.RemovedAtBlock != nil {
				t.Errorf("RemovedAtBlock = %d, want nil", *got.RemovedAtBlock)
			}
		})
	}
}
