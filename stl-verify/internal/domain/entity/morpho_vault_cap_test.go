package entity

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestNewMorphoVaultCap(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()
	validIDData := []byte{0x01, 0x02, 0x03, 0x04}
	// cap_id = keccak256(id_data) on-chain; the entity enforces it.
	validCapID := crypto.Keccak256(validIDData)

	tests := []struct {
		name        string
		vaultID     int64
		capID       []byte
		idData      []byte
		absoluteCap *big.Int
		relativeCap *big.Int
		block       int64
		version     int
		timestamp   time.Time
		wantErr     bool
		errContains string
	}{
		{
			// A nonzero block version so the BlockVersion assertion below is not
			// satisfied by the struct's zero value.
			name: "valid", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(1_000_000), relativeCap: big.NewInt(500_000_000_000_000_000),
			block: 24481834, version: 3, timestamp: ts,
		},
		{
			name: "valid zero caps", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(0), relativeCap: big.NewInt(0),
			block: 24481834, version: 0, timestamp: ts,
		},
		{
			name: "zero vault id", vaultID: 0, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(1), block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "wrong cap id length", vaultID: 1, capID: make([]byte, 20), idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(1), block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "capID must be 32 bytes",
		},
		{
			name: "empty id data", vaultID: 1, capID: validCapID, idData: []byte{},
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(1), block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "idData must not be empty",
		},
		{
			name: "cap id not keccak of id data", vaultID: 1, capID: make([]byte, 32), idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(1), block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "capID must equal keccak256(idData)",
		},
		{
			name: "nil absolute cap", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: nil, relativeCap: big.NewInt(1), block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "absoluteCap must not be nil",
		},
		{
			name: "negative absolute cap", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(-1), relativeCap: big.NewInt(1), block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "absoluteCap must be non-negative",
		},
		{
			name: "nil relative cap", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: nil, block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "relativeCap must not be nil",
		},
		{
			name: "negative relative cap", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(-1), block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "relativeCap must be non-negative",
		},
		{
			name: "zero block", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(1), block: 0, version: 0, timestamp: ts,
			wantErr: true, errContains: "blockNumber must be positive",
		},
		{
			name: "negative block version", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(1), block: 1, version: -1, timestamp: ts,
			wantErr: true, errContains: "blockVersion must be non-negative",
		},
		{
			name: "zero timestamp", vaultID: 1, capID: validCapID, idData: validIDData,
			absoluteCap: big.NewInt(1), relativeCap: big.NewInt(1), block: 1, version: 0, timestamp: time.Time{},
			wantErr: true, errContains: "timestamp must not be zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoVaultCap(tt.vaultID, tt.capID, tt.idData, tt.absoluteCap, tt.relativeCap, tt.block, tt.version, tt.timestamp)
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
			// Every constructed field is asserted: absolute and relative caps are both
			// unscaled ints of the same type, so checking only one would pass a
			// constructor that swapped them.
			if got.MorphoVaultID != tt.vaultID {
				t.Errorf("MorphoVaultID = %d, want %d", got.MorphoVaultID, tt.vaultID)
			}
			if !bytes.Equal(got.CapID, tt.capID) {
				t.Errorf("CapID = %x, want %x", got.CapID, tt.capID)
			}
			if !bytes.Equal(got.IDData, tt.idData) {
				t.Errorf("IDData = %x, want %x", got.IDData, tt.idData)
			}
			if got.AbsoluteCap.Cmp(tt.absoluteCap) != 0 {
				t.Errorf("AbsoluteCap = %s, want %s", got.AbsoluteCap, tt.absoluteCap)
			}
			if got.RelativeCap.Cmp(tt.relativeCap) != 0 {
				t.Errorf("RelativeCap = %s, want %s", got.RelativeCap, tt.relativeCap)
			}
			if got.BlockNumber != tt.block {
				t.Errorf("BlockNumber = %d, want %d", got.BlockNumber, tt.block)
			}
			if got.BlockVersion != tt.version {
				t.Errorf("BlockVersion = %d, want %d", got.BlockVersion, tt.version)
			}
			if !got.Timestamp.Equal(tt.timestamp) {
				t.Errorf("Timestamp = %v, want %v", got.Timestamp, tt.timestamp)
			}
		})
	}
}
