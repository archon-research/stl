package entity

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestNewMorphoVaultFee(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()
	addr20 := make([]byte, 20)
	zeroAddr := make([]byte, 20)
	// The two recipients must be DISTINCT in the success case, or a constructor
	// that swapped them would still satisfy every assertion.
	perfRecipient := bytes.Repeat([]byte{0xa1}, 20)
	mgmtRecipient := bytes.Repeat([]byte{0xb2}, 20)

	tests := []struct {
		name        string
		vaultID     int64
		perfFee     *big.Int
		mgmtFee     *big.Int
		perfRecip   []byte
		mgmtRecip   []byte
		block       int64
		version     int
		timestamp   time.Time
		wantErr     bool
		errContains string
	}{
		{
			name: "valid full config", vaultID: 7,
			perfFee: big.NewInt(100_000_000_000_000_000), mgmtFee: big.NewInt(3170979198),
			perfRecip: perfRecipient, mgmtRecip: mgmtRecipient, block: 24765805, version: 2, timestamp: ts,
		},
		{
			name: "valid zero fees and zero-address recipients", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(0),
			perfRecip: zeroAddr, mgmtRecip: zeroAddr, block: 24765788, version: 0, timestamp: ts,
		},
		{
			name: "zero vault id", vaultID: 0,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(0), perfRecip: addr20, mgmtRecip: addr20,
			block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "nil performance fee", vaultID: 1,
			perfFee: nil, mgmtFee: big.NewInt(0), perfRecip: addr20, mgmtRecip: addr20,
			block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "performanceFee must not be nil",
		},
		{
			name: "negative performance fee", vaultID: 1,
			perfFee: big.NewInt(-1), mgmtFee: big.NewInt(0), perfRecip: addr20, mgmtRecip: addr20,
			block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "performanceFee must be non-negative",
		},
		{
			name: "nil management fee", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: nil, perfRecip: addr20, mgmtRecip: addr20,
			block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "managementFee must not be nil",
		},
		{
			name: "negative management fee", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(-1), perfRecip: addr20, mgmtRecip: addr20,
			block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "managementFee must be non-negative",
		},
		{
			name: "short performance recipient", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(0), perfRecip: make([]byte, 19), mgmtRecip: addr20,
			block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "performanceFeeRecipient must be 20 bytes",
		},
		{
			name: "long management recipient", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(0), perfRecip: addr20, mgmtRecip: make([]byte, 21),
			block: 1, version: 0, timestamp: ts,
			wantErr: true, errContains: "managementFeeRecipient must be 20 bytes",
		},
		{
			name: "zero block", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(0), perfRecip: addr20, mgmtRecip: addr20,
			block: 0, version: 0, timestamp: ts,
			wantErr: true, errContains: "blockNumber must be positive",
		},
		{
			name: "negative block version", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(0), perfRecip: addr20, mgmtRecip: addr20,
			block: 1, version: -1, timestamp: ts,
			wantErr: true, errContains: "blockVersion must be non-negative",
		},
		{
			name: "zero timestamp", vaultID: 1,
			perfFee: big.NewInt(0), mgmtFee: big.NewInt(0), perfRecip: addr20, mgmtRecip: addr20,
			block: 1, version: 0, timestamp: time.Time{},
			wantErr: true, errContains: "timestamp must not be zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoVaultFee(tt.vaultID, tt.perfFee, tt.mgmtFee, tt.perfRecip, tt.mgmtRecip, tt.block, tt.version, tt.timestamp)
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
			if got.PerformanceFee.Cmp(tt.perfFee) != 0 {
				t.Errorf("PerformanceFee = %s, want %s", got.PerformanceFee, tt.perfFee)
			}
			if got.ManagementFee.Cmp(tt.mgmtFee) != 0 {
				t.Errorf("ManagementFee = %s, want %s", got.ManagementFee, tt.mgmtFee)
			}
			if !bytes.Equal(got.PerformanceFeeRecipient, tt.perfRecip) {
				t.Errorf("PerformanceFeeRecipient = %x, want %x", got.PerformanceFeeRecipient, tt.perfRecip)
			}
			if !bytes.Equal(got.ManagementFeeRecipient, tt.mgmtRecip) {
				t.Errorf("ManagementFeeRecipient = %x, want %x", got.ManagementFeeRecipient, tt.mgmtRecip)
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
