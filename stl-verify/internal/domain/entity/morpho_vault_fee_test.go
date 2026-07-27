package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestNewMorphoVaultFee(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()
	addr20 := make([]byte, 20)
	zeroAddr := make([]byte, 20)

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
			name: "valid full config", vaultID: 1,
			perfFee: big.NewInt(100_000_000_000_000_000), mgmtFee: big.NewInt(3170979198),
			perfRecip: addr20, mgmtRecip: addr20, block: 24765805, version: 0, timestamp: ts,
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
			if got.PerformanceFee.Cmp(tt.perfFee) != 0 {
				t.Errorf("PerformanceFee = %s, want %s", got.PerformanceFee, tt.perfFee)
			}
			if got.ManagementFee.Cmp(tt.mgmtFee) != 0 {
				t.Errorf("ManagementFee = %s, want %s", got.ManagementFee, tt.mgmtFee)
			}
		})
	}
}
